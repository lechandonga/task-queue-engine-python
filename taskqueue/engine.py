"""对外门面：组合存储、去重、队列准入、调度器、消费者组，并负责重启恢复。

恢复语义：
- 所有任务状态与位点在提交时即落 SQLite（WAL + synchronous=FULL）；
- 启动时：RUNNING 但租约已失效的任务退回 RETRY_WAIT（attempt 不重置），
  不丢任务、不重置重试预算；COMPLETED/DEAD 状态不变（已生效副作用不重复）；
  消费者组过期的事件认领被放开，可被重新认领；
- 去重窗口持久化：重启后窗口内的重复提交仍被拦截。

线程模型：Dispatcher 内部 worker 线程持续处理所有 namespace 的到期任务；
事件侧由调用方驱动 ``ConsumerGroup.process_batch``（也可自行起线程循环）。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from taskqueue.config import EngineConfig
from taskqueue.consumer import ConsumerGroup, EventHandler
from taskqueue.dedup import Deduplicator
from taskqueue.dispatcher import Dispatcher, TaskHandler
from taskqueue.errors import InvalidArgumentError, TaskQueueError
from taskqueue.metrics import MetricsRegistry
from taskqueue.models import DeadLetter, TaskRecord, TaskStatus, now_ms
from taskqueue.queue_engine import BoundedQueueGate
from taskqueue.schema import TaskPayload
from taskqueue.storage import Storage

_logger = logging.getLogger("taskqueue.engine")


class TaskQueueEngine:
    def __init__(self, cfg: EngineConfig | None = None,
                 handlers: dict[str, TaskHandler] | None = None,
                 time_func: Callable[[], int] | None = None,
                 logger: logging.Logger | None = None) -> None:
        self._cfg = cfg or EngineConfig()
        self._time = time_func or now_ms
        self._log = logger or _logger
        self._storage = Storage(self._cfg.db_path)
        self._metrics = MetricsRegistry(time_func=self._time)
        self._dedup = Deduplicator(self._storage, self._cfg.dedup, self._time)
        self._gate = BoundedQueueGate(self._storage, self._cfg.queue, self._time)
        self._dispatcher = Dispatcher(
            self._storage, self._cfg, dict(handlers or {}),
            on_complete=self._on_task_complete,
            on_retry=self._metrics.record_retry,
            on_dead=self._on_task_dead,
            time_func=self._time,
            logger=self._log,
        )
        self._started = False
        self._lock = threading.Lock()

    # ---- 生命周期 ------------------------------------------------------
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._storage.open()
            self._recover()
            self._dispatcher.start()
            self._started = True

    def close(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._dispatcher.stop()
            self._gate.close()
            self._storage.close()
            self._started = False

    def __enter__(self) -> TaskQueueEngine:
        self.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _recover(self) -> None:
        """重启恢复：租约已失效的 RUNNING 任务/事件认领重新可调度。

        恢复以“当前时间”为基准：上次进程已经死掉多久是未知的，租约点是
        持久化的绝对时间戳，过期即放回，语义确定。
        """
        ts = self._time()
        n_tasks = self._storage.expire_leases(ts, self._cfg.visibility_timeout_ms)
        n_acks = self._storage.expire_group_leases(ts, self._cfg.visibility_timeout_ms)
        swept = self._storage.dedup_sweep(ts)
        self._log.info(
            "recovery tasks=%d event_leases=%d dedup_swept=%d 判定依据=启动恢复",
            n_tasks, n_acks, swept)

    def _on_task_complete(self, latency_ms: int) -> None:
        self._metrics.record_complete(latency_ms)
        self._gate.notify_space()

    def _on_task_dead(self) -> None:
        self._metrics.record_dead()
        self._gate.notify_space()

    # ---- 任务 ----------------------------------------------------------
    def submit(self, namespace: str, payload: dict[str, Any] | TaskPayload, *,
               queue: str = "default", dedup_key: str | None = None,
               delay_ms: int = 0) -> TaskRecord:
        """提交任务。

        - ``payload`` 经 :class:`TaskPayload` 校验/迁移（非法记录明确拒绝）；
        - 同 ``dedup_key`` 在窗口内只接受一次（重复返回首次任务，记 dedup_hits）；
        - 队列超限时按配置 reject/delay。
        """
        if not isinstance(namespace, str) or not namespace:
            raise InvalidArgumentError("namespace must be a non-empty string")
        if delay_ms < 0:
            raise InvalidArgumentError("delay_ms must be >= 0")
        if dedup_key is not None and not isinstance(dedup_key, str):
            raise InvalidArgumentError("dedup_key must be a string or None")

        payload_obj = (
            payload if isinstance(payload, TaskPayload)
            else TaskPayload.from_dict(payload)
        )
        data = payload_obj.to_dict()
        ts = self._time()
        task = TaskRecord(
            id=TaskRecord.new_id(), namespace=namespace, queue=queue,
            payload=data, status=TaskStatus.PENDING,
            dedup_key=dedup_key,
            created_at_ms=ts, updated_at_ms=ts, run_at_ms=ts + delay_ms,
        )
        window = self._cfg.dedup.window_ms if dedup_key is not None else None
        try:
            status, task_id = self._gate.submit(task, window_ms=window)
        except TaskQueueError:
            self._metrics.record_reject()
            raise
        if status == "dedup_hit":
            self._metrics.record_dedup_hit()
            existing = self._storage.require_task(task_id)
            existing.dedup_hit = True
            self._log.info(
                "task_id=%s offset=- dedup_key=%s 判定依据=窗口内重复提交，返回首次任务",
                task_id, dedup_key)
            return existing
        self._metrics.record_submit()
        self._dispatcher.wakeup()
        self._log.info(
            "task_id=%s offset=- ns=%s queue=%s 判定依据=新任务已接受",
            task_id, namespace, queue)
        return self._storage.require_task(task_id)

    def get_task(self, task_id: str) -> TaskRecord:
        return self._storage.require_task(task_id)

    def cancel_task(self, task_id: str) -> str:
        """取消未开始/等待重试的任务。

        - 成功取消返回 ``"cancelled"``；已是终态返回 ``"already_terminal"``
          （已生效的副作用不会因取消而回滚或重复）；
        - 正在执行返回 ``"running"``（不与执行结果竞态），调用方可稍后重试；
        - 任务不存在抛 :class:`NotFoundError`。
        """
        self._storage.require_task(task_id)
        result = self._storage.cancel_task(task_id, self._time())
        self._log.info("task_id=%s offset=- 判定依据=取消请求结果=%s",
                       task_id, result)
        if result == "cancelled":
            self._gate.notify_space()
        return result

    def list_dead_letters(self, namespace: str, queue: str | None = None,
                          limit: int = 100, offset: int = 0) -> list[DeadLetter]:
        return self._storage.list_dead_letters(namespace, queue, limit, offset)

    def wait_idle(self, timeout: float = 5.0, poll: float = 0.01,
                  namespace: str | None = None) -> bool:
        """等待已提交任务全部进入终态（测试/优雅停机用）。

        判定依据：目标 namespace 下不存在 PENDING/RETRY_WAIT/RUNNING 任务。
        """
        import time as _wall
        deadline = _wall.monotonic() + timeout
        while _wall.monotonic() < deadline:
            if self._storage.count_unfinished(namespace) == 0:
                return True
            _wall.sleep(poll)
        return False

    # ---- 事件 ----------------------------------------------------------
    def publish_event(self, namespace: str, topic: str,
                      payload: dict[str, Any]) -> int:
        if not isinstance(payload, dict):
            raise InvalidArgumentError("event payload must be an object")
        ev = self._storage.append_event(namespace, topic, payload, self._time())
        return ev.offset

    def create_consumer_group(self, namespace: str, topic: str, group: str,
                              handler: EventHandler,
                              consumer_id: str | None = None) -> ConsumerGroup:
        return ConsumerGroup(
            self._storage, self._cfg, namespace, topic, group, handler,
            consumer_id=consumer_id,
            on_consumed=self._metrics.record_event_consumed,
            on_commit_rejected=self._metrics.record_commit_rejected,
            time_func=self._time, logger=self._log,
        )

    def run_consumers_once(self, groups: list[ConsumerGroup],
                           limit: int = 32) -> int:
        """让一组消费者各处理一批（驱动事件循环的便捷方法，测试用）。"""
        total = 0
        for g in groups:
            total += g.process_batch(limit)
        return total

    # ---- 观测 ----------------------------------------------------------
    def metrics(self) -> dict[str, int | float]:
        snap = self._metrics.snapshot()
        return snap

    @property
    def storage(self) -> Storage:
        return self._storage

    @property
    def dispatcher(self) -> Dispatcher:
        return self._dispatcher
