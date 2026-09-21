"""任务调度与执行：worker 线程池认领任务、执行处理器、处理重试/超时/死信。

关键保证：
- 认领通过数据库单条原子 UPDATE 完成，多 worker 不会重复认领；
- worker 只在持有自己的租约时才能完成/改期任务（SQL 带 leased_by 条件），
  因此租约过期被他人重投后，原 worker 的迟到结果不会覆盖新状态，杜绝重复完成；
- 超时通过执行线程 + join 超时检测：超时计为一次可重试失败。Python 无法强杀
  线程，迟到的副作用风险由处理器幂等性兜底（at-least-once 语义）；
- 超过最大次数或不可重试错误进入死信表，可检索；
- 每轮循环先做租约过期回收（崩溃恢复后不卡死），再清理过期去重键。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from taskqueue.config import EngineConfig
from taskqueue.errors import (
    NonRetryableTaskError,
    TaskQueueError,
    TimeoutError,
    normalize_error,
)
from taskqueue.models import TaskRecord, now_ms
from taskqueue.retry import is_retryable, next_delay_ms
from taskqueue.storage import Storage

# 处理器签名：(payload dict) -> Any；可抛 RetryableTaskError / NonRetryableTaskError
TaskHandler = Callable[[dict[str, Any]], Any]

_log = logging.getLogger("taskqueue.dispatcher")


class _ExecOutcome:
    __slots__ = ("ok", "error")

    def __init__(self) -> None:
        self.ok = False
        self.error: BaseException | None = None


class Dispatcher:
    def __init__(self, storage: Storage, cfg: EngineConfig,
                 handlers: dict[str, TaskHandler],
                 on_complete: Callable[[int], None] | None = None,
                 on_retry: Callable[[], None] | None = None,
                 on_dead: Callable[[], None] | None = None,
                 time_func: Callable[[], int] = now_ms,
                 logger: logging.Logger | None = None) -> None:
        self._storage = storage
        self._cfg = cfg
        self._handlers = handlers
        self._on_complete = on_complete
        self._on_retry = on_retry
        self._on_dead = on_dead
        self._time = time_func
        self._log = logger or _log
        self._workers: list[threading.Thread] = []
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._reaper = threading.Event()

    # ---- 生命周期 ------------------------------------------------------
    def start(self) -> None:
        if self._workers:
            return
        self._stop.clear()
        if self._cfg.worker_count <= 0:
            return  # 纯手动驱动模式（测试/嵌入）：不启动后台线程
        for i in range(max(1, self._cfg.worker_count)):
            th = threading.Thread(
                target=self._loop, name=f"tq-worker-{i}", daemon=True)
            th.start()
            self._workers.append(th)

    def stop(self, timeout: float | None = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        for th in self._workers:
            th.join(timeout=timeout)
        self._workers = []

    def wakeup(self) -> None:
        self._wake.set()

    def _loop(self) -> None:
        last_reap = 0
        while not self._stop.is_set():
            ts = self._time()
            # 每 200ms（逻辑时钟外仍用墙钟睡眠节流）做一次过期回收
            if ts - last_reap >= 200:
                self.recover_expired()
                self._storage.dedup_sweep(ts)
                last_reap = ts
            n = self.run_once("%")
            if n == 0:
                self._wake.wait(timeout=0.05)
                self._wake.clear()

    def recover_expired(self) -> None:
        """恢复入口：过期任务租约退回重试队列、过期事件认领放开。"""
        ts = self._time()
        n_tasks = self._storage.expire_leases(ts, self._cfg.visibility_timeout_ms)
        n_acks = self._storage.expire_group_leases(ts, self._cfg.visibility_timeout_ms)
        if n_tasks or n_acks:
            self._log.info(
                "recover task_count=%d event_leases=%d 判定依据=租约过期重新可认领",
                n_tasks, n_acks,
            )

    # ---- 供测试直接驱动 -------------------------------------------------
    def run_once(self, namespace: str = "%") -> int:
        """同步认领并处理一批任务，返回处理条数。"""
        ts = self._time()
        tasks = self._storage.fetch_due_tasks(
            namespace, now=ts, limit=16,
            worker_id=f"sync-{threading.get_ident()}",
            lease_ms=self._cfg.visibility_timeout_ms,
        )
        for task in tasks:
            self._execute(task)
        return len(tasks)

    # ---- 单任务执行 -----------------------------------------------------
    def _handler_for(self, task: TaskRecord) -> tuple[str, TaskHandler]:
        name = task.payload.get("metadata", {}).get("handler")
        if not isinstance(name, str) or not name:
            raise NonRetryableTaskError(
                f"task {task.id} payload.metadata.handler missing or not a string")
        handler = self._handlers.get(name)
        if handler is None:
            raise NonRetryableTaskError(f"no handler registered for {name!r}")
        return name, handler

    def _run_handler(self, handler: TaskHandler, task: TaskRecord) -> _ExecOutcome:
        outcome = _ExecOutcome()

        def target() -> None:
            try:
                handler(task.payload)
                outcome.ok = True
            except BaseException as exc:  # 处理器里的任何异常都要被归类，不透出线程
                outcome.error = exc

        th = threading.Thread(
            target=target, name=f"exec-{task.id[:8]}", daemon=True)
        th.start()
        timeout = self._cfg.retry.execution_timeout_ms
        th.join(timeout=(timeout / 1000.0) if timeout else None)
        if th.is_alive():
            outcome.ok = False
            outcome.error = TimeoutError(
                f"handler exceeded execution_timeout_ms={timeout}")
        return outcome

    def _execute(self, task: TaskRecord) -> None:
        ts = self._time()
        started = ts
        name = "?"
        try:
            name, handler = self._handler_for(task)
        except NonRetryableTaskError as exc:
            self._to_dead_or_reschedule(task, exc, started)
            return

        outcome = self._run_handler(handler, task)
        if outcome.ok:
            if self._storage.complete_task(task.id, self._worker_id(task), self._time()):
                latency = self._time() - started
                self._log.info(
                    "task_id=%s offset=- handler=%s attempt=%d 判定依据=执行成功完成",
                    task.id, name, task.attempt)
                if self._on_complete:
                    self._on_complete(latency)
            else:
                self._log.info(
                    "task_id=%s offset=- handler=%s attempt=%d 判定依据=租约已转移，"
                    "迟到成功结果被丢弃", task.id, name, task.attempt)
            return

        assert outcome.error is not None
        self._to_dead_or_reschedule(task, outcome.error, started, handler_name=name)

    def _worker_id(self, task: TaskRecord) -> str:
        # 认领时写入的 leased_by 即为当前执行者身份
        return task.leased_by or f"sync-{threading.get_ident()}"

    def _to_dead_or_reschedule(self, task: TaskRecord, exc: BaseException,
                               started: int, handler_name: str = "?") -> None:
        worker = self._worker_id(task)
        normalized = normalize_error(exc)
        retryable = is_retryable(exc)
        ts = self._time()
        if retryable and task.attempt < self._cfg.retry.max_attempts:
            delay = next_delay_ms(self._cfg.retry, task.attempt)
            ok = self._storage.reschedule_task(
                task.id, worker, run_at_ms=ts + delay,
                error=str(normalized), now=ts)
            if ok:
                self._log.info(
                    "task_id=%s offset=- handler=%s attempt=%d 判定依据=可重试错误"
                    " category=%s 退避 %dms", task.id, handler_name, task.attempt,
                    normalized.category.value, delay)
                if self._on_retry:
                    self._on_retry()
            else:
                self._log.info(
                    "task_id=%s offset=- handler=%s attempt=%d 判定依据=租约已转移，"
                    "迟到失败结果不写入", task.id, handler_name, task.attempt)
            return

        reason = f"{normalized.category.value}: {normalized.message}"
        dl = self._storage.fail_task_to_dead(task.id, reason, ts, worker_id=worker)
        if dl is not None:
            self._log.info(
                "task_id=%s offset=- handler=%s attempt=%d 判定依据=%s 进入死信",
                task.id, handler_name, task.attempt,
                "超过重试上限" if retryable else "不可重试错误")
            if self._on_dead:
                self._on_dead()
