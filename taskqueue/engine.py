"""任务引擎：提交、异步执行、重试、死信、恢复。

语义要点：
- 提交即持久化（先落库再排队），重启后未完成任务自动续跑；
- 认领-执行-落终态均在 store 事务内判定，崩溃边界清晰：
  崩溃时 RUNNING 任务在下次 start() 被重置为 PENDING 重跑，
  已 SUCCEEDED 的任务不会重复执行；
- 处理器需幂等；跨重启的副作用去重由提交时的 dedup_key 保证；
- 所有对外错误经 errors.normalize 归一化，不透出底层异常。
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from typing import Callable

from . import payload as payload_codec
from .errors import (QueueError, ResourceExhaustedError, ValidationError,
                     normalize)
from .metrics import Metrics
from .models import Task, TaskStatus
from .queue import BacklogLimiter, NamespaceLimit, OverflowPolicy
from .store import Store

logger = logging.getLogger("taskqueue.engine")


class RetryableError(Exception):
    """处理器抛出：可重试错误，按退避策略重试。"""


class FatalError(Exception):
    """处理器抛出：不可重试错误，直接进入死信。"""


@dataclass
class EngineConfig:
    max_queue_size: int = 1024            # 单命名空间最大积压任务数
    max_backlog_bytes: int = 4 * 1024 * 1024
    overflow_policy: OverflowPolicy = OverflowPolicy.REJECT
    defer_timeout_seconds: float = 1.0    # DEFER 策略下等待额度的上限
    max_retries: int = 3
    backoff_base_seconds: float = 0.05    # 退避基数：base * 2^(attempts-1)
    task_timeout_seconds: float = 30.0
    dedup_window_seconds: float = 3600.0  # 去重窗口有效期（确定语义）
    workers: int = 4
    poll_interval_seconds: float = 0.01
    claim_batch: int = 16


class TaskEngine:
    def __init__(self, config: EngineConfig | None = None,
                 db_path: str = ":memory:") -> None:
        self.config = config or EngineConfig()
        self.store = Store(db_path)
        self.metrics = Metrics()
        self._limiter = BacklogLimiter(
            NamespaceLimit(self.config.max_queue_size,
                           self.config.max_backlog_bytes),
            self.config.overflow_policy)
        self._handlers: dict[str, Callable[[dict], None]] = {}
        # 两个池：包装池跑 _run_one（会阻塞等结果），处理器池跑用户函数，
        # 分开避免“池内提交任务并等待”造成的线程池死锁。
        self._dispatch_pool: ThreadPoolExecutor | None = None
        self._handler_pool: ThreadPoolExecutor | None = None
        self._dispatcher: threading.Thread | None = None
        self._stop = threading.Event()
        self._started = False

    # ---------- 处理器注册 ----------

    def register_handler(self, kind: str,
                         handler: Callable[[dict], None]) -> None:
        if not kind:
            raise ValidationError("handler kind must be non-empty")
        self._handlers[kind] = handler

    # ---------- 提交 ----------

    def submit(self, namespace: str, payload: dict,
               dedup_key: str | None = None,
               task_id: str | None = None) -> tuple[str, bool]:
        """提交任务，返回 (task_id, created)。

        created=False 表示命中去重窗口内的相同 dedup_key，返回已有任务，
        不会产生重复执行或重复副作用。
        """
        if not namespace:
            raise ValidationError("namespace must be non-empty")
        try:
            blob = payload_codec.encode(payload)  # 结构校验在此完成
        except QueueError:
            raise
        except Exception as exc:  # 底层异常不透出
            raise normalize(exc) from exc
        size = len(blob)
        now = time.time()
        # 容量判定之前先看去重：积压打满时，有效期内的相同去重键仍应直接
        # 返回已有任务，而不是误报资源不足。命中不新增积压、不占额度。
        if dedup_key is not None:
            existing = self.store.find_dedup(dedup_key, now)
            if existing is not None:
                self.metrics.incr("dedup_hits")
                logger.info("dedup hit (pre-admit) key=%s -> existing"
                            " task=%s (判定依据: 去重窗口内, 不受容量限制)",
                            dedup_key, existing.task_id)
                return existing.task_id, False
        admitted = self._admit(namespace, size)
        if not admitted:
            raise ResourceExhaustedError(
                f"namespace '{namespace}' backlog full after defer window")
        task = Task(task_id=task_id or uuid.uuid4().hex,
                    namespace=namespace, payload=payload,
                    dedup_key=dedup_key)
        try:
            stored, created = self.store.insert_task(
                task, self.config.max_retries, now,
                self.config.dedup_window_seconds)
        except QueueError:
            self._limiter.release(namespace, size)
            raise
        except Exception as exc:
            self._limiter.release(namespace, size)
            raise normalize(exc) from exc
        if not created:
            self._limiter.release(namespace, size)  # 未新增积压，退回额度
            self.metrics.incr("dedup_hits")
            logger.info("dedup hit key=%s -> existing task=%s",
                        dedup_key, stored.task_id)
            return stored.task_id, False
        self.metrics.incr("submitted")
        logger.info("submitted task=%s ns=%s kind=%s dedup=%s",
                    stored.task_id, namespace, payload.get("kind"),
                    dedup_key)
        return stored.task_id, True

    def _admit(self, namespace: str, size: int) -> bool:
        if self.config.overflow_policy is OverflowPolicy.DEFER:
            return self._limiter.wait_for_capacity(
                namespace, size, self.config.defer_timeout_seconds)
        return self._limiter.admit(namespace, size)

    # ---------- 生命周期 ----------

    def start(self) -> None:
        if self._started:
            return
        recovered = self.store.recover(time.time())
        if recovered:
            logger.info("recovered %d RUNNING tasks to PENDING", recovered)
        self.metrics.incr("recovered", recovered)
        self._stop.clear()
        self._dispatch_pool = ThreadPoolExecutor(
            max_workers=self.config.workers, thread_name_prefix="tq-dispatch")
        self._handler_pool = ThreadPoolExecutor(
            max_workers=self.config.workers, thread_name_prefix="tq-handler")
        self._dispatcher = threading.Thread(
            target=self._dispatch_loop, name="tq-dispatcher", daemon=True)
        self._dispatcher.start()
        self._started = True

    def stop(self, drain: bool = True) -> None:
        self._stop.set()
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=5)
        if self._dispatch_pool is not None:
            self._dispatch_pool.shutdown(wait=drain)
        if self._handler_pool is not None:
            self._handler_pool.shutdown(wait=False)
        self._started = False

    def close(self) -> None:
        self.stop()
        self.store.close()

    # ---------- 查询 ----------

    def get_task(self, task_id: str) -> Task | None:
        return self.store.get_task(task_id)

    def list_dead(self, namespace: str | None = None) -> list[Task]:
        return self.store.list_dead(namespace)

    def cancel(self, task_id: str) -> None:
        task = self.store.get_task(task_id)
        self.store.cancel(task_id, time.time())
        if task is not None:
            self._release(task)
        self.metrics.incr("cancelled")

    def set_namespace_limit(self, namespace: str, max_tasks: int,
                            max_bytes: int) -> None:
        self._limiter.set_limit(namespace, NamespaceLimit(max_tasks,
                                                          max_bytes))

    # ---------- 调度与执行 ----------

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            try:
                claimed = self.store.claim_due(time.time(),
                                               self.config.claim_batch)
            except Exception as exc:  # 存储层异常归一化记录，不崩溃
                logger.error("claim failed: %s", normalize(exc).message)
                claimed = []
            for task in claimed:
                assert self._dispatch_pool is not None
                self._dispatch_pool.submit(self._run_one, task)
            if not claimed:
                self._stop.wait(self.config.poll_interval_seconds)

    def _run_one(self, task: Task) -> None:
        started = time.monotonic()
        handler = self._handlers.get(task.payload.get("kind", ""))
        now = time.time()
        if handler is None:
            self.store.mark_failed(task.task_id, "no handler registered",
                                   retryable=False, next_run_at=0, now=now)
            self._finish(task, "dead")
            logger.warning("task=%s dead: no handler for kind=%r",
                           task.task_id, task.payload.get("kind"))
            return
        assert self._handler_pool is not None
        future = self._handler_pool.submit(handler, task.payload["args"])
        try:
            future.result(timeout=self.config.task_timeout_seconds)
        except FutureTimeout:
            self._after_failure(task, "handler timed out", retryable=True,
                                now=time.time())
            return
        except RetryableError as exc:
            self._after_failure(task, str(exc), retryable=True,
                                now=time.time())
            return
        except FatalError as exc:
            self._after_failure(task, str(exc), retryable=False,
                                now=time.time())
            return
        except Exception as exc:  # 未知异常按可重试处理，超限后入死信
            self._after_failure(task, f"unexpected: {type(exc).__name__}",
                                retryable=True, now=time.time())
            return
        try:
            self.store.mark_succeeded(task.task_id, time.time())
        except QueueError as exc:
            # 例如任务已被取消：状态以库为准，不强行覆盖
            logger.info("task=%s succeed skipped: %s", task.task_id,
                        exc.message)
            self._release(task)
            return
        self._finish(task, "succeeded")
        self.metrics.record_latency(time.monotonic() - started)
        logger.info("task=%s succeeded attempts=%d", task.task_id,
                    task.attempts)

    def _after_failure(self, task: Task, error: str, retryable: bool,
                       now: float) -> None:
        delay = (self.config.backoff_base_seconds
                 * (2 ** max(task.attempts - 1, 0)))
        status = self.store.mark_failed(task.task_id, error, retryable,
                                        next_run_at=now + delay, now=now)
        if status is TaskStatus.DEAD:
            self._finish(task, "dead")
            logger.warning("task=%s dead after %d attempts: %s",
                           task.task_id, task.attempts, error)
        else:
            self.metrics.incr("retried")
            logger.info("task=%s retry in %.3fs (attempt %d): %s",
                        task.task_id, delay, task.attempts, error)

    def _finish(self, task: Task, outcome: str) -> None:
        self.metrics.incr(outcome)
        self._release(task)

    def _release(self, task: Task) -> None:
        size = len(payload_codec.encode(task.payload))
        self._limiter.release(task.namespace, size)
