"""有界任务队列：命名空间/队列相互隔离，长度与积压内存有上限。

准入与落库的一致性：
- 真实账本（待调度数量、积压字节）以 SQLite 为准，原子判定在
  ``Storage.submit_task`` 内完成，本类不做可能失真的内存预扣；
- ``overflow_policy``：
  - ``"reject"``：额度不足立即抛 :class:`ResourceExhaustedError`；
  - ``"delay"``：当前线程在条件变量上等待，直到其他任务离开队列后被唤醒重试，
    或 ``close()`` 被调用（抛错），绝不无界占用内存（等待本身只占一个线程栈）。
- 不同 ``(namespace, queue)`` 的额度统计按 namespace 独立（队列是 namespace
  内的进一步隔离标签）。
"""

from __future__ import annotations

import threading
from typing import Callable

from taskqueue.config import QueueConfig
from taskqueue.errors import ResourceExhaustedError
from taskqueue.models import TaskRecord, now_ms
from taskqueue.storage import Storage


class BoundedQueueGate:
    def __init__(self, storage: Storage, cfg: QueueConfig,
                 time_func: Callable[[], int] = now_ms) -> None:
        self._storage = storage
        self._cfg = cfg
        self._time = time_func
        self._space_cond = threading.Condition()
        self._closed = False

    def close(self) -> None:
        with self._space_cond:
            self._closed = True
            self._space_cond.notify_all()

    def submit(self, task: TaskRecord, window_ms: int | None = None) -> tuple[str, str]:
        """按策略提交任务，返回 Storage 的 ``(status, task_id)`` 结果。

        ``"delay"`` 策略下在条件变量上等待空位；等待被关闭打断时抛
        :class:`ResourceExhaustedError`。
        """
        if self._cfg.overflow_policy == "reject":
            return self._try_once(task, window_ms)
        if self._cfg.overflow_policy == "delay":
            return self._submit_with_delay(task, window_ms)
        raise ResourceExhaustedError(
            f"unknown overflow_policy: {self._cfg.overflow_policy}"
        )

    def _try_once(self, task: TaskRecord, window_ms: int | None) -> tuple[str, str]:
        status, tid = self._storage.submit_task(
            task,
            now=self._time(),
            max_pending=self._cfg.max_pending,
            max_backlog_bytes=self._cfg.max_backlog_bytes,
            window_ms=window_ms,
        )
        if status == "full":
            raise ResourceExhaustedError(
                f"queue limit reached for namespace={task.namespace} "
                f"queue={task.queue} (max_pending={self._cfg.max_pending}, "
                f"max_backlog_bytes={self._cfg.max_backlog_bytes})"
            )
        return status, tid

    def _submit_with_delay(self, task: TaskRecord,
                           window_ms: int | None) -> tuple[str, str]:
        with self._space_cond:
            while True:
                if self._closed:
                    raise ResourceExhaustedError("queue gate closed")
                # cond 持有期间调用 storage：storage 自己有锁，顺序固定为
                # cond -> storage，不会反向加锁，无死锁。
                status, tid = self._try_once_locked(task, window_ms)
                if status != "full":
                    return status, tid
                self._space_cond.wait(timeout=1.0)

    def _try_once_locked(self, task: TaskRecord, window_ms: int | None) -> tuple[str, str]:
        try:
            return self._storage.submit_task(
                task,
                now=self._time(),
                max_pending=self._cfg.max_pending,
                max_backlog_bytes=self._cfg.max_backlog_bytes,
                window_ms=window_ms,
            )
        except ResourceExhaustedError:
            return "full", ""

    def notify_space(self) -> None:
        """任务离开待调度集合（完成/转死信）后唤醒 delay 等待者。"""
        with self._space_cond:
            self._space_cond.notify_all()

    # ---- 观测辅助 ------------------------------------------------------
    def usage(self, namespace: str) -> tuple[int, int]:
        """返回 ``(待调度数, 未终态积压字节)``。"""
        return (
            self._storage.count_pending(namespace),
            self._storage.backlog_bytes(namespace),
        )
