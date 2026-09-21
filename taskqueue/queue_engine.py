"""有界任务队列：命名空间/队列相互隔离，长度与积压内存有上限。

- 每个 ``(namespace, queue)`` 独立统计待调度数量与积压字节；
- 超限按 ``overflow_policy`` 处理：
  - ``reject``：抛 :class:`ResourceExhaustedError`；
  - ``delay``：阻塞等待空位（可被关闭唤醒），不做无界占用；
- 真实账本以 Storage 为准（可跨重启恢复），本类只负责准入与等待唤醒。
"""

from __future__ import annotations

import threading
from typing import Callable

from taskqueue.config import QueueConfig
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
        raise NotImplementedError

    def admit(self, task: TaskRecord, payload_bytes: int) -> None:
        """准入检查：超限则拒绝（抛错）或阻塞等待空位。"""
        raise NotImplementedError

    def notify_space(self) -> None:
        """任务被取走/完成后唤醒等待中的提交者。"""
        raise NotImplementedError
