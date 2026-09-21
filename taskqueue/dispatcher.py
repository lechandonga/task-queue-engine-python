"""任务调度与执行：worker 线程池认领任务、执行处理器、处理重试/超时/死信。

关键保证：
- 认领通过数据库原子 UPDATE 完成，多 worker 不会重复认领；
- worker 只在持有自己的租约时才能完成/改期任务，防止取消与租约过期交错造成重复执行；
- 超时通过执行线程 + 协作取消实现；超时/取消计为一次可重试失败；
- 超过最大次数或不可重试错误进入死信表，可检索。
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from taskqueue.config import EngineConfig
from taskqueue.errors import TaskQueueError
from taskqueue.models import TaskRecord
from taskqueue.storage import Storage

# 处理器签名：(payload dict) -> Any；可抛 RetryableTaskError / NonRetryableTaskError
TaskHandler = Callable[[dict[str, Any]], Any]


class Dispatcher:
    def __init__(self, storage: Storage, cfg: EngineConfig,
                 handlers: dict[str, TaskHandler],
                 logger: Any | None = None) -> None:
        self._storage = storage
        self._cfg = cfg
        self._handlers = handlers
        self._log = logger
        self._workers: list[threading.Thread] = []
        self._stop = threading.Event()
        self._wake = threading.Event()

    def start(self) -> None:
        raise NotImplementedError

    def stop(self, timeout: float | None = None) -> None:
        raise NotImplementedError

    def wakeup(self) -> None:
        raise NotImplementedError

    # ---- 供测试直接驱动 -------------------------------------------------
    def run_once(self, namespace: str = "%") -> int:
        """同步认领并处理一批任务，返回处理条数。"""
        raise NotImplementedError

    def _execute(self, task: TaskRecord) -> None:
        raise NotImplementedError
