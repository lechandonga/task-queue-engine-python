"""积压限制器：按命名空间隔离的队列长度与内存上限。

策略：
- reject：超限立即抛 ResourceExhaustedError；
- defer：超限时不报错，由调用方延迟投递（返回 False）。
空闲时不持有任务本体，只维护计数，资源占用有界。
"""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass

from .errors import ResourceExhaustedError, ValidationError


class OverflowPolicy(enum.Enum):
    REJECT = "reject"
    DEFER = "defer"


@dataclass
class NamespaceLimit:
    max_tasks: int = 1024          # 单命名空间最大积压任务数
    max_bytes: int = 4 * 1024 * 1024  # 单命名空间最大积压字节数


class BacklogLimiter:
    """按命名空间统计积压量，超限按策略拒绝或要求延迟。"""

    def __init__(self, default: NamespaceLimit | None = None,
                 policy: OverflowPolicy = OverflowPolicy.REJECT) -> None:
        self._default = default or NamespaceLimit()
        self._policy = policy
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._limits: dict[str, NamespaceLimit] = {}
        self._tasks: dict[str, int] = {}
        self._bytes: dict[str, int] = {}

    def set_limit(self, namespace: str, limit: NamespaceLimit) -> None:
        if limit.max_tasks <= 0 or limit.max_bytes <= 0:
            raise ValidationError("limits must be positive")
        with self._lock:
            self._limits[namespace] = limit

    def admit(self, namespace: str, size_bytes: int) -> bool:
        """尝试为一笔新任务登记积压额度。

        返回 True 表示已登记；REJECT 策略下超限抛
        ResourceExhaustedError，DEFER 策略下返回 False（调用方延迟）。
        """
        if size_bytes < 0:
            raise ValidationError("size_bytes must be >= 0")
        with self._lock:
            limit = self._limits.get(namespace, self._default)
            tasks = self._tasks.get(namespace, 0)
            bytes_ = self._bytes.get(namespace, 0)
            if tasks + 1 > limit.max_tasks or bytes_ + size_bytes > limit.max_bytes:
                if self._policy is OverflowPolicy.REJECT:
                    raise ResourceExhaustedError(
                        f"namespace '{namespace}' backlog full:"
                        f" tasks={tasks}/{limit.max_tasks},"
                        f" bytes={bytes_}/{limit.max_bytes}")
                return False
            self._tasks[namespace] = tasks + 1
            self._bytes[namespace] = bytes_ + size_bytes
            return True

    def release(self, namespace: str, size_bytes: int) -> None:
        """任务离开积压（执行完成/进入死信/取消）时释放额度。"""
        with self._cond:
            self._tasks[namespace] = max(0, self._tasks.get(namespace, 0) - 1)
            self._bytes[namespace] = max(
                0, self._bytes.get(namespace, 0) - size_bytes)
            self._cond.notify_all()

    def wait_for_capacity(self, namespace: str, size_bytes: int,
                          timeout: float) -> bool:
        """DEFER 策略：等待出现可用额度并登记，超时返回 False。"""
        import time
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                limit = self._limits.get(namespace, self._default)
                tasks = self._tasks.get(namespace, 0)
                bytes_ = self._bytes.get(namespace, 0)
                if (tasks + 1 <= limit.max_tasks
                        and bytes_ + size_bytes <= limit.max_bytes):
                    self._tasks[namespace] = tasks + 1
                    self._bytes[namespace] = bytes_ + size_bytes
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)

    def usage(self, namespace: str) -> tuple[int, int]:
        with self._lock:
            return (self._tasks.get(namespace, 0),
                    self._bytes.get(namespace, 0))
