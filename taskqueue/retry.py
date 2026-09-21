"""退避策略：决定下一次重试的延迟，以及错误是否可重试。"""

from __future__ import annotations

from taskqueue.config import RetryConfig
from taskqueue.errors import (
    NonRetryableTaskError,
    RetryableTaskError,
    TaskQueueError,
    TimeoutError,
)


def is_retryable(exc: BaseException) -> bool:
    """判定异常是否允许重试。

    - :class:`NonRetryableTaskError` 明确不可重试；
    - :class:`RetryableTaskError` / 超时 / Cancelled 以外的未知内部异常默认不重试。
    """
    raise NotImplementedError


def next_delay_ms(cfg: RetryConfig, attempt: int) -> int:
    """指数退避：``min(base * multiplier**(attempt-1), max)``，结果确定无抖动。"""
    raise NotImplementedError
