"""退避策略：决定下一次重试的延迟，以及错误是否可重试。"""

from __future__ import annotations

from taskqueue.config import RetryConfig
from taskqueue.errors import (
    CancelledError,
    NonRetryableTaskError,
    RetryableTaskError,
    TimeoutError,
)


def is_retryable(exc: BaseException) -> bool:
    """判定异常是否允许重试。

    - :class:`NonRetryableTaskError` 明确不可重试；
    - :class:`RetryableTaskError` 可重试；
    - 执行超时（自定义或内置 ``TimeoutError``）与队列层的 Cancelled 可重试；
    - 其余未知异常默认不重试（避免对未知副作用重复执行）。
    """
    import builtins

    if isinstance(exc, NonRetryableTaskError):
        return False
    if isinstance(exc, (RetryableTaskError, TimeoutError, CancelledError)):
        return True
    if isinstance(exc, builtins.TimeoutError):
        return True
    return False


def next_delay_ms(cfg: RetryConfig, attempt: int) -> int:
    """指数退避：``min(base * multiplier**(attempt-1), max)``。

    无随机抖动，结果完全确定、可复现；``attempt`` 从 1 起（第一次失败后的等待）。
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    raw = cfg.base_delay_ms * (cfg.multiplier ** (attempt - 1))
    return int(min(cfg.max_delay_ms, raw))
