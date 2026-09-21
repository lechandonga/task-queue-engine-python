"""错误归一化：所有对外暴露的错误都映射为可区分的错误类别。

底层异常不得直接透出，统一包装为 :class:`TaskQueueError` 的子类。
"""

from __future__ import annotations

import builtins
from enum import Enum


class ErrorCategory(str, Enum):
    """错误类别（对外稳定枚举）。"""

    INVALID_ARGUMENT = "invalid_argument"
    NOT_FOUND = "not_found"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    CONFLICT = "conflict"
    PRECONDITION_FAILED = "precondition_failed"
    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"
    INTERNAL = "internal"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskQueueError(Exception):
    """所有任务队列错误的基类。"""

    category: ErrorCategory = ErrorCategory.INTERNAL

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.cause = cause

    def __str__(self) -> str:  # pragma: no cover - 简单格式
        return f"[{self.category.value}] {self.message}"


class InvalidArgumentError(TaskQueueError):
    category = ErrorCategory.INVALID_ARGUMENT


class NotFoundError(TaskQueueError):
    category = ErrorCategory.NOT_FOUND


class ResourceExhaustedError(TaskQueueError):
    """队列长度/积压内存超限等资源不足。"""

    category = ErrorCategory.RESOURCE_EXHAUSTED


class ConflictError(TaskQueueError):
    """重复提交（且策略为拒绝）等冲突。"""

    category = ErrorCategory.CONFLICT


class PreconditionFailedError(TaskQueueError):
    """位点回退、重复提交位点、去重键冲突状态非法等。"""

    category = ErrorCategory.PRECONDITION_FAILED


class RetryableTaskError(TaskQueueError):
    """任务处理器主动声明：该错误可以退避重试。"""

    category = ErrorCategory.RETRYABLE


class NonRetryableTaskError(TaskQueueError):
    """任务处理器主动声明：该错误不可重试，直接进死信。"""

    category = ErrorCategory.NON_RETRYABLE


class CancelledError(TaskQueueError):
    category = ErrorCategory.CANCELLED


class TimeoutError(TaskQueueError):
    category = ErrorCategory.TIMEOUT


class InternalError(TaskQueueError):
    category = ErrorCategory.INTERNAL


def normalize_error(exc: BaseException) -> TaskQueueError:
    """把底层异常归一化为 :class:`TaskQueueError`。

    - 已是 :class:`TaskQueueError` 的直接返回；
    - Python 内置 ``ValueError``/``TypeError`` 归一为参数错误（发生在输入解析边界时）；
    - 内置 ``TimeoutError`` 归一为超时类别；
    - 其余未知异常一律包装为 INTERNAL，保留 ``cause`` 链但不直接透出。
    """
    if isinstance(exc, TaskQueueError):
        return exc
    if isinstance(exc, builtins.TimeoutError):
        return TimeoutError(str(exc) or "execution timed out", cause=exc)
    if isinstance(exc, (ValueError, TypeError)):
        return InvalidArgumentError(str(exc) or "invalid argument", cause=exc)
    return InternalError(f"unexpected error: {type(exc).__name__}: {exc}", cause=exc)
