"""归一化错误类别：底层异常不直接透出。"""

from __future__ import annotations

import enum


class ErrorCategory(enum.Enum):
    VALIDATION = "validation"        # 参数错误
    RESOURCE_EXHAUSTED = "resource_exhausted"  # 资源不足（队列满等）
    CONFLICT = "conflict"            # 状态冲突（位点回退、重复提交等）
    NOT_FOUND = "not_found"
    INTERNAL = "internal"            # 内部异常


class QueueError(Exception):
    """所有对外错误的基类，携带归一化类别。"""

    category: ErrorCategory = ErrorCategory.INTERNAL

    def __init__(self, message: str, *, detail: object = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail

    def to_dict(self) -> dict:
        return {"category": self.category.value, "message": self.message}


class ValidationError(QueueError):
    category = ErrorCategory.VALIDATION


class ResourceExhaustedError(QueueError):
    category = ErrorCategory.RESOURCE_EXHAUSTED


class ConflictError(QueueError):
    category = ErrorCategory.CONFLICT


class NotFoundError(QueueError):
    category = ErrorCategory.NOT_FOUND


class InternalError(QueueError):
    category = ErrorCategory.INTERNAL


def normalize(exc: BaseException) -> QueueError:
    """把任意底层异常映射为归一化错误，不透出原始异常类型。"""
    if isinstance(exc, QueueError):
        return exc
    return InternalError("internal error", detail=type(exc).__name__)
