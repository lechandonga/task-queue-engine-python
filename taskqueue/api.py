"""HTTP API（FastAPI）：把错误类别映射为 HTTP 状态码，底层异常不透出。"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from taskqueue.errors import ErrorCategory, TaskQueueError, normalize_error

# 错误类别 -> HTTP 状态码
_HTTP_STATUS = {
    ErrorCategory.INVALID_ARGUMENT: 400,
    ErrorCategory.NOT_FOUND: 404,
    ErrorCategory.RESOURCE_EXHAUSTED: 429,
    ErrorCategory.CONFLICT: 409,
    ErrorCategory.PRECONDITION_FAILED: 412,
    ErrorCategory.CANCELLED: 499,
    ErrorCategory.TIMEOUT: 504,
    ErrorCategory.RETRYABLE: 500,
    ErrorCategory.NON_RETRYABLE: 500,
    ErrorCategory.INTERNAL: 500,
}


def create_app(engine: Any | None = None) -> FastAPI:
    raise NotImplementedError
