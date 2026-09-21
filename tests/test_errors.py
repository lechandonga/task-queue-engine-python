"""错误归一化测试：底层异常不得直接透出，类别必须可区分。"""

import builtins
import logging

import pytest

from taskqueue.errors import (
    ErrorCategory,
    InternalError,
    InvalidArgumentError,
    NonRetryableTaskError,
    PreconditionFailedError,
    ResourceExhaustedError,
    TimeoutError,
    normalize_error,
)

log = logging.getLogger("test.errors")


def test_known_error_passes_through():
    err = ResourceExhaustedError("queue full")
    out = normalize_error(err)
    log.info("task_id=- offset=- 判定依据=已知错误原样透传 category=%s", out.category.value)
    assert out is err
    assert out.category is ErrorCategory.RESOURCE_EXHAUSTED


def test_builtin_value_error_mapped_to_invalid_argument():
    out = normalize_error(ValueError("bad field"))
    log.info("task_id=- offset=- 判定依据=ValueError 归一化 category=%s", out.category.value)
    assert isinstance(out, InvalidArgumentError)
    assert out.cause is not None  # 保留原因链


def test_builtin_timeout_mapped_to_timeout():
    out = normalize_error(builtins.TimeoutError())
    log.info("task_id=- offset=- 判定依据=TimeoutError 归一化 category=%s", out.category.value)
    assert isinstance(out, TimeoutError)


def test_unknown_exception_wrapped_as_internal():
    raw = RuntimeError("boom")
    out = normalize_error(raw)
    log.info("task_id=- offset=- 判定依据=未知异常包装 INTERNAL category=%s", out.category.value)
    assert isinstance(out, InternalError)
    assert out.category is ErrorCategory.INTERNAL
    assert out.cause is raw
    # 对外字符串不直接是底层异常类
    assert "RuntimeError" in str(out)  # 仅作为诊断信息附在 INTERNAL 文案内


@pytest.mark.parametrize(
    "exc_cls,expected",
    [
        (InvalidArgumentError, ErrorCategory.INVALID_ARGUMENT),
        (PreconditionFailedError, ErrorCategory.PRECONDITION_FAILED),
        (NonRetryableTaskError, ErrorCategory.NON_RETRYABLE),
    ],
)
def test_error_categories_are_distinct(exc_cls, expected):
    assert exc_cls("x").category is expected
