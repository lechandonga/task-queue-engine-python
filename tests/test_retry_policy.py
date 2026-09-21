"""退避策略与可重试判定的单元测试。"""

import logging

from taskqueue.config import RetryConfig
from taskqueue.errors import (
    CancelledError,
    NonRetryableTaskError,
    RetryableTaskError,
)
from taskqueue.retry import is_retryable, next_delay_ms

log = logging.getLogger("test.retry")


def test_backoff_is_deterministic_and_capped():
    cfg = RetryConfig(base_delay_ms=10, multiplier=2.0, max_delay_ms=100)
    seq = [next_delay_ms(cfg, a) for a in range(1, 6)]
    log.info("task_id=- offset=- 判定依据=确定性退避序列 seq=%s", seq)
    assert seq == [10, 20, 40, 80, 100]


def test_retryability_classification():
    assert is_retryable(RetryableTaskError("again"))
    assert is_retryable(CancelledError("cancel"))
    assert not is_retryable(NonRetryableTaskError("fatal"))
    assert not is_retryable(RuntimeError("unknown"))
    log.info("task_id=- offset=- 判定依据=可重试分类完成")
