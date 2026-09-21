"""重试与死信测试：退避重试、不可重试立即死信、超时可重试、超上限进死信。"""

import logging
import time as wall

from taskqueue.config import (
    DedupConfig,
    EngineConfig,
    QueueConfig,
    RetryConfig,
)
from taskqueue.dispatcher import Dispatcher
from taskqueue.errors import NonRetryableTaskError, RetryableTaskError
from taskqueue.models import TaskRecord, TaskStatus
from taskqueue.storage import Storage

log = logging.getLogger("test.retry_dl")


class Clock:
    def __init__(self):
        self.t = 10_000

    def __call__(self):
        return self.t

    def add(self, ms):
        self.t += ms


def cfg(**kw):
    return EngineConfig(
        retry=RetryConfig(max_attempts=3, base_delay_ms=100, multiplier=2.0,
                          max_delay_ms=1000, execution_timeout_ms=100),
        queue=QueueConfig(max_pending=1000),
        dedup=DedupConfig(window_ms=10_000),
        visibility_timeout_ms=10_000,
        worker_count=1,
        **kw,
    )


def insert(st, tid, created=10_000, run_at=10_000):
    st.insert_task(TaskRecord(
        id=tid, namespace="ns", queue="default",
        payload={"metadata": {"handler": "h"}},
        status=TaskStatus.PENDING, created_at_ms=created,
        updated_at_ms=created, run_at_ms=run_at,
    ))


def test_retryable_error_backs_off_then_succeeds(tmp_path):
    clock = Clock()
    with Storage(str(tmp_path / "r.db")) as st:
        attempts = {"n": 0}

        def flaky(payload):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RetryableTaskError("transient")

        d = Dispatcher(st, cfg(), {"h": flaky}, time_func=clock)
        insert(st, "t1")
        # 第 1 次：认领并失败 -> RETRY_WAIT 10100
        assert d.run_once("ns") == 1
        t = st.get_task("t1")
        assert t.status is TaskStatus.RETRY_WAIT and t.attempt == 1
        assert t.run_at_ms == 10_100  # base 100ms
        log.info("task_id=t1 offset=- attempt=1 判定依据=退避至 run_at=%d", t.run_at_ms)
        # 退避未到：不被认领
        clock.add(99)
        assert d.run_once("ns") == 0
        clock.add(1)  # 10100
        assert d.run_once("ns") == 1
        assert st.get_task("t1").status is TaskStatus.RETRY_WAIT
        log.info("task_id=t1 offset=- attempt=2 判定依据=第二次失败退避完成")
        # 第三次成功
        clock.add(200)
        assert d.run_once("ns") == 1
        assert st.get_task("t1").status is TaskStatus.COMPLETED
        assert attempts["n"] == 3
        log.info("task_id=t1 offset=- attempt=3 判定依据=重试后成功，总执行 3 次")


def test_non_retryable_goes_dead_immediately(tmp_path):
    clock = Clock()
    with Storage(str(tmp_path / "r.db")) as st:
        def fatal(payload):
            raise NonRetryableTaskError("bad data")

        d = Dispatcher(st, cfg(), {"h": fatal}, time_func=clock)
        insert(st, "t2")
        d.run_once("ns")
        t = st.get_task("t2")
        assert t.status is TaskStatus.DEAD
        dl = st.list_dead_letters("ns")
        assert len(dl) == 1 and dl[0].attempts == 1
        assert "non_retryable" in dl[0].reason
        log.info("task_id=t2 offset=- attempts=1 判定依据=不可重试直接死信 reason=%s",
                 dl[0].reason)


def test_max_attempts_exceeded_goes_dead(tmp_path):
    clock = Clock()
    with Storage(str(tmp_path / "r.db")) as st:
        calls = {"n": 0}

        def always_fail(payload):
            calls["n"] += 1
            raise RetryableTaskError("nope")

        d = Dispatcher(st, cfg(), {"h": always_fail}, time_func=clock)
        insert(st, "t3")
        # 第 1 次失败：ts=10000 + 100 -> 10100；时钟走到 10500
        d.run_once("ns")
        assert st.get_task("t3").run_at_ms == 10_100
        clock.add(500)
        # 第 2 次失败：ts=10500 + 200 -> 10700；时钟走到 11000
        d.run_once("ns")
        assert st.get_task("t3").run_at_ms == 10_700
        clock.add(500)
        # 第 3 次仍失败 -> 超上限进死信
        d.run_once("ns")
        assert st.get_task("t3").status is TaskStatus.DEAD
        dl = st.list_dead_letters("ns")[0]
        assert dl.attempts == 3
        assert calls["n"] == 3
        log.info("task_id=t3 offset=- attempts=3 判定依据=超过 max_attempts 进死信")


def test_timeout_counts_as_retryable_failure(tmp_path):
    clock = Clock()
    with Storage(str(tmp_path / "r.db")) as st:
        def slow(payload):
            wall.sleep(0.3)  # 超过 execution_timeout_ms=100

        d = Dispatcher(st, cfg(), {"h": slow}, time_func=clock)
        insert(st, "t4")
        d.run_once("ns")
        t = st.get_task("t4")
        assert t.status is TaskStatus.RETRY_WAIT
        assert "timeout" in (t.last_error or "")
        log.info("task_id=t4 offset=- 判定依据=执行超时计一次可重试失败 error=%s",
                 t.last_error)


def test_unknown_exception_is_non_retryable(tmp_path):
    clock = Clock()
    with Storage(str(tmp_path / "r.db")) as st:
        def boom(payload):
            raise RuntimeError("unexpected")

        d = Dispatcher(st, cfg(), {"h": boom}, time_func=clock)
        insert(st, "t5")
        d.run_once("ns")
        assert st.get_task("t5").status is TaskStatus.DEAD
        dl = st.list_dead_letters("ns")[0]
        assert dl.reason.startswith("internal:")  # 归一化为 INTERNAL 后入死信
        log.info("task_id=t5 offset=- 判定依据=未知异常归一化 internal 后死信 reason=%s",
                 dl.reason)


def test_missing_handler_goes_dead(tmp_path):
    clock = Clock()
    with Storage(str(tmp_path / "r.db")) as st:
        d = Dispatcher(st, cfg(), {}, time_func=clock)
        insert(st, "t6")
        d.run_once("ns")
        assert st.get_task("t6").status is TaskStatus.DEAD
        log.info("task_id=t6 offset=- 判定依据=无处理器注册，明确死信而非卡死")
