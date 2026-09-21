"""故障恢复测试：进程重启后已完成不重复、未完成不丢失、位点可续跑、去重窗口保留。"""

import logging

from taskqueue.config import EngineConfig, QueueConfig, RetryConfig
from taskqueue.engine import TaskQueueEngine
from taskqueue.errors import RetryableTaskError
from taskqueue.models import TaskStatus, now_ms

log = logging.getLogger("test.recovery")


def make_cfg(tmp_path, **kw):
    return EngineConfig(
        db_path=str(tmp_path / "r.db"),
        worker_count=0,  # 纯同步驱动：用 dispatcher.run_once 精确控制执行时机
        queue=QueueConfig(max_pending=1000),
        retry=RetryConfig(max_attempts=5, base_delay_ms=10,
                          max_delay_ms=100, execution_timeout_ms=2000),
        visibility_timeout_ms=10_000,
        **kw,
    )


def test_completed_tasks_are_not_reexecuted_after_restart(tmp_path):
    db = str(tmp_path / "r.db")
    side_effects = []
    cfg = make_cfg(tmp_path)
    with TaskQueueEngine(cfg, handlers={"h": lambda p: side_effects.append("job1")}) as eng:
        t = eng.submit("ns", {"args": ["job1"], "metadata": {"handler": "h"}})
        eng.dispatcher.run_once("ns")
        assert side_effects == ["job1"]
        tid = t.id

    # 模拟进程重启：全新引擎打开同一个 db
    cfg2 = make_cfg(tmp_path)
    with TaskQueueEngine(cfg2, handlers={"h": lambda p: side_effects.append("job1")}) as eng2:
        eng2.dispatcher.run_once("ns")  # 没有可执行任务
        assert side_effects == ["job1"]
        assert eng2.get_task(tid).status is TaskStatus.COMPLETED
        log.info("task_id=%s offset=- 判定依据=重启后 COMPLETED 不重复执行", tid)


class MovableClock:
    """单调可控时钟：默认等于创建时刻，可显式推进，重启后可继续沿用。"""

    def __init__(self):
        self.t = now_ms()
        self._advance = 0

    def __call__(self):
        return self.t + self._advance

    def advance(self, ms):
        self._advance += ms


def test_inflight_task_is_redelivered_not_lost_after_restart(tmp_path):
    clock = MovableClock()
    calls = []

    # 第一轮：任务被认领为 RUNNING 后“进程崩溃”（未完成即关闭）
    with TaskQueueEngine(make_cfg(tmp_path), time_func=clock,
                         handlers={"h": lambda p: calls.append("job2")}) as eng:
        t = eng.submit("ns", {"args": ["job2"], "metadata": {"handler": "h"}})
        got = eng.storage.fetch_due_tasks(
            "ns", now=clock(), limit=1, worker_id="crashed-worker", lease_ms=10)
        assert [x.id for x in got] == [t.id]

    # 重启：时钟推进超过租约到期点 -> 恢复退回 RETRY_WAIT 并重新执行
    clock.advance(60_000)
    with TaskQueueEngine(make_cfg(tmp_path), time_func=clock,
                         handlers={"h": lambda p: calls.append("job2")}) as eng2:
        eng2.dispatcher.run_once("ns")
        assert calls == ["job2"]
        assert eng2.get_task(t.id).status is TaskStatus.COMPLETED
        log.info("task_id=%s offset=- 判定依据=崩溃时 RUNNING 任务重启后重投完成，不丢失",
                 t.id)


def test_retry_budget_survives_restart(tmp_path):
    attempts = {"n": 0}

    def fail(payload):
        attempts["n"] += 1
        raise RetryableTaskError("still failing")

    cfg = make_cfg(tmp_path)
    with TaskQueueEngine(cfg, handlers={"h": fail}) as eng:
        t = eng.submit("ns", {"args": ["job3"], "metadata": {"handler": "h"}})
        eng.dispatcher.run_once("ns")  # attempt 1 -> RETRY_WAIT
        assert eng.get_task(t.id).attempt == 1

    # 重启：用可控时钟逐步推进让退避到期，继续消耗剩余 4 次预算后进死信
    clock2 = MovableClock()
    with TaskQueueEngine(make_cfg(tmp_path), time_func=clock2,
                         handlers={"h": fail}) as eng2:
        for _ in range(10):
            clock2.advance(60_000)  # 让所有退避立即到期
            eng2.dispatcher.run_once("ns")
            if eng2.get_task(t.id).status is TaskStatus.DEAD:
                break
        assert eng2.get_task(t.id).status is TaskStatus.DEAD
        assert attempts["n"] == 5
        dl = eng2.list_dead_letters("ns")
        assert dl[0].task_id == t.id and dl[0].attempts == 5
        log.info("task_id=%s offset=- attempts=5 判定依据=重试预算跨重启保留，耗尽后死信",
                 t.id)


def test_consumer_offset_resumes_after_restart(tmp_path):
    cfg = make_cfg(tmp_path)
    with TaskQueueEngine(cfg) as eng:
        for i in range(5):
            eng.publish_event("ns", "top", {"i": i})
        g = eng.create_consumer_group("ns", "top", "g", lambda p: None)
        g.process_batch(3)
        assert g.committed_offset == 3

    cfg2 = make_cfg(tmp_path)
    with TaskQueueEngine(cfg2) as eng2:
        g2 = eng2.create_consumer_group("ns", "top", "g", lambda p: None)
        assert g2.committed_offset == 3  # 位点持久化
        g2.process_batch(10)
        assert g2.committed_offset == 5
        log.info("task_id=- offset=4 判定依据=消费者位点跨重启从 3 续跑到 5")


def test_dedup_window_survives_restart(tmp_path):
    cfg = make_cfg(tmp_path)
    with TaskQueueEngine(cfg, handlers={"h": lambda p: None}) as eng:
        t1 = eng.submit("ns", {"args": ["x"], "metadata": {"handler": "h"}},
                        dedup_key="persist-key")
        eng.dispatcher.run_once("ns")

    # 重启时钟仅前移 1s（默认窗口 60s）：同 key 重复提交必须仍被拦截
    clock3 = MovableClock()
    clock3.advance(1000)
    with TaskQueueEngine(make_cfg(tmp_path), time_func=clock3,
                         handlers={"h": lambda p: None}) as eng2:
        t2 = eng2.submit("ns", {"args": ["x"], "metadata": {"handler": "h"}},
                         dedup_key="persist-key")
        assert t2.id == t1.id
        assert eng2.metrics()["dedup_hits"] == 1
        log.info("task_id=%s offset=- 判定依据=去重窗口持久化，重启后窗口内仍拦截", t1.id)
