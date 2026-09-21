"""消费者组位点测试：原子提交、重复/回退/越位拒绝、租约重投、组隔离。"""

import logging

import pytest

from taskqueue.config import EngineConfig
from taskqueue.consumer import ConsumerGroup
from taskqueue.errors import ErrorCategory, PreconditionFailedError
from taskqueue.storage import Storage

log = logging.getLogger("test.consumer")


def base_cfg():
    return EngineConfig(visibility_timeout_ms=10_000, worker_count=1)


def publish(st, n, ns="ns", topic="top"):
    for i in range(n):
        st.append_event(ns, topic, {"i": i}, now=100)


def mk_consumer(st, group, handler, consumer_id=None, cfg=None, ns="ns", topic="top"):
    return ConsumerGroup(st, cfg or base_cfg(), ns, topic, group, handler,
                         consumer_id=consumer_id)


def test_happy_path_offsets(tmp_path):
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 3)
        seen = []
        c = mk_consumer(st, "g1", seen.append, consumer_id="c1")
        assert c.process_batch(10) == 3
        assert c.committed_offset == 3
        assert [p["i"] for p in seen] == [0, 1, 2]
        log.info("task_id=- offset=2 consumer=c1 判定依据=3 条顺序处理位点推进到 3")


def test_duplicate_commit_is_idempotent(tmp_path):
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 2)
        c = mk_consumer(st, "g1", lambda p: None, consumer_id="c1")
        c.process_batch(1)
        assert c.committed_offset == 1
        assert c.commit(0) is True  # 重复提交幂等
        assert c.committed_offset == 1
        log.info("task_id=- offset=0 consumer=c1 判定依据=重复提交幂等不改变位点")


def test_gap_commit_rejected(tmp_path):
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 3)
        c = mk_consumer(st, "g1", lambda p: None, consumer_id="c1")
        c.poll(10)  # 仅认领，不处理
        with pytest.raises(PreconditionFailedError) as ei:
            c.commit(2)  # 跳过 0、1
        assert ei.value.category is ErrorCategory.PRECONDITION_FAILED
        assert c.committed_offset == 0
        log.info("task_id=- offset=2 consumer=c1 判定依据=越位提交拒绝，位点保持 0")


def test_regression_commit_rejected(tmp_path):
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 2)
        c = mk_consumer(st, "g1", lambda p: None, consumer_id="c1")
        c.process_batch(2)
        with pytest.raises(PreconditionFailedError):
            c.commit(-1)
        log.info("task_id=- offset=-1 consumer=c1 判定依据=负位点回退拒绝")


def test_consumer_without_lease_cannot_commit(tmp_path):
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 1)
        c1 = mk_consumer(st, "g1", lambda p: None, consumer_id="c1")
        c2 = mk_consumer(st, "g1", lambda p: None, consumer_id="c2")
        c1.poll(10)
        with pytest.raises(PreconditionFailedError) as ei:
            c2.commit(0)
        assert "not_leased" in ei.value.message
        log.info("task_id=- offset=0 consumer=c2 判定依据=非持租约者提交拒绝")


def test_lease_expiry_allows_redelivery_but_not_double_commit(tmp_path):
    cfg = EngineConfig(visibility_timeout_ms=50, worker_count=1)
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 1)
        dead = mk_consumer(st, "g1", lambda p: None, consumer_id="dead", cfg=cfg)
        # 用 50ms 租约显式认领（到期点 150）
        st.fetch_events_for_group("ns", "top", "g1", "dead", 100, 10, 50)
        # 租约 150 过期；先执行恢复回收，再由 c1 重新认领并提交
        assert st.expire_group_leases(now=150, lease_ms=50) == 1
        c1 = mk_consumer(st, "g1", lambda p: None, consumer_id="c1", cfg=cfg)
        evs = c1.poll(10)
        assert [e.offset for e in evs] == [0]
        assert c1.commit(0) is True
        # 位点已前进到 1：原消费者的迟到提交最多被识别为重复（幂等），
        # 不可能把位点改回去
        assert dead.commit(0) is True
        assert st.committed_offset("ns", "top", "g1") == 1
        log.info("task_id=- offset=0 判定依据=崩溃消费者迟到提交不改变位点")


def test_expired_old_lease_commit_rejected_before_progress(tmp_path):
    cfg = EngineConfig(visibility_timeout_ms=50, worker_count=1)
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 1)
        st.fetch_events_for_group("ns", "top", "g1", "dead", 100, 10, 50)
        st.expire_group_leases(now=150, lease_ms=50)
        c1 = mk_consumer(st, "g1", lambda p: None, consumer_id="c1", cfg=cfg)
        c1.poll(10)
        with pytest.raises(PreconditionFailedError) as ei:
            mk_consumer(st, "g1", lambda p: None, consumer_id="dead",
                        cfg=cfg).commit(0)
        assert "not_leased" in ei.value.message
        assert st.committed_offset("ns", "top", "g1") == 0
        log.info("task_id=- offset=0 判定依据=位点未前进时过期租约提交被 not_leased 拒绝")


def test_groups_are_isolated(tmp_path):
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 2)
        ga = mk_consumer(st, "gA", lambda p: None, consumer_id="a")
        gb = mk_consumer(st, "gB", lambda p: None, consumer_id="b")
        ga.process_batch(10)
        assert ga.committed_offset == 2
        assert gb.committed_offset == 0
        gb.process_batch(10)
        assert gb.committed_offset == 2
        log.info("task_id=- offset=- 判定依据=不同消费组独立各自消费全量")


def test_handler_failure_prevents_offset_advance(tmp_path):
    with Storage(str(tmp_path / "c.db")) as st:
        publish(st, 2)

        def fail_first(payload):
            if payload["i"] == 0:
                raise RuntimeError("processing failed")

        c = mk_consumer(st, "g1", fail_first, consumer_id="c1")
        with pytest.raises(RuntimeError):
            c.process_batch(10)
        assert c.committed_offset == 0
        log.info("task_id=- offset=0 判定依据=处理失败位点不前进，事件不丢失")
