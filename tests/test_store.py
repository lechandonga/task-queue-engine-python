"""存储层：去重窗口、原子认领、位点单调性、重启恢复。"""

import logging

import pytest

from taskqueue.errors import ConflictError
from taskqueue.models import Task, TaskStatus
from taskqueue.store import Store

log = logging.getLogger("test.store")


@pytest.fixture()
def store(tmp_path):
    s = Store(str(tmp_path / "q.db"))
    yield s
    s.close()


def _task(tid, ns="ns", dedup=None):
    return Task(task_id=tid, namespace=ns,
                payload={"kind": "k", "args": {}, "priority": 0},
                dedup_key=dedup)


def test_dedup_window_deterministic(store):
    t0 = 1000.0
    _, created = store.insert_task(_task("a", dedup="k1"), 3, t0, 60.0)
    assert created
    # 窗口内：重复提交返回已有任务，不产生新记录
    existing, created = store.insert_task(_task("b", dedup="k1"), 3,
                                          t0 + 59.9, 60.0)
    assert not created and existing.task_id == "a"
    log.info("dedup key=k1 within window -> existing task=a (判定依据:"
             " expires_at=%.1f > now=%.1f)", t0 + 60, t0 + 59.9)
    # 窗口过期：允许重新提交
    fresh, created = store.insert_task(_task("c", dedup="k1"), 3,
                                       t0 + 60.1, 60.0)
    assert created and fresh.task_id == "c"
    log.info("dedup key=k1 expired -> new task=c created")


def test_claim_is_atomic_no_double_claim(store):
    for i in range(5):
        store.insert_task(_task(f"t{i}"), 3, 0.0, 0.0)
    first = store.claim_due(1.0, 3)
    second = store.claim_due(1.0, 3)
    ids = [t.task_id for t in first] + [t.task_id for t in second]
    log.info("claimed ids=%s (判定依据: 两次认领不得有交集)", ids)
    assert len(ids) == len(set(ids)) == 5
    assert store.claim_due(1.0, 3) == []


def test_offset_monotonic(store):
    store.commit_offset("g", "tp", 3, 0.0)
    with pytest.raises(ConflictError):
        store.commit_offset("g", "tp", 3, 0.0)   # 重复提交
    with pytest.raises(ConflictError):
        store.commit_offset("g", "tp", 2, 0.0)   # 回退
    store.commit_offset("g", "tp", 4, 0.0)
    assert store.get_offset("g", "tp") == 4
    log.info("offset g/tp=4 committed; dup/regression rejected")


def test_restart_recovers_running(tmp_path):
    db = str(tmp_path / "q.db")
    s1 = Store(db)
    s1.insert_task(_task("t1"), 3, 0.0, 0.0)
    s1.insert_task(_task("t2"), 3, 0.0, 0.0)
    claimed = s1.claim_due(1.0, 2)
    s1.mark_succeeded("t1", 2.0)
    s1.close()  # 模拟进程退出：t2 停留在 RUNNING

    s2 = Store(db)  # 重启
    recovered = s2.recover(3.0)
    t1, t2 = s2.get_task("t1"), s2.get_task("t2")
    log.info("after restart: t1=%s t2=%s recovered=%d (判定依据:"
             " 已完成不回退, 未完成重置为 PENDING)",
             t1.status.value, t2.status.value, recovered)
    assert t1.status == TaskStatus.SUCCEEDED
    assert t2.status == TaskStatus.PENDING and recovered == 1
    # 位点与事件日志同样持久
    s2.append_event("tp", {"n": 1}, 4.0)
    s2.commit_offset("g", "tp", 1, 4.0)
    s2.close()
    s3 = Store(db)
    assert s3.get_offset("g", "tp") == 1
    assert s3.read_events("tp", 0, 10) == [(0, {"n": 1})]
    s3.close()
