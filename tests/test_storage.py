"""存储层一致性测试：原子认领、租约恢复、死信、位点提交判定。"""

import logging
import threading

from taskqueue.models import DeadLetter, TaskRecord, TaskStatus, now_ms
from taskqueue.storage import Storage

log = logging.getLogger("test.storage")


def make_task(tid="t1", ns="ns", queue="q", status=TaskStatus.PENDING,
              dedup_key=None, run_at=0, created=1000):
    return TaskRecord(
        id=tid, namespace=ns, queue=queue, payload={"v": tid},
        status=status, dedup_key=dedup_key, created_at_ms=created,
        updated_at_ms=created, run_at_ms=run_at,
    )


def test_atomic_claim_no_duplicate_leases(tmp_path):
    db = str(tmp_path / "s.db")
    with Storage(db) as st:
        for i in range(20):
            st.insert_task(make_task(f"t{i:02d}"))
        claimed: list[list[str]] = [[], []]

        def claim(worker_idx):
            got = st.fetch_due_tasks("ns", now=100, limit=10,
                                    worker_id=f"w{worker_idx}", lease_ms=1000)
            claimed[worker_idx] = [t.id for t in got]

        t0 = threading.Thread(target=claim, args=(0,))
        t1 = threading.Thread(target=claim, args=(1,))
        t0.start(); t1.start(); t0.join(); t1.join()
        all_ids = sorted(claimed[0] + claimed[1])
        log.info("task_id=%s offset=- 判定依据=两 worker 原子认领结果互斥", all_ids)
        assert len(all_ids) == 20
        assert set(claimed[0]).isdisjoint(claimed[1])


def test_only_lease_holder_can_complete(tmp_path):
    db = str(tmp_path / "s.db")
    with Storage(db) as st:
        st.insert_task(make_task())
        got = st.fetch_due_tasks("ns", 100, 1, "owner", 1000)
        assert len(got) == 1
        # 非持租约者不能完成
        assert st.complete_task("t1", "intruder", now=200) is False
        assert st.complete_task("t1", "owner", now=200) is True
        assert st.get_task("t1").status is TaskStatus.COMPLETED
        log.info("task_id=t1 offset=- 判定依据=仅租约持有者可完成")


def test_lease_expiry_makes_task_reclaimable_without_reset(tmp_path):
    db = str(tmp_path / "s.db")
    with Storage(db) as st:
        st.insert_task(make_task())
        st.fetch_due_tasks("ns", 100, 1, "w1", lease_ms=50)  # 150 到期
        t = st.get_task("t1")
        assert t.status is TaskStatus.RUNNING and t.attempt == 1
        # 崩溃恢复：过期后退回 RETRY_WAIT，可被重新认领，attempt 继续累加
        n = st.expire_leases(now=200, lease_ms=50)
        assert n == 1
        got = st.fetch_due_tasks("ns", 200, 1, "w2", 50)
        assert [x.id for x in got] == ["t1"]
        assert st.get_task("t1").attempt == 2
        log.info("task_id=t1 offset=- 判定依据=租约过期重投 attempt=2 不重置预算")


def test_dead_letter_moved_atomically(tmp_path):
    db = str(tmp_path / "s.db")
    with Storage(db) as st:
        st.insert_task(make_task())
        dl = st.fail_task_to_dead("t1", "non_retryable:boom", now=300)
        assert isinstance(dl, DeadLetter)
        assert st.get_task("t1").status is TaskStatus.DEAD
        rows = st.list_dead_letters("ns")
        assert len(rows) == 1 and rows[0].task_id == "t1"
        rows_q = st.list_dead_letters("ns", queue="other")
        assert rows_q == []
        log.info("task_id=t1 offset=- 判定依据=死信原子落库可检索 reason=%s", dl.reason)


def test_dedup_accept_and_expire(tmp_path):
    db = str(tmp_path / "s.db")
    with Storage(db) as st:
        t1 = make_task("a1", dedup_key="k")
        inserted, tid = st.insert_task_with_dedup(t1, window_ms=100, now=1000)
        assert inserted and tid == "a1"
        t2 = make_task("a2", dedup_key="k")
        inserted2, tid2 = st.insert_task_with_dedup(t2, window_ms=100, now=1050)
        assert inserted2 is False and tid2 == "a1"
        log.info("task_id=%s offset=- 判定依据=窗口内重复提交命中首次任务", tid2)
        # 过期后允许同 key 再次提交
        assert st.dedup_sweep(now=1100) == 1
        inserted3, tid3 = st.insert_task_with_dedup(t2, window_ms=100, now=1101)
        assert inserted3 and tid3 == "a2"
        log.info("task_id=a2 offset=- 判定依据=窗口过期后同 key 可重新提交")


def test_dedup_concurrent_single_insert(tmp_path):
    db = str(tmp_path / "s.db")
    results: list[tuple[bool, str]] = []
    lock = threading.Lock()
    with Storage(db) as st:
        def submit(i):
            t = make_task(f"c{i}", dedup_key="same")
            r = st.insert_task_with_dedup(t, window_ms=10000, now=1000)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        accepted = [r for r in results if r[0]]
        log.info("task_id=%s offset=- 判定依据=%d 并发提交仅 1 个落库",
                 [r[1] for r in accepted], len(accepted))
        assert len(accepted) == 1
        assert len({r[1] for r in results}) == 1


def test_commit_offset_decisions(tmp_path):
    db = str(tmp_path / "s.db")
    with Storage(db) as st:
        for i in range(3):
            st.append_event("ns", "top", {"i": i}, now=100)
        st.ensure_group("ns", "top", "g")
        evs = st.fetch_events_for_group("ns", "top", "g", "c1", now=100,
                                        limit=10, lease_ms=1000)
        assert [e.offset for e in evs] == [0, 1, 2]

        # 越位提交被拒
        assert st.commit_offset("ns", "top", "g", 5, "c1", 100) == "gap"
        # 非持租约者提交被拒
        assert st.commit_offset("ns", "top", "g", 0, "c2", 100) == "not_leased"
        # 正常顺序提交
        assert st.commit_offset("ns", "top", "g", 0, "c1", 100) == "committed"
        assert st.committed_offset("ns", "top", "g") == 1
        # 重复提交幂等
        assert st.commit_offset("ns", "top", "g", 0, "c1", 100) == "duplicate"
        # 回退到更旧位点仍是重复（幂等），负数才是 regression
        assert st.commit_offset("ns", "top", "g", -1, "c1", 100) == "regression"
        # 持租约者身份随 fetch 更新：c1 继续提交 1
        assert st.commit_offset("ns", "top", "g", 1, "c1", 100) == "committed"
        log.info("task_id=- offset=1 判定依据=顺序提交/重复/越位/非租约者全部正确区分")


def test_event_lease_redelivery_after_expiry(tmp_path):
    db = str(tmp_path / "s.db")
    with Storage(db) as st:
        st.append_event("ns", "top", {"x": 1}, now=100)
        st.fetch_events_for_group("ns", "top", "g", "dead-consumer", now=100,
                                  limit=10, lease_ms=50)  # 150 到期
        # c1 在租约期内拿不到（now=149 < 150）
        assert st.fetch_events_for_group("ns", "top", "g", "c1", 149, 10, 1000) == []
        # 到期点（now=150）之后可被 c1 重新认领，attempts 累加
        evs = st.fetch_events_for_group("ns", "top", "g", "c1", 150, 10, 1000)
        assert [e.offset for e in evs] == [0]
        assert st.commit_offset("ns", "top", "g", 0, "c1", 150) == "committed"
        # 已提交的事件不再投递
        assert st.fetch_events_for_group("ns", "top", "g", "c1", 150, 10, 1000) == []
        log.info("task_id=- offset=0 判定依据=租约过期重投且提交后不再投递")
