"""去重窗口测试：窗口内一次执行、过期语义确定、并发单落库、重复不占额度。"""

import logging
import threading

from taskqueue.config import DedupConfig, QueueConfig
from taskqueue.models import TaskRecord, TaskStatus
from taskqueue.queue_engine import BoundedQueueGate
from taskqueue.storage import Storage

log = logging.getLogger("test.dedup")


class FakeClock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def make_task(tid, key, ns="ns", created=1000):
    return TaskRecord(
        id=tid, namespace=ns, queue="default", payload={"v": tid},
        status=TaskStatus.PENDING, dedup_key=key,
        created_at_ms=created, updated_at_ms=created, run_at_ms=created,
    )


def test_dedup_hit_returns_first_task_and_skips_queue_limit(tmp_path):
    clock = FakeClock(1000)
    with Storage(str(tmp_path / "d.db")) as st:
        gate = BoundedQueueGate(st, QueueConfig(max_pending=1), time_func=clock)
        s1, id1 = gate.submit(make_task("a1", "k"), window_ms=100)
        assert s1 == "inserted"
        # 队列已满（max_pending=1），但重复提交仍然命中，不被拒绝
        s2, id2 = gate.submit(make_task("a2", "k"), window_ms=100)
        assert s2 == "dedup_hit" and id2 == "a1"
        log.info("task_id=%s offset=- 判定依据=窗口内重复命中首次任务且不占额度", id2)
        assert st.count_pending("ns") == 1


def test_window_expiry_is_clock_deterministic(tmp_path):
    clock = FakeClock(1000)
    cfg = DedupConfig(window_ms=100)
    with Storage(str(tmp_path / "d.db")) as st:
        gate = BoundedQueueGate(st, QueueConfig(max_pending=100), time_func=clock)
        gate.submit(make_task("a1", "k"), window_ms=cfg.window_ms)
        # 1099 仍在窗口 [1000, 1100) 内
        clock.t = 1099
        s, _ = gate.submit(make_task("a2", "k"), window_ms=cfg.window_ms)
        assert s == "dedup_hit"
        # 1100 为过期边界（expires_at <= now 即过期）
        clock.t = 1100
        s, tid = gate.submit(make_task("a3", "k"), window_ms=cfg.window_ms)
        assert s == "inserted" and tid == "a3"
        log.info("task_id=a3 offset=- 判定依据=过期边界 now>=expires_at 可重新提交")


def test_concurrent_same_key_single_execution(tmp_path):
    with Storage(str(tmp_path / "d.db")) as st:
        gate = BoundedQueueGate(st, QueueConfig(max_pending=1000))
        results = []
        lock = threading.Lock()

        def submit(i):
            s, tid = gate.submit(make_task(f"t{i}", "same-key"), window_ms=10_000)
            with lock:
                results.append((s, tid))

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        inserted = [r for r in results if r[0] == "inserted"]
        assert len(inserted) == 1
        first = inserted[0][1]
        assert all(tid == first for _, tid in results)
        log.info("task_id=%s offset=- 判定依据=16 并发同 key 仅一次落库", first)
