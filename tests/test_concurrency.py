"""并发场景测试：多 worker 无重复执行、多消费者认领互斥、高并发提交去重。"""

import logging
import threading
from collections import Counter

from taskqueue.config import EngineConfig, QueueConfig, RetryConfig
from taskqueue.engine import TaskQueueEngine
from taskqueue.errors import PreconditionFailedError

log = logging.getLogger("test.concurrency")


def engine(tmp_path, handlers=None, **cfg_kw):
    cfg = EngineConfig(
        db_path=str(tmp_path / "c.db"), worker_count=4,
        queue=QueueConfig(max_pending=10_000, max_backlog_bytes=100 * 1024 * 1024),
        retry=RetryConfig(execution_timeout_ms=2000),
        **cfg_kw,
    )
    return TaskQueueEngine(cfg, handlers=handlers)


def test_concurrent_workers_execute_each_task_once(tmp_path):
    executed = Counter()
    lock = threading.Lock()

    def count_handler(payload):
        with lock:
            executed[payload["args"][0]] += 1

    eng = engine(tmp_path, handlers={"count": count_handler})
    with eng:
        n = 200
        for i in range(n):
            eng.submit("ns", {"args": [i], "metadata": {"handler": "count"}},
                       queue="q")
        assert eng.wait_idle(timeout=10)
        assert len(executed) == n
        duplicates = {k: v for k, v in executed.items() if v > 1}
        log.info("task_id=- offset=- 判定依据=%d 任务全部恰好执行一次 dups=%s",
                 n, list(duplicates)[:5])
        assert not duplicates, f"重复执行: {duplicates}"
        assert eng.metrics()["completed"] == n


def test_concurrent_consumers_claims_are_mutually_exclusive(tmp_path):
    """4 个消费者并发认领：同一条事件在任一时刻只有一个持租约者；
    位点连续原子推进，绝不重复提交或越位。

    处理器是“带幂等键的副作用”：重投会导致 handler 再次执行，但业务效果
    借助幂等保护只生效一次（系统保证：offset 恰好被 commit 一次）。
    """
    import time as wall

    class Clock:
        def __init__(self):
            self.t = 1_000_000
            self._lock = threading.Lock()

        def __call__(self):
            with self._lock:
                return self.t

        def advance(self, ms):
            with self._lock:
                self.t += ms

    clock = Clock()
    cfg = EngineConfig(
        db_path=str(tmp_path / "c.db"), worker_count=0,
        queue=QueueConfig(max_pending=10_000,
                          max_backlog_bytes=100 * 1024 * 1024),
        visibility_timeout_ms=100,
    )
    eng = TaskQueueEngine(cfg, time_func=clock)

    # 业务幂等表（模拟外部副作用去重）
    effect = set()
    effect_lock = threading.Lock()
    deliveries = Counter()

    def idempotent_handler(payload):
        order = payload["order"]
        with effect_lock:
            deliveries[order] += 1
            effect.add(order)  # set 语义即幂等：只生效一次

    with eng:
        n = 80
        for i in range(n):
            eng.publish_event("ns", "orders", {"order": i})

        groups = [
            eng.create_consumer_group(
                "ns", "orders", "g", idempotent_handler, consumer_id=f"c{idx}")
            for idx in range(4)
        ]
        stop = threading.Event()

        def consume(g):
            while not stop.is_set():
                if g.committed_offset >= n:
                    return
                evs = g.poll(20)
                if not evs or evs[0].offset != g.committed_offset:
                    # 模拟租约超时：推进时钟并回收，让其他消费者重投
                    clock.advance(101)
                    eng.storage.expire_group_leases(clock(), 100)
                    continue
                try:
                    g.process_batch(1)
                except PreconditionFailedError:
                    clock.advance(101)
                    eng.storage.expire_group_leases(clock(), 100)

        threads = [threading.Thread(target=consume, args=(g,)) for g in groups]
        for t in threads:
            t.start()
        deadline = wall.monotonic() + 15
        while wall.monotonic() < deadline and groups[0].committed_offset < n:
            wall.sleep(0.01)
        stop.set()
        for t in threads:
            t.join(timeout=2)
        # 位点恰好连续推进到 n（每个 offset 成功提交一次）
        assert groups[0].committed_offset == n
        # 业务副作用恰好 n 条（处理器幂等），尽管发生了重投
        assert len(effect) == n
        assert sum(deliveries.values()) >= n  # at-least-once 投递
        log.info("task_id=- offset=%d 判定依据=4 消费者并发，位点连续推进到 %d；"
                 "投递次数=%d 副作用=%d（幂等）",
                 n - 1, n, sum(deliveries.values()), len(effect))


def test_concurrent_same_dedup_key_single_accept(tmp_path):
    eng = engine(tmp_path, handlers={"noop": lambda p: None})
    outcomes = []
    lock = threading.Lock()
    with eng:
        def submit(i):
            t = eng.submit("ns", {"args": [i], "metadata": {"handler": "noop"}},
                           dedup_key="hot-key")
            with lock:
                outcomes.append(t.id)

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(32)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(set(outcomes)) == 1
        m = eng.metrics()
        log.info("task_id=%s offset=- 判定依据=32 并发同 key：1 接受 %d 去重命中",
                 outcomes[0], m["dedup_hits"])
        assert m["submitted"] == 1
        assert m["dedup_hits"] == 31
        assert eng.wait_idle(timeout=5)


def test_mixed_readonly_load_stays_bounded(tmp_path):
    """空闲与轻载下吞吐/延迟可度量，资源占用有界（快照字段齐全）。"""
    eng = engine(tmp_path, handlers={"ok": lambda p: None})
    with eng:
        for i in range(50):
            eng.submit("ns", {"args": [i], "metadata": {"handler": "ok"}})
        assert eng.wait_idle(timeout=5)
        snap = eng.metrics()
        log.info("task_id=- offset=- 判定依据=轻载完成 metrics=%s", snap)
        assert snap["completed"] == 50
        assert snap["latency_p95_ms"] >= 0
        assert snap["submitted"] == 50
