"""消费者组：组内并行消费、位点连续提交、失败/崩溃重投、在途上限。"""

import logging
import threading
import time

import pytest

from taskqueue.consumer import ConsumerGroup
from taskqueue.errors import ConflictError
from taskqueue.store import Store

log = logging.getLogger("test.consumer")


@pytest.fixture()
def store(tmp_path):
    s = Store(str(tmp_path / "q.db"))
    yield s
    s.close()


def test_parallel_consumers_same_group_no_duplicate(store):
    now = time.time()
    for i in range(100):
        store.append_event("tp", {"n": i}, now)
    seen, lock = [], threading.Lock()
    stop = threading.Event()

    def make_handler(tag):
        def handler(p):
            with lock:
                seen.append((tag, p["n"]))
        return handler

    consumers = [ConsumerGroup(store, "g", "tp", make_handler(f"c{i}"))
                 for i in range(4)]

    def loop(c):
        while not stop.is_set():
            c.poll_once(8)
            time.sleep(0.005)

    threads = [threading.Thread(target=loop, args=(c,)) for c in consumers]
    for t in threads:
        t.start()
    deadline = time.time() + 10
    while store.get_offset("g", "tp") < 100 and time.time() < deadline:
        time.sleep(0.02)
    stop.set()
    for t in threads:
        t.join()
    for c in consumers:
        c.close()
    values = sorted(n for _, n in seen)
    log.info("consumed=%d distinct=%d offset=%d (判定依据: 组内并行"
             " 每条事件只成功处理一次)", len(values), len(set(values)),
             store.get_offset("g", "tp"))
    assert values == list(range(100))
    assert store.get_offset("g", "tp") == 100


def test_offset_commit_atomic_and_reject(store):
    g = ConsumerGroup(store, "g", "tp", lambda p: None)
    g.commit(5)
    with pytest.raises(ConflictError) as e1:
        g.commit(5)  # 重复提交
    with pytest.raises(ConflictError) as e2:
        g.commit(2)  # 回退
    log.info("offset g/tp=5; dup commit rejected: %s; regression"
             " rejected: %s", e1.value.message, e2.value.message)
    g.commit(6)
    assert g.committed_offset == 6


def test_failure_keeps_offset_and_redelivers(store):
    now = time.time()
    for i in range(3):
        store.append_event("tp", {"n": i}, now)
    seen, calls = [], {"n": 0}

    def flaky(p):
        calls["n"] += 1
        if p["n"] == 1 and calls["n"] <= 2:
            raise RuntimeError("transient")
        seen.append(p["n"])

    g = ConsumerGroup(store, "g", "tp", flaky)
    # 并行语义：offset=1 失败立即放弃认领（nack），不阻塞同批其余事件；
    # offset=0 已确认 -> 位点=1，offset=2 已确认但被缺口 1 挡住，不可见。
    assert g.poll_once(10) == 2
    assert g.committed_offset == 1
    log.info("failure at offset=1: committed stays 1 (判定依据: 失败事件"
             " 放弃认领, 缺口挡住 offset=2 的完成)")
    assert g.poll_once(10) == 1  # 重投 offset=1 成功，位点连续推进到 3
    assert seen == [0, 2, 1] and g.committed_offset == 3
    log.info("redelivery: seen=%s committed=%d (判定依据: 失败事件重做"
             " 一次后位点连续, 无重复成功)", seen, g.committed_offset)


def test_restart_resumes_from_committed_offset(tmp_path):
    db = str(tmp_path / "q.db")
    now = time.time()
    s1 = Store(db)
    for i in range(10):
        s1.append_event("tp", {"n": i}, now)
    seen = []
    g1 = ConsumerGroup(s1, "g", "tp", lambda p: seen.append(p["n"]))
    assert g1.poll_once(4) == 4
    g1.close()
    s1.close()  # 模拟进程退出

    s2 = Store(db)  # 重启：从已提交位点继续，不重投、不丢
    g2 = ConsumerGroup(s2, "g", "tp", lambda p: seen.append(p["n"]))
    assert g2.committed_offset == 4
    assert g2.poll_once(100) == 6
    log.info("restart: seen=%s offset=%d (判定依据: 从已提交位点续消费)",
             seen, g2.committed_offset)
    assert seen == list(range(10))
    g2.close()
    s2.close()


def test_groups_are_independent(store):
    now = time.time()
    store.append_event("tp", {"n": 0}, now)
    a, b = [], []
    ga = ConsumerGroup(store, "ga", "tp", a.append)
    gb = ConsumerGroup(store, "gb", "tp", b.append)
    assert ga.poll_once() == 1 and gb.poll_once() == 1
    assert a == [{"n": 0}] and b == [{"n": 0}]
    log.info("groups ga/gb consume independently: offsets=%d/%d",
             ga.committed_offset, gb.committed_offset)


# ---------- 组内并行：多消费者处理大量事件 ----------

def test_multi_consumers_parallel_large_batch(store):
    n_events = 1000
    n_consumers = 6
    now = time.time()
    for i in range(n_events):
        store.append_event("tp", {"n": i}, now)

    seen: list[tuple[str, int]] = []
    seen_lock = threading.Lock()
    stop = threading.Event()

    def make_handler(tag):
        def handler(p):
            with seen_lock:
                seen.append((tag, p["n"]))
        return handler

    consumers = [
        ConsumerGroup(store, "g", "tp", make_handler(f"c{i}"),
                      owner=f"owner-{i}", lease_ttl=2.0, max_inflight=40)
        for i in range(n_consumers)]

    def loop(c):
        while not stop.is_set():
            c.poll_once(16)
            time.sleep(0.002)

    threads = [threading.Thread(target=loop, args=(c,))
               for c in consumers]
    for t in threads:
        t.start()
    deadline = time.time() + 20
    last = -1
    while time.time() < deadline:
        last = store.get_offset("g", "tp")
        if last >= n_events:
            break
        time.sleep(0.02)
    stop.set()
    for t in threads:
        t.join()
    for c in consumers:
        c.close()

    values = [n for _, n in seen]
    by_owner: dict[str, list[int]] = {}
    for tag, n in seen:
        by_owner.setdefault(tag, []).append(n)
    log.info("events=%d handled=%d distinct=%d committed_offset=%d;"
             " per-owner=%s (判定依据: 多消费者并行, 每条事件仅被一个"
             " 消费者成功处理, 位点连续到末尾)",
             n_events, len(values), len(set(values)),
             store.get_offset("g", "tp"),
             {k: len(v) for k, v in sorted(by_owner.items())})
    assert len(values) == n_events
    assert len(set(values)) == n_events          # 不重复
    assert sorted(values) == list(range(n_events))  # 不丢、不跳号
    assert store.get_offset("g", "tp") == n_events


def test_inflight_cap_bounded(store):
    for i in range(50):
        store.append_event("tp", {"n": i}, time.time())
    hold = threading.Event()
    active = {"n": 0}
    peak = {"n": 0}
    lock = threading.Lock()

    def slow_handler(_p):
        with lock:
            active["n"] += 1
            peak["n"] = max(peak["n"], active["n"])
        hold.wait(2)
        with lock:
            active["n"] -= 1

    consumers = [ConsumerGroup(store, "g", "tp", slow_handler,
                               owner=f"o{i}", lease_ttl=30.0,
                               max_inflight=10)
                 for i in range(4)]
    threads = []
    for c in consumers:
        t = threading.Thread(target=c.poll_once, args=(8,))
        t.start()
        threads.append(t)
    time.sleep(0.3)
    inflight = store.inflight_count("g", "tp", time.time())
    log.info("inflight under load=%d cap=10 peak_concurrent_handlers=%d"
             " (判定依据: 高压下在途与并发处理器均有界)",
             inflight, peak["n"])
    assert inflight <= 10
    assert peak["n"] <= 10
    hold.set()
    for t in threads:
        t.join()
    for c in consumers:
        c.close()


# ---------- 消费者中途崩溃：未确认事件租约到期后被接手重投 ----------

def test_crashed_consumer_events_redelivered(store):
    for i in range(20):
        store.append_event("tp", {"n": i}, 0.0)

    done: list[int] = []

    # 快消费者 B：只认领 0..4 后“崩溃”（不再续租、不 ack），租约很快过期
    crashed = ConsumerGroup(store, "g", "tp", lambda p: None,
                            owner="crashed", lease_ttl=0.3,
                            max_inflight=20)
    batch = store.claim_events("g", "tp", crashed.owner, 5, 0.3, 20, 0.0)
    assert [o for o, _ in batch] == [0, 1, 2, 3, 4]
    assert store.inflight_count("g", "tp", 0.1) == 5  # 租约内：在途 5 条
    assert store.inflight_count("g", "tp", 1.0) == 0  # 崩溃后租约到期
    log.info("crashed owner claimed offsets=0..4 then died without ack;"
             " inflight 5 -> 0 after ttl (判定依据: 未确认事件等租约到期"
             " 可接手)")

    survivor = ConsumerGroup(store, "g", "tp",
                             lambda p: done.append(p["n"]),
                             owner="survivor", lease_ttl=5.0,
                             max_inflight=20, clock=lambda: 10.0)
    # 租约已过期：幸存者接手全部 20 条（含 0..4 重做）
    processed = survivor.poll_once(50)
    log.info("survivor redelivered+processed=%d committed_offset=%d"
             " (判定依据: 崩溃者未确认的事件被重做, 之后位点连续)",
             processed, survivor.committed_offset)
    assert sorted(done) == list(range(20))
    assert survivor.committed_offset == 20
    # 崩溃者“复活”后的迟到确认不能改变结果
    late = store.ack_event("g", "tp", 0, "crashed", 11.0)
    assert late == (False, 20)
    survivor.close()


# ---------- 乱序完成：后完成的事件不能越过中间缺口 ----------

def test_out_of_order_commits_stay_contiguous(store):
    for i in range(5):
        store.append_event("tp", {"n": i}, 0.0)
    s = Store(":memory:")
    for i in range(5):
        s.append_event("t", {"n": i}, 0.0)
    s.claim_events("gg", "t", "g1", 5, 100.0, 10, 0.0)
    boundary_log = []
    for o in (4, 3, 0, 2, 1):  # 故意从后往前确认
        _, committed = s.ack_event("gg", "t", o, "g1", 1.0)
        boundary_log.append((o, committed))
    log.info("out-of-order ack sequence (offset -> committed boundary):"
             " %s (判定依据: 边界始终是连续前缀, 缺口补齐才推进)",
             boundary_log)
    assert boundary_log == [(4, 0), (3, 0), (0, 1), (2, 1), (1, 5)]
    assert s.get_offset("gg", "t") == 5
    s.close()


# ---------- 重投不产生重复位点提交 ----------

def test_redelivery_no_double_offset_commit(store):
    for i in range(3):
        store.append_event("tp", {"n": i}, 0.0)
    attempts = {"n": 0}

    def fail_twice(p):
        attempts["n"] += 1
        if p["n"] == 0 and attempts["n"] <= 2:
            raise RuntimeError("boom")

    g = ConsumerGroup(store, "g", "tp", fail_twice, owner="g1",
                      lease_ttl=100.0, max_inflight=10)
    g.poll_once(3)  # 0 失败(nack), 1,2 成功但被缺口挡住
    assert g.committed_offset == 0
    g.poll_once(3)  # 0 重投成功，连续推进到 3
    log.info("after redelivery committed_offset=%d handler_invocations=%d"
             " (判定依据: 同一事件重投后仅一次位点提交, 不算两次成功)",
             g.committed_offset, attempts["n"])
    assert g.committed_offset == 3
    assert attempts["n"] == 4  # 0 处理 3 次（2 失败 1 成功）+ 1,2 各 1 次
    # 再次重复确认已提交事件：幂等拒绝
    assert store.ack_event("g", "tp", 0, "g1", 1.0) == (False, 3)
    g.close()
