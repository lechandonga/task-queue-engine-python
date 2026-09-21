"""消费者组：组内单活、位点原子提交、失败重投、重启后续消费。"""

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
    assert g.poll_once(10) == 1  # 处理完 offset=0 后在 offset=1 失败
    assert g.committed_offset == 1
    log.info("failure at offset=1: committed stays 1 (判定依据: 失败事件"
             " 不推进位点)")
    assert g.poll_once(10) == 2  # 重投 offset=1,2
    assert seen == [0, 1, 2] and g.committed_offset == 3


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
