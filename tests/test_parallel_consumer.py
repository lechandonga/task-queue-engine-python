"""组内并行消费：多消费者并行、崩溃重投、乱序连续推进、在途上限、
队列满时重复去重键提交。"""

import logging
import threading
import time

import pytest

from taskqueue.consumer import ParallelConsumer
from taskqueue.engine import EngineConfig, TaskEngine
from taskqueue.errors import ResourceExhaustedError
from taskqueue.store import Store

log = logging.getLogger("test.parallel")


@pytest.fixture()
def store(tmp_path):
    s = Store(str(tmp_path / "q.db"))
    yield s
    s.close()


def test_parallel_group_consumes_many_events(store):
    """同一组 4 个消费者并行处理 200 条事件：不重叠、不丢、位点连续到 200。"""
    now = time.time()
    for i in range(200):
        store.append_event("tp", {"n": i}, now)
    seen, owners, lock = [], set(), threading.Lock()
    counted = {"n": 0}
    stop = threading.Event()

    def make_handler(tag):
        def handler(p):
            with lock:
                seen.append(p["n"])
                owners.add(tag)
        return handler

    consumers = [ParallelConsumer(store, "g", "tp", make_handler(f"c{i}"),
                                  max_inflight=32)
                 for i in range(4)]

    def loop(c):
        while not stop.is_set():
            n = c.poll_once(8)
            with lock:
                counted["n"] += n
            if n == 0:
                time.sleep(0.005)

    threads = [threading.Thread(target=loop, args=(c,)) for c in consumers]
    for t in threads:
        t.start()
    deadline = time.time() + 15
    while store.get_offset("g", "tp") < 200 and time.time() < deadline:
        time.sleep(0.02)
    stop.set()
    for t in threads:
        t.join()
    for c in consumers:
        c.close()
    log.info("parallel: events=0..199 consumed=%d distinct=%d acked=%d"
             " owners=%s committed=%d (判定依据: 每条事件只被成功确认一次,"
             " 位点连续推进到最后一条之后)",
             len(seen), len(set(seen)), counted["n"], sorted(owners),
             store.get_offset("g", "tp"))
    assert sorted(seen) == list(range(200))      # 不丢、不重
    assert counted["n"] == 200                   # 成功计数不重复
    assert len(owners) >= 2                      # 确实多实例并行
    assert store.get_offset("g", "tp") == 200    # 位点连续推进到末尾


def test_crash_consumer_events_redelivered(store):
    """消费者崩溃（认领后未确认）: TTL 过期后事件被组内他人重投。"""
    clock = [1000.0]
    for i in range(5):
        store.append_event("tp", {"n": i}, clock[0])
    seen_a, seen_b = [], []

    def flaky_a(p):
        if p["n"] == 0:
            seen_a.append(p["n"])
            return
        # 模拟硬崩溃：BaseException 不被 nack 捕获，认领原样残留
        raise KeyboardInterrupt("simulated crash")

    a = ParallelConsumer(store, "g", "tp", flaky_a, owner="a",
                         claim_ttl=1.0, clock=lambda: clock[0])
    with pytest.raises(KeyboardInterrupt):
        a.poll_once(5)  # 确认 offset=0 后崩溃，1..4 认领残留
    assert store.get_offset("g", "tp") == 1
    log.info("crash: owner=a acked offset=0 then died; committed=%d"
             " inflight=%d (判定依据: 未确认事件留在在途集合)",
             store.get_offset("g", "tp"),
             store.inflight_count("g", "tp", clock[0]))

    b = ParallelConsumer(store, "g", "tp", lambda p: seen_b.append(p["n"]),
                         owner="b", claim_ttl=1.0,
                         clock=lambda: clock[0])
    assert b.poll_once(5) == 0  # 认领未过期，他人暂时接不走
    clock[0] += 2.0             # 越过 claim_ttl，认领过期
    assert b.poll_once(5) == 4  # 重投 offset=1..4
    log.info("redeliver: owner=b took offsets=%s committed=%d"
             " (判定依据: 崩溃消费者未确认事件被重投且只成功一次)",
             seen_b, store.get_offset("g", "tp"))
    assert seen_b == [1, 2, 3, 4]
    assert seen_a + seen_b == [0, 1, 2, 3, 4]
    assert store.get_offset("g", "tp") == 5


def test_out_of_order_completion_offset_contiguous(store):
    """乱序完成：后面先确认不越过空缺，缺口补齐后位点连续推进。"""
    now = time.time()
    for i in range(3):
        store.append_event("tp", {"n": i}, now)
    gate = threading.Event()
    done_a = []

    def slow_a(p):
        gate.wait(5)  # offset=0 阻塞，强制乱序
        done_a.append(p["n"])

    a = ParallelConsumer(store, "g", "tp", slow_a, owner="a")
    b = ParallelConsumer(store, "g", "tp", lambda p: None, owner="b")
    ta = threading.Thread(target=a.poll_once, args=(1,))
    ta.start()
    deadline = time.time() + 5
    while store.inflight_count("g", "tp", time.time()) < 1 \
            and time.time() < deadline:
        time.sleep(0.01)

    assert b.poll_once(2) == 2  # offset=1,2 先完成
    assert store.get_offset("g", "tp") == 0
    log.info("out-of-order: acked offsets=[1,2] while 0 pending;"
             " committed=%d (判定依据: 位点不越过中间空缺)",
             store.get_offset("g", "tp"))

    gate.set()  # 缺口补齐
    ta.join()
    log.info("gap filled: acked offset=0 -> committed=%d"
             " (判定依据: 缺口补齐后连续推进到 3)",
             store.get_offset("g", "tp"))
    assert done_a == [0]
    assert store.get_offset("g", "tp") == 3


def test_inflight_cap_and_close_releases(store):
    """在途上限：认领有界；消费者退出释放认领后他人立即接手。"""
    now = time.time()
    for i in range(10):
        store.append_event("tp", {"n": i}, now)
    first = store.claim_events("g", "tp", "x", limit=10, ttl=60,
                               now=now, max_inflight=3)
    second = store.claim_events("g", "tp", "y", limit=10, ttl=60,
                                now=now, max_inflight=3)
    log.info("cap: first claim offsets=%s, second=%s, inflight=%d/3"
             " (判定依据: 在途数不超过 max_inflight)",
             [o for o, _ in first], [o for o, _ in second],
             store.inflight_count("g", "tp", now))
    assert [o for o, _ in first] == [0, 1, 2]
    assert second == []
    assert store.inflight_count("g", "tp", now) == 3

    released = store.release_claims("g", "tp", "x")  # x 退出
    third = store.claim_events("g", "tp", "y", limit=10, ttl=60,
                               now=now, max_inflight=3)
    log.info("close: released=%d, y re-claimed offsets=%s"
             " (判定依据: 退出释放的认领可被立即重投)",
             released, [o for o, _ in third])
    assert released == 3
    assert [o for o, _ in third] == [0, 1, 2]


def test_duplicate_ack_not_counted(store):
    """重投后的重复确认：不重复计数、不产生重复位点提交。"""
    now = time.time()
    store.append_event("tp", {"n": 0}, now)
    store.claim_events("g", "tp", "x", limit=1, ttl=60, now=now,
                       max_inflight=4)
    counted1, committed1 = store.ack_event("g", "tp", 0, now)
    counted2, committed2 = store.ack_event("g", "tp", 0, now)  # 重复确认
    log.info("dup ack: first=(%s, committed=%d) second=(%s, committed=%d)"
             " (判定依据: 重复确认不计数、位点不重复提交)",
             counted1, committed1, counted2, committed2)
    assert (counted1, committed1) == (True, 1)
    assert (counted2, committed2) == (False, 1)
    # 已提交的事件不会再被认领重投
    assert store.claim_events("g", "tp", "y", limit=1, ttl=60, now=now,
                              max_inflight=4) == []


def test_full_queue_returns_existing_task_on_dedup_hit(tmp_path):
    """命名空间积压打满时，命中有效去重键的提交返回已有任务。"""
    eng = TaskEngine(EngineConfig(max_queue_size=1),
                     db_path=str(tmp_path / "q.db"))
    try:
        id1, created1 = eng.submit("ns", {"kind": "k", "args": {"n": 1}},
                                   dedup_key="dup")
        assert created1
        with pytest.raises(ResourceExhaustedError):
            eng.submit("ns", {"kind": "k", "args": {"n": 2}})  # 队列已满
        id2, created2 = eng.submit("ns", {"kind": "k", "args": {"n": 1}},
                                   dedup_key="dup")  # 满员但命中去重键
        log.info("full-queue dedup: existing=%s resubmit=(%s, created=%s)"
                 " pending=%d (判定依据: 队列满时相同去重键返回已有任务,"
                 " 不报资源不足、不新增积压)",
                 id1, id2, created2, eng.store.pending_count("ns"))
        assert id2 == id1 and not created2
        assert eng.store.pending_count("ns") == 1
    finally:
        eng.close()
