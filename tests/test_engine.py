"""引擎：并发提交与执行、去重、重试退避、死信、背压、重启恢复。"""

import logging
import threading
import time

import pytest

from taskqueue.engine import (EngineConfig, FatalError, RetryableError,
                              TaskEngine)
from taskqueue.errors import ResourceExhaustedError, ValidationError
from taskqueue.models import TaskStatus
from taskqueue.queue import OverflowPolicy

log = logging.getLogger("test.engine")


def make_engine(tmp_path, **kw):
    cfg = EngineConfig(poll_interval_seconds=0.005,
                       backoff_base_seconds=0.01, **kw)
    return TaskEngine(cfg, db_path=str(tmp_path / "q.db"))


def wait_status(engine, task_id, status, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = engine.get_task(task_id)
        if task is not None and task.status == status:
            return task
        time.sleep(0.01)
    raise AssertionError(
        f"task {task_id} did not reach {status}: "
        f"{engine.get_task(task_id).status}")


def test_concurrent_submit_and_execute(tmp_path):
    eng = make_engine(tmp_path, workers=8)
    done, lock = [], threading.Lock()

    def handler(args):
        with lock:
            done.append(args["n"])

    eng.register_handler("add", handler)
    eng.start()
    try:
        ids = []

        def submit_range(start):
            for i in range(start, start + 25):
                ids.append(eng.submit("ns", {"kind": "add",
                                             "args": {"n": i}})[0])

        threads = [threading.Thread(target=submit_range, args=(s,))
                   for s in range(0, 200, 25)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(set(ids)) == 200
        deadline = time.time() + 15
        while len(done) < 200 and time.time() < deadline:
            time.sleep(0.02)
        log.info("executed=%d unique=%d (判定依据: 每任务恰好执行一次)",
                 len(done), len(set(done)))
        assert len(done) == 200 and len(set(done)) == 200
        snap = eng.metrics.snapshot()
        log.info("metrics: %s", snap)
        assert snap["counters"]["succeeded"] == 200
    finally:
        eng.close()


def test_concurrent_dedup_same_key(tmp_path):
    eng = make_engine(tmp_path)
    eng.register_handler("noop", lambda a: None)
    eng.start()
    try:
        results = []

        def submit():
            results.append(eng.submit("ns", {"kind": "noop", "args": {}},
                                      dedup_key="same-key"))

        threads = [threading.Thread(target=submit) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        created = [r for r in results if r[1]]
        task_ids = {r[0] for r in results}
        log.info("dedup: created=%d distinct_ids=%d (判定依据: 并发下同键"
                 " 仅一条任务)", len(created), len(task_ids))
        assert len(created) == 1 and len(task_ids) == 1
    finally:
        eng.close()


def test_retry_backoff_then_dead(tmp_path):
    eng = make_engine(tmp_path, max_retries=2)
    attempts = []

    def always_fail(args):
        attempts.append(time.monotonic())
        raise RetryableError("transient")

    eng.register_handler("flaky", always_fail)
    eng.start()
    try:
        tid, _ = eng.submit("ns", {"kind": "flaky", "args": {}})
        task = wait_status(eng, tid, TaskStatus.DEAD)
        gaps = [round(b - a, 3) for a, b in zip(attempts, attempts[1:])]
        log.info("task=%s attempts=%d gaps=%s -> DEAD (判定依据: 退避间隔"
                 " 递增且 attempts=max_retries+1)", tid, task.attempts, gaps)
        assert task.attempts == 3  # max_retries=2 -> 3 次尝试
        assert gaps[1] >= gaps[0]  # 指数退避
        dead = eng.list_dead("ns")
        assert [t.task_id for t in dead] == [tid]
    finally:
        eng.close()


def test_fatal_goes_dead_immediately(tmp_path):
    eng = make_engine(tmp_path, max_retries=5)
    eng.register_handler("bad", lambda a: (_ for _ in ()).throw(
        FatalError("invalid")))
    eng.start()
    try:
        tid, _ = eng.submit("ns", {"kind": "bad", "args": {}})
        task = wait_status(eng, tid, TaskStatus.DEAD)
        log.info("task=%s attempts=%d -> DEAD (判定依据: 不可重试错误"
                 " 直接死信)", tid, task.attempts)
        assert task.attempts == 1
    finally:
        eng.close()


def test_backpressure_reject(tmp_path):
    eng = make_engine(tmp_path, max_queue_size=4, workers=1)
    gate = threading.Event()
    eng.register_handler("slow", lambda a: gate.wait(5))
    eng.start()
    try:
        for _ in range(4):
            eng.submit("ns", {"kind": "slow", "args": {}})
        with pytest.raises(ResourceExhaustedError) as ei:
            eng.submit("ns", {"kind": "slow", "args": {}})
        log.info("backpressure: %s (category=%s)", ei.value.message,
                 ei.value.category.value)
        # 命名空间隔离：别的命名空间不受影响
        eng.submit("other", {"kind": "slow", "args": {}})
    finally:
        gate.set()
        eng.close()


def test_backpressure_defer(tmp_path):
    eng = make_engine(tmp_path, max_queue_size=1, workers=1,
                      overflow_policy=OverflowPolicy.DEFER,
                      defer_timeout_seconds=5.0)
    gate = threading.Event()
    eng.register_handler("slow", lambda a: gate.wait(5))
    eng.start()
    try:
        eng.submit("ns", {"kind": "slow", "args": {}})  # 占满唯一额度
        admitted = []

        def deferred_submit():
            admitted.append(eng.submit("ns", {"kind": "slow", "args": {}}))

        t = threading.Thread(target=deferred_submit)
        t.start()
        time.sleep(0.2)
        assert not admitted  # 无额度时被延迟而非拒绝
        gate.set()           # 第一个任务完成，释放额度
        t.join(timeout=10)
        assert not t.is_alive()
        tid, created = admitted[0]
        assert created
        log.info("defer: task=%s admitted after capacity freed "
                 "(判定依据: DEFER 策略等待额度而非立即拒绝)", tid)
    finally:
        gate.set()
        eng.close()


def test_restart_recovery(tmp_path):
    db = str(tmp_path / "q.db")
    cfg = EngineConfig(poll_interval_seconds=0.005, workers=1)
    ran = []

    eng1 = TaskEngine(cfg, db_path=db)
    eng1.register_handler("rec", lambda a: ran.append(a["n"]))
    eng1.start()
    tid1, _ = eng1.submit("ns", {"kind": "rec", "args": {"n": 1}})
    wait_status(eng1, tid1, TaskStatus.SUCCEEDED)
    # 提交一批后立刻停止（不排空），模拟故障重启
    pending_ids = [eng1.submit("ns", {"kind": "rec", "args": {"n": i}})[0]
                   for i in range(2, 6)]
    eng1.stop(drain=False)
    eng1.store.close()

    eng2 = TaskEngine(cfg, db_path=db)  # 重启续跑
    eng2.register_handler("rec", lambda a: ran.append(a["n"]))
    eng2.start()
    try:
        for tid in pending_ids:
            wait_status(eng2, tid, TaskStatus.SUCCEEDED)
        log.info("restart: ran=%s (判定依据: 已完成不重复, 未完成不丢)", ran)
        assert ran.count(1) == 1            # 重启前已完成的不重复执行
        assert sorted(ran) == [1, 2, 3, 4, 5]  # 未完成的全部续跑
    finally:
        eng2.close()


def test_validation_error_normalized(tmp_path):
    eng = make_engine(tmp_path)
    with pytest.raises(ValidationError) as ei:
        eng.submit("ns", {"args": {}})  # 缺 kind
    assert ei.value.category.value == "validation"
    log.info("validation error: %s", ei.value.to_dict())
    with pytest.raises(ValidationError):
        eng.submit("", {"kind": "x"})


def test_cancel_terminal_rejected(tmp_path):
    from taskqueue.errors import ConflictError
    eng = make_engine(tmp_path)
    eng.register_handler("noop", lambda a: None)
    eng.start()
    try:
        tid, _ = eng.submit("ns", {"kind": "noop", "args": {}})
        wait_status(eng, tid, TaskStatus.SUCCEEDED)
        with pytest.raises(ConflictError) as ei:
            eng.cancel(tid)
        log.info("cancel terminal rejected: %s", ei.value.message)
    finally:
        eng.close()


def test_dedup_hit_when_backlog_full(tmp_path):
    """积压打满时，有效期内相同去重键直接返回已有任务，不报资源不足。"""
    eng = make_engine(tmp_path, max_queue_size=3, workers=1)
    gate = threading.Event()
    eng.register_handler("slow", lambda a: gate.wait(10))
    eng.start()
    try:
        tid1, created1 = eng.submit("ns", {"kind": "slow", "args": {"n": 1}},
                                   dedup_key="stable-key")
        _, created2 = eng.submit("ns", {"kind": "slow", "args": {"n": 2}},
                                dedup_key="k2")
        _, created3 = eng.submit("ns", {"kind": "slow", "args": {"n": 3}},
                                dedup_key="k3")
        assert all((created1, created2, created3))
        # 等待 3 笔都被认领执行（在途积压占满额度）
        deadline = time.time() + 5
        while eng.store.pending_count("ns") < 3 and time.time() < deadline:
            time.sleep(0.01)
        assert eng.store.pending_count("ns") == 3

        # 同键（即使 payload 不同）：窗口内命中，直接返回原任务
        again, created = eng.submit("ns",
                                    {"kind": "slow", "args": {"n": 999}},
                                    dedup_key="stable-key")
        log.info("backlog full, valid dedup key -> task=%s created=%s"
                 " (判定依据: 去重判定先于容量判定, 返回已有任务)",
                 again, created)
        assert again == tid1 and created is False

        # 全新去重键：容量确实不足，仍然报资源不足
        with pytest.raises(ResourceExhaustedError) as ei:
            eng.submit("ns", {"kind": "slow", "args": {}}, dedup_key="new")
        log.info("backlog full, new key -> %s (判定依据: 未命中去重才走"
                 " 容量拒绝)", ei.value.message)

        # 积压释放后，同键仍返回同一任务（窗口内）
        gate.set()
        deadline = time.time() + 10
        while eng.store.pending_count("ns") and time.time() < deadline:
            time.sleep(0.02)
        tid_last, created_last = eng.submit(
            "ns", {"kind": "slow", "args": {}}, dedup_key="stable-key")
        assert tid_last == tid1 and created_last is False
        log.info("after drain, same key still maps to task=%s"
                 " (判定依据: 去重窗口内任务身份稳定)", tid_last)
    finally:
        gate.set()
        eng.close()
