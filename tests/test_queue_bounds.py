"""有界队列测试：命名空间隔离、超限拒绝/delay 策略、内存上限。"""

import logging
import threading

import pytest

from taskqueue.config import QueueConfig
from taskqueue.errors import ErrorCategory, ResourceExhaustedError
from taskqueue.models import TaskRecord, TaskStatus
from taskqueue.queue_engine import BoundedQueueGate
from taskqueue.storage import Storage

log = logging.getLogger("test.queue")


def make_task(tid, ns="ns", queue="default", payload=None):
    return TaskRecord(
        id=tid, namespace=ns, queue=queue,
        payload=payload if payload is not None else {"v": tid},
        status=TaskStatus.PENDING, created_at_ms=1, updated_at_ms=1, run_at_ms=0,
    )


def test_reject_when_max_pending_exceeded(tmp_path):
    with Storage(str(tmp_path / "q.db")) as st:
        gate = BoundedQueueGate(st, QueueConfig(max_pending=2, overflow_policy="reject"))
        gate.submit(make_task("a"))
        gate.submit(make_task("b"))
        with pytest.raises(ResourceExhaustedError) as ei:
            gate.submit(make_task("c"))
        assert ei.value.category is ErrorCategory.RESOURCE_EXHAUSTED
        log.info("task_id=c offset=- 判定依据=队列满拒绝 category=%s", ei.value.category.value)
        # 取走一个后又可以提交
        st.fetch_due_tasks("ns", now=1, limit=10, worker_id="w", lease_ms=1000)
        gate.submit(make_task("d"))
        assert st.count_pending("ns") == 1


def test_namespaces_are_isolated(tmp_path):
    with Storage(str(tmp_path / "q.db")) as st:
        gate = BoundedQueueGate(st, QueueConfig(max_pending=1, overflow_policy="reject"))
        gate.submit(make_task("a", ns="ns1"))
        gate.submit(make_task("b", ns="ns2"))  # 另一命名空间互不影响
        with pytest.raises(ResourceExhaustedError):
            gate.submit(make_task("c", ns="ns1"))
        pending = {ns: gate.usage(ns)[0] for ns in ("ns1", "ns2")}
        log.info("task_id=- offset=- 判定依据=命名空间隔离 pending=%s", pending)
        assert pending == {"ns1": 1, "ns2": 1}


def test_backlog_bytes_limit(tmp_path):
    with Storage(str(tmp_path / "q.db")) as st:
        gate = BoundedQueueGate(
            st, QueueConfig(max_pending=1000, max_backlog_bytes=50, overflow_policy="reject"))
        gate.submit(make_task("a", payload={"data": "x" * 30}))
        with pytest.raises(ResourceExhaustedError):
            gate.submit(make_task("b", payload={"data": "y" * 30}))
        log.info("task_id=b offset=- 判定依据=积压字节超限拒绝 usage=%s", gate.usage("ns"))


def test_delay_policy_blocks_until_space(tmp_path):
    with Storage(str(tmp_path / "q.db")) as st:
        gate = BoundedQueueGate(st, QueueConfig(max_pending=1, overflow_policy="delay"))
        gate.submit(make_task("a"))
        result = {}

        def delayed_submit():
            # 应阻塞，直到 a 被取走
            status, tid = gate.submit(make_task("b"))
            result["status"] = status
            result["tid"] = tid

        th = threading.Thread(target=delayed_submit)
        th.start()
        th.join(timeout=0.3)
        assert th.is_alive(), "delay 策略应当阻塞等待空位"
        st.fetch_due_tasks("ns", now=1, limit=10, worker_id="w", lease_ms=1000)
        gate.notify_space()
        th.join(timeout=2.0)
        assert not th.is_alive()
        assert result["status"] == "inserted"
        log.info("task_id=%s offset=- 判定依据=delay 策略空位出现后放行", result["tid"])


def test_delay_policy_unblocks_on_close(tmp_path):
    with Storage(str(tmp_path / "q.db")) as st:
        gate = BoundedQueueGate(st, QueueConfig(max_pending=1, overflow_policy="delay"))
        gate.submit(make_task("a"))
        err = {}

        def delayed_submit():
            try:
                gate.submit(make_task("b"))
            except ResourceExhaustedError as e:
                err["e"] = e

        th = threading.Thread(target=delayed_submit)
        th.start()
        th.join(timeout=0.3)
        gate.close()
        th.join(timeout=2.0)
        assert isinstance(err.get("e"), ResourceExhaustedError)
        log.info("task_id=b offset=- 判定依据=关闭后 delay 等待被明确拒绝")
