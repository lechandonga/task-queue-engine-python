"""错误分类端到端：参数错误、资源不足、内部异常在引擎/HTTP 边界可区分。"""

import logging

import pytest

from taskqueue.config import EngineConfig, QueueConfig
from taskqueue.engine import TaskQueueEngine
from taskqueue.errors import (
    ErrorCategory,
    InvalidArgumentError,
    ResourceExhaustedError,
    TaskQueueError,
)

log = logging.getLogger("test.error_class")


def test_three_categories_are_distinguishable(tmp_path):
    eng = TaskQueueEngine(
        EngineConfig(db_path=str(tmp_path / "e.db"), worker_count=0,
                     queue=QueueConfig(max_pending=1)),
        handlers={"h": lambda p: None})
    with eng:
        # 1) 参数错误
        with pytest.raises(TaskQueueError) as ei1:
            eng.submit("", {"metadata": {"handler": "h"}})
        assert ei1.value.category is ErrorCategory.INVALID_ARGUMENT

        with pytest.raises(TaskQueueError) as ei2:
            eng.submit("ns", {"args": 123})
        assert ei2.value.category is ErrorCategory.INVALID_ARGUMENT
        log.info("task_id=- offset=- 判定依据=空namespace/类型不符 -> invalid_argument")

        # 2) 资源不足
        eng.submit("ns", {"metadata": {"handler": "h"}})
        with pytest.raises(ResourceExhaustedError) as ei3:
            eng.submit("ns", {"metadata": {"handler": "h"}})
        assert ei3.value.category is ErrorCategory.RESOURCE_EXHAUSTED
        assert eng.metrics()["rejected"] == 1
        log.info("task_id=- offset=- 判定依据=队列满 -> resource_exhausted 并计数")

        # 3) 内部异常（处理器抛出未知错误）归一化后入死信，不透出底层类
        eng2 = TaskQueueEngine(
            EngineConfig(db_path=str(tmp_path / "e2.db"), worker_count=0),
            handlers={"boom": lambda p: (_ for _ in ()).throw(RuntimeError("x"))})
        with eng2:
            t = eng2.submit("ns", {"metadata": {"handler": "boom"}})
            eng2.dispatcher.run_once("ns")
            dl = eng2.list_dead_letters("ns")[0]
            assert dl.reason.startswith("internal:")
            assert "RuntimeError" in dl.reason  # 诊断信息保留，但类别是 internal
            log.info("task_id=%s offset=- 判定依据=未知异常归一 internal 入死信",
                     t.id)
