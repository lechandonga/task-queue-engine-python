"""对外门面：组合存储、去重、队列准入、调度器、消费者组，并负责重启恢复。

恢复语义：
- 所有任务状态与位点在提交时即落 SQLite（WAL + fsync）；
- 启动时：RUNNING 但租约已失效的任务按其 attempt 重新排队（不丢任务、不重置计数）；
  COMPLETED/DEAD 状态不变（已生效副作用不重复）；
  消费者组未过期的租约保留，过期的重新可认领。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from taskqueue.config import EngineConfig
from taskqueue.consumer import ConsumerGroup, EventHandler
from taskqueue.dispatcher import Dispatcher, TaskHandler
from taskqueue.models import DeadLetter, TaskRecord
from taskqueue.queue_engine import BoundedQueueGate
from taskqueue.storage import Storage
from taskqueue.metrics import MetricsRegistry

# 处理器表：{handler_name: handler}，任务通过 payload.metadata["handler"] 路由


class TaskQueueEngine:
    def __init__(self, cfg: EngineConfig | None = None,
                 handlers: dict[str, TaskHandler] | None = None,
                 logger: Any | None = None) -> None:
        raise NotImplementedError

    # ---- 生命周期 ------------------------------------------------------
    def start(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def __enter__(self) -> TaskQueueEngine:
        self.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---- 任务 ----------------------------------------------------------
    def submit(self, namespace: str, payload: dict[str, Any], *,
               queue: str = "default", dedup_key: str | None = None,
               delay_ms: int = 0) -> TaskRecord:
        raise NotImplementedError

    def get_task(self, task_id: str) -> TaskRecord:
        raise NotImplementedError

    def list_dead_letters(self, namespace: str, queue: str | None = None,
                          limit: int = 100, offset: int = 0) -> list[DeadLetter]:
        raise NotImplementedError

    def wait_idle(self, timeout: float = 5.0) -> bool:
        """等待所有已提交任务进入终态（测试用）。"""
        raise NotImplementedError

    # ---- 事件 ----------------------------------------------------------
    def publish_event(self, namespace: str, topic: str,
                      payload: dict[str, Any]) -> int:
        raise NotImplementedError

    def create_consumer_group(self, namespace: str, topic: str, group: str,
                              handler: EventHandler) -> ConsumerGroup:
        raise NotImplementedError

    # ---- 观测 ----------------------------------------------------------
    def metrics(self) -> dict[str, int | float]:
        raise NotImplementedError
