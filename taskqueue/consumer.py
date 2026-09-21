"""消费者组与位点管理。

模型（Kafka 风格的简化版）：
- 每个 ``(namespace, topic, group)`` 维护一个连续位点 ``committed``；
- 组内多个 consumer 共同认领 ``committed`` 之后尚未完成的事件，租约制防重复；
- ``commit(offset)`` 原子推进：接受 ``offset == committed``（幂等），
  拒绝 ``offset < committed``（回退）与 ``offset > committed + 1``（越位），
  以及从未被本组认领过的 offset（防止跳过事件直接提交）；
- 租约到期未提交的事件可被同组其他消费者重新认领（at-least-once 投递，
  处理器需保持幂等；成功处理只生效一次由处理器幂等键保证）。
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from taskqueue.config import EngineConfig
from taskqueue.models import EventRecord, now_ms
from taskqueue.storage import Storage

EventHandler = Callable[[dict[str, Any]], None]


class ConsumerGroup:
    def __init__(self, storage: Storage, cfg: EngineConfig, namespace: str,
                 topic: str, group: str, handler: EventHandler,
                 consumer_id: str | None = None, logger: Any | None = None) -> None:
        self._storage = storage
        self._cfg = cfg
        self._ns = namespace
        self._topic = topic
        self._group = group
        self._handler = handler
        self._consumer_id = consumer_id or f"c-{threading.get_ident()}-{id(self):x}"
        self._log = logger

    @property
    def consumer_id(self) -> str:
        return self._consumer_id

    @property
    def committed_offset(self) -> int:
        """下一条待处理的 offset。"""
        raise NotImplementedError

    def poll(self, limit: int = 10) -> list[EventRecord]:
        """认领一批事件（含租约内未完成的重投）。"""
        raise NotImplementedError

    def process_batch(self, limit: int = 10) -> int:
        """认领 -> 顺序处理 -> 原子提交位点；返回处理条数。"""
        raise NotImplementedError

    def commit(self, offset: int) -> bool:
        """提交位点。回退/越位抛 :class:`PreconditionFailedError`；重复提交返回 True。"""
        raise NotImplementedError
