"""消费者组与位点管理。

模型（简化 Kafka 风格）：
- 每个 ``(namespace, topic, group)`` 维护连续位点 ``committed_offset``，
  语义为“下一条待处理 offset”，初始 0；
- 组内多个 consumer 共同认领 ``committed`` 之后尚未完成的事件，租约制防重复；
- ``commit(offset)``：
  - 正常推进：恰好提交当前位点，原子推进到 offset+1；
  - ``offset < committed``：重复提交，幂等返回，不报错（明确可复现）；
  - ``offset > committed``（跳位/越位）或 offset<0（回退）：抛
    :class:`PreconditionFailedError`，位点绝不从中间或未来位置继续；
  - 未持租约（被别人抢走/从未认领）：同样抛 :class:`PreconditionFailedError`；
- 租约到期未提交的事件可被同组其他消费者重新认领（at-least-once 投递；
  “成功处理只生效一次”需处理器用业务幂等键保证，位点层面保证不重复提交）。
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any
import uuid

from taskqueue.config import EngineConfig
from taskqueue.errors import PreconditionFailedError
from taskqueue.models import EventRecord, now_ms
from taskqueue.storage import Storage

EventHandler = Callable[[dict[str, Any]], None]

# commit_offset 存储判定 -> 对外行为
_REJECTED_RESULTS = {"regression", "gap", "not_leased"}


class ConsumerGroup:
    def __init__(self, storage: Storage, cfg: EngineConfig, namespace: str,
                 topic: str, group: str, handler: EventHandler,
                 consumer_id: str | None = None,
                 on_consumed: Callable[[], None] | None = None,
                 on_commit_rejected: Callable[[], None] | None = None,
                 time_func: Callable[[], int] = now_ms,
                 logger: Any | None = None) -> None:
        self._storage = storage
        self._cfg = cfg
        self._ns = namespace
        self._topic = topic
        self._group = group
        self._handler = handler
        self._consumer_id = consumer_id or f"c-{uuid.uuid4().hex[:10]}"
        self._on_consumed = on_consumed
        self._on_commit_rejected = on_commit_rejected
        self._time = time_func
        self._log = logger
        self._storage.ensure_group(namespace, topic, group)

    @property
    def consumer_id(self) -> str:
        return self._consumer_id

    @property
    def committed_offset(self) -> int:
        """下一条待处理的 offset。"""
        return self._storage.committed_offset(self._ns, self._topic, self._group)

    def poll(self, limit: int = 10) -> list[EventRecord]:
        """认领一批事件（含租约内未完成的重投）。"""
        return self._storage.fetch_events_for_group(
            self._ns, self._topic, self._group, self._consumer_id,
            now=self._time(), limit=limit,
            lease_ms=self._cfg.visibility_timeout_ms,
        )

    def process_batch(self, limit: int = 10) -> int:
        """认领 -> 逐条处理 -> 原子提交位点；返回成功处理并提交的条数。

        处理器抛错时停止本批：该事件仍处于 pending 租约，后续会重试，
        位点不前进，保证“没处理成功的绝不跳过”。
        """
        events = self.poll(limit)
        processed = 0
        for ev in events:
            self._handler(ev.payload)
            result = self._storage.commit_offset(
                self._ns, self._topic, self._group, ev.offset,
                self._consumer_id, self._time())
            if result in _REJECTED_RESULTS:
                if self._on_commit_rejected:
                    self._on_commit_rejected()
                raise PreconditionFailedError(
                    f"commit offset {ev.offset} rejected: {result} "
                    f"(group={self._group}, consumer={self._consumer_id})")
            processed += 1
            if self._on_consumed:
                self._on_consumed()
            if self._log:
                self._log.info(
                    "task_id=- offset=%d consumer=%s 判定依据=处理成功位点提交=%s",
                    ev.offset, self._consumer_id, result)
        return processed

    def commit(self, offset: int) -> bool:
        """显式提交位点。重复提交返回 True（幂等）；回退/越位/非租约者抛错。"""
        result = self._storage.commit_offset(
            self._ns, self._topic, self._group, offset,
            self._consumer_id, self._time())
        if result in _REJECTED_RESULTS:
            if self._on_commit_rejected:
                self._on_commit_rejected()
            raise PreconditionFailedError(
                f"commit offset {offset} rejected: {result} "
                f"(group={self._group}, consumer={self._consumer_id})")
        return True
