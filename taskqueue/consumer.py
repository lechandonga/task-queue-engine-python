"""消费者组与位点管理。

语义：
- 同一 (group, topic) 内通过租约保证单活消费者，多实例并行时
  不会重复认领同一批事件；
- 每条事件处理成功后才推进位点，位点提交原子生效；
- 重复提交相同位点或位点回退被 store 层明确拒绝（ConflictError），
  不会从中间继续；
- 处理器抛错时位点不前进，事件会被重新投递（at-least-once），
  处理器需幂等。
"""

from __future__ import annotations

import logging
import time
import uuid

from .errors import ConflictError, normalize
from .metrics import Metrics
from .store import Store

logger = logging.getLogger("taskqueue.consumer")


class ConsumerGroup:
    def __init__(self, store: Store, group: str, topic: str,
                 handler, *, owner: str | None = None,
                 lease_ttl: float = 5.0, metrics: Metrics | None = None,
                 clock=time.time) -> None:
        if not group or not topic:
            from .errors import ValidationError
            raise ValidationError("group and topic must be non-empty")
        self.store = store
        self.group = group
        self.topic = topic
        self.handler = handler
        self.owner = owner or uuid.uuid4().hex
        self.lease_ttl = lease_ttl
        self.metrics = metrics or Metrics()
        self._clock = clock

    @property
    def committed_offset(self) -> int:
        """已提交位点：下一条待消费事件的 offset。"""
        return self.store.get_offset(self.group, self.topic)

    def poll_once(self, max_events: int = 16) -> int:
        """拉取并处理一批事件，返回成功处理的条数。

        未持有租约时返回 0（组内其他实例活跃中）。
        某条事件处理失败：停止本批，位点停留在该事件处，下次重投。
        """
        now = self._clock()
        if not self.store.acquire_lease(self.group, self.topic, self.owner,
                                        self.lease_ttl, now):
            logger.info("group=%s topic=%s owner=%s: lease held by other",
                        self.group, self.topic, self.owner)
            return 0
        offset = self.committed_offset
        events = self.store.read_events(self.topic, offset, max_events)
        processed = 0
        for ev_offset, payload in events:
            try:
                self.handler(payload)
            except Exception as exc:
                err = normalize(exc)
                logger.warning(
                    "group=%s topic=%s offset=%d failed: %s (%s);"
                    " offset stays at %d", self.group, self.topic,
                    ev_offset, err.message, err.category.value, offset)
                self.metrics.incr("consume_failed")
                break
            # 处理成功后原子推进位点；重复/回退会被 store 拒绝
            self.store.commit_offset(self.group, self.topic,
                                     ev_offset + 1, self._clock())
            processed += 1
            self.metrics.incr("consumed")
            logger.info("group=%s topic=%s consumed offset=%d -> %d",
                        self.group, self.topic, ev_offset, ev_offset + 1)
        return processed

    def commit(self, offset: int) -> None:
        """显式提交位点（外部手动管理时）。重复或回退抛 ConflictError。"""
        self.store.commit_offset(self.group, self.topic, offset,
                                 self._clock())

    def close(self) -> None:
        self.store.release_lease(self.group, self.topic, self.owner)
