"""消费者组：组内并行认领、连续位点、失败/崩溃重投。

语义：
- 同一 (group, topic) 可同时存在多个消费者并行处理：每条事件由 store
  的 event_claims 行保证同一时刻只被一个消费者认领，组内处理不重叠；
- 每条事件处理成功后调用 ack，位点只连续推进到“最大无缺口前缀”：
  乱序完成时后完成的事件不会越过中间缺口，缺口补齐后连续推进，
  不跳号、不丢事件；
- ack 是幂等去重的：重复确认、被接手后的迟到确认都返回 applied=False，
  不会把同一条事件算成两次成功，也不产生重复的位点提交；
- 处理失败（nack）或消费者崩溃（认领租约到期）后，只要事件尚未被
  连续确认，就能被组内其他消费者接手重做（at-least-once，处理器需幂等）；
- max_inflight 限定同一消费组在途事件数上限：积压再多也不会无限认领，
  空闲与高压下资源占用均有界。
"""

from __future__ import annotations

import logging
import time
import uuid

from .errors import normalize
from .metrics import Metrics
from .store import Store

logger = logging.getLogger("taskqueue.consumer")


class ConsumerGroup:
    def __init__(self, store: Store, group: str, topic: str,
                 handler, *, owner: str | None = None,
                 lease_ttl: float = 5.0, metrics: Metrics | None = None,
                 max_inflight: int = 32, clock=time.time) -> None:
        if not group or not topic:
            from .errors import ValidationError
            raise ValidationError("group and topic must be non-empty")
        if max_inflight <= 0:
            from .errors import ValidationError
            raise ValidationError("max_inflight must be positive")
        self.store = store
        self.group = group
        self.topic = topic
        self.handler = handler
        self.owner = owner or uuid.uuid4().hex
        self.lease_ttl = lease_ttl
        self.metrics = metrics or Metrics()
        self.max_inflight = max_inflight
        self._clock = clock

    @property
    def committed_offset(self) -> int:
        """已连续提交位点：下一条待消费事件的 offset。"""
        return self.store.get_offset(self.group, self.topic)

    def inflight(self) -> int:
        """组内当前在途（已认领未确认且未过期）事件数。"""
        return self.store.inflight_count(self.group, self.topic,
                                         self._clock())

    def poll_once(self, max_events: int = 16) -> int:
        """拉取并处理一批事件，返回本消费者本次成功确认的条数。

        与组内其他消费者并行：认领互不重叠；单条处理失败时放弃该条
        认领（可立即被重投），其余已成功的事件照常连续提交。
        """
        now = self._clock()
        events = self.store.claim_events(
            self.group, self.topic, self.owner, max_events,
            self.lease_ttl, self.max_inflight, now)
        if not events:
            return 0
        logger.info(
            "group=%s topic=%s owner=%s claimed offsets=%s"
            " inflight=%d/%d", self.group, self.topic, self.owner,
            [o for o, _ in events], self.inflight(), self.max_inflight)
        processed = 0
        for ev_offset, payload in events:
            # 处理前续租，避免长耗时事件在处理途中被误判崩溃而重投
            self.store.renew_claims(self.group, self.topic, self.owner,
                                    self.lease_ttl, self._clock())
            try:
                self.handler(payload)
            except Exception as exc:
                err = normalize(exc)
                self.store.nack_event(self.group, self.topic,
                                      ev_offset, self.owner)
                self.metrics.incr("consume_failed")
                logger.warning(
                    "group=%s topic=%s owner=%s offset=%d failed: %s (%s);"
                    " claim released for redelivery, committed stays %d",
                    self.group, self.topic, self.owner, ev_offset,
                    err.message, err.category.value, self.committed_offset)
                continue
            applied, committed = self.store.ack_event(
                self.group, self.topic, ev_offset, self.owner,
                self._clock())
            if applied:
                processed += 1
                self.metrics.incr("consumed")
                logger.info(
                    "group=%s topic=%s owner=%s acked event offset=%d"
                    " committed_offset=%d (判定依据: 连续无缺口前缀)",
                    self.group, self.topic, self.owner,
                    ev_offset, committed)
            else:
                logger.info(
                    "group=%s topic=%s owner=%s stale ack offset=%d"
                    " ignored, committed_offset=%d (判定依据: 认领已失效/"
                    "重复确认, 不计两次成功)",
                    self.group, self.topic, self.owner,
                    ev_offset, committed)
        return processed

    def commit(self, offset: int) -> None:
        """显式提交位点（外部手动管理时）。重复或回退抛 ConflictError。"""
        self.store.commit_offset(self.group, self.topic, offset,
                                 self._clock())

    def close(self) -> None:
        """退出消费组：释放遗留的组租约（旧语义）。

        已认领未确认的事件不在此主动放弃：优雅退出时它们等租约到期后
        被组内其他消费者接手，避免“已处理完但尚未 ack”的事件被过早重投。
        """
        self.store.release_lease(self.group, self.topic, self.owner)
