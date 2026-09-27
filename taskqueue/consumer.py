"""消费者组与位点管理。

ConsumerGroup（租约单活模式）：
- 同一 (group, topic) 内通过租约保证单活消费者，多实例并行时
  不会重复认领同一批事件；
- 每条事件处理成功后才推进位点，位点提交原子生效；
- 重复提交相同位点或位点回退被 store 层明确拒绝（ConflictError），
  不会从中间继续；
- 处理器抛错时位点不前进，事件会被重新投递（at-least-once），
  处理器需幂等。

ParallelConsumer（组内并行模式）：
- 同一消费组内多实例真正并行：认领原子化，同一时刻同一条事件
  只被一个实例处理，组内互不重叠；
- 组在途事件数有可配置上限（max_inflight），积压时有界；
- 位点连续推进：乱序完成不越过空缺，重投不产生重复确认；
- 失败/崩溃的事件由组内其他实例重投，不会永久卡住。
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


class ParallelConsumer:
    """组内并行消费者：同一消费组内多实例真正并行处理同一事件流。

    语义：
    - 认领由 store 层原子完成：同一时刻同一条事件只被一个实例持有，
      组内各实例处理的事件不重叠；
    - 组在途事件数受 max_inflight 约束，积压时不会无限认领，
      空闲时不持有任何事件，资源占用有界；
    - 处理成功后 ack 幂等推进位点：乱序完成时先记入 acks，
      位点只在中间无空缺时连续推进，不跳号、不丢事件；
    - 处理失败立即 nack 释放认领，实例崩溃时认领随 TTL 过期，
      事件都会被组内其他实例重投（at-least-once，处理器需幂等）；
    - 重投后的重复 ack 不计数、不产生重复位点提交。
    """

    def __init__(self, store: Store, group: str, topic: str,
                 handler, *, owner: str | None = None,
                 claim_ttl: float = 5.0, max_inflight: int = 64,
                 metrics: Metrics | None = None,
                 clock=time.time) -> None:
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
        self.claim_ttl = claim_ttl
        self.max_inflight = max_inflight
        self.metrics = metrics or Metrics()
        self._clock = clock

    @property
    def committed_offset(self) -> int:
        """组对外可见的完成位点：连续无空缺地推进到的下一条 offset。"""
        return self.store.get_offset(self.group, self.topic)

    def poll_once(self, max_events: int = 8) -> int:
        """认领并处理一批事件，返回本次新确认成功的条数。

        认领量同时受 max_events 与组在途上限 max_inflight 约束。
        单条失败不影响本批其他事件：失败事件立即释放认领等待重投。
        """
        now = self._clock()
        events = self.store.claim_events(
            self.group, self.topic, self.owner, max_events,
            self.claim_ttl, now, self.max_inflight)
        if not events:
            return 0
        offsets = [o for o, _ in events]
        logger.info("group=%s topic=%s owner=%s claimed offsets=%s"
                    " inflight<=%d", self.group, self.topic, self.owner,
                    offsets, self.max_inflight)
        done = 0
        for ev_offset, payload in events:
            try:
                self.handler(payload)
            except Exception as exc:
                err = normalize(exc)
                self.store.nack_event(self.group, self.topic, ev_offset,
                                      self.owner)
                self.metrics.incr("consume_failed")
                logger.warning(
                    "group=%s topic=%s offset=%d failed: %s (%s);"
                    " claim released for redelivery", self.group,
                    self.topic, ev_offset, err.message, err.category.value)
                continue
            counted, committed = self.store.ack_event(
                self.group, self.topic, ev_offset, self._clock())
            if counted:
                done += 1
                self.metrics.incr("consumed")
                logger.info("group=%s topic=%s acked offset=%d ->"
                            " committed=%d", self.group, self.topic,
                            ev_offset, committed)
            else:
                # 重投导致的重复确认：不计数、不重复提交位点
                logger.info("group=%s topic=%s offset=%d already acked;"
                            " skip (committed=%d)", self.group, self.topic,
                            ev_offset, committed)
        return done

    def close(self) -> None:
        """退出：释放全部在途认领，事件立即可被组内他人接手。"""
        released = self.store.release_claims(self.group, self.topic,
                                             self.owner)
        if released:
            logger.info("group=%s topic=%s owner=%s released %d claims"
                        " on close", self.group, self.topic, self.owner,
                        released)
