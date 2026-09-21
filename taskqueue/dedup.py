"""幂等与去重：去重窗口内相同 dedup_key 的提交只产生一次执行。

去重窗口语义（确定且可复现）：
- 首次提交 key 时记录 ``expires_at = 提交时刻(now) + window_ms``；
- 在 ``now < expires_at`` 内，重复提交命中已有记录（无论原任务处于
  PENDING/RUNNING/RETRY_WAIT/COMPLETED/DEAD 哪种状态），都返回首次任务的 id，
  不会产生第二次执行或副作用，也不占用队列额度；
- ``now >= expires_at`` 后该 key 视为过期，清理（sweep 或提交时惰性清理）后
  允许同 key 再次提交；
- 时间由调用方注入（``time_func``），测试可完全复现；
- 判定与任务插入在同一个 SQLite IMMEDIATE 事务内完成（见
  ``Storage.submit_task``），并发提交同 key 只有一个落库。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from taskqueue.config import DedupConfig
from taskqueue.models import now_ms
from taskqueue.storage import Storage


@dataclass(frozen=True)
class DedupView:
    """去重窗口查询视图。"""

    hit: bool                  # 当前是否有效命中
    task_id: str | None        # 命中时的首次任务 id


class Deduplicator:
    """去重窗口管理器：只做查询与过期清理；提交原子性在 Storage 内。"""

    def __init__(self, storage: Storage, cfg: DedupConfig,
                 time_func: Callable[[], int] = now_ms) -> None:
        self._storage = storage
        self._cfg = cfg
        self._time = time_func

    @property
    def window_ms(self) -> int:
        return self._cfg.window_ms

    def lookup(self, namespace: str, key: str) -> DedupView:
        hit, task_id = self._storage.dedup_seen(namespace, key, self._time())
        return DedupView(hit=hit, task_id=task_id)

    def sweep(self) -> int:
        """清理过期键，返回清理数量。过期语义：expires_at <= now 即过期。"""
        return self._storage.dedup_sweep(self._time())
