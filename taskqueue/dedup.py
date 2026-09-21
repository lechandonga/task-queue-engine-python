"""幂等与去重：去重窗口内相同 dedup_key 的提交只产生一次执行。

去重窗口语义（确定且可复现）：
- 首次提交 key 时记录 ``expires_at = 提交时刻 + window_ms``；
- 在 ``now < expires_at`` 内，重复提交命中已有记录（无论原任务成功/失败/进行中），
  都返回首次任务的 id，不会产生第二次执行或副作用；
- ``now >= expires_at`` 后该 key 视为过期，清理后允许再次提交；
- 时间由调用方注入（``time_func``），测试可完全复现。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from taskqueue.config import DedupConfig
from taskqueue.models import now_ms
from taskqueue.storage import Storage


@dataclass
class DedupResult:
    accepted: bool            # True = 新接受；False = 命中去重
    task_id: str              # accepted 为新任务 id；否则为首次任务 id


class Deduplicator:
    def __init__(self, storage: Storage, cfg: DedupConfig,
                 time_func: Callable[[], int] = now_ms) -> None:
        self._storage = storage
        self._cfg = cfg
        self._time = time_func

    def check_or_register(self, namespace: str, key: str,
                          task_id_factory: Callable[[], str]) -> DedupResult:
        """检查 key；不存在则登记并返回 accepted。过期判定以注入时钟为准。"""
        raise NotImplementedError

    def attach(self, namespace: str, key: str, task_id: str) -> None:
        """把已存在的任务与 dedup_key 绑定（提交事务成功后调用）。"""
        raise NotImplementedError

    def sweep(self) -> int:
        """清理过期键，返回清理数量。"""
        raise NotImplementedError
