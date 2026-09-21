"""持久化层：单文件 SQLite，WAL 模式，所有状态变更走短事务。

线程模型：本存储对连接加锁串行化所有写操作（SQLite 默认连接跨线程需
``check_same_thread=False``），保证多 worker / 多消费者并发下的原子语义。
"""

from __future__ import annotations

import sqlite3
import threading
from typing import Any, Iterable

from taskqueue.models import AckState, DeadLetter, EventRecord, TaskRecord, TaskStatus


class Storage:
    """SQLite 持久化存储。所有方法要么原子提交，要么抛错不留半成品。"""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ---- 生命周期 ------------------------------------------------------
    def open(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def __enter__(self) -> Storage:
        self.open()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---- 任务 ----------------------------------------------------------
    def insert_task(self, task: TaskRecord) -> None:
        raise NotImplementedError

    def get_task(self, task_id: str) -> TaskRecord | None:
        raise NotImplementedError

    def get_task_by_dedup(self, namespace: str, dedup_key: str) -> TaskRecord | None:
        raise NotImplementedError

    def fetch_due_tasks(self, namespace: str, now: int, limit: int,
                        worker_id: str, lease_ms: int) -> list[TaskRecord]:
        """原子认领：把到期的 PENDING/RETRY_WAIT 任务置为 RUNNING 并加租约。"""
        raise NotImplementedError

    def complete_task(self, task_id: str, worker_id: str, now: int) -> bool:
        raise NotImplementedError

    def reschedule_task(self, task_id: str, worker_id: str, run_at_ms: int,
                        error: str, now: int) -> bool:
        raise NotImplementedError

    def fail_task_to_dead(self, task_id: str, reason: str, now: int) -> DeadLetter | None:
        raise NotImplementedError

    def list_dead_letters(self, namespace: str, queue: str | None = None,
                         limit: int = 100, offset: int = 0) -> list[DeadLetter]:
        raise NotImplementedError

    def expire_leases(self, now: int, lease_ms: int) -> int:
        """把租约超时的 RUNNING 任务退回 RETRY_WAIT/PENDING，返回处理条数。"""
        raise NotImplementedError

    def count_pending(self, namespace: str) -> int:
        raise NotImplementedError

    def backlog_bytes(self, namespace: str) -> int:
        raise NotImplementedError

    # ---- 去重窗口 ------------------------------------------------------
    def dedup_seen(self, namespace: str, key: str) -> bool:
        raise NotImplementedError

    def dedup_put(self, namespace: str, key: str, task_id: str, expires_at_ms: int) -> None:
        raise NotImplementedError

    def dedup_sweep(self, now: int) -> int:
        raise NotImplementedError

    # ---- 事件与消费者组 ------------------------------------------------
    def append_event(self, namespace: str, topic: str, payload: dict[str, Any],
                     now: int) -> EventRecord:
        raise NotImplementedError

    def read_events(self, namespace: str, topic: str, offset: int, limit: int) -> list[EventRecord]:
        raise NotImplementedError

    def ensure_group(self, namespace: str, topic: str, group: str) -> None:
        raise NotImplementedError

    def fetch_events_for_group(self, namespace: str, topic: str, group: str,
                               consumer: str, now: int, limit: int,
                               lease_ms: int) -> list[EventRecord]:
        """原子认领组内未 ack 且未被他人租用的事件。"""
        raise NotImplementedError

    def commit_offset(self, namespace: str, topic: str, group: str,
                      offset: int, consumer: str, now: int) -> bool:
        """原子提交位点。重复提交成功（幂等），回退/越位返回 False。"""
        raise NotImplementedError

    def committed_offset(self, namespace: str, topic: str, group: str) -> int:
        """返回该组已连续提交的最大位点（下一条待处理 offset）。"""
        raise NotImplementedError

    def expire_group_leases(self, now: int, lease_ms: int) -> int:
        raise NotImplementedError
