"""领域模型：任务、事件、死信记录、消费者组状态。"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    PENDING = "pending"        # 已提交，等待调度
    RUNNING = "running"        # 被 worker 认领执行中
    RETRY_WAIT = "retry_wait"  # 失败后退避等待
    COMPLETED = "completed"
    DEAD = "dead"              # 进入死信
    CANCELLED = "cancelled"


@dataclass
class TaskRecord:
    id: str
    namespace: str
    queue: str
    payload: dict[str, Any]
    status: TaskStatus
    attempt: int = 0
    dedup_key: str | None = None
    created_at_ms: int = 0
    updated_at_ms: int = 0
    run_at_ms: int = 0          # 最早可执行时间（退避/延迟）
    leased_by: str | None = None
    lease_expires_ms: int = 0
    last_error: str | None = None
    # 瞬态（不落库）：本次提交是否命中去重窗口
    dedup_hit: bool = False

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex


@dataclass
class DeadLetter:
    task_id: str
    namespace: str
    queue: str
    payload: dict[str, Any]
    attempts: int
    reason: str
    failed_at_ms: int


@dataclass
class EventRecord:
    """事件流中的一条记录（offset 单调递增，从 0 开始）。"""

    offset: int
    namespace: str
    topic: str
    payload: dict[str, Any]
    created_at_ms: int


@dataclass
class AckState:
    """消费者组对单条事件的处理状态。"""

    offset: int
    status: str                 # "pending" | "acked"
    leased_by: str | None = None
    lease_expires_ms: int = 0
    attempts: int = 0


def now_ms() -> int:
    return int(time.time() * 1000)
