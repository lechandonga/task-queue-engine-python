"""核心数据模型（骨架）。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED_RETRYABLE = "failed_retryable"
    DEAD = "dead"
    CANCELLED = "cancelled"


@dataclass
class Task:
    task_id: str = ""
    namespace: str = "default"
    payload: dict = field(default_factory=dict)
    dedup_key: str | None = None
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class Event:
    topic: str = ""
    offset: int = 0
    payload: dict = field(default_factory=dict)
