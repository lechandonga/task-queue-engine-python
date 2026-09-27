"""本地任务队列与事件消费子系统。"""

from .errors import (
    QueueError,
    ErrorCategory,
    ValidationError,
    ResourceExhaustedError,
    ConflictError,
    NotFoundError,
    InternalError,
)
from .models import Task, TaskStatus, Event
from .engine import TaskEngine, EngineConfig
from .consumer import ConsumerGroup, ParallelConsumer

__all__ = [
    "QueueError",
    "ErrorCategory",
    "ValidationError",
    "ResourceExhaustedError",
    "ConflictError",
    "NotFoundError",
    "InternalError",
    "Task",
    "TaskStatus",
    "Event",
    "TaskEngine",
    "EngineConfig",
    "ConsumerGroup",
    "ParallelConsumer",
]
