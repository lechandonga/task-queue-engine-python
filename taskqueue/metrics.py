"""可度量性：吞吐、延迟、队列深度、拒绝数；空闲时无后台开销。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Callable

from taskqueue.models import now_ms


@dataclass
class Metrics:
    submitted: int = 0
    completed: int = 0
    failed_dead: int = 0
    retried: int = 0
    dedup_hits: int = 0
    rejected: int = 0
    events_consumed: int = 0
    commit_rejected: int = 0
    # 完成延迟（毫秒），保留最近样本用于分位数
    _latencies_ms: list[int] = field(default_factory=list)
    _latencies_max_samples: int = 1024

    def snapshot(self) -> dict[str, int | float]:
        raise NotImplementedError


class MetricsRegistry:
    def __init__(self, time_func: Callable[[], int] = now_ms) -> None:
        self._m = Metrics()
        self._lock = threading.Lock()
        self._time = time_func

    def record_submit(self) -> None: ...
    def record_dedup_hit(self) -> None: ...
    def record_reject(self) -> None: ...
    def record_complete(self, latency_ms: int) -> None: ...
    def record_retry(self) -> None: ...
    def record_dead(self) -> None: ...
    def record_event_consumed(self) -> None: ...
    def record_commit_rejected(self) -> None: ...

    def snapshot(self) -> dict[str, int | float]:
        raise NotImplementedError
