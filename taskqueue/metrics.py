"""可度量性：吞吐、延迟、队列深度、拒绝数；空闲时无后台开销。

计数器用一把短锁保护；延迟样本保存在有界列表内（最多
``_latencies_max_samples`` 条），内存占用有界，不会随运行时间无限增长。
所有指标只在事件发生时写入，无后台线程。
"""

from __future__ import annotations

import threading
from collections import deque
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
    latencies_ms: deque[int] = field(
        default_factory=lambda: deque(maxlen=1024))

    def snapshot(self) -> dict[str, int | float]:
        xs = sorted(self.latencies_ms)

        def pct(p: float) -> float:
            if not xs:
                return 0.0
            idx = min(len(xs) - 1, int(p * len(xs)))
            return float(xs[idx])

        return {
            "submitted": self.submitted,
            "completed": self.completed,
            "failed_dead": self.failed_dead,
            "retried": self.retried,
            "dedup_hits": self.dedup_hits,
            "rejected": self.rejected,
            "events_consumed": self.events_consumed,
            "commit_rejected": self.commit_rejected,
            "latency_p50_ms": pct(0.50),
            "latency_p95_ms": pct(0.95),
            "latency_p99_ms": pct(0.99),
            "latency_max_ms": float(xs[-1]) if xs else 0.0,
        }


class MetricsRegistry:
    def __init__(self, time_func: Callable[[], int] = now_ms) -> None:
        self._m = Metrics()
        self._lock = threading.Lock()
        self._time = time_func

    def record_submit(self) -> None:
        with self._lock:
            self._m.submitted += 1

    def record_dedup_hit(self) -> None:
        with self._lock:
            self._m.dedup_hits += 1

    def record_reject(self) -> None:
        with self._lock:
            self._m.rejected += 1

    def record_complete(self, latency_ms: int) -> None:
        with self._lock:
            self._m.completed += 1
            self._m.latencies_ms.append(max(0, int(latency_ms)))

    def record_retry(self) -> None:
        with self._lock:
            self._m.retried += 1

    def record_dead(self) -> None:
        with self._lock:
            self._m.failed_dead += 1

    def record_event_consumed(self) -> None:
        with self._lock:
            self._m.events_consumed += 1

    def record_commit_rejected(self) -> None:
        with self._lock:
            self._m.commit_rejected += 1

    def snapshot(self) -> dict[str, int | float]:
        with self._lock:
            return self._m.snapshot()
