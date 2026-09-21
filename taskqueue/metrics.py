"""吞吐与延迟度量：线程安全计数器 + 延迟分布（有界样本）。"""

from __future__ import annotations

import threading
import time
from collections import deque


class Metrics:
    def __init__(self, window_size: int = 1024) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._latencies: deque[float] = deque(maxlen=window_size)
        self._started_at = time.monotonic()

    def incr(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + n

    def record_latency(self, seconds: float) -> None:
        with self._lock:
            self._latencies.append(seconds)

    def snapshot(self) -> dict:
        with self._lock:
            counters = dict(self._counters)
            lat = sorted(self._latencies)
        uptime = max(time.monotonic() - self._started_at, 1e-9)
        succeeded = counters.get("succeeded", 0)
        snap: dict = {
            "counters": counters,
            "uptime_seconds": round(uptime, 3),
            "throughput_per_second": round(succeeded / uptime, 3),
        }
        if lat:
            snap["latency_ms"] = {
                "count": len(lat),
                "avg": round(sum(lat) / len(lat) * 1000, 3),
                "p50": round(lat[len(lat) // 2] * 1000, 3),
                "p99": round(lat[min(len(lat) - 1,
                                     int(len(lat) * 0.99))] * 1000, 3),
                "max": round(lat[-1] * 1000, 3),
            }
        else:
            snap["latency_ms"] = {"count": 0}
        return snap
