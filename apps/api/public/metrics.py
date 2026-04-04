from __future__ import annotations

import statistics
from collections import deque
from dataclasses import dataclass
from hashlib import md5
from typing import Any


@dataclass(frozen=True, slots=True)
class APIMetricEvent:
    endpoint: str
    method: str
    status_code: int
    response_time_ms: float
    cache_hit: bool
    user_id: str | None
    params_hash: str


class MetricsRegistry:
    def __init__(self, *, max_events: int = 2048) -> None:
        self._events: deque[APIMetricEvent] = deque(maxlen=max_events)

    def record(self, event: APIMetricEvent) -> None:
        self._events.append(event)

    def render_prometheus(self) -> str:
        if not self._events:
            return "\n".join(
                [
                    "# HELP api_requests_total Total API requests recorded.",
                    "# TYPE api_requests_total counter",
                    "api_requests_total 0",
                    "# HELP api_requests_by_status_total Total API requests by status class.",
                    "# TYPE api_requests_by_status_total counter",
                    'api_requests_by_status_total{status_class="2xx"} 0',
                    'api_requests_by_status_total{status_class="4xx"} 0',
                    'api_requests_by_status_total{status_class="5xx"} 0',
                ],
            )

        response_times = [event.response_time_ms for event in self._events]
        cache_hits = sum(1 for event in self._events if event.cache_hit)
        status_counts = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0}
        for event in self._events:
            status_class = f"{int(event.status_code) // 100}xx"
            if status_class in status_counts:
                status_counts[status_class] += 1
        p99 = _percentile(response_times, 0.99)
        lines = [
            "# HELP api_requests_total Total API requests recorded.",
            "# TYPE api_requests_total counter",
            f"api_requests_total {len(self._events)}",
            "# HELP api_requests_by_status_total Total API requests by status class.",
            "# TYPE api_requests_by_status_total counter",
            *[
                f'api_requests_by_status_total{{status_class="{status_class}"}} {count}'
                for status_class, count in status_counts.items()
            ],
            "# HELP api_response_time_p99_ms Rolling p99 response time in milliseconds.",
            "# TYPE api_response_time_p99_ms gauge",
            f"api_response_time_p99_ms {p99:.2f}",
            "# HELP api_cache_hit_rate Rolling cache hit ratio.",
            "# TYPE api_cache_hit_rate gauge",
            f"api_cache_hit_rate {cache_hits / len(self._events):.4f}",
        ]
        return "\n".join(lines)


def hash_params(params: dict[str, Any]) -> str:
    payload = "|".join(f"{key}={params[key]}" for key in sorted(params))
    return md5(payload.encode("utf-8")).hexdigest()


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * quantile))))
    return ordered[index]
