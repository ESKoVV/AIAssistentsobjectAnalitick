from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from apps.api.public.cache import TopCache
from apps.api.public.config import APIConfig
from apps.api.public.errors import ServiceUnavailableError
from apps.api.public.repository import (
    HealthSnapshot,
    SnapshotRecord,
)
from apps.api.public.service import TopAPIService
from apps.api.schemas.top import TopQueryParams

def _build_service(repository: object) -> TopAPIService:
    return TopAPIService(
        repository=repository,
        cache=TopCache(redis_dsn=None, ttl_seconds=300),
        config=APIConfig(database_url="postgresql://test:test@localhost:5432/test"),
    )


def test_get_top_requires_snapshot_and_does_not_fallback() -> None:
    class StubRepository:
        def fetch_snapshot(self, *, period_hours: int, as_of=None):
            del period_hours, as_of
            return None

    service = _build_service(StubRepository())

    with pytest.raises(ServiceUnavailableError) as exc_info:
        service.get_top(TopQueryParams(period="24h"), use_cache=False)

    assert exc_info.value.error_code == "ranking_unavailable"


def test_get_health_requires_snapshot_pipeline() -> None:
    class StubRepository:
        def fetch_health_snapshot(self, *, freshness_threshold_minutes: int) -> HealthSnapshot:
            del freshness_threshold_minutes
            raise RuntimeError("ranking snapshots are not available")

    service = _build_service(StubRepository())

    with pytest.raises(ServiceUnavailableError) as exc_info:
        service.get_health()

    assert exc_info.value.error_code == "ranking_unavailable"


def test_get_top_returns_snapshot_data_when_available() -> None:
    now = datetime.now(UTC)

    class StubRepository:
        def fetch_snapshot(self, *, period_hours: int, as_of=None):
            del period_hours, as_of
            return SnapshotRecord(
                ranking_id="ranking-1",
                computed_at=now,
                period_start=now - timedelta(hours=24),
                period_end=now,
                top_n=10,
                period_hours=24,
            )

        def fetch_snapshot_items(self, *, ranking_id: str, cluster_ids=None, category=None):
            del ranking_id, cluster_ids, category
            return []

    service = _build_service(StubRepository())
    response, cache_hit = service.get_top(TopQueryParams(period="24h"), use_cache=False)

    assert cache_hit is False
    assert response.total_clusters == 0
