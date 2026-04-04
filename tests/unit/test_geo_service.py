from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.api.public.cache import TopCache
from apps.api.public.config import APIConfig
from apps.api.public.repository import SnapshotItemRecord, SnapshotRecord
from apps.api.public.service import TopAPIService
from apps.api.schemas.top import TopQueryParams


def test_get_geo_returns_clusters_with_region_coordinates() -> None:
    now = datetime.now(UTC)

    class StubRepository:
        def fetch_snapshot(self, *, period_hours: int, as_of=None):
            del period_hours, as_of
            return SnapshotRecord(
                ranking_id="ranking-geo-1",
                computed_at=now,
                period_start=now - timedelta(hours=24),
                period_end=now,
                top_n=10,
                period_hours=24,
            )

        def fetch_snapshot_items(self, *, ranking_id: str, cluster_ids=None, category=None):
            del ranking_id, cluster_ids, category
            return [
                SnapshotItemRecord(
                    cluster_id="cluster-geo-1",
                    rank=1,
                    score=0.84,
                    summary="Жалобы на вывоз мусора и переполненные контейнеры",
                    category="housing",
                    category_label="ЖКХ",
                    key_phrases=["мусор", "контейнеры"],
                    mention_count=150,
                    unique_authors=103,
                    unique_sources=3,
                    reach_total=250000,
                    growth_rate=2.4,
                    geo_regions=["Ростов-на-Дону", "Таганрог"],
                    score_breakdown={
                        "volume": 0.8,
                        "dynamics": 0.7,
                        "sentiment": 0.6,
                        "reach": 0.5,
                        "geo": 0.4,
                        "source": 0.3,
                    },
                    sample_doc_ids=["doc-1"],
                    sentiment_score=-0.5,
                    is_new=False,
                    is_growing=True,
                    sources=[],
                    sample_posts=[],
                    timeline=[],
                ),
            ]

    service = TopAPIService(
        repository=StubRepository(),
        cache=TopCache(redis_dsn=None, ttl_seconds=300),
        config=APIConfig(database_url="postgresql://test:test@localhost:5432/test"),
    )

    response, cache_hit = service.get_geo(TopQueryParams(period="24h"), use_cache=False)

    assert cache_hit is False
    assert len(response.clusters) == 1
    cluster = response.clusters[0]
    assert cluster.cluster_id == "cluster-geo-1"
    assert cluster.mention_count == 150
    assert cluster.geo_regions == ["Ростов-на-Дону", "Таганрог"]
    assert [(point.region, round(point.lat, 4), round(point.lon, 4)) for point in cluster.geo_points] == [
        ("Ростов-на-Дону", 47.2357, 39.7015),
        ("Таганрог", 47.2086, 38.9369),
    ]
