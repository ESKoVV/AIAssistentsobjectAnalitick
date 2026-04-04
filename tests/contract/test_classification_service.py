from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from apps.api.public.cache import TopCache
from apps.api.public.config import APIConfig
from apps.api.public.repository import SnapshotItemRecord, SnapshotRecord
from apps.api.public.service import TopAPIService
from apps.api.schemas.top import TopQueryParams
from apps.preprocessing.enrichment import EnrichedDocument, enrich_metadata
from apps.preprocessing.geo_enrichment import GeoEnrichedDocument
from tests.helpers import build_enriched_document


BASE_FIELDS = {
    "doc_id",
    "source_type",
    "source_id",
    "parent_id",
    "text",
    "media_type",
    "created_at",
    "collected_at",
    "author_id",
    "is_official",
    "reach",
    "likes",
    "reposts",
    "comments_count",
    "region_hint",
    "geo_lat",
    "geo_lon",
    "raw_payload",
    "language",
    "language_confidence",
    "is_supported_language",
    "filter_status",
    "filter_reasons",
    "quality_weight",
    "anomaly_flags",
    "anomaly_confidence",
    "normalized_text",
    "token_count",
    "cleanup_flags",
    "text_sha256",
    "duplicate_group_id",
    "near_duplicate_flag",
    "duplicate_cluster_size",
    "canonical_doc_id",
    "region_id",
    "municipality_id",
    "geo_confidence",
    "geo_source",
    "geo_evidence",
}


def test_enrich_metadata_preserves_base_fields_and_adds_category() -> None:
    enriched = build_enriched_document("мусор не вывозили третью неделю")
    payload = asdict(enriched)
    for key in ("engagement", "metadata_version", "category", "category_label", "category_confidence", "secondary_category"):
        payload.pop(key, None)

    geo_document = GeoEnrichedDocument(**payload)
    classified = enrich_metadata(geo_document)

    assert isinstance(classified, EnrichedDocument)
    assert BASE_FIELDS.issubset(asdict(classified).keys())
    assert classified.category == "housing"
    assert classified.category_label == "ЖКХ"


def test_top_api_filters_clusters_by_category() -> None:
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
            del ranking_id, cluster_ids
            items = [
                SnapshotItemRecord(
                    cluster_id="cluster-housing",
                    rank=1,
                    score=0.9,
                    summary="Проблемы с ЖКХ",
                    category="housing",
                    category_label="ЖКХ",
                    key_phrases=["мусор", "вывоз"],
                    mention_count=20,
                    unique_authors=15,
                    unique_sources=3,
                    reach_total=1000,
                    growth_rate=1.5,
                    geo_regions=["Волгоград"],
                    score_breakdown={
                        "volume": 0.8,
                        "dynamics": 0.7,
                        "sentiment": 0.6,
                        "reach": 0.5,
                        "geo": 0.4,
                        "source": 0.3,
                    },
                    sample_doc_ids=["doc-1"],
                    sentiment_score=-0.6,
                    is_new=False,
                    is_growing=True,
                    sources=[],
                    sample_posts=[],
                    timeline=[],
                ),
                SnapshotItemRecord(
                    cluster_id="cluster-roads",
                    rank=2,
                    score=0.7,
                    summary="Проблемы с дорогами",
                    category="roads",
                    category_label="Дороги и транспорт",
                    key_phrases=["яма", "дорога"],
                    mention_count=10,
                    unique_authors=9,
                    unique_sources=2,
                    reach_total=500,
                    growth_rate=1.2,
                    geo_regions=["Волгоград"],
                    score_breakdown={
                        "volume": 0.7,
                        "dynamics": 0.6,
                        "sentiment": 0.5,
                        "reach": 0.4,
                        "geo": 0.3,
                        "source": 0.2,
                    },
                    sample_doc_ids=["doc-2"],
                    sentiment_score=-0.5,
                    is_new=False,
                    is_growing=False,
                    sources=[],
                    sample_posts=[],
                    timeline=[],
                ),
            ]
            if category is None:
                return items
            return [item for item in items if item.category == category]

    service = TopAPIService(
        repository=StubRepository(),
        cache=TopCache(redis_dsn=None, ttl_seconds=300),
        config=APIConfig(database_url="postgresql://test:test@localhost:5432/test"),
    )

    response, _ = service.get_top(TopQueryParams(period="24h", limit=10, category="housing"), use_cache=False)

    assert [item.cluster_id for item in response.items] == ["cluster-housing"]
    assert all(item.category == "housing" for item in response.items)
