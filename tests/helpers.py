from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from apps.ml.embeddings.schema import EmbeddedDocument
from apps.ml.clustering.schema import Cluster, ClusterDocumentRecord
from apps.preprocessing.enrichment import EnrichedDocument
from apps.preprocessing.filtering.schema import FilterStatus
from apps.preprocessing.normalization import MediaType, SourceType


def build_enriched_document(
    text: str = "Во дворе нет горячей воды",
    *,
    doc_id: str = "vk_post:embedded-1",
) -> EnrichedDocument:
    timestamp = datetime(2026, 4, 3, 9, 0, tzinfo=UTC)
    return EnrichedDocument(
        doc_id=doc_id,
        source_type=SourceType.VK_POST,
        source_id="embedded-1",
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-1",
        is_official=False,
        reach=500,
        likes=15,
        reposts=2,
        comments_count=4,
        region_hint="Волгоград",
        geo_lat=None,
        geo_lon=None,
        raw_payload={"text": text},
        language="ru",
        language_confidence=0.99,
        is_supported_language=True,
        filter_status=FilterStatus.PASS,
        filter_reasons=(),
        quality_weight=1.0,
        normalized_text=text,
        token_count=len(text.split()),
        cleanup_flags=("whitespace_normalized",),
        text_sha256="abc123",
        duplicate_group_id=f"dup:{doc_id}",
        near_duplicate_flag=False,
        duplicate_cluster_size=1,
        canonical_doc_id=doc_id,
        region_id="volgograd-oblast",
        municipality_id="volgograd",
        geo_confidence=0.9,
        geo_source="text_toponym",
        geo_evidence=("matched_toponym:Волгоград",),
        engagement=21,
        metadata_version="meta-v1",
    )


def build_embedded_document() -> EmbeddedDocument:
    document = build_enriched_document()
    document_payload = asdict(document)
    document_payload.pop("token_count", None)
    return EmbeddedDocument(
        **document_payload,
        embedding=[0.6, 0.8],
        model_name="intfloat/multilingual-e5-large",
        model_version="checkpoint-hash",
        embedded_at=document.created_at,
        text_used="passage: [vk_post] Во дворе нет горячей воды",
        token_count=8,
        truncated=False,
    )


def build_cluster_document_record(
    *,
    doc_id: str = "doc-1",
    embedding: list[float] | None = None,
    author_id: str = "author-1",
    source_type: SourceType = SourceType.VK_POST,
    reach: int = 100,
    created_at: datetime | None = None,
    region: str | None = "volgograd-oblast",
) -> ClusterDocumentRecord:
    return ClusterDocumentRecord(
        doc_id=doc_id,
        embedding=embedding or [1.0, 0.0],
        author_id=author_id,
        source_type=source_type,
        reach=reach,
        created_at=created_at or datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
        region=region,
    )


def build_cluster(
    *,
    cluster_id: str = "cluster-1",
    doc_ids: list[str] | None = None,
    centroid: list[float] | None = None,
    created_at: datetime | None = None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    size: int = 2,
    unique_authors: int = 2,
    unique_sources: int = 1,
    reach_total: int = 200,
    earliest_doc_at: datetime | None = None,
    latest_doc_at: datetime | None = None,
    growth_rate: float = 1.0,
    geo_regions: list[str] | None = None,
    noise: bool = False,
    cohesion_score: float = 0.9,
    algorithm_params: dict[str, object] | None = None,
) -> Cluster:
    timestamp = created_at or datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    start = period_start or datetime(2026, 4, 4, 0, 0, tzinfo=UTC)
    end = period_end or timestamp
    earliest = earliest_doc_at or start
    latest = latest_doc_at or end
    return Cluster(
        cluster_id=cluster_id,
        doc_ids=doc_ids or ["doc-1", "doc-2"],
        centroid=centroid or [1.0, 0.0],
        created_at=timestamp,
        period_start=start,
        period_end=end,
        size=size,
        unique_authors=unique_authors,
        unique_sources=unique_sources,
        reach_total=reach_total,
        earliest_doc_at=earliest,
        latest_doc_at=latest,
        growth_rate=growth_rate,
        geo_regions=geo_regions or ["volgograd-oblast"],
        noise=noise,
        cohesion_score=cohesion_score,
        algorithm_params=algorithm_params or {"min_cluster_size": 10},
    )
