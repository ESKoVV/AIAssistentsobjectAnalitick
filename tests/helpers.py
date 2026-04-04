from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from apps.ml.embeddings.schema import EmbeddedDocument
from apps.ml.clustering.schema import Cluster, ClusterDocumentRecord
from apps.ml.ranking.schema import RankedCluster, ScoreBreakdown
from apps.ml.summarization.schema import ClusterDescription, StoredClusterDescription, SummarizationDocumentRecord
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
        anomaly_flags=(),
        anomaly_confidence=0.0,
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
        category="housing",
        category_label="ЖКХ",
        category_confidence=0.75,
        secondary_category=None,
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


def build_summarization_document_record(
    *,
    doc_id: str = "doc-1",
    author_id: str = "author-1",
    source_type: SourceType = SourceType.VK_POST,
    text: str = "Во дворе снова нет горячей воды и жители обсуждают сроки восстановления подачи.",
    created_at: datetime | None = None,
    region: str | None = "volgograd-oblast",
    embedding: list[float] | None = None,
) -> SummarizationDocumentRecord:
    return SummarizationDocumentRecord(
        doc_id=doc_id,
        author_id=author_id,
        source_type=source_type,
        text=text,
        created_at=created_at or datetime(2026, 4, 4, 9, 0, tzinfo=UTC),
        region=region,
        embedding=embedding or [1.0, 0.0],
    )


def build_cluster_description(
    *,
    cluster_id: str = "cluster-1",
    summary: str = (
        "Жители обсуждают перебои с горячей водой в жилых домах и сроки восстановления подачи. "
        "В сообщениях упоминаются адреса домов и ожидание завершения ремонтных работ."
    ),
    key_phrases: list[str] | None = None,
    sample_doc_ids: list[str] | None = None,
    model_name: str = "gpt-4o",
    prompt_version: str = "prompt-hash",
    generated_at: datetime | None = None,
    input_token_count: int = 120,
    output_token_count: int = 48,
    generation_time_ms: int = 850,
    fallback_used: bool = False,
) -> ClusterDescription:
    return ClusterDescription(
        cluster_id=cluster_id,
        summary=summary,
        key_phrases=key_phrases
        or [
            "нет горячей воды",
            "сроки восстановления подачи",
            "ремонтные работы",
            "жители обсуждают адреса домов",
            "ожидание завершения работ",
        ],
        sample_doc_ids=sample_doc_ids or ["doc-1", "doc-2", "doc-3"],
        model_name=model_name,
        prompt_version=prompt_version,
        generated_at=generated_at or datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        input_token_count=input_token_count,
        output_token_count=output_token_count,
        generation_time_ms=generation_time_ms,
        fallback_used=fallback_used,
    )


def build_stored_cluster_description(
    *,
    cluster_id: str = "cluster-1",
    needs_review: bool = False,
    cluster_size_at_generation: int = 10,
    prompt_version: str = "prompt-hash",
    generated_at: datetime | None = None,
) -> StoredClusterDescription:
    return StoredClusterDescription(
        description=build_cluster_description(
            cluster_id=cluster_id,
            prompt_version=prompt_version,
            generated_at=generated_at,
        ),
        needs_review=needs_review,
        cluster_size_at_generation=cluster_size_at_generation,
    )


def build_score_breakdown(
    *,
    volume_score: float = 0.8,
    dynamics_score: float = 0.7,
    sentiment_score: float = 0.9,
    reach_score: float = 0.6,
    geo_score: float = 0.5,
    source_score: float = 0.4,
    weights: dict[str, float] | None = None,
) -> ScoreBreakdown:
    return ScoreBreakdown(
        volume_score=volume_score,
        dynamics_score=dynamics_score,
        sentiment_score=sentiment_score,
        reach_score=reach_score,
        geo_score=geo_score,
        source_score=source_score,
        weights=weights
        or {
            "volume": 0.25,
            "dynamics": 0.25,
            "sentiment": 0.20,
            "reach": 0.15,
            "geo": 0.10,
            "source": 0.05,
        },
    )


def build_ranked_cluster(
    *,
    cluster_id: str = "cluster-1",
    rank: int = 1,
    score: float = 0.82,
    score_breakdown: ScoreBreakdown | None = None,
    summary: str = "Жители обсуждают перебои с горячей водой и сроки восстановления подачи.",
    key_phrases: list[str] | None = None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    size: int = 80,
    growth_rate: float = 3.2,
    reach_total: int = 100000,
    geo_regions: list[str] | None = None,
    unique_sources: int = 3,
    unique_authors: int = 12,
    sentiment_score: float = -0.8,
    is_new: bool = True,
    is_growing: bool = True,
    sample_doc_ids: list[str] | None = None,
    category: str = "housing",
    category_label: str = "ЖКХ",
) -> RankedCluster:
    return RankedCluster(
        cluster_id=cluster_id,
        rank=rank,
        score=score,
        score_breakdown=score_breakdown or build_score_breakdown(),
        summary=summary,
        key_phrases=key_phrases or ["нет горячей воды", "сроки восстановления подачи"],
        period_start=period_start or datetime(2026, 4, 4, 8, 0, tzinfo=UTC),
        period_end=period_end or datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        size=size,
        mention_count=size,
        growth_rate=growth_rate,
        reach_total=reach_total,
        geo_regions=geo_regions or ["volgograd-oblast", "astrakhan-oblast"],
        unique_sources=unique_sources,
        unique_authors=unique_authors,
        sentiment_score=sentiment_score,
        is_new=is_new,
        is_growing=is_growing,
        sample_doc_ids=sample_doc_ids or ["doc-1", "doc-2", "doc-3"],
        category=category,
        category_label=category_label,
    )


def write_summarization_prompt(
    path: Path,
    *,
    system_prompt: str = "Сформируй нейтральное описание по текстам.",
    user_prompt_template: str = (
        "Размер: {size}\n"
        "Период: {period_start} - {period_end}\n"
        "Источники: {source_types}\n"
        "Регионы: {geo_regions}\n"
        "{texts}\n"
        "ОПИСАНИЕ и ФРАЗЫ обязательны."
    ),
) -> Path:
    path.write_text(
        "\n".join(
            [
                "# Prompt",
                "",
                "## Task Goal",
                "Сформировать описание темы кластера.",
                "",
                "## Allowed Input Fields",
                "- size",
                "- period",
                "- source_types",
                "- geo_regions",
                "- texts",
                "",
                "## Forbidden Behavior",
                "- Не придумывать факты",
                "",
                "## Output Schema",
                "- ОПИСАНИЕ",
                "- ФРАЗЫ",
                "",
                "## Tone Constraints",
                "- Нейтральный стиль",
                "",
                "## System Prompt",
                "```text",
                system_prompt,
                "```",
                "",
                "## User Prompt Template",
                "```text",
                user_prompt_template,
                "```",
                "",
            ],
        ),
        encoding="utf-8",
    )
    return path
