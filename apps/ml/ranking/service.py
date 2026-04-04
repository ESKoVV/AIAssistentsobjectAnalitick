from __future__ import annotations

import math
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Sequence
from uuid import uuid4

from apps.ml.clustering.schema import Cluster
from apps.ml.summarization.schema import StoredClusterDescription

from .config import RankingServiceConfig
from .schema import (
    RankingComputationResult,
    RankingDocumentRecord,
    RankingMetrics,
    RankingRecord,
    RankedCluster,
    RankedSamplePost,
    RankedSourceSummary,
    RankedTimelinePoint,
    RankingsUpdatedEvent,
    ScoreBreakdown,
    StoredRankingSnapshot,
)
from .source_urls import extract_source_url
from .storage import RankingRepositoryProtocol


@dataclass(frozen=True, slots=True)
class _WindowClusterView:
    cluster_id: str
    size: int
    unique_authors: int
    unique_sources: int
    reach_total: int
    growth_rate: float
    geo_regions: tuple[str, ...]
    earliest_doc_at: datetime
    latest_doc_at: datetime
    noise: bool


@dataclass(frozen=True, slots=True)
class _WindowClusterAggregate:
    cluster: Cluster
    view: _WindowClusterView
    summary: str
    key_phrases: list[str]
    sentiment_score: float
    sample_doc_ids: list[str]
    sources: list[RankedSourceSummary]
    sample_posts: list[RankedSamplePost]
    timeline: list[RankedTimelinePoint]


def normalize_volume(size: int, all_sizes: Sequence[int]) -> float:
    return _log_normalize(size, all_sizes)


def normalize_dynamics(growth_rate: float) -> float:
    if growth_rate <= 1.0:
        return max(0.0, (float(growth_rate) - 0.5) / 0.5) * 0.3
    if growth_rate <= 0.0:
        return 0.0
    return min(1.0, math.log(float(growth_rate)) / math.log(5))


def normalize_sentiment(sentiment: float) -> float:
    normalized = max(-1.0, min(1.0, float(sentiment)))
    return (1.0 - normalized) / 2.0


def normalize_reach(reach_total: int, all_reaches: Sequence[int]) -> float:
    return _log_normalize(reach_total, all_reaches)


def normalize_geo(
    geo_regions: Sequence[str],
    total_regions: int,
    *,
    geo_max_coverage_ratio: float,
) -> float:
    unique_regions = len(set(geo_regions))
    threshold = max(total_regions * geo_max_coverage_ratio, 1.0)
    return _clamp(unique_regions / threshold)


def normalize_sources(unique_sources: int, *, source_type_count: int) -> float:
    return _clamp(max(0.0, float(unique_sources)) / float(max(source_type_count, 1)))


def compute_score_breakdown(
    cluster: Cluster | _WindowClusterView,
    sentiment: float,
    *,
    all_sizes: Sequence[int],
    all_reaches: Sequence[int],
    total_regions: int,
    weights: dict[str, float],
    geo_max_coverage_ratio: float,
    source_type_count: int,
) -> ScoreBreakdown:
    return ScoreBreakdown(
        volume_score=normalize_volume(cluster.size, all_sizes),
        dynamics_score=normalize_dynamics(cluster.growth_rate),
        sentiment_score=normalize_sentiment(sentiment),
        reach_score=normalize_reach(cluster.reach_total, all_reaches),
        geo_score=normalize_geo(
            cluster.geo_regions,
            total_regions,
            geo_max_coverage_ratio=geo_max_coverage_ratio,
        ),
        source_score=normalize_sources(
            cluster.unique_sources,
            source_type_count=source_type_count,
        ),
        weights=dict(weights),
    )


def total_score(breakdown: ScoreBreakdown) -> float:
    weights = breakdown.weights
    score = (
        breakdown.volume_score * weights["volume"]
        + breakdown.dynamics_score * weights["dynamics"]
        + breakdown.sentiment_score * weights["sentiment"]
        + breakdown.reach_score * weights["reach"]
        + breakdown.geo_score * weights["geo"]
        + breakdown.source_score * weights["source"]
    )
    return _clamp(score)


def should_exclude(
    cluster: Cluster | _WindowClusterView,
    description: StoredClusterDescription | None,
    *,
    now: datetime,
    min_cluster_size_for_ranking: int,
    stale_after_hours: int,
) -> tuple[bool, str]:
    if cluster.noise:
        return True, "noise_cluster"
    if cluster.size < min_cluster_size_for_ranking:
        return True, "too_small"

    age_hours = (now - cluster.latest_doc_at).total_seconds() / 3600
    if age_hours > stale_after_hours:
        return True, "stale"
    if description is None:
        return True, "missing_description"
    if description.needs_review:
        return True, "needs_review"
    return False, ""


def rank_clusters(
    clusters: Sequence[Cluster],
    documents_by_cluster: dict[str, Sequence[RankingDocumentRecord]],
    descriptions: dict[str, StoredClusterDescription],
    *,
    now: datetime,
    period_hours: int,
    weights: dict[str, float],
    min_cluster_size_for_ranking: int,
    stale_after_hours: int,
    new_cluster_hours: int,
    growing_threshold: float,
    geo_max_coverage_ratio: float,
    source_type_count: int,
) -> tuple[list[RankedCluster], dict[str, int]]:
    period_start = now - timedelta(hours=period_hours)
    period_end = now
    exclusion_reasons: dict[str, int] = defaultdict(int)
    aggregates: list[_WindowClusterAggregate] = []

    for cluster in clusters:
        description = descriptions.get(cluster.cluster_id)
        window_documents = [
            document
            for document in documents_by_cluster.get(cluster.cluster_id, ())
            if period_start <= document.created_at <= period_end
        ]
        if not window_documents:
            exclusion_reasons["empty_window"] += 1
            continue
        if description is None:
            exclusion_reasons["missing_description"] += 1
            continue
        aggregate = _build_window_aggregate(
            cluster=cluster,
            documents=window_documents,
            timeline_documents=documents_by_cluster.get(cluster.cluster_id, ()),
            description=description,
            now=now,
            period_start=period_start,
            period_end=period_end,
        )
        if aggregate is None:
            exclusion_reasons["aggregate_failed"] += 1
            continue

        excluded, reason = should_exclude(
            aggregate.view,
            description,
            now=now,
            min_cluster_size_for_ranking=min_cluster_size_for_ranking,
            stale_after_hours=stale_after_hours,
        )
        if excluded:
            exclusion_reasons[reason] += 1
            continue
        aggregates.append(aggregate)

    all_sizes = [aggregate.view.size for aggregate in aggregates]
    all_reaches = [aggregate.view.reach_total for aggregate in aggregates]
    total_regions = len(
        {
            region
            for aggregate in aggregates
            for region in aggregate.view.geo_regions
        },
    )

    ranked_items: list[RankedCluster] = []
    for aggregate in aggregates:
        breakdown = compute_score_breakdown(
            aggregate.view,
            aggregate.sentiment_score,
            all_sizes=all_sizes,
            all_reaches=all_reaches,
            total_regions=total_regions,
            weights=weights,
            geo_max_coverage_ratio=geo_max_coverage_ratio,
            source_type_count=source_type_count,
        )
        ranked_items.append(
            RankedCluster(
                cluster_id=aggregate.cluster.cluster_id,
                rank=0,
                score=total_score(breakdown),
                score_breakdown=breakdown,
                summary=aggregate.summary,
                key_phrases=list(aggregate.key_phrases),
                period_start=period_start,
                period_end=period_end,
                size=aggregate.view.size,
                mention_count=aggregate.view.size,
                growth_rate=aggregate.view.growth_rate,
                reach_total=aggregate.view.reach_total,
                geo_regions=list(aggregate.view.geo_regions),
                unique_sources=aggregate.view.unique_sources,
                unique_authors=aggregate.view.unique_authors,
                sentiment_score=aggregate.sentiment_score,
                is_new=(now - aggregate.view.earliest_doc_at).total_seconds() < new_cluster_hours * 3600,
                is_growing=aggregate.view.growth_rate > growing_threshold,
                sample_doc_ids=list(aggregate.sample_doc_ids),
                sources=list(aggregate.sources),
                sample_posts=list(aggregate.sample_posts),
                timeline=list(aggregate.timeline),
            ),
        )

    ranked_items.sort(
        key=lambda item: (-item.score, -item.mention_count, -item.reach_total, item.cluster_id),
    )
    for rank, item in enumerate(ranked_items, start=1):
        item.rank = rank

    return ranked_items, dict(exclusion_reasons)


def build_ranking_metrics(
    *,
    computed_at: datetime,
    candidates_total: int,
    exclusion_reasons: dict[str, int],
    items: Sequence[RankedCluster],
    previous_snapshot: StoredRankingSnapshot | None,
    runtime_ms: float,
    top_n: int,
) -> RankingMetrics:
    top_items = list(items[:top_n])
    if top_items:
        top_scores = [item.score for item in top_items]
        score_range = (min(top_scores), max(top_scores))
    else:
        score_range = (0.0, 0.0)

    score_gap = (top_items[0].score - top_items[1].score) if len(top_items) >= 2 else 0.0
    current_ids = {item.cluster_id for item in top_items}
    previous_ids = (
        {item.cluster_id for item in previous_snapshot.items[:top_n]}
        if previous_snapshot is not None
        else set()
    )
    return RankingMetrics(
        computed_at=computed_at,
        candidates_total=candidates_total,
        candidates_excluded=sum(exclusion_reasons.values()),
        exclusion_reasons=dict(exclusion_reasons),
        top10_score_range=score_range,
        score_gap_1_2=score_gap,
        new_entries=len(current_ids - previous_ids),
        dropped_entries=len(previous_ids - current_ids),
        runtime_ms=runtime_ms,
    )


def explain_rank(item: RankedCluster) -> str:
    reasons = [f"{item.mention_count} упоминаний"]
    if item.is_new:
        reasons.append("тема появилась менее 3 часов назад")
    if item.is_growing:
        reasons.append(f"рост в {item.growth_rate:.1f}x за последние 6 часов")
    if len(set(item.geo_regions)) > 1:
        reasons.append(f"охватывает {len(set(item.geo_regions))} районов")

    breakdown = item.score_breakdown
    weights = breakdown.weights
    components = {
        "объём упоминаний": breakdown.volume_score * weights["volume"],
        "динамика роста": breakdown.dynamics_score * weights["dynamics"],
        "негативная тональность": breakdown.sentiment_score * weights["sentiment"],
        "охват аудитории": breakdown.reach_score * weights["reach"],
        "гео-покрытие": breakdown.geo_score * weights["geo"],
        "разнообразие источников": breakdown.source_score * weights["source"],
    }
    top_factors = sorted(components.items(), key=lambda component: component[1], reverse=True)[:2]
    factors_text = " и ".join(name for name, _ in top_factors)
    return f"Ранг {item.rank}: {'; '.join(reasons)}. Основные факторы: {factors_text}."


class RankingService:
    def __init__(
        self,
        *,
        repository: RankingRepositoryProtocol,
        config: RankingServiceConfig,
    ) -> None:
        self._repository = repository
        self._config = config

    def initialize(self) -> None:
        self._repository.ensure_schema()
        self._repository.ensure_upstream_dependencies()

    def refresh_current_window(
        self,
        *,
        now: datetime | None = None,
        period_hours: int | None = None,
        mode: str = "descriptions_updated",
    ) -> RankingComputationResult:
        computed_at = now or datetime.now(UTC)
        window_hours = period_hours or max(self._config.snapshot_period_hours)
        return self._refresh_single_window(
            computed_at=computed_at,
            period_hours=window_hours,
            mode=mode,
        )

    def refresh_all_windows(
        self,
        *,
        now: datetime | None = None,
        mode: str = "descriptions_updated",
    ) -> tuple[RankingComputationResult, ...]:
        computed_at = now or datetime.now(UTC)
        return tuple(
            self._refresh_single_window(
                computed_at=computed_at,
                period_hours=period_hours,
                mode=mode,
            )
            for period_hours in self._config.snapshot_period_hours
        )

    def _refresh_single_window(
        self,
        *,
        computed_at: datetime,
        period_hours: int,
        mode: str,
    ) -> RankingComputationResult:
        started_at = time.perf_counter()
        clusters = self._repository.load_clusters()
        cluster_ids = [cluster.cluster_id for cluster in clusters]
        descriptions = self._repository.load_descriptions_by_ids(cluster_ids)
        documents_by_cluster = self._repository.load_cluster_documents(cluster_ids)
        previous_snapshot = self._repository.load_latest_ranking_snapshot(period_hours=period_hours)

        items, exclusion_reasons = rank_clusters(
            clusters,
            documents_by_cluster,
            descriptions,
            now=computed_at,
            period_hours=period_hours,
            weights=self._config.weights,
            min_cluster_size_for_ranking=self._config.min_cluster_size_for_ranking,
            stale_after_hours=self._config.stale_after_hours,
            new_cluster_hours=self._config.new_cluster_hours,
            growing_threshold=self._config.growing_threshold,
            geo_max_coverage_ratio=self._config.geo_max_coverage_ratio,
            source_type_count=self._config.source_type_count,
        )

        period_start = computed_at - timedelta(hours=period_hours)
        period_end = computed_at
        ranking = RankingRecord(
            ranking_id=str(uuid4()),
            computed_at=computed_at,
            period_start=period_start,
            period_end=period_end,
            weights_config=self._config.weights_config_payload(),
            top_n=self._config.top_n,
            period_hours=period_hours,
        )
        self._repository.save_ranking(ranking=ranking, items=items)

        runtime_ms = (time.perf_counter() - started_at) * 1000
        metrics = build_ranking_metrics(
            computed_at=computed_at,
            candidates_total=len(clusters),
            exclusion_reasons=exclusion_reasons,
            items=items,
            previous_snapshot=previous_snapshot,
            runtime_ms=runtime_ms,
            top_n=self._config.top_n,
        )
        event = RankingsUpdatedEvent(
            ranking_id=ranking.ranking_id,
            computed_at=ranking.computed_at,
            period_start=ranking.period_start,
            period_end=ranking.period_end,
            top_n=ranking.top_n,
            mode=mode,
        )
        return RankingComputationResult(
            ranking=ranking,
            items=tuple(items),
            metrics=metrics,
            event=event,
        )


def _build_window_aggregate(
    *,
    cluster: Cluster,
    documents: Sequence[RankingDocumentRecord],
    timeline_documents: Sequence[RankingDocumentRecord],
    description: StoredClusterDescription | None,
    now: datetime,
    period_start: datetime,
    period_end: datetime,
) -> _WindowClusterAggregate | None:
    window_documents = list(documents)
    if not window_documents or description is None:
        return None

    sorted_docs = sorted(window_documents, key=lambda document: (document.created_at, document.doc_id))
    reach_total = sum(document.reach for document in sorted_docs)
    geo_regions = tuple(
        sorted({document.region for document in sorted_docs if document.region}),
    )
    sources = _build_source_summaries(sorted_docs)
    sample_doc_ids, sample_posts = _select_sample_posts(
        documents=sorted_docs,
        sample_doc_ids=description.description.sample_doc_ids,
    )
    timeline = _build_timeline(timeline_documents, now=now)
    view = _WindowClusterView(
        cluster_id=cluster.cluster_id,
        size=len(sorted_docs),
        unique_authors=len({document.author_id for document in sorted_docs}),
        unique_sources=len({document.source_type for document in sorted_docs}),
        reach_total=reach_total,
        growth_rate=_compute_growth_rate(sorted_docs, now=now),
        geo_regions=geo_regions,
        earliest_doc_at=min(document.created_at for document in sorted_docs),
        latest_doc_at=max(document.created_at for document in sorted_docs),
        noise=cluster.noise,
    )
    return _WindowClusterAggregate(
        cluster=cluster,
        view=view,
        summary=description.description.summary,
        key_phrases=list(description.description.key_phrases),
        sentiment_score=_weighted_sentiment(sorted_docs),
        sample_doc_ids=sample_doc_ids,
        sources=sources,
        sample_posts=sample_posts,
        timeline=timeline,
    )


def _build_source_summaries(documents: Sequence[RankingDocumentRecord]) -> list[RankedSourceSummary]:
    counts: dict[str, int] = defaultdict(int)
    for document in documents:
        counts[document.source_type] += 1
    return [
        RankedSourceSummary(source_type=source_type, count=count)
        for source_type, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _select_sample_posts(
    *,
    documents: Sequence[RankingDocumentRecord],
    sample_doc_ids: Sequence[str],
) -> tuple[list[str], list[RankedSamplePost]]:
    documents_by_id = {document.doc_id: document for document in documents}
    selected: list[RankingDocumentRecord] = []
    seen_doc_ids: set[str] = set()

    for doc_id in sample_doc_ids:
        document = documents_by_id.get(doc_id)
        if document is None or doc_id in seen_doc_ids:
            continue
        selected.append(document)
        seen_doc_ids.add(doc_id)
        if len(selected) >= 20:
            break

    if len(selected) < 20:
        for document in sorted(documents, key=lambda item: (item.created_at, item.doc_id), reverse=True):
            if document.doc_id in seen_doc_ids:
                continue
            selected.append(document)
            seen_doc_ids.add(document.doc_id)
            if len(selected) >= 20:
                break

    sample_posts = [
        RankedSamplePost(
            doc_id=document.doc_id,
            text_preview=_preview_text(document.text),
            source_type=document.source_type,
            created_at=document.created_at,
            reach=document.reach,
            source_url=extract_source_url(
                source_type=document.source_type,
                source_id=document.source_id,
                raw_payload=document.raw_payload,
            ),
        )
        for document in selected[:5]
    ]
    return [document.doc_id for document in selected], sample_posts


def _weighted_sentiment(documents: Sequence[RankingDocumentRecord]) -> float:
    weighted_sum = 0.0
    total_weight = 0
    for document in documents:
        if document.sentiment_score is None:
            continue
        weight = max(document.reach, 1)
        weighted_sum += float(document.sentiment_score) * weight
        total_weight += weight
    if total_weight == 0:
        return 0.0
    return weighted_sum / total_weight


def _compute_growth_rate(documents: Sequence[RankingDocumentRecord], *, now: datetime) -> float:
    recent_start = now - timedelta(hours=6)
    previous_start = now - timedelta(hours=12)
    recent = sum(1 for document in documents if recent_start <= document.created_at <= now)
    previous = sum(1 for document in documents if previous_start <= document.created_at < recent_start)
    return float(recent) / float(max(previous, 1))


def _build_timeline(
    documents: Sequence[RankingDocumentRecord],
    *,
    now: datetime,
    hours: int = 72,
) -> list[RankedTimelinePoint]:
    window_end = _floor_to_hour(now)
    window_start = window_end - timedelta(hours=hours - 1)
    buckets: dict[datetime, dict[str, int]] = {
        window_start + timedelta(hours=index): {"count": 0, "reach": 0}
        for index in range(hours)
    }
    for document in documents:
        hour = _floor_to_hour(document.created_at)
        if hour not in buckets:
            continue
        buckets[hour]["count"] += 1
        buckets[hour]["reach"] += document.reach

    timeline: list[RankedTimelinePoint] = []
    previous_count = 0
    for hour in sorted(buckets):
        count = buckets[hour]["count"]
        reach = buckets[hour]["reach"]
        growth_rate = float(count) / float(max(previous_count, 1)) if previous_count or count else 0.0
        timeline.append(
            RankedTimelinePoint(
                hour=hour,
                count=count,
                reach=reach,
                growth_rate=growth_rate,
            ),
        )
        previous_count = count
    return timeline


def _floor_to_hour(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def _preview_text(text: str) -> str:
    normalized = " ".join(text.split())
    return normalized[:200]


def _log_normalize(value: int, population: Sequence[int]) -> float:
    if not population:
        return 0.0
    normalized_value = math.log1p(max(0, int(value)))
    max_value = math.log1p(max(max(0, int(item)) for item in population))
    if max_value <= 0.0:
        return 0.0
    return _clamp(normalized_value / max_value)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
