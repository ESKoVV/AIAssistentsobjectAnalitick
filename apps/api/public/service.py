from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from apps.api.schemas.top import (
    ClusterDetailResponse,
    ClusterDocument,
    ClusterDocumentsQueryParams,
    ClusterDocumentsResponse,
    GranularityLiteral,
    HealthResponse,
    HistoryBucket,
    HistoryQueryParams,
    HistoryResponse,
    SamplePost,
    ScoreBreakdown,
    SourceSummary,
    TimelinePoint,
    TimelineResponse,
    TopItem,
    TopQueryParams,
    TopResponse,
    UrgencyLevel,
)

from .cache import TopCache
from .config import APIConfig, UrgencyConfig
from .errors import NotFoundError, SemanticValidationError, StaleDataError
from .repository import PublicAPIRepository, SnapshotItemRecord, SnapshotRecord


def compute_urgency(item: SnapshotItemRecord, config: UrgencyConfig) -> tuple[UrgencyLevel, str]:
    reasons: list[str] = []

    if item.growth_rate >= config.critical_growth_rate:
        reasons.append(f"рост в {item.growth_rate:.1f}x за 6 часов")
        return UrgencyLevel.CRITICAL, "; ".join(reasons)

    if item.is_new and item.mention_count >= config.critical_new_size:
        reasons.append(f"новая тема с {item.mention_count} упоминаниями за 3 часа")
        return UrgencyLevel.CRITICAL, "; ".join(reasons)

    if len(item.geo_regions) >= config.critical_geo_spread:
        reasons.append(f"охватывает {len(item.geo_regions)} районов")
        if item.sentiment_score < -0.6:
            return UrgencyLevel.CRITICAL, "; ".join(reasons)

    if item.score >= config.high_score_threshold:
        reasons.append(f"высокий суммарный балл {item.score:.2f}")
        return UrgencyLevel.HIGH, "; ".join(reasons)

    if item.is_growing:
        reasons.append("активный рост")
        return UrgencyLevel.HIGH, "; ".join(reasons)

    if item.score >= config.medium_score_threshold:
        return UrgencyLevel.MEDIUM, "устойчивое упоминание"

    return UrgencyLevel.LOW, "фоновый уровень"


class TopAPIService:
    def __init__(
        self,
        *,
        repository: PublicAPIRepository,
        cache: TopCache,
        config: APIConfig,
    ) -> None:
        self._repository = repository
        self._cache = cache
        self._config = config

    def get_top(self, params: TopQueryParams, *, use_cache: bool = True) -> tuple[TopResponse, bool]:
        self._validate_top_params(params)
        if use_cache:
            cached = self._cache.get(params)
            if cached is not None:
                return cached, True

        snapshot = self._repository.fetch_snapshot(
            period_hours=_period_to_hours(params.period),
            as_of=params.as_of,
        )
        if snapshot is None:
            raise NotFoundError(
                error_code="ranking_snapshot_not_found",
                message="Снимок рейтинга для заданного периода не найден",
            )
        if params.as_of is None:
            self._ensure_fresh(snapshot)

        items = self._repository.fetch_snapshot_items(ranking_id=snapshot.ranking_id)
        filtered = [
            item
            for item in items
            if _matches_top_filters(item, region=params.region, source=params.source)
        ]
        total_clusters = len(filtered)
        top_items = [
            self._to_top_item(item, rank=index)
            for index, item in enumerate(filtered[: params.limit], start=1)
        ]
        response = TopResponse(
            computed_at=snapshot.computed_at,
            period_start=snapshot.period_start,
            period_end=snapshot.period_end,
            total_clusters=total_clusters,
            items=top_items,
        )
        if use_cache:
            self._cache.set(params, response)
        return response, False

    def get_cluster_detail(self, cluster_id: str) -> ClusterDetailResponse:
        snapshot_and_item = self._repository.fetch_latest_snapshot_item_for_cluster(cluster_id=cluster_id)
        if snapshot_and_item is None:
            raise NotFoundError(
                error_code="cluster_not_found",
                message=f"Кластер с ID {cluster_id} не найден",
            )
        snapshot, item = snapshot_and_item
        top_item = self._to_top_item(item, rank=item.rank)
        return ClusterDetailResponse(
            **top_item.model_dump(),
            sample_doc_ids=item.sample_doc_ids[:20],
            all_regions=item.geo_regions,
            timeline=[
                TimelinePoint(
                    hour=point.hour,
                    count=point.count,
                    reach=point.reach,
                    growth_rate=point.growth_rate,
                )
                for point in item.timeline
            ],
        )

    def get_cluster_documents(
        self,
        cluster_id: str,
        params: ClusterDocumentsQueryParams,
    ) -> ClusterDocumentsResponse:
        snapshot_and_item = self._repository.fetch_latest_snapshot_item_for_cluster(cluster_id=cluster_id)
        if snapshot_and_item is None:
            raise NotFoundError(
                error_code="cluster_not_found",
                message=f"Кластер с ID {cluster_id} не найден",
            )
        documents, total = self._repository.fetch_cluster_documents(
            cluster_id=cluster_id,
            page=params.page,
            page_size=params.page_size,
            source_type=params.source_type,
            region=params.region,
        )
        items = [
            ClusterDocument(
                doc_id=document.doc_id,
                source_id=document.source_id,
                source_type=document.source_type,
                author_id=document.author_id,
                text=document.text,
                text_preview=_preview_text(document.text),
                created_at=document.created_at,
                collected_at=document.collected_at,
                reach=document.reach,
                likes=document.likes,
                reposts=document.reposts,
                comments_count=document.comments_count,
                is_official=document.is_official,
                parent_id=document.parent_id,
                region=document.region,
                source_url=document.source_url,
                raw_payload=document.raw_payload,
            )
            for document in documents
        ]
        return ClusterDocumentsResponse(
            cluster_id=cluster_id,
            page=params.page,
            page_size=params.page_size,
            total=total,
            items=items,
        )

    def get_cluster_timeline(self, cluster_id: str) -> TimelineResponse:
        snapshot_and_item = self._repository.fetch_latest_snapshot_item_for_cluster(cluster_id=cluster_id)
        if snapshot_and_item is None:
            raise NotFoundError(
                error_code="cluster_not_found",
                message=f"Кластер с ID {cluster_id} не найден",
            )
        timeline = self._repository.fetch_cluster_timeline(cluster_id=cluster_id, now=datetime.now(timezone.utc))
        return TimelineResponse(
            cluster_id=cluster_id,
            points=[
                TimelinePoint(
                    hour=point.hour,
                    count=point.count,
                    reach=point.reach,
                    growth_rate=point.growth_rate,
                )
                for point in timeline
            ],
        )

    def get_history(self, params: HistoryQueryParams) -> HistoryResponse:
        if params.to_dt <= params.from_dt:
            raise SemanticValidationError(
                error_code="invalid_history_range",
                message="to_dt должен быть больше from_dt",
            )
        snapshots = self._repository.fetch_history_snapshots(
            from_dt=params.from_dt,
            to_dt=params.to_dt,
            period_hours=24,
        )
        if not snapshots:
            return HistoryResponse(
                from_dt=params.from_dt,
                to_dt=params.to_dt,
                granularity=params.granularity,
                buckets=[],
            )

        selected_by_bucket: dict[datetime, SnapshotRecord] = {}
        bucket_delta = _granularity_to_delta(params.granularity)
        for snapshot in snapshots:
            bucket_start = _bucket_start(snapshot.computed_at, params.granularity)
            existing = selected_by_bucket.get(bucket_start)
            if existing is None or snapshot.computed_at > existing.computed_at:
                selected_by_bucket[bucket_start] = snapshot

        buckets: list[HistoryBucket] = []
        for bucket_start_dt in sorted(selected_by_bucket):
            snapshot = selected_by_bucket[bucket_start_dt]
            items = self._repository.fetch_snapshot_items(ranking_id=snapshot.ranking_id)
            buckets.append(
                HistoryBucket(
                    bucket_start=bucket_start_dt,
                    bucket_end=bucket_start_dt + bucket_delta,
                    computed_at=snapshot.computed_at,
                    items=[
                        self._to_top_item(item, rank=index)
                        for index, item in enumerate(items[: snapshot.top_n], start=1)
                    ],
                ),
            )
        return HistoryResponse(
            from_dt=params.from_dt,
            to_dt=params.to_dt,
            granularity=params.granularity,
            buckets=buckets,
        )

    def get_health(self) -> HealthResponse:
        health = self._repository.fetch_health_snapshot(
            freshness_threshold_minutes=self._config.freshness_threshold_minutes,
        )
        if health.ranking_age_minutes <= self._config.freshness_threshold_minutes:
            status = "ok"
        elif health.ranking_age_minutes <= self._config.freshness_threshold_minutes * 2:
            status = "degraded"
        else:
            status = "down"
        return HealthResponse(
            status=status,
            last_ranking_at=health.last_ranking_at,
            ranking_age_minutes=health.ranking_age_minutes,
            documents_last_hour=health.documents_last_hour,
            pipeline_status=health.pipeline_status,
        )

    def invalidate_cache(self) -> None:
        self._cache.invalidate_all()

    def warm_cache(self) -> None:
        for query in self._config.warmup_queries:
            try:
                self.get_top(
                    TopQueryParams(
                        period=query.period,
                        limit=query.limit,
                        region=query.region,
                        source=query.source,
                    ),
                )
            except Exception:  # noqa: BLE001
                continue

    def _to_top_item(self, item: SnapshotItemRecord, *, rank: int) -> TopItem:
        urgency, urgency_reason = compute_urgency(item, self._config.urgency)
        return TopItem(
            rank=rank,
            cluster_id=item.cluster_id,
            summary=item.summary,
            key_phrases=item.key_phrases,
            urgency=urgency,
            urgency_reason=urgency_reason,
            mention_count=item.mention_count,
            unique_authors=item.unique_authors,
            reach_total=item.reach_total,
            growth_rate=item.growth_rate,
            is_new=item.is_new,
            is_growing=item.is_growing,
            geo_regions=item.geo_regions,
            sources=[
                SourceSummary(source_type=source.source_type, count=source.count)
                for source in item.sources
            ],
            sample_posts=[
                SamplePost(
                    doc_id=sample.doc_id,
                    text_preview=sample.text_preview,
                    source_type=sample.source_type,
                    created_at=sample.created_at,
                    reach=sample.reach,
                    source_url=sample.source_url,
                )
                for sample in item.sample_posts
            ],
            score=item.score,
            score_breakdown=ScoreBreakdown(**item.score_breakdown),
        )

    def _validate_top_params(self, params: TopQueryParams) -> None:
        if params.as_of is not None and params.as_of > datetime.now(timezone.utc):
            raise SemanticValidationError(
                error_code="as_of_in_future",
                message="Параметр as_of не может указывать в будущее",
            )

    def _ensure_fresh(self, snapshot: SnapshotRecord) -> None:
        age_minutes = (datetime.now(timezone.utc) - snapshot.computed_at).total_seconds() / 60
        if age_minutes > self._config.freshness_threshold_minutes:
            raise StaleDataError()


def _matches_top_filters(
    item: SnapshotItemRecord,
    *,
    region: str | None,
    source: str | None,
) -> bool:
    if region and region not in item.geo_regions:
        return False
    if source and all(entry.source_type != source for entry in item.sources):
        return False
    return True


def _period_to_hours(period: str) -> int:
    return int(period.removesuffix("h"))


def _preview_text(text: str) -> str:
    return " ".join(text.split())[:200]


def _granularity_to_delta(granularity: GranularityLiteral) -> timedelta:
    if granularity == "hourly":
        return timedelta(hours=1)
    if granularity == "6h":
        return timedelta(hours=6)
    return timedelta(days=1)


def _bucket_start(value: datetime, granularity: GranularityLiteral) -> datetime:
    normalized = value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    if granularity == "hourly":
        return normalized
    if granularity == "6h":
        hour = (normalized.hour // 6) * 6
        return normalized.replace(hour=hour)
    return normalized.replace(hour=0)
