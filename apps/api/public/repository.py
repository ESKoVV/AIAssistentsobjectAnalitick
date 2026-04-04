from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

from apps.ml.ranking.source_urls import extract_source_url


@dataclass(frozen=True, slots=True)
class SnapshotRecord:
    ranking_id: str
    computed_at: datetime
    period_start: datetime
    period_end: datetime
    top_n: int
    period_hours: int


@dataclass(frozen=True, slots=True)
class SnapshotSourceRecord:
    source_type: str
    count: int


@dataclass(frozen=True, slots=True)
class SnapshotSampleRecord:
    doc_id: str
    text_preview: str
    source_type: str
    created_at: datetime
    reach: int
    source_url: str | None


@dataclass(frozen=True, slots=True)
class SnapshotTimelineRecord:
    hour: datetime
    count: int
    reach: int
    growth_rate: float


@dataclass(frozen=True, slots=True)
class SnapshotItemRecord:
    cluster_id: str
    rank: int
    score: float
    summary: str
    category: str
    category_label: str
    key_phrases: list[str]
    mention_count: int
    unique_authors: int
    unique_sources: int
    reach_total: int
    growth_rate: float
    geo_regions: list[str]
    score_breakdown: dict[str, float]
    sample_doc_ids: list[str]
    sentiment_score: float
    is_new: bool
    is_growing: bool
    sources: list[SnapshotSourceRecord] = field(default_factory=list)
    sample_posts: list[SnapshotSampleRecord] = field(default_factory=list)
    timeline: list[SnapshotTimelineRecord] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class LiveDocumentRecord:
    doc_id: str
    source_id: str
    source_type: str
    author_id: str
    text: str
    created_at: datetime
    collected_at: datetime
    reach: int
    likes: int
    reposts: int
    comments_count: int
    is_official: bool
    parent_id: str | None
    region: str | None
    raw_payload: dict[str, Any]
    source_url: str | None


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    last_ranking_at: datetime
    ranking_age_minutes: int
    documents_last_hour: int
    pipeline_status: dict[str, str]


class PublicAPIRepository:
    def __init__(self, dsn: str, *, documents_table: str = "normalized_messages") -> None:
        self._dsn = dsn
        self._documents_table = documents_table

    def fetch_snapshot(self, *, period_hours: int, as_of: datetime | None = None) -> SnapshotRecord | None:
        query = """
            SELECT ranking_id, computed_at, period_start, period_end, top_n, period_hours
            FROM rankings
            WHERE period_hours = %(period_hours)s
        """
        params: dict[str, Any] = {"period_hours": period_hours}
        if as_of is not None:
            query += " AND computed_at <= %(as_of)s"
            params["as_of"] = as_of
        query += " ORDER BY computed_at DESC LIMIT 1"

        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()
            connection.commit()
        if row is None:
            return None
        return SnapshotRecord(
            ranking_id=str(row["ranking_id"]),
            computed_at=row["computed_at"],
            period_start=row["period_start"],
            period_end=row["period_end"],
            top_n=int(row["top_n"]),
            period_hours=int(row["period_hours"]),
        )

    def fetch_snapshot_item(self, *, ranking_id: str, cluster_id: str) -> SnapshotItemRecord | None:
        items = self.fetch_snapshot_items(ranking_id=ranking_id, cluster_ids=[cluster_id])
        return items[0] if items else None

    def fetch_latest_snapshot_item_for_cluster(
        self,
        *,
        cluster_id: str,
        period_hours: int = 24,
    ) -> tuple[SnapshotRecord, SnapshotItemRecord] | None:
        query = """
            SELECT r.ranking_id, r.computed_at, r.period_start, r.period_end, r.top_n, r.period_hours
            FROM rankings r
            JOIN ranking_items ri ON ri.ranking_id = r.ranking_id
            WHERE ri.cluster_id = %(cluster_id)s
              AND r.period_hours = %(period_hours)s
            ORDER BY r.computed_at DESC
            LIMIT 1
        """
        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(query, {"cluster_id": cluster_id, "period_hours": period_hours})
                row = cursor.fetchone()
            connection.commit()
        if row is None:
            return None
        snapshot = SnapshotRecord(
            ranking_id=str(row["ranking_id"]),
            computed_at=row["computed_at"],
            period_start=row["period_start"],
            period_end=row["period_end"],
            top_n=int(row["top_n"]),
            period_hours=int(row["period_hours"]),
        )
        item = self.fetch_snapshot_item(ranking_id=snapshot.ranking_id, cluster_id=cluster_id)
        if item is None:
            return None
        return snapshot, item

    def fetch_snapshot_items(
        self,
        *,
        ranking_id: str,
        cluster_ids: Sequence[str] | None = None,
        category: str | None = None,
    ) -> list[SnapshotItemRecord]:
        query = """
            SELECT cluster_id, rank, score, summary, category, category_label, key_phrases, mention_count, unique_authors,
                   unique_sources, reach_total, growth_rate, geo_regions, score_breakdown,
                   sample_doc_ids, sentiment_score, is_new, is_growing
            FROM ranking_items
            WHERE ranking_id = %(ranking_id)s
        """
        params: dict[str, Any] = {"ranking_id": ranking_id}
        if cluster_ids:
            query += " AND cluster_id = ANY(%(cluster_ids)s)"
            params["cluster_ids"] = list(cluster_ids)
        if category:
            query += " AND category = %(category)s"
            params["category"] = category
        query += " ORDER BY rank ASC, cluster_id ASC"

        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
            connection.commit()

        if not rows:
            return []

        cluster_id_list = [str(row["cluster_id"]) for row in rows]
        sources_map = self._fetch_sources(ranking_id=ranking_id, cluster_ids=cluster_id_list)
        samples_map = self._fetch_samples(ranking_id=ranking_id, cluster_ids=cluster_id_list)
        timeline_map = self._fetch_timeline(ranking_id=ranking_id, cluster_ids=cluster_id_list)

        items: list[SnapshotItemRecord] = []
        for row in rows:
            cluster_id = str(row["cluster_id"])
            breakdown_payload = dict(row["score_breakdown"])
            items.append(
                SnapshotItemRecord(
                    cluster_id=cluster_id,
                    rank=int(row["rank"]),
                    score=float(row["score"]),
                    summary=str(row["summary"]),
                    category=str(row["category"]),
                    category_label=str(row["category_label"]),
                    key_phrases=[str(item) for item in row["key_phrases"]],
                    mention_count=int(row["mention_count"]),
                    unique_authors=int(row["unique_authors"]),
                    unique_sources=int(row["unique_sources"]),
                    reach_total=int(row["reach_total"]),
                    growth_rate=float(row["growth_rate"]),
                    geo_regions=[str(item) for item in row["geo_regions"]],
                    score_breakdown={
                        "volume": float(breakdown_payload["volume_score"]),
                        "dynamics": float(breakdown_payload["dynamics_score"]),
                        "sentiment": float(breakdown_payload["sentiment_score"]),
                        "reach": float(breakdown_payload["reach_score"]),
                        "geo": float(breakdown_payload["geo_score"]),
                        "source": float(breakdown_payload["source_score"]),
                    },
                    sample_doc_ids=[str(item) for item in row["sample_doc_ids"]],
                    sentiment_score=float(row["sentiment_score"]),
                    is_new=bool(row["is_new"]),
                    is_growing=bool(row["is_growing"]),
                    sources=sources_map.get(cluster_id, []),
                    sample_posts=samples_map.get(cluster_id, []),
                    timeline=timeline_map.get(cluster_id, []),
                ),
            )
        return items

    def fetch_cluster_documents(
        self,
        *,
        cluster_id: str,
        page: int,
        page_size: int,
        source_type: str | None = None,
        region: str | None = None,
    ) -> tuple[list[LiveDocumentRecord], int]:
        params: dict[str, Any] = {
            "cluster_id": cluster_id,
            "limit": page_size,
            "offset": (page - 1) * page_size,
        }
        filters = ["cd.cluster_id = %(cluster_id)s"]
        if source_type:
            filters.append("d.source_type = %(source_type)s")
            params["source_type"] = source_type
        if region:
            filters.append("d.region_hint = %(region)s")
            params["region"] = region
        where_clause = " AND ".join(filters)

        query = f"""
            SELECT d.doc_id, d.source_id, d.source_type, d.author_id, d.text, d.created_at, d.collected_at,
                   d.reach, d.likes, d.reposts, d.comments_count, d.is_official, d.parent_id, d.region_hint,
                   d.raw_payload
            FROM cluster_documents cd
            JOIN {self._documents_table} d ON d.doc_id = cd.doc_id
            WHERE {where_clause}
            ORDER BY d.created_at DESC, d.doc_id DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        count_query = f"""
            SELECT COUNT(*)
            FROM cluster_documents cd
            JOIN {self._documents_table} d ON d.doc_id = cd.doc_id
            WHERE {where_clause}
        """

        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                cursor.execute(count_query, params)
                total = int(cursor.fetchone()["count"])
            connection.commit()

        documents = [
            LiveDocumentRecord(
                doc_id=str(row["doc_id"]),
                source_id=str(row["source_id"]),
                source_type=str(row["source_type"]),
                author_id=str(row["author_id"]),
                text=str(row["text"]),
                created_at=row["created_at"],
                collected_at=row["collected_at"],
                reach=int(row["reach"]),
                likes=int(row["likes"]),
                reposts=int(row["reposts"]),
                comments_count=int(row["comments_count"]),
                is_official=bool(row["is_official"]),
                parent_id=str(row["parent_id"]) if row["parent_id"] is not None else None,
                region=str(row["region_hint"]) if row["region_hint"] is not None else None,
                raw_payload=dict(row["raw_payload"]),
                source_url=extract_source_url(
                    source_type=str(row["source_type"]),
                    source_id=str(row["source_id"]),
                    raw_payload=dict(row["raw_payload"]),
                ),
            )
            for row in rows
        ]
        return documents, total

    def fetch_cluster_timeline(self, *, cluster_id: str, now: datetime, hours: int = 72) -> list[SnapshotTimelineRecord]:
        window_start = now - timedelta(hours=hours)
        query = f"""
            SELECT date_trunc('hour', d.created_at) AS hour,
                   COUNT(*) AS count,
                   COALESCE(SUM(d.reach), 0) AS reach
            FROM cluster_documents cd
            JOIN {self._documents_table} d ON d.doc_id = cd.doc_id
            WHERE cd.cluster_id = %(cluster_id)s
              AND d.created_at >= %(window_start)s
              AND d.created_at <= %(window_end)s
            GROUP BY hour
            ORDER BY hour ASC
        """
        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(
                    query,
                    {"cluster_id": cluster_id, "window_start": window_start, "window_end": now},
                )
                rows = cursor.fetchall()
            connection.commit()
        counts = {row["hour"].astimezone(timezone.utc): (int(row["count"]), int(row["reach"])) for row in rows}
        return _build_complete_timeline(counts=counts, now=now, hours=hours)

    def fetch_history_snapshots(
        self,
        *,
        from_dt: datetime,
        to_dt: datetime,
        period_hours: int,
    ) -> list[SnapshotRecord]:
        query = """
            SELECT ranking_id, computed_at, period_start, period_end, top_n, period_hours
            FROM rankings
            WHERE period_hours = %(period_hours)s
              AND computed_at >= %(from_dt)s
              AND computed_at <= %(to_dt)s
            ORDER BY computed_at ASC
        """
        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(query, {"period_hours": period_hours, "from_dt": from_dt, "to_dt": to_dt})
                rows = cursor.fetchall()
            connection.commit()
        return [
            SnapshotRecord(
                ranking_id=str(row["ranking_id"]),
                computed_at=row["computed_at"],
                period_start=row["period_start"],
                period_end=row["period_end"],
                top_n=int(row["top_n"]),
                period_hours=int(row["period_hours"]),
            )
            for row in rows
        ]

    def fetch_health_snapshot(self, *, freshness_threshold_minutes: int) -> HealthSnapshot:
        now = datetime.now(timezone.utc)
        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                ranking_row = self._safe_fetch_latest(cursor, "rankings", "computed_at")
                embeddings_row = self._safe_fetch_latest(cursor, "embeddings", "embedded_at")
                clustering_row = self._safe_fetch_latest(cursor, "cluster_runs", "run_at")
                summarization_row = self._safe_fetch_latest(cursor, "cluster_descriptions", "generated_at")
                documents_last_hour = self._safe_count_recent_documents(
                    cursor,
                    window_start=now - timedelta(hours=1),
                )
            connection.commit()

        if ranking_row is None:
            raise RuntimeError("ranking snapshots are not available")

        last_ranking_at = ranking_row["computed_at"]
        ranking_age_minutes = int((now - last_ranking_at).total_seconds() / 60)
        pipeline_status = {
            "embedding": _freshness_status(embeddings_row["embedded_at"] if embeddings_row else None, now, freshness_threshold_minutes),
            "clustering": _freshness_status(clustering_row["run_at"] if clustering_row else None, now, freshness_threshold_minutes),
            "summarization": _freshness_status(
                summarization_row["generated_at"] if summarization_row else None,
                now,
                freshness_threshold_minutes,
            ),
            "ranking": _freshness_status(last_ranking_at, now, freshness_threshold_minutes),
        }

        return HealthSnapshot(
            last_ranking_at=last_ranking_at,
            ranking_age_minutes=ranking_age_minutes,
            documents_last_hour=documents_last_hour,
            pipeline_status=pipeline_status,
        )

    def _fetch_sources(
        self,
        *,
        ranking_id: str,
        cluster_ids: Sequence[str],
    ) -> dict[str, list[SnapshotSourceRecord]]:
        query = """
            SELECT cluster_id, source_type, count
            FROM ranking_item_sources
            WHERE ranking_id = %(ranking_id)s
              AND cluster_id = ANY(%(cluster_ids)s)
            ORDER BY cluster_id ASC, count DESC, source_type ASC
        """
        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(query, {"ranking_id": ranking_id, "cluster_ids": list(cluster_ids)})
                rows = cursor.fetchall()
            connection.commit()
        grouped: dict[str, list[SnapshotSourceRecord]] = defaultdict(list)
        for row in rows:
            grouped[str(row["cluster_id"])].append(
                SnapshotSourceRecord(source_type=str(row["source_type"]), count=int(row["count"])),
            )
        return grouped

    def _fetch_samples(
        self,
        *,
        ranking_id: str,
        cluster_ids: Sequence[str],
    ) -> dict[str, list[SnapshotSampleRecord]]:
        query = """
            SELECT cluster_id, doc_id, text_preview, source_type, created_at, reach, source_url
            FROM ranking_item_samples
            WHERE ranking_id = %(ranking_id)s
              AND cluster_id = ANY(%(cluster_ids)s)
            ORDER BY cluster_id ASC, ordinal ASC
        """
        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(query, {"ranking_id": ranking_id, "cluster_ids": list(cluster_ids)})
                rows = cursor.fetchall()
            connection.commit()
        grouped: dict[str, list[SnapshotSampleRecord]] = defaultdict(list)
        for row in rows:
            grouped[str(row["cluster_id"])].append(
                SnapshotSampleRecord(
                    doc_id=str(row["doc_id"]),
                    text_preview=str(row["text_preview"]),
                    source_type=str(row["source_type"]),
                    created_at=row["created_at"],
                    reach=int(row["reach"]),
                    source_url=str(row["source_url"]) if row["source_url"] is not None else None,
                ),
            )
        return grouped

    def _fetch_timeline(
        self,
        *,
        ranking_id: str,
        cluster_ids: Sequence[str],
    ) -> dict[str, list[SnapshotTimelineRecord]]:
        query = """
            SELECT cluster_id, hour, count, reach, growth_rate
            FROM ranking_item_timeline_hours
            WHERE ranking_id = %(ranking_id)s
              AND cluster_id = ANY(%(cluster_ids)s)
            ORDER BY cluster_id ASC, hour ASC
        """
        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                cursor.execute(query, {"ranking_id": ranking_id, "cluster_ids": list(cluster_ids)})
                rows = cursor.fetchall()
            connection.commit()
        grouped: dict[str, list[SnapshotTimelineRecord]] = defaultdict(list)
        for row in rows:
            grouped[str(row["cluster_id"])].append(
                SnapshotTimelineRecord(
                    hour=row["hour"],
                    count=int(row["count"]),
                    reach=int(row["reach"]),
                    growth_rate=float(row["growth_rate"]),
                ),
            )
        return grouped

    def _connect(self):  # type: ignore[no-untyped-def]
        import psycopg

        return psycopg.connect(self._dsn)

    def _dict_row_factory(self):  # type: ignore[no-untyped-def]
        import psycopg

        return psycopg.rows.dict_row

    def _safe_fetch_latest(self, cursor, table: str, column: str):  # type: ignore[no-untyped-def]
        try:
            cursor.execute(f"SELECT {column} FROM {table} ORDER BY {column} DESC LIMIT 1")
        except Exception:  # noqa: BLE001
            return None
        return cursor.fetchone()

    def _safe_count_recent_documents(self, cursor, *, window_start: datetime) -> int:  # type: ignore[no-untyped-def]
        try:
            cursor.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {self._documents_table}
                WHERE inserted_at >= %(window_start)s
                """,
                {"window_start": window_start},
            )
        except Exception:  # noqa: BLE001
            return 0
        row = cursor.fetchone()
        if row is None:
            return 0
        return int(row["count"])


def _build_complete_timeline(
    *,
    counts: dict[datetime, tuple[int, int]],
    now: datetime,
    hours: int,
) -> list[SnapshotTimelineRecord]:
    end = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=hours - 1)
    timeline: list[SnapshotTimelineRecord] = []
    previous_count = 0
    for index in range(hours):
        hour = start + timedelta(hours=index)
        count, reach = counts.get(hour, (0, 0))
        growth_rate = float(count) / float(max(previous_count, 1)) if previous_count or count else 0.0
        timeline.append(
            SnapshotTimelineRecord(
                hour=hour,
                count=count,
                reach=reach,
                growth_rate=growth_rate,
            ),
        )
        previous_count = count
    return timeline


def _freshness_status(
    timestamp: datetime | None,
    now: datetime,
    freshness_threshold_minutes: int,
) -> str:
    if timestamp is None:
        return "down"
    age_minutes = (now - timestamp).total_seconds() / 60
    if age_minutes <= freshness_threshold_minutes:
        return "ok"
    if age_minutes <= freshness_threshold_minutes * 2:
        return "degraded"
    return "down"
