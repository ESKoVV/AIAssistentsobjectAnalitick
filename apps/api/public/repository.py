from __future__ import annotations

import base64
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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


@dataclass(frozen=True, slots=True)
class LiveDocumentsSource:
    table_name: str
    kind: str


@dataclass(frozen=True, slots=True)
class LiveSourceRow:
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
    category: str
    category_label: str
    ml_summary: str | None
    ml_score: float | None
    ml_processed_at: datetime | None


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

    def fetch_live_snapshot(
        self,
        *,
        period_hours: int,
        as_of: datetime | None = None,
        category: str | None = None,
        limit: int = 50,
    ) -> tuple[SnapshotRecord, list[SnapshotItemRecord]]:
        computed_at = as_of or datetime.now(UTC)
        period_end = computed_at.astimezone(UTC)
        period_start = period_end - timedelta(hours=period_hours)

        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                source = self._resolve_live_documents_source(cursor)
                if source is None:
                    return _empty_live_snapshot(
                        period_hours=period_hours,
                        period_start=period_start,
                        period_end=period_end,
                    )
                rows = self._fetch_live_rows(
                    cursor,
                    source=source,
                    period_start=period_start,
                    period_end=period_end,
                )
            connection.commit()

        snapshot, items = _build_live_snapshot_payload(
            rows,
            period_hours=period_hours,
            period_start=period_start,
            period_end=period_end,
            category=category,
            limit=limit,
        )
        return snapshot, items

    def fetch_live_cluster_detail(
        self,
        *,
        cluster_id: str,
        period_hours: int = 24,
    ) -> tuple[SnapshotRecord, SnapshotItemRecord] | None:
        snapshot, items = self.fetch_live_snapshot(period_hours=period_hours, limit=200)
        for item in items:
            if item.cluster_id == cluster_id:
                return snapshot, item
        return None

    def fetch_live_cluster_documents(
        self,
        *,
        cluster_id: str,
        page: int,
        page_size: int,
        source_type: str | None = None,
        region: str | None = None,
        period_hours: int = 24,
    ) -> tuple[list[LiveDocumentRecord], int]:
        group = _parse_live_cluster_id(cluster_id)
        if group is None:
            return [], 0

        now = datetime.now(UTC)
        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                source = self._resolve_live_documents_source(cursor)
                if source is None:
                    return [], 0
                rows = self._fetch_live_rows(
                    cursor,
                    source=source,
                    period_start=now - timedelta(hours=period_hours),
                    period_end=now,
                )
            connection.commit()

        documents = [
            _live_row_to_document(row)
            for row in rows
            if _row_matches_live_cluster(row, group)
            and (source_type is None or row.source_type == source_type)
            and (region is None or row.region == region)
        ]
        documents.sort(key=lambda item: (item.created_at, item.doc_id), reverse=True)
        total = len(documents)
        start = max(page - 1, 0) * page_size
        return documents[start : start + page_size], total

    def fetch_live_cluster_timeline(
        self,
        *,
        cluster_id: str,
        now: datetime,
        hours: int = 72,
    ) -> list[SnapshotTimelineRecord]:
        group = _parse_live_cluster_id(cluster_id)
        if group is None:
            return []

        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                source = self._resolve_live_documents_source(cursor)
                if source is None:
                    return []
                rows = self._fetch_live_rows(
                    cursor,
                    source=source,
                    period_start=now - timedelta(hours=hours),
                    period_end=now,
                )
            connection.commit()

        counts: dict[datetime, tuple[int, int]] = defaultdict(lambda: (0, 0))
        for row in rows:
            if not _row_matches_live_cluster(row, group):
                continue
            hour = row.created_at.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
            current_count, current_reach = counts[hour]
            counts[hour] = (current_count + 1, current_reach + row.reach)
        return _build_complete_timeline(counts=dict(counts), now=now, hours=hours)

    def fetch_live_health_snapshot(self, *, freshness_threshold_minutes: int) -> HealthSnapshot:
        now = datetime.now(UTC)
        default_timestamp = now - timedelta(minutes=freshness_threshold_minutes * 3)

        with self._connect() as connection:
            with connection.cursor(row_factory=self._dict_row_factory()) as cursor:
                source = self._resolve_live_documents_source(cursor)
                latest_doc_at = self._safe_fetch_latest_live_document_at(cursor, source)
                latest_ml_at = self._safe_fetch_latest(cursor, "ml_results", "processed_at")
                documents_last_hour = self._safe_count_recent_live_documents(
                    cursor,
                    source=source,
                    window_start=now - timedelta(hours=1),
                )
            connection.commit()

        latest_doc_timestamp = latest_doc_at["timestamp"] if latest_doc_at else None
        latest_ml_timestamp = latest_ml_at["processed_at"] if latest_ml_at else None
        last_ranking_at = latest_ml_timestamp or latest_doc_timestamp or default_timestamp
        ranking_age_minutes = int((now - last_ranking_at).total_seconds() / 60)
        pipeline_status = {
            "embedding": "legacy" if source is not None else "down",
            "clustering": "legacy" if source is not None else "down",
            "summarization": _freshness_status(latest_ml_timestamp, now, freshness_threshold_minutes),
            "ranking": _freshness_status(latest_ml_timestamp or latest_doc_timestamp, now, freshness_threshold_minutes),
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

    def _resolve_live_documents_source(self, cursor) -> LiveDocumentsSource | None:  # type: ignore[no-untyped-def]
        candidates = [self._documents_table]
        if self._documents_table != "normalized_documents":
            candidates.append("normalized_documents")
        if self._documents_table != "normalized_messages":
            candidates.append("normalized_messages")

        for table_name in candidates:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %(table_name)s
                """,
                {"table_name": table_name},
            )
            columns = {str(row["column_name"]) for row in cursor.fetchall()}
            if "doc_id" not in columns:
                continue
            if {"created_at", "collected_at", "source_id", "raw_payload"}.issubset(columns):
                kind = "modern_messages" if table_name == "normalized_messages" else "legacy_documents"
                return LiveDocumentsSource(table_name=table_name, kind=kind)
        return None

    def _fetch_live_rows(
        self,
        cursor,
        *,
        source: LiveDocumentsSource,
        period_start: datetime,
        period_end: datetime,
    ) -> list[LiveSourceRow]:  # type: ignore[no-untyped-def]
        if source.kind == "modern_messages":
            query = f"""
                SELECT d.doc_id,
                       d.source_id,
                       d.source_type,
                       COALESCE(NULLIF(d.author_id, ''), 'unknown') AS author_id,
                       d.text,
                       d.created_at,
                       d.collected_at,
                       COALESCE(d.reach, 0) AS reach,
                       COALESCE(d.likes, 0) AS likes,
                       COALESCE(d.reposts, 0) AS reposts,
                       COALESCE(d.comments_count, 0) AS comments_count,
                       COALESCE(d.is_official, FALSE) AS is_official,
                       d.parent_id,
                       d.region_hint AS region,
                       COALESCE(d.raw_payload, '{{}}'::jsonb) AS raw_payload,
                       COALESCE(NULLIF(d.category, ''), NULLIF(mr.category, ''), 'other') AS category,
                       COALESCE(NULLIF(d.category_label, ''), %(default_category_label)s) AS category_label,
                       mr.summary AS ml_summary,
                       mr.score AS ml_score,
                       mr.processed_at AS ml_processed_at
                FROM {source.table_name} d
                LEFT JOIN ml_results mr ON mr.doc_id = d.doc_id
                WHERE d.created_at >= %(period_start)s
                  AND d.created_at <= %(period_end)s
                  AND COALESCE(NULLIF(d.filter_status, ''), 'pass') <> 'drop'
                ORDER BY d.created_at DESC, d.doc_id DESC
            """
        else:
            query = f"""
                SELECT d.doc_id,
                       d.source_id,
                       d.source_type,
                       COALESCE(NULLIF(d.author_id, ''), 'unknown') AS author_id,
                       d.text,
                       d.created_at,
                       d.collected_at,
                       COALESCE(d.reach, 0) AS reach,
                       COALESCE(d.likes, 0) AS likes,
                       COALESCE(d.reposts, 0) AS reposts,
                       COALESCE(d.comments_count, 0) AS comments_count,
                       COALESCE(d.is_official, FALSE) AS is_official,
                       d.parent_id,
                       d.region_hint AS region,
                       COALESCE(d.raw_payload, '{{}}'::jsonb) AS raw_payload,
                       COALESCE(NULLIF(mr.category, ''), 'other') AS category,
                       %(default_category_label)s AS category_label,
                       mr.summary AS ml_summary,
                       mr.score AS ml_score,
                       mr.processed_at AS ml_processed_at
                FROM {source.table_name} d
                LEFT JOIN ml_results mr ON mr.doc_id = d.doc_id
                WHERE d.created_at >= %(period_start)s
                  AND d.created_at <= %(period_end)s
                ORDER BY d.created_at DESC, d.doc_id DESC
            """

        cursor.execute(
            query,
            {
                "period_start": period_start,
                "period_end": period_end,
                "default_category_label": "Прочее",
            },
        )
        return [
            LiveSourceRow(
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
                region=str(row["region"]) if row["region"] is not None else None,
                raw_payload=dict(row["raw_payload"]),
                category=_normalize_category(str(row["category"])),
                category_label=_normalize_category_label(
                    str(row["category_label"]),
                    category=str(row["category"]),
                ),
                ml_summary=str(row["ml_summary"]) if row["ml_summary"] is not None else None,
                ml_score=float(row["ml_score"]) if row["ml_score"] is not None else None,
                ml_processed_at=row["ml_processed_at"],
            )
            for row in cursor.fetchall()
        ]

    def _safe_fetch_latest_live_document_at(self, cursor, source: LiveDocumentsSource | None):  # type: ignore[no-untyped-def]
        if source is None:
            return None
        try:
            cursor.execute(
                f"""
                SELECT MAX(created_at) AS timestamp
                FROM {source.table_name}
                """,
            )
        except Exception:  # noqa: BLE001
            return None
        return cursor.fetchone()

    def _safe_count_recent_live_documents(
        self,
        cursor,
        *,
        source: LiveDocumentsSource | None,
        window_start: datetime,
    ) -> int:  # type: ignore[no-untyped-def]
        if source is None:
            return 0
        try:
            cursor.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {source.table_name}
                WHERE created_at >= %(window_start)s
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


def _empty_live_snapshot(
    *,
    period_hours: int,
    period_start: datetime,
    period_end: datetime,
) -> tuple[SnapshotRecord, list[SnapshotItemRecord]]:
    snapshot = SnapshotRecord(
        ranking_id=f"live:{period_hours}h:{period_end.isoformat()}",
        computed_at=period_end,
        period_start=period_start,
        period_end=period_end,
        top_n=10,
        period_hours=period_hours,
    )
    return snapshot, []


def _build_live_snapshot_payload(
    rows: Sequence[LiveSourceRow],
    *,
    period_hours: int,
    period_start: datetime,
    period_end: datetime,
    category: str | None,
    limit: int,
) -> tuple[SnapshotRecord, list[SnapshotItemRecord]]:
    if category:
        normalized_category = _normalize_category(category)
        filtered_rows = [row for row in rows if row.category == normalized_category]
    else:
        filtered_rows = list(rows)

    computed_at = _latest_row_timestamp(filtered_rows) or period_end
    snapshot = SnapshotRecord(
        ranking_id=f"live:{period_hours}h:{computed_at.isoformat()}",
        computed_at=computed_at,
        period_start=period_start,
        period_end=period_end,
        top_n=min(max(limit, 1), 50),
        period_hours=period_hours,
    )

    if not filtered_rows:
        return snapshot, []

    grouped: dict[tuple[str, str], list[LiveSourceRow]] = defaultdict(list)
    for row in filtered_rows:
        grouped[_live_group_key(row)].append(row)

    prepared_items = [
        _prepare_live_group_item(group_rows, computed_at=computed_at, period_start=period_start, period_end=period_end)
        for group_rows in grouped.values()
    ]
    prepared_items = [item for item in prepared_items if item is not None]
    if not prepared_items:
        return snapshot, []

    mention_values = [item["mention_count"] for item in prepared_items]
    reach_values = [item["reach_total"] for item in prepared_items]
    growth_values = [item["growth_rate"] for item in prepared_items]

    scored_items: list[tuple[dict[str, Any], dict[str, float], float]] = []
    for item in prepared_items:
        breakdown = _build_live_score_breakdown(
            mention_count=int(item["mention_count"]),
            reach_total=int(item["reach_total"]),
            growth_rate=float(item["growth_rate"]),
            geo_count=len(item["geo_regions"]),
            unique_sources=int(item["unique_sources"]),
            mention_values=mention_values,
            reach_values=reach_values,
            growth_values=growth_values,
        )
        scored_items.append((item, breakdown, _weighted_live_score(breakdown)))

    items: list[SnapshotItemRecord] = []
    for rank, (item, breakdown, score) in enumerate(
        sorted(
            scored_items,
            key=lambda payload: (payload[2], int(payload[0]["mention_count"]), str(payload[0]["cluster_id"])),
            reverse=True,
        )[: snapshot.top_n],
        start=1,
    ):
        items.append(
            SnapshotItemRecord(
                cluster_id=str(item["cluster_id"]),
                rank=rank,
                score=score,
                summary=str(item["summary"]),
                category=str(item["category"]),
                category_label=str(item["category_label"]),
                key_phrases=list(item["key_phrases"]),
                mention_count=int(item["mention_count"]),
                unique_authors=int(item["unique_authors"]),
                unique_sources=int(item["unique_sources"]),
                reach_total=int(item["reach_total"]),
                growth_rate=float(item["growth_rate"]),
                geo_regions=list(item["geo_regions"]),
                score_breakdown=breakdown,
                sample_doc_ids=[sample.doc_id for sample in item["sample_posts"]],
                sentiment_score=0.0,
                is_new=bool(item["is_new"]),
                is_growing=bool(item["is_growing"]),
                sources=list(item["sources"]),
                sample_posts=list(item["sample_posts"]),
                timeline=list(item["timeline"]),
            ),
        )
    return snapshot, items


def _prepare_live_group_item(
    rows: Sequence[LiveSourceRow],
    *,
    computed_at: datetime,
    period_start: datetime,
    period_end: datetime,
) -> dict[str, Any] | None:
    if not rows:
        return None
    sorted_rows = sorted(rows, key=lambda row: (row.created_at, row.doc_id), reverse=True)
    first_row = sorted_rows[0]
    category = first_row.category
    category_label = _select_most_common_label(sorted_rows)
    region = _select_primary_region(sorted_rows)
    geo_regions = sorted({row.region for row in sorted_rows if row.region})
    sources = _build_live_sources(sorted_rows)
    sample_posts = _build_live_sample_posts(sorted_rows)
    growth_rate = _compute_live_growth_rate(sorted_rows, now=computed_at)
    timeline = _build_live_timeline(sorted_rows, now=computed_at)
    summary = _select_live_summary(sorted_rows, fallback_label=category_label)
    return {
        "cluster_id": _build_live_cluster_id(category, region),
        "category": category,
        "category_label": category_label,
        "summary": summary,
        "key_phrases": _extract_live_key_phrases(sorted_rows, fallback_label=category_label),
        "mention_count": len(sorted_rows),
        "unique_authors": len({row.author_id for row in sorted_rows}),
        "unique_sources": len({row.source_type for row in sorted_rows}),
        "reach_total": sum(row.reach for row in sorted_rows),
        "growth_rate": growth_rate,
        "geo_regions": geo_regions,
        "is_new": min(row.created_at for row in sorted_rows) >= computed_at - timedelta(hours=3),
        "is_growing": growth_rate >= 2.0,
        "sources": sources,
        "sample_posts": sample_posts,
        "timeline": timeline,
        "period_start": period_start,
        "period_end": period_end,
    }


def _build_live_sources(rows: Sequence[LiveSourceRow]) -> list[SnapshotSourceRecord]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row.source_type] += 1
    return [
        SnapshotSourceRecord(source_type=source_type, count=count)
        for source_type, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _build_live_sample_posts(rows: Sequence[LiveSourceRow], *, limit: int = 5) -> list[SnapshotSampleRecord]:
    selected = sorted(
        rows,
        key=lambda row: (row.reach, row.created_at, row.doc_id),
        reverse=True,
    )[:limit]
    return [
        SnapshotSampleRecord(
            doc_id=row.doc_id,
            text_preview=_preview_text(row.text),
            source_type=row.source_type,
            created_at=row.created_at,
            reach=row.reach,
            source_url=extract_source_url(
                source_type=row.source_type,
                source_id=row.source_id,
                raw_payload=row.raw_payload,
            ),
        )
        for row in selected
    ]


def _build_live_timeline(rows: Sequence[LiveSourceRow], *, now: datetime, hours: int = 72) -> list[SnapshotTimelineRecord]:
    counts: dict[datetime, tuple[int, int]] = defaultdict(lambda: (0, 0))
    for row in rows:
        hour = row.created_at.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        current_count, current_reach = counts[hour]
        counts[hour] = (current_count + 1, current_reach + row.reach)
    return _build_complete_timeline(counts=dict(counts), now=now, hours=hours)


def _compute_live_growth_rate(rows: Sequence[LiveSourceRow], *, now: datetime) -> float:
    recent_start = now - timedelta(hours=6)
    previous_start = recent_start - timedelta(hours=6)
    recent_count = sum(1 for row in rows if recent_start <= row.created_at <= now)
    previous_count = sum(1 for row in rows if previous_start <= row.created_at < recent_start)
    if recent_count == 0 and previous_count == 0:
        return 0.0
    if previous_count == 0:
        return float(recent_count)
    return float(recent_count) / float(previous_count)


def _build_live_score_breakdown(
    *,
    mention_count: int,
    reach_total: int,
    growth_rate: float,
    geo_count: int,
    unique_sources: int,
    mention_values: Sequence[int],
    reach_values: Sequence[int],
    growth_values: Sequence[float],
) -> dict[str, float]:
    volume = _log_normalize(mention_count, mention_values)
    reach = _log_normalize(reach_total, reach_values)
    dynamics = _normalize_live_dynamics(growth_rate, growth_values)
    geo = min(float(geo_count) / 3.0, 1.0)
    source = min(float(unique_sources) / 4.0, 1.0)
    return {
        "volume": volume,
        "dynamics": dynamics,
        "sentiment": 0.5,
        "reach": reach,
        "geo": geo,
        "source": source,
    }


def _weighted_live_score(breakdown: dict[str, float]) -> float:
    weights = {
        "volume": 0.25,
        "dynamics": 0.25,
        "sentiment": 0.20,
        "reach": 0.15,
        "geo": 0.10,
        "source": 0.05,
    }
    return round(sum(float(breakdown[key]) * weight for key, weight in weights.items()), 4)


def _log_normalize(value: int, values: Sequence[int]) -> float:
    sanitized = [max(int(item), 0) for item in values if item is not None]
    if not sanitized:
        return 0.0
    max_value = max(sanitized)
    if max_value <= 0:
        return 0.0
    return min(math.log1p(max(int(value), 0)) / math.log1p(max_value), 1.0)


def _normalize_live_dynamics(value: float, values: Sequence[float]) -> float:
    positive_values = [max(float(item), 0.0) for item in values]
    max_value = max(positive_values, default=0.0)
    if max_value <= 0.0:
        return 0.0
    if value <= 1.0:
        return max(value, 0.0) / max(max_value, 1.0) * 0.5
    return min(math.log1p(value) / math.log1p(max_value), 1.0)


def _select_live_summary(rows: Sequence[LiveSourceRow], *, fallback_label: str) -> str:
    for row in rows:
        summary = (row.ml_summary or "").strip()
        if summary:
            return summary
    for row in rows:
        text = _preview_text(row.text)
        if text:
            return text
    return fallback_label


def _extract_live_key_phrases(rows: Sequence[LiveSourceRow], *, fallback_label: str) -> list[str]:
    counter: dict[str, int] = defaultdict(int)
    for row in rows[:20]:
        for token in re.findall(r"[A-Za-zА-Яа-яЁё]{4,}", f"{row.ml_summary or ''} {row.text}".casefold()):
            if token in _STOP_WORDS:
                continue
            counter[token] += 1
    if not counter:
        return [fallback_label]
    return [token for token, _ in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:5]]


def _live_row_to_document(row: LiveSourceRow) -> LiveDocumentRecord:
    return LiveDocumentRecord(
        doc_id=row.doc_id,
        source_id=row.source_id,
        source_type=row.source_type,
        author_id=row.author_id,
        text=row.text,
        created_at=row.created_at,
        collected_at=row.collected_at,
        reach=row.reach,
        likes=row.likes,
        reposts=row.reposts,
        comments_count=row.comments_count,
        is_official=row.is_official,
        parent_id=row.parent_id,
        region=row.region,
        raw_payload=row.raw_payload,
        source_url=extract_source_url(
            source_type=row.source_type,
            source_id=row.source_id,
            raw_payload=row.raw_payload,
        ),
    )


def _row_matches_live_cluster(row: LiveSourceRow, group: tuple[str, str | None]) -> bool:
    category, region = group
    if row.category != category:
        return False
    if region is None:
        return row.region is None
    return row.region == region


def _live_group_key(row: LiveSourceRow) -> tuple[str, str]:
    return row.category, row.region or ""


def _build_live_cluster_id(category: str, region: str | None) -> str:
    return f"live:{_encode_live_segment(category)}:{_encode_live_segment(region or '')}"


def _parse_live_cluster_id(cluster_id: str) -> tuple[str, str | None] | None:
    if not cluster_id.startswith("live:"):
        return None
    parts = cluster_id.split(":", 2)
    if len(parts) != 3:
        return None
    category = _decode_live_segment(parts[1])
    region = _decode_live_segment(parts[2])
    return category, region or None


def _encode_live_segment(value: str) -> str:
    encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def _decode_live_segment(value: str) -> str:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}".encode("ascii")).decode("utf-8")


def _normalize_category(value: str) -> str:
    normalized = value.strip().lower()
    return normalized or "other"


def _normalize_category_label(value: str, *, category: str) -> str:
    normalized = value.strip()
    if normalized and normalized != "Прочее":
        return normalized
    return _CATEGORY_LABELS.get(_normalize_category(category), "Прочее")


def _select_most_common_label(rows: Sequence[LiveSourceRow]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[_normalize_category_label(row.category_label, category=row.category)] += 1
    label, _ = max(counts.items(), key=lambda item: (item[1], item[0]))
    return label


def _select_primary_region(rows: Sequence[LiveSourceRow]) -> str | None:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        if row.region:
            counts[row.region] += 1
    if not counts:
        return None
    region, _ = max(counts.items(), key=lambda item: (item[1], item[0]))
    return region


def _latest_row_timestamp(rows: Sequence[LiveSourceRow]) -> datetime | None:
    timestamps = [
        row.ml_processed_at or row.created_at
        for row in rows
    ]
    return max(timestamps) if timestamps else None


_CATEGORY_LABELS = {
    "housing": "ЖКХ",
    "roads": "Дороги и транспорт",
    "health": "Здравоохранение",
    "education": "Образование",
    "ecology": "Экология",
    "safety": "Безопасность",
    "other": "Прочее",
    "unclassified": "Прочее",
}

_STOP_WORDS = {
    "были",
    "будет",
    "вновь",
    "вода",
    "время",
    "городе",
    "граждан",
    "жители",
    "житель",
    "из-за",
    "когда",
    "который",
    "между",
    "нужно",
    "области",
    "пишут",
    "после",
    "проблема",
    "проблемы",
    "районе",
    "снова",
    "сегодня",
    "ситуация",
    "также",
    "только",
    "через",
    "этого",
}


def _preview_text(text: str) -> str:
    return " ".join(text.split())[:200]
