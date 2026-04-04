from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, Sequence

from apps.ml.clustering.schema import Cluster
from apps.ml.summarization.schema import ClusterDescription, StoredClusterDescription

from .schema import (
    RankingDocumentRecord,
    RankingRecord,
    RankedCluster,
    ScoreBreakdown,
    StoredRankingItem,
    StoredRankingSnapshot,
)


CREATE_RANKINGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS rankings (
    ranking_id TEXT PRIMARY KEY,
    computed_at TIMESTAMPTZ NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    weights_config JSONB NOT NULL,
    top_n INTEGER NOT NULL DEFAULT 10,
    period_hours INTEGER NOT NULL DEFAULT 24
)
"""

CREATE_RANKING_ITEMS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ranking_items (
    ranking_id TEXT NOT NULL REFERENCES rankings(ranking_id) ON DELETE CASCADE,
    cluster_id TEXT NOT NULL REFERENCES clusters(cluster_id),
    rank INTEGER NOT NULL,
    score FLOAT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    key_phrases TEXT[] NOT NULL DEFAULT '{}',
    mention_count INTEGER NOT NULL DEFAULT 0,
    unique_authors INTEGER NOT NULL DEFAULT 0,
    unique_sources INTEGER NOT NULL DEFAULT 0,
    reach_total BIGINT NOT NULL DEFAULT 0,
    growth_rate FLOAT NOT NULL DEFAULT 0.0,
    geo_regions TEXT[] NOT NULL DEFAULT '{}',
    score_breakdown JSONB NOT NULL,
    sample_doc_ids TEXT[] NOT NULL DEFAULT '{}',
    sentiment_score FLOAT NOT NULL,
    is_new BOOLEAN NOT NULL,
    is_growing BOOLEAN NOT NULL,
    category TEXT NOT NULL DEFAULT 'other',
    category_label TEXT NOT NULL DEFAULT 'Прочее',
    PRIMARY KEY (ranking_id, cluster_id)
)
"""

ALTER_RANKINGS_PERIOD_HOURS_SQL = """
ALTER TABLE rankings
ADD COLUMN IF NOT EXISTS period_hours INTEGER NOT NULL DEFAULT 24
"""

ALTER_RANKING_ITEMS_COLUMNS_SQL = (
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS summary TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS key_phrases TEXT[] NOT NULL DEFAULT '{}'",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS mention_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS unique_authors INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS unique_sources INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS reach_total BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS growth_rate FLOAT NOT NULL DEFAULT 0.0",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS geo_regions TEXT[] NOT NULL DEFAULT '{}'",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS sample_doc_ids TEXT[] NOT NULL DEFAULT '{}'",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'other'",
    "ALTER TABLE ranking_items ADD COLUMN IF NOT EXISTS category_label TEXT NOT NULL DEFAULT 'Прочее'",
)

CREATE_RANKING_ITEM_SOURCES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ranking_item_sources (
    ranking_id TEXT NOT NULL REFERENCES rankings(ranking_id) ON DELETE CASCADE,
    cluster_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (ranking_id, cluster_id, source_type)
)
"""

CREATE_RANKING_ITEM_SAMPLES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ranking_item_samples (
    ranking_id TEXT NOT NULL REFERENCES rankings(ranking_id) ON DELETE CASCADE,
    cluster_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    doc_id TEXT NOT NULL,
    text_preview TEXT NOT NULL,
    source_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    reach BIGINT NOT NULL,
    source_url TEXT NULL,
    PRIMARY KEY (ranking_id, cluster_id, ordinal)
)
"""

CREATE_RANKING_ITEM_TIMELINE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ranking_item_timeline_hours (
    ranking_id TEXT NOT NULL REFERENCES rankings(ranking_id) ON DELETE CASCADE,
    cluster_id TEXT NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    count INTEGER NOT NULL,
    reach BIGINT NOT NULL,
    growth_rate FLOAT NOT NULL,
    PRIMARY KEY (ranking_id, cluster_id, hour)
)
"""

CREATE_RANKINGS_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS rankings_period_computed_at_desc_idx
ON rankings (period_hours, computed_at DESC)
"""

CREATE_RANKING_ITEMS_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ranking_items_ranking_rank_idx
ON ranking_items (ranking_id, rank)
"""


class RankingRepositoryProtocol(Protocol):
    def ensure_schema(self) -> None:
        ...

    def ensure_upstream_dependencies(self) -> None:
        ...

    def load_clusters(self) -> tuple[Cluster, ...]:
        ...

    def load_cluster_documents(
        self,
        cluster_ids: Sequence[str],
    ) -> dict[str, tuple[RankingDocumentRecord, ...]]:
        ...

    def load_descriptions_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, StoredClusterDescription]:
        ...

    def load_latest_ranking_snapshot(self, *, period_hours: int | None = None) -> StoredRankingSnapshot | None:
        ...

    def save_ranking(self, *, ranking: RankingRecord, items: Sequence[RankedCluster]) -> None:
        ...


class InMemoryRankingRepository:
    def __init__(self) -> None:
        self.clusters: dict[str, Cluster] = {}
        self.descriptions: dict[str, StoredClusterDescription] = {}
        self.documents: dict[str, RankingDocumentRecord] = {}
        self.document_sentiments: dict[str, float] = {}
        self.document_reaches: dict[str, int] = {}
        self.document_quality_weights: dict[str, float] = {}
        self.rankings: list[StoredRankingSnapshot] = []
        self.upstream_ready = True

    def ensure_schema(self) -> None:
        return None

    def ensure_upstream_dependencies(self) -> None:
        if not self.upstream_ready:
            raise RuntimeError(
                "required upstream tables clusters/cluster_descriptions/cluster_documents/documents/document_sentiments are not available",
            )

    def load_clusters(self) -> tuple[Cluster, ...]:
        return tuple(self.clusters[cluster_id] for cluster_id in sorted(self.clusters))

    def load_cluster_documents(
        self,
        cluster_ids: Sequence[str],
    ) -> dict[str, tuple[RankingDocumentRecord, ...]]:
        grouped: dict[str, tuple[RankingDocumentRecord, ...]] = {}
        for cluster_id in cluster_ids:
            cluster = self.clusters.get(cluster_id)
            if cluster is None:
                continue
            documents: list[RankingDocumentRecord] = []
            source_pool = ("vk_post", "rss_article", "portal_appeal", "max_post")
            known_doc_ids = list(cluster.doc_ids)
            synthetic_needed = max(cluster.size - len(known_doc_ids), 0)
            effective_doc_ids = known_doc_ids + [
                f"{cluster.cluster_id}-synthetic-{index + 1}"
                for index in range(synthetic_needed)
            ]
            sentiment_values = list(self.document_sentiments.values())
            default_sentiment = (
                sum(float(value) for value in sentiment_values) / len(sentiment_values)
                if sentiment_values
                else 0.0
            )
            per_doc_reach = max(int(cluster.reach_total / max(cluster.size, 1)), 0)
            total_span_seconds = max(
                int((cluster.latest_doc_at - cluster.earliest_doc_at).total_seconds()),
                0,
            )

            for index, doc_id in enumerate(effective_doc_ids):
                document = self.documents.get(doc_id)
                if document is None:
                    offset_seconds = int(total_span_seconds * (index / max(len(effective_doc_ids) - 1, 1)))
                    created_at = cluster.earliest_doc_at + timedelta(seconds=offset_seconds)
                    documents.append(
                        RankingDocumentRecord(
                            doc_id=doc_id,
                            source_id=doc_id,
                            author_id=f"author-{(index % max(cluster.unique_authors, 1)) + 1}",
                            source_type=source_pool[index % min(max(cluster.unique_sources, 1), len(source_pool))],
                            text=f"Document {doc_id}",
                            created_at=created_at,
                            reach=self.document_reaches.get(doc_id, per_doc_reach),
                            region=cluster.geo_regions[index % len(cluster.geo_regions)] if cluster.geo_regions else None,
                            raw_payload={},
                            quality_weight=self.document_quality_weights.get(doc_id, 1.0),
                            sentiment_score=self.document_sentiments.get(doc_id, default_sentiment),
                            category="other",
                            category_label="Прочее",
                        ),
                    )
                    continue
                documents.append(
                    RankingDocumentRecord(
                        doc_id=document.doc_id,
                        source_id=document.source_id,
                        author_id=document.author_id,
                        source_type=document.source_type,
                        text=document.text,
                        created_at=document.created_at,
                        reach=document.reach,
                        region=document.region,
                        raw_payload=dict(document.raw_payload),
                        quality_weight=self.document_quality_weights.get(doc_id, document.quality_weight),
                        sentiment_score=self.document_sentiments.get(doc_id, document.sentiment_score),
                        category=document.category,
                        category_label=document.category_label,
                    ),
                )
            grouped[cluster_id] = tuple(documents)
        return grouped

    def load_descriptions_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, StoredClusterDescription]:
        return {
            cluster_id: self.descriptions[cluster_id]
            for cluster_id in cluster_ids
            if cluster_id in self.descriptions
        }

    def load_latest_ranking_snapshot(self, *, period_hours: int | None = None) -> StoredRankingSnapshot | None:
        if not self.rankings:
            return None
        for snapshot in reversed(self.rankings):
            if period_hours is None or snapshot.ranking.period_hours == period_hours:
                return snapshot
        return None

    def save_ranking(self, *, ranking: RankingRecord, items: Sequence[RankedCluster]) -> None:
        snapshot = StoredRankingSnapshot(
            ranking=ranking,
            items=tuple(
                StoredRankingItem(
                    cluster_id=item.cluster_id,
                    rank=item.rank,
                    score=item.score,
                    score_breakdown=item.score_breakdown,
                    sentiment_score=item.sentiment_score,
                    mention_count=item.mention_count,
                    reach_total=item.reach_total,
                    growth_rate=item.growth_rate,
                    geo_regions=tuple(item.geo_regions),
                    unique_authors=item.unique_authors,
                    unique_sources=item.unique_sources,
                    is_new=item.is_new,
                    is_growing=item.is_growing,
                    category=item.category,
                    category_label=item.category_label,
                )
                for item in items
            ),
        )
        self.rankings.append(snapshot)


class PostgresRankingRepository:
    def __init__(
        self,
        dsn: str,
        *,
        documents_table: str = "normalized_messages",
        sentiments_table: str = "document_sentiments",
    ) -> None:
        self._dsn = dsn
        self._documents_table = documents_table
        self._sentiments_table = sentiments_table

    def ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_RANKINGS_TABLE_SQL)
                cursor.execute(CREATE_RANKING_ITEMS_TABLE_SQL)
                cursor.execute(ALTER_RANKINGS_PERIOD_HOURS_SQL)
                for statement in ALTER_RANKING_ITEMS_COLUMNS_SQL:
                    cursor.execute(statement)
                cursor.execute(CREATE_RANKING_ITEM_SOURCES_TABLE_SQL)
                cursor.execute(CREATE_RANKING_ITEM_SAMPLES_TABLE_SQL)
                cursor.execute(CREATE_RANKING_ITEM_TIMELINE_TABLE_SQL)
                cursor.execute(CREATE_RANKINGS_INDEX_SQL)
                cursor.execute(CREATE_RANKING_ITEMS_INDEX_SQL)
            connection.commit()

    def ensure_upstream_dependencies(self) -> None:
        required_tables = {
            "clusters",
            "cluster_documents",
            "cluster_descriptions",
            self._documents_table,
            self._sentiments_table,
        }
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = ANY(%s)
                    """,
                    (list(required_tables),),
                )
                rows = {row[0] for row in cursor.fetchall()}
            connection.commit()

        if rows != required_tables:
            raise RuntimeError(
                "required upstream tables clusters/cluster_descriptions/cluster_documents/documents/document_sentiments are not available",
            )

    def load_clusters(self) -> tuple[Cluster, ...]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cluster_id, centroid, size, unique_authors, unique_sources, reach_total,
                           growth_rate, cohesion_score, noise, period_start, period_end,
                           earliest_doc_at, latest_doc_at, geo_regions, algorithm_params, created_at
                    FROM clusters
                    ORDER BY cluster_id ASC
                    """,
                )
                rows = cursor.fetchall()
            connection.commit()
        return tuple(_row_to_cluster(row) for row in rows)

    def load_cluster_documents(
        self,
        cluster_ids: Sequence[str],
    ) -> dict[str, tuple[RankingDocumentRecord, ...]]:
        if not cluster_ids:
            return {}

        query = f"""
            SELECT cd.cluster_id,
                   d.doc_id,
                   d.source_id,
                   d.author_id,
                   d.source_type,
                   d.text,
                   d.created_at,
                   d.reach,
                   d.region_hint,
                   d.raw_payload,
                   COALESCE(d.quality_weight, 1.0) AS quality_weight,
                   ds.sentiment_score,
                   COALESCE(d.category, 'other') AS category,
                   COALESCE(d.category_label, 'Прочее') AS category_label
            FROM cluster_documents cd
            JOIN {self._documents_table} d ON d.doc_id = cd.doc_id
            LEFT JOIN {self._sentiments_table} ds ON ds.doc_id = cd.doc_id
            WHERE cd.cluster_id = ANY(%s)
            ORDER BY cd.cluster_id ASC, d.created_at ASC, d.doc_id ASC
        """
        grouped: dict[str, list[RankingDocumentRecord]] = defaultdict(list)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (list(cluster_ids),))
                for row in cursor.fetchall():
                    grouped[str(row[0])].append(_row_to_document_record(row[1:]))
            connection.commit()
        return {cluster_id: tuple(records) for cluster_id, records in grouped.items()}

    def load_descriptions_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, StoredClusterDescription]:
        if not cluster_ids:
            return {}

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cluster_id, summary, key_phrases, sample_doc_ids, model_name, prompt_version,
                           generated_at, input_token_count, output_token_count, generation_time_ms,
                           fallback_used, needs_review, cluster_size_at_generation
                    FROM cluster_descriptions
                    WHERE cluster_id = ANY(%s)
                    """,
                    (list(cluster_ids),),
                )
                rows = cursor.fetchall()
            connection.commit()
        return {
            description.description.cluster_id: description
            for description in (_row_to_stored_description(row) for row in rows)
        }

    def load_latest_ranking_snapshot(self, *, period_hours: int | None = None) -> StoredRankingSnapshot | None:
        ranking_query = """
            SELECT ranking_id, computed_at, period_start, period_end, weights_config, top_n, period_hours
            FROM rankings
        """
        params: tuple[Any, ...] = ()
        if period_hours is not None:
            ranking_query += " WHERE period_hours = %s"
            params = (period_hours,)
        ranking_query += " ORDER BY computed_at DESC LIMIT 1"

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(ranking_query, params)
                ranking_row = cursor.fetchone()
                if ranking_row is None:
                    connection.commit()
                    return None

                cursor.execute(
                    """
                    SELECT cluster_id, rank, score, score_breakdown, sentiment_score, mention_count,
                           reach_total, growth_rate, geo_regions, unique_authors, unique_sources,
                           is_new, is_growing, category, category_label
                    FROM ranking_items
                    WHERE ranking_id = %s
                    ORDER BY rank ASC, cluster_id ASC
                    """,
                    (str(ranking_row[0]),),
                )
                item_rows = cursor.fetchall()
            connection.commit()

        ranking = RankingRecord(
            ranking_id=str(ranking_row[0]),
            computed_at=ranking_row[1],
            period_start=ranking_row[2],
            period_end=ranking_row[3],
            weights_config=dict(ranking_row[4]),
            top_n=int(ranking_row[5]),
            period_hours=int(ranking_row[6]),
        )
        items = tuple(_row_to_stored_ranking_item(row) for row in item_rows)
        return StoredRankingSnapshot(ranking=ranking, items=items)

    def save_ranking(self, *, ranking: RankingRecord, items: Sequence[RankedCluster]) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO rankings (
                        ranking_id, computed_at, period_start, period_end, weights_config, top_n, period_hours
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                    """,
                    (
                        ranking.ranking_id,
                        ranking.computed_at,
                        ranking.period_start,
                        ranking.period_end,
                        json.dumps(ranking.weights_config),
                        ranking.top_n,
                        ranking.period_hours,
                    ),
                )
                for item in items:
                    cursor.execute(
                        """
                        INSERT INTO ranking_items (
                            ranking_id, cluster_id, rank, score, summary, key_phrases, mention_count,
                            unique_authors, unique_sources, reach_total, growth_rate, geo_regions,
                            score_breakdown, sample_doc_ids, sentiment_score, is_new, is_growing,
                            category, category_label
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            ranking.ranking_id,
                            item.cluster_id,
                            item.rank,
                            item.score,
                            item.summary,
                            item.key_phrases,
                            item.mention_count,
                            item.unique_authors,
                            item.unique_sources,
                            item.reach_total,
                            item.growth_rate,
                            item.geo_regions,
                            json.dumps(_score_breakdown_payload(item.score_breakdown)),
                            item.sample_doc_ids,
                            item.sentiment_score,
                            item.is_new,
                            item.is_growing,
                            item.category,
                            item.category_label,
                        ),
                    )
                    for source in item.sources:
                        cursor.execute(
                            """
                            INSERT INTO ranking_item_sources (ranking_id, cluster_id, source_type, count)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (
                                ranking.ranking_id,
                                item.cluster_id,
                                source.source_type,
                                source.count,
                            ),
                        )
                    for ordinal, sample in enumerate(item.sample_posts, start=1):
                        cursor.execute(
                            """
                            INSERT INTO ranking_item_samples (
                                ranking_id, cluster_id, ordinal, doc_id, text_preview, source_type,
                                created_at, reach, source_url
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                ranking.ranking_id,
                                item.cluster_id,
                                ordinal,
                                sample.doc_id,
                                sample.text_preview,
                                sample.source_type,
                                sample.created_at,
                                sample.reach,
                                sample.source_url,
                            ),
                        )
                    for point in item.timeline:
                        cursor.execute(
                            """
                            INSERT INTO ranking_item_timeline_hours (
                                ranking_id, cluster_id, hour, count, reach, growth_rate
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                ranking.ranking_id,
                                item.cluster_id,
                                point.hour,
                                point.count,
                                point.reach,
                                point.growth_rate,
                            ),
                        )
            connection.commit()

    def _connect(self):  # type: ignore[no-untyped-def]
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "PostgresRankingRepository requires 'psycopg[binary]' to be installed",
            ) from exc

        return psycopg.connect(self._dsn)


def _row_to_cluster(row: Sequence[Any]) -> Cluster:
    return Cluster(
        cluster_id=str(row[0]),
        doc_ids=[],
        centroid=_parse_vector(row[1]),
        size=int(row[2]),
        unique_authors=int(row[3]),
        unique_sources=int(row[4]),
        reach_total=int(row[5]),
        growth_rate=float(row[6]),
        cohesion_score=float(row[7]),
        noise=bool(row[8]),
        period_start=row[9],
        period_end=row[10],
        earliest_doc_at=row[11],
        latest_doc_at=row[12],
        geo_regions=list(row[13]),
        algorithm_params=dict(row[14]),
        created_at=row[15],
    )


def _row_to_document_record(row: Sequence[Any]) -> RankingDocumentRecord:
    return RankingDocumentRecord(
        doc_id=str(row[0]),
        source_id=str(row[1]),
        author_id=str(row[2]),
        source_type=str(row[3]),
        text=str(row[4]),
        created_at=row[5],
        reach=int(row[6]),
        region=str(row[7]) if row[7] is not None else None,
        raw_payload=dict(row[8]),
        quality_weight=float(row[9]),
        sentiment_score=float(row[10]) if row[10] is not None else None,
        category=str(row[11]),
        category_label=str(row[12]),
    )


def _row_to_stored_description(row: Sequence[Any]) -> StoredClusterDescription:
    return StoredClusterDescription(
        description=ClusterDescription(
            cluster_id=str(row[0]),
            summary=str(row[1]),
            key_phrases=[str(item) for item in row[2]],
            sample_doc_ids=[str(item) for item in row[3]],
            model_name=str(row[4]),
            prompt_version=str(row[5]),
            generated_at=row[6],
            input_token_count=int(row[7]),
            output_token_count=int(row[8]),
            generation_time_ms=int(row[9]),
            fallback_used=bool(row[10]),
        ),
        needs_review=bool(row[11]),
        cluster_size_at_generation=int(row[12]),
    )


def _row_to_stored_ranking_item(row: Sequence[Any]) -> StoredRankingItem:
    return StoredRankingItem(
        cluster_id=str(row[0]),
        rank=int(row[1]),
        score=float(row[2]),
        score_breakdown=_score_breakdown_from_payload(dict(row[3])),
        sentiment_score=float(row[4]),
        mention_count=int(row[5]),
        reach_total=int(row[6]),
        growth_rate=float(row[7]),
        geo_regions=tuple(str(item) for item in row[8]),
        unique_authors=int(row[9]),
        unique_sources=int(row[10]),
        is_new=bool(row[11]),
        is_growing=bool(row[12]),
        category=str(row[13]),
        category_label=str(row[14]),
    )


def _score_breakdown_payload(breakdown: ScoreBreakdown) -> dict[str, Any]:
    return {
        "volume_score": breakdown.volume_score,
        "dynamics_score": breakdown.dynamics_score,
        "sentiment_score": breakdown.sentiment_score,
        "reach_score": breakdown.reach_score,
        "geo_score": breakdown.geo_score,
        "source_score": breakdown.source_score,
        "weights": dict(breakdown.weights),
    }


def _score_breakdown_from_payload(payload: dict[str, Any]) -> ScoreBreakdown:
    return ScoreBreakdown(
        volume_score=float(payload["volume_score"]),
        dynamics_score=float(payload["dynamics_score"]),
        sentiment_score=float(payload["sentiment_score"]),
        reach_score=float(payload["reach_score"]),
        geo_score=float(payload["geo_score"]),
        source_score=float(payload["source_score"]),
        weights={str(key): float(value) for key, value in dict(payload["weights"]).items()},
    )


def _parse_vector(value: Any) -> list[float]:
    if isinstance(value, list):
        return [float(item) for item in value]
    if isinstance(value, tuple):
        return [float(item) for item in value]
    if isinstance(value, str):
        normalized = value.strip("[]")
        if not normalized:
            return []
        return [float(item) for item in normalized.split(",")]
    return [float(item) for item in value]
