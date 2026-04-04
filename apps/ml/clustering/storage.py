from __future__ import annotations

import json
import pickle
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, Protocol, Sequence

from apps.preprocessing.normalization import SourceType

from .schema import (
    BufferedCandidate,
    Cluster,
    ClusterAssignment,
    ClusterDocumentRecord,
    ClusteringMetrics,
    ClusteringParams,
    ClustersUpdatedEvent,
    ClustererSnapshot,
    ClusterRunRecord,
)


CREATE_CLUSTERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS clusters (
    cluster_id TEXT PRIMARY KEY,
    centroid vector(%s),
    size INTEGER NOT NULL,
    unique_authors INTEGER NOT NULL,
    unique_sources INTEGER NOT NULL,
    reach_total INTEGER NOT NULL,
    growth_rate FLOAT NOT NULL,
    cohesion_score FLOAT NOT NULL,
    noise BOOLEAN NOT NULL DEFAULT FALSE,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    earliest_doc_at TIMESTAMPTZ NOT NULL,
    latest_doc_at TIMESTAMPTZ NOT NULL,
    geo_regions TEXT[] NOT NULL DEFAULT '{}',
    algorithm_params JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
)
"""

CREATE_CLUSTER_DOCUMENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cluster_documents (
    cluster_id TEXT NOT NULL REFERENCES clusters(cluster_id),
    doc_id TEXT NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL,
    strength FLOAT NOT NULL DEFAULT 1.0,
    PRIMARY KEY (cluster_id, doc_id)
)
"""

CREATE_CLUSTER_SNAPSHOTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS clusterer_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    clusterer_blob BYTEA NOT NULL,
    params JSONB NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    label_to_cluster_id JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
)
"""

CREATE_CLUSTER_BUFFER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cluster_candidate_buffer (
    doc_id TEXT PRIMARY KEY,
    buffered_at TIMESTAMPTZ NOT NULL,
    last_strength FLOAT NOT NULL
)
"""

CREATE_CLUSTER_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cluster_runs (
    run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    params JSONB NOT NULL,
    n_documents INTEGER NOT NULL,
    n_clusters INTEGER NOT NULL,
    n_noise INTEGER NOT NULL,
    noise_ratio FLOAT NOT NULL,
    avg_cohesion FLOAT NOT NULL,
    runtime_seconds FLOAT NOT NULL,
    snapshot_id TEXT,
    run_at TIMESTAMPTZ NOT NULL
)
"""

CREATE_CLUSTER_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS clusters_centroid_ivfflat_idx
ON clusters USING ivfflat (centroid vector_cosine_ops)
WITH (lists = 50)
"""

CREATE_CLUSTER_PERIOD_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS clusters_noise_period_end_idx
ON clusters (noise, period_end)
"""


class ClusteringRepositoryProtocol(Protocol):
    def ensure_schema(self) -> None:
        ...

    def ensure_upstream_dependencies(self) -> None:
        ...

    def fetch_documents_for_window(
        self,
        *,
        period_start: datetime,
        period_end: datetime,
    ) -> tuple[ClusterDocumentRecord, ...]:
        ...

    def fetch_unassigned_documents_since(self, *, since: datetime) -> tuple[ClusterDocumentRecord, ...]:
        ...

    def fetch_documents_for_clusters(self, cluster_ids: Sequence[str]) -> dict[str, tuple[ClusterDocumentRecord, ...]]:
        ...

    def load_latest_snapshot(self) -> ClustererSnapshot | None:
        ...

    def load_latest_clusters(self) -> tuple[Cluster, ...]:
        ...

    def load_clusters_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, Cluster]:
        ...

    def save_full_recompute(
        self,
        *,
        clusters: Sequence[Cluster],
        assignments: Sequence[ClusterAssignment],
        snapshot: ClustererSnapshot | None,
        metrics: ClusteringMetrics,
    ) -> None:
        ...

    def save_online_updates(
        self,
        *,
        assignments: Sequence[ClusterAssignment],
        buffered_candidates: Sequence[BufferedCandidate],
        updated_clusters: Sequence[Cluster],
    ) -> None:
        ...


class InMemoryClusteringRepository:
    def __init__(self) -> None:
        self.documents: dict[str, ClusterDocumentRecord] = {}
        self.clusters: dict[str, Cluster] = {}
        self.cluster_documents: dict[str, dict[str, ClusterAssignment]] = defaultdict(dict)
        self.buffered_candidates: dict[str, BufferedCandidate] = {}
        self.snapshot: ClustererSnapshot | None = None
        self.runs: list[ClusterRunRecord] = []
        self.upstream_ready = True

    def ensure_schema(self) -> None:
        return None

    def ensure_upstream_dependencies(self) -> None:
        if not self.upstream_ready:
            raise RuntimeError("required upstream tables embeddings/documents are not available")

    def fetch_documents_for_window(
        self,
        *,
        period_start: datetime,
        period_end: datetime,
    ) -> tuple[ClusterDocumentRecord, ...]:
        return tuple(
            document
            for document in self.documents.values()
            if period_start <= document.created_at <= period_end
        )

    def fetch_unassigned_documents_since(self, *, since: datetime) -> tuple[ClusterDocumentRecord, ...]:
        assigned_doc_ids = {
            doc_id
            for cluster_assignments in self.cluster_documents.values()
            for doc_id in cluster_assignments
        }
        return tuple(
            document
            for document in self.documents.values()
            if document.created_at > since
            and document.doc_id not in assigned_doc_ids
            and document.doc_id not in self.buffered_candidates
        )

    def fetch_documents_for_clusters(self, cluster_ids: Sequence[str]) -> dict[str, tuple[ClusterDocumentRecord, ...]]:
        return {
            cluster_id: tuple(
                self.documents[doc_id]
                for doc_id in self.cluster_documents.get(cluster_id, {})
                if doc_id in self.documents
            )
            for cluster_id in cluster_ids
        }

    def load_latest_snapshot(self) -> ClustererSnapshot | None:
        return self.snapshot

    def load_latest_clusters(self) -> tuple[Cluster, ...]:
        return tuple(self.clusters.values())

    def load_clusters_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, Cluster]:
        return {
            cluster_id: self.clusters[cluster_id]
            for cluster_id in cluster_ids
            if cluster_id in self.clusters
        }

    def save_full_recompute(
        self,
        *,
        clusters: Sequence[Cluster],
        assignments: Sequence[ClusterAssignment],
        snapshot: ClustererSnapshot | None,
        metrics: ClusteringMetrics,
    ) -> None:
        self.clusters = {cluster.cluster_id: cluster for cluster in clusters}
        self.cluster_documents = defaultdict(dict)
        self.buffered_candidates = {}
        for assignment in assignments:
            self.cluster_documents[assignment.cluster_id][assignment.doc_id] = assignment
        self.snapshot = snapshot
        self.runs.append(
            ClusterRunRecord(
                run_id=f"run:{len(self.runs) + 1}",
                mode="full_recompute",
                params=clusters[0].algorithm_params if clusters else {},
                n_documents=metrics.n_documents,
                n_clusters=metrics.n_clusters,
                n_noise=metrics.n_noise,
                noise_ratio=metrics.noise_ratio,
                avg_cohesion=metrics.avg_cohesion,
                runtime_seconds=metrics.runtime_seconds,
                snapshot_id=snapshot.snapshot_id if snapshot else None,
                run_at=metrics.run_at,
            ),
        )

    def save_online_updates(
        self,
        *,
        assignments: Sequence[ClusterAssignment],
        buffered_candidates: Sequence[BufferedCandidate],
        updated_clusters: Sequence[Cluster],
    ) -> None:
        for assignment in assignments:
            self.cluster_documents[assignment.cluster_id][assignment.doc_id] = assignment
            self.buffered_candidates.pop(assignment.doc_id, None)
        for candidate in buffered_candidates:
            self.buffered_candidates[candidate.doc_id] = candidate
        for cluster in updated_clusters:
            self.clusters[cluster.cluster_id] = cluster


class PostgresClusteringRepository:
    def __init__(
        self,
        dsn: str,
        *,
        embeddings_table: str = "embeddings",
        documents_table: str = "normalized_messages",
    ) -> None:
        self._dsn = dsn
        self._embeddings_table = embeddings_table
        self._documents_table = documents_table

    def ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_CLUSTERS_TABLE_SQL, (1024,))
                cursor.execute(CREATE_CLUSTER_DOCUMENTS_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_SNAPSHOTS_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_BUFFER_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_RUNS_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_INDEX_SQL)
                cursor.execute(CREATE_CLUSTER_PERIOD_INDEX_SQL)
            connection.commit()

    def ensure_upstream_dependencies(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name IN (%s, %s)
                    """,
                    (self._embeddings_table, self._documents_table),
                )
                rows = {row[0] for row in cursor.fetchall()}
            connection.commit()

        required_tables = {self._embeddings_table, self._documents_table}
        if rows != required_tables:
            raise RuntimeError("required upstream tables embeddings/documents are not available")

    def fetch_documents_for_window(
        self,
        *,
        period_start: datetime,
        period_end: datetime,
    ) -> tuple[ClusterDocumentRecord, ...]:
        return self._fetch_documents(
            """
            WHERE d.created_at >= %s AND d.created_at <= %s
            """,
            (period_start, period_end),
        )

    def fetch_unassigned_documents_since(self, *, since: datetime) -> tuple[ClusterDocumentRecord, ...]:
        return self._fetch_documents(
            """
            WHERE d.created_at > %s
              AND NOT EXISTS (
                    SELECT 1 FROM cluster_documents cd WHERE cd.doc_id = e.doc_id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM cluster_candidate_buffer cb WHERE cb.doc_id = e.doc_id
                )
            """,
            (since,),
        )

    def fetch_documents_for_clusters(self, cluster_ids: Sequence[str]) -> dict[str, tuple[ClusterDocumentRecord, ...]]:
        if not cluster_ids:
            return {}

        query = f"""
            SELECT cd.cluster_id, e.doc_id, e.embedding, d.author_id, d.source_type, d.reach, d.created_at, d.region_hint
            FROM cluster_documents cd
            JOIN {self._embeddings_table} e ON e.doc_id = cd.doc_id
            JOIN {self._documents_table} d ON d.doc_id = e.doc_id
            WHERE cd.cluster_id = ANY(%s)
        """
        grouped: dict[str, list[ClusterDocumentRecord]] = defaultdict(list)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (list(cluster_ids),))
                for row in cursor.fetchall():
                    grouped[row[0]].append(_row_to_document_record(row[1:]))
            connection.commit()
        return {cluster_id: tuple(documents) for cluster_id, documents in grouped.items()}

    def load_latest_snapshot(self) -> ClustererSnapshot | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT snapshot_id, clusterer_blob, params, period_start, period_end, label_to_cluster_id, created_at
                    FROM clusterer_snapshots
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                )
                row = cursor.fetchone()
            connection.commit()

        if row is None:
            return None

        return ClustererSnapshot(
            snapshot_id=row[0],
            clusterer=pickle.loads(bytes(row[1])),
            params=_params_from_dict(dict(row[2])),
            period_start=row[3],
            period_end=row[4],
            label_to_cluster_id={int(key): str(value) for key, value in dict(row[5]).items()},
            created_at=row[6],
        )

    def load_latest_clusters(self) -> tuple[Cluster, ...]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cluster_id, centroid, size, unique_authors, unique_sources, reach_total,
                           growth_rate, cohesion_score, noise, period_start, period_end,
                           earliest_doc_at, latest_doc_at, geo_regions, algorithm_params, created_at
                    FROM clusters
                    """,
                )
                rows = cursor.fetchall()
            connection.commit()
        return tuple(_row_to_cluster(row) for row in rows)

    def load_clusters_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, Cluster]:
        if not cluster_ids:
            return {}

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cluster_id, centroid, size, unique_authors, unique_sources, reach_total,
                           growth_rate, cohesion_score, noise, period_start, period_end,
                           earliest_doc_at, latest_doc_at, geo_regions, algorithm_params, created_at
                    FROM clusters
                    WHERE cluster_id = ANY(%s)
                    """,
                    (list(cluster_ids),),
                )
                rows = cursor.fetchall()
            connection.commit()
        return {cluster.cluster_id: cluster for cluster in (_row_to_cluster(row) for row in rows)}

    def save_full_recompute(
        self,
        *,
        clusters: Sequence[Cluster],
        assignments: Sequence[ClusterAssignment],
        snapshot: ClustererSnapshot | None,
        metrics: ClusteringMetrics,
    ) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if clusters:
                    dimension = len(clusters[0].centroid)
                else:
                    dimension = 1024
                cursor.execute(CREATE_CLUSTERS_TABLE_SQL, (dimension,))
                cursor.execute(CREATE_CLUSTER_DOCUMENTS_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_SNAPSHOTS_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_BUFFER_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_RUNS_TABLE_SQL)
                cursor.execute("DELETE FROM cluster_documents")
                cursor.execute("DELETE FROM clusters")
                cursor.execute("DELETE FROM clusterer_snapshots")
                cursor.execute("DELETE FROM cluster_candidate_buffer")
                self._upsert_clusters(cursor, clusters)
                self._insert_assignments(cursor, assignments)
                if snapshot is not None:
                    cursor.execute(
                        """
                        INSERT INTO clusterer_snapshots (
                            snapshot_id, clusterer_blob, params, period_start, period_end, label_to_cluster_id, created_at
                        )
                        VALUES (%s, %s, %s::jsonb, %s, %s, %s::jsonb, %s)
                        """,
                        (
                            snapshot.snapshot_id,
                            pickle.dumps(snapshot.clusterer),
                            json.dumps(snapshot.params.to_dict()),
                            snapshot.period_start,
                            snapshot.period_end,
                            json.dumps(snapshot.label_to_cluster_id),
                            snapshot.created_at,
                        ),
                    )
                cursor.execute(
                    """
                    INSERT INTO cluster_runs (
                        run_id, mode, params, n_documents, n_clusters, n_noise,
                        noise_ratio, avg_cohesion, runtime_seconds, snapshot_id, run_at
                    )
                    VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        f"run:{metrics.run_at.isoformat()}",
                        "full_recompute",
                        json.dumps(clusters[0].algorithm_params if clusters else {}),
                        metrics.n_documents,
                        metrics.n_clusters,
                        metrics.n_noise,
                        metrics.noise_ratio,
                        metrics.avg_cohesion,
                        metrics.runtime_seconds,
                        snapshot.snapshot_id if snapshot else None,
                        metrics.run_at,
                    ),
                )
            connection.commit()

    def save_online_updates(
        self,
        *,
        assignments: Sequence[ClusterAssignment],
        buffered_candidates: Sequence[BufferedCandidate],
        updated_clusters: Sequence[Cluster],
    ) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                self._upsert_clusters(cursor, updated_clusters)
                self._insert_assignments(cursor, assignments)
                for candidate in buffered_candidates:
                    cursor.execute(
                        """
                        INSERT INTO cluster_candidate_buffer (doc_id, buffered_at, last_strength)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (doc_id) DO UPDATE SET
                            buffered_at = EXCLUDED.buffered_at,
                            last_strength = EXCLUDED.last_strength
                        """,
                        (candidate.doc_id, candidate.buffered_at, candidate.last_strength),
                    )
                for assignment in assignments:
                    cursor.execute(
                        "DELETE FROM cluster_candidate_buffer WHERE doc_id = %s",
                        (assignment.doc_id,),
                    )
            connection.commit()

    def _fetch_documents(self, where_clause: str, params: tuple[object, ...]) -> tuple[ClusterDocumentRecord, ...]:
        query = f"""
            SELECT e.doc_id, e.embedding, d.author_id, d.source_type, d.reach, d.created_at, d.region_hint
            FROM {self._embeddings_table} e
            JOIN {self._documents_table} d ON d.doc_id = e.doc_id
            {where_clause}
            ORDER BY d.created_at ASC, e.doc_id ASC
        """
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
            connection.commit()
        return tuple(_row_to_document_record(row) for row in rows)

    def _upsert_clusters(self, cursor: Any, clusters: Sequence[Cluster]) -> None:
        for cluster in clusters:
            cursor.execute(
                """
                INSERT INTO clusters (
                    cluster_id, centroid, size, unique_authors, unique_sources, reach_total,
                    growth_rate, cohesion_score, noise, period_start, period_end,
                    earliest_doc_at, latest_doc_at, geo_regions, algorithm_params, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (cluster_id) DO UPDATE SET
                    centroid = EXCLUDED.centroid,
                    size = EXCLUDED.size,
                    unique_authors = EXCLUDED.unique_authors,
                    unique_sources = EXCLUDED.unique_sources,
                    reach_total = EXCLUDED.reach_total,
                    growth_rate = EXCLUDED.growth_rate,
                    cohesion_score = EXCLUDED.cohesion_score,
                    noise = EXCLUDED.noise,
                    period_start = EXCLUDED.period_start,
                    period_end = EXCLUDED.period_end,
                    earliest_doc_at = EXCLUDED.earliest_doc_at,
                    latest_doc_at = EXCLUDED.latest_doc_at,
                    geo_regions = EXCLUDED.geo_regions,
                    algorithm_params = EXCLUDED.algorithm_params,
                    created_at = EXCLUDED.created_at
                """,
                (
                    cluster.cluster_id,
                    _vector_literal(cluster.centroid),
                    cluster.size,
                    cluster.unique_authors,
                    cluster.unique_sources,
                    cluster.reach_total,
                    cluster.growth_rate,
                    cluster.cohesion_score,
                    cluster.noise,
                    cluster.period_start,
                    cluster.period_end,
                    cluster.earliest_doc_at,
                    cluster.latest_doc_at,
                    cluster.geo_regions,
                    json.dumps(cluster.algorithm_params),
                    cluster.created_at,
                ),
            )

    def _insert_assignments(self, cursor: Any, assignments: Sequence[ClusterAssignment]) -> None:
        for assignment in assignments:
            cursor.execute(
                """
                INSERT INTO cluster_documents (cluster_id, doc_id, assigned_at, strength)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (cluster_id, doc_id) DO UPDATE SET
                    assigned_at = EXCLUDED.assigned_at,
                    strength = EXCLUDED.strength
                """,
                (
                    assignment.cluster_id,
                    assignment.doc_id,
                    assignment.assigned_at,
                    assignment.strength,
                ),
            )

    def _connect(self):  # type: ignore[no-untyped-def]
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "PostgresClusteringRepository requires 'psycopg[binary]' to be installed",
            ) from exc

        return psycopg.connect(self._dsn)


def _row_to_document_record(row: Sequence[Any]) -> ClusterDocumentRecord:
    return ClusterDocumentRecord(
        doc_id=str(row[0]),
        embedding=_parse_vector(row[1]),
        author_id=str(row[2]),
        source_type=SourceType(row[3]),
        reach=int(row[4]),
        created_at=row[5],
        region=str(row[6]) if row[6] is not None else None,
    )


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


def _vector_literal(values: Iterable[float]) -> str:
    return "[" + ",".join(f"{float(value):.12g}" for value in values) + "]"


def _params_from_dict(payload: dict[str, Any]) -> ClusteringParams:
    return ClusteringParams(
        min_cluster_size=int(payload["min_cluster_size"]),
        min_samples=int(payload["min_samples"]),
        assignment_strength_threshold=float(payload["assignment_strength_threshold"]),
        reconcile_similarity_threshold=float(payload["reconcile_similarity_threshold"]),
        full_recompute_window_hours=int(payload["full_recompute_window_hours"]),
        growth_recent_hours=int(payload["growth_recent_hours"]),
        growth_previous_hours=int(payload["growth_previous_hours"]),
    )
