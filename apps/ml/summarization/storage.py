from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Iterable, Protocol, Sequence

from apps.ml.clustering.schema import Cluster
from apps.preprocessing.normalization import SourceType

from .schema import (
    ClusterDescription,
    DescriptionHistoryRecord,
    LLMCostRecord,
    StoredClusterDescription,
    SummarizationDocumentRecord,
)


CREATE_CLUSTER_DESCRIPTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cluster_descriptions (
    cluster_id TEXT PRIMARY KEY REFERENCES clusters(cluster_id),
    summary TEXT NOT NULL,
    key_phrases TEXT[] NOT NULL DEFAULT '{}',
    sample_doc_ids TEXT[] NOT NULL DEFAULT '{}',
    model_name TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    input_token_count INTEGER NOT NULL,
    output_token_count INTEGER NOT NULL,
    generation_time_ms INTEGER NOT NULL,
    fallback_used BOOLEAN NOT NULL DEFAULT FALSE,
    needs_review BOOLEAN NOT NULL DEFAULT FALSE,
    cluster_size_at_generation INTEGER NOT NULL
)
"""

CREATE_CLUSTER_DESCRIPTIONS_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cluster_descriptions_history (
    id BIGSERIAL PRIMARY KEY,
    cluster_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    superseded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

CREATE_LLM_COSTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS llm_costs (
    id BIGSERIAL PRIMARY KEY,
    cluster_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    model_name TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    input_token_count INTEGER NOT NULL,
    output_token_count INTEGER NOT NULL,
    estimated_cost_usd DOUBLE PRECISION NOT NULL,
    generation_time_ms INTEGER NOT NULL,
    fallback_used BOOLEAN NOT NULL DEFAULT FALSE,
    validation_error TEXT
)
"""

CREATE_CLUSTER_DESCRIPTIONS_GENERATED_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS cluster_descriptions_generated_at_idx
ON cluster_descriptions (generated_at DESC)
"""

CREATE_LLM_COSTS_CLUSTER_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS llm_costs_cluster_requested_at_idx
ON llm_costs (cluster_id, requested_at DESC)
"""


class SummarizationRepositoryProtocol(Protocol):
    def ensure_schema(self) -> None:
        ...

    def ensure_upstream_dependencies(self) -> None:
        ...

    def load_clusters_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, Cluster]:
        ...

    def fetch_documents_for_clusters(
        self,
        cluster_ids: Sequence[str],
    ) -> dict[str, tuple[SummarizationDocumentRecord, ...]]:
        ...

    def load_descriptions_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, StoredClusterDescription]:
        ...

    def save_description(self, description: StoredClusterDescription) -> None:
        ...

    def record_llm_cost(self, record: LLMCostRecord) -> None:
        ...


class InMemorySummarizationRepository:
    def __init__(self) -> None:
        self.clusters: dict[str, Cluster] = {}
        self.documents_by_cluster: dict[str, tuple[SummarizationDocumentRecord, ...]] = {}
        self.descriptions: dict[str, StoredClusterDescription] = {}
        self.history: list[DescriptionHistoryRecord] = []
        self.llm_costs: list[LLMCostRecord] = []
        self.upstream_ready = True

    def ensure_schema(self) -> None:
        return None

    def ensure_upstream_dependencies(self) -> None:
        if not self.upstream_ready:
            raise RuntimeError("required upstream tables clusters/cluster_documents/embeddings/documents are not available")

    def load_clusters_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, Cluster]:
        return {
            cluster_id: self.clusters[cluster_id]
            for cluster_id in cluster_ids
            if cluster_id in self.clusters
        }

    def fetch_documents_for_clusters(
        self,
        cluster_ids: Sequence[str],
    ) -> dict[str, tuple[SummarizationDocumentRecord, ...]]:
        return {
            cluster_id: self.documents_by_cluster.get(cluster_id, ())
            for cluster_id in cluster_ids
        }

    def load_descriptions_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, StoredClusterDescription]:
        return {
            cluster_id: self.descriptions[cluster_id]
            for cluster_id in cluster_ids
            if cluster_id in self.descriptions
        }

    def save_description(self, description: StoredClusterDescription) -> None:
        existing = self.descriptions.get(description.description.cluster_id)
        if existing is not None:
            self.history.append(
                DescriptionHistoryRecord(
                    cluster_id=existing.description.cluster_id,
                    summary=existing.description.summary,
                    prompt_version=existing.description.prompt_version,
                    generated_at=existing.description.generated_at,
                    superseded_at=datetime.now(UTC),
                ),
            )
        self.descriptions[description.description.cluster_id] = description

    def record_llm_cost(self, record: LLMCostRecord) -> None:
        self.llm_costs.append(record)


class PostgresSummarizationRepository:
    def __init__(
        self,
        dsn: str,
        *,
        embeddings_table: str = "embeddings",
        documents_table: str = "documents",
    ) -> None:
        self._dsn = dsn
        self._embeddings_table = embeddings_table
        self._documents_table = documents_table

    def ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_CLUSTER_DESCRIPTIONS_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_DESCRIPTIONS_HISTORY_TABLE_SQL)
                cursor.execute(CREATE_LLM_COSTS_TABLE_SQL)
                cursor.execute(CREATE_CLUSTER_DESCRIPTIONS_GENERATED_INDEX_SQL)
                cursor.execute(CREATE_LLM_COSTS_CLUSTER_INDEX_SQL)
            connection.commit()

    def ensure_upstream_dependencies(self) -> None:
        required_tables = {
            "clusters",
            "cluster_documents",
            self._embeddings_table,
            self._documents_table,
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
            raise RuntimeError("required upstream tables clusters/cluster_documents/embeddings/documents are not available")

    def load_clusters_by_ids(self, cluster_ids: Sequence[str]) -> dict[str, Cluster]:
        if not cluster_ids:
            return {}

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT c.cluster_id, c.centroid, c.size, c.unique_authors, c.unique_sources, c.reach_total,
                           c.growth_rate, c.cohesion_score, c.noise, c.period_start, c.period_end,
                           c.earliest_doc_at, c.latest_doc_at, c.geo_regions, c.algorithm_params, c.created_at,
                           COALESCE(
                               ARRAY_AGG(cd.doc_id ORDER BY cd.assigned_at) FILTER (WHERE cd.doc_id IS NOT NULL),
                               '{}'
                           ) AS doc_ids
                    FROM clusters c
                    LEFT JOIN cluster_documents cd ON cd.cluster_id = c.cluster_id
                    WHERE c.cluster_id = ANY(%s)
                    GROUP BY c.cluster_id, c.centroid, c.size, c.unique_authors, c.unique_sources, c.reach_total,
                             c.growth_rate, c.cohesion_score, c.noise, c.period_start, c.period_end,
                             c.earliest_doc_at, c.latest_doc_at, c.geo_regions, c.algorithm_params, c.created_at
                    """,
                    (list(cluster_ids),),
                )
                rows = cursor.fetchall()
            connection.commit()
        return {
            cluster.cluster_id: cluster
            for cluster in (_row_to_cluster(row) for row in rows)
        }

    def fetch_documents_for_clusters(
        self,
        cluster_ids: Sequence[str],
    ) -> dict[str, tuple[SummarizationDocumentRecord, ...]]:
        if not cluster_ids:
            return {}

        query = f"""
            SELECT cd.cluster_id, e.doc_id, e.embedding, d.author_id, d.source_type, d.text, d.created_at, d.region_hint
            FROM cluster_documents cd
            JOIN {self._embeddings_table} e ON e.doc_id = cd.doc_id
            JOIN {self._documents_table} d ON d.doc_id = e.doc_id
            WHERE cd.cluster_id = ANY(%s)
            ORDER BY cd.cluster_id ASC, d.created_at ASC, e.doc_id ASC
        """
        grouped: dict[str, list[SummarizationDocumentRecord]] = defaultdict(list)
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

    def save_description(self, description: StoredClusterDescription) -> None:
        cluster_id = description.description.cluster_id
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT summary, prompt_version, generated_at
                    FROM cluster_descriptions
                    WHERE cluster_id = %s
                    """,
                    (cluster_id,),
                )
                existing_row = cursor.fetchone()
                if existing_row is not None:
                    cursor.execute(
                        """
                        INSERT INTO cluster_descriptions_history (
                            cluster_id, summary, prompt_version, generated_at, superseded_at
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            cluster_id,
                            str(existing_row[0]),
                            str(existing_row[1]),
                            existing_row[2],
                            datetime.now(UTC),
                        ),
                    )

                cursor.execute(
                    """
                    INSERT INTO cluster_descriptions (
                        cluster_id, summary, key_phrases, sample_doc_ids, model_name, prompt_version,
                        generated_at, input_token_count, output_token_count, generation_time_ms,
                        fallback_used, needs_review, cluster_size_at_generation
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cluster_id) DO UPDATE SET
                        summary = EXCLUDED.summary,
                        key_phrases = EXCLUDED.key_phrases,
                        sample_doc_ids = EXCLUDED.sample_doc_ids,
                        model_name = EXCLUDED.model_name,
                        prompt_version = EXCLUDED.prompt_version,
                        generated_at = EXCLUDED.generated_at,
                        input_token_count = EXCLUDED.input_token_count,
                        output_token_count = EXCLUDED.output_token_count,
                        generation_time_ms = EXCLUDED.generation_time_ms,
                        fallback_used = EXCLUDED.fallback_used,
                        needs_review = EXCLUDED.needs_review,
                        cluster_size_at_generation = EXCLUDED.cluster_size_at_generation
                    """,
                    (
                        cluster_id,
                        description.description.summary,
                        description.description.key_phrases,
                        description.description.sample_doc_ids,
                        description.description.model_name,
                        description.description.prompt_version,
                        description.description.generated_at,
                        description.description.input_token_count,
                        description.description.output_token_count,
                        description.description.generation_time_ms,
                        description.description.fallback_used,
                        description.needs_review,
                        description.cluster_size_at_generation,
                    ),
                )
            connection.commit()

    def record_llm_cost(self, record: LLMCostRecord) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO llm_costs (
                        cluster_id, attempt_number, model_name, prompt_version, requested_at,
                        input_token_count, output_token_count, estimated_cost_usd, generation_time_ms,
                        fallback_used, validation_error
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        record.cluster_id,
                        record.attempt_number,
                        record.model_name,
                        record.prompt_version,
                        record.requested_at,
                        record.input_token_count,
                        record.output_token_count,
                        record.estimated_cost_usd,
                        record.generation_time_ms,
                        record.fallback_used,
                        record.validation_error,
                    ),
                )
            connection.commit()

    def _connect(self):  # type: ignore[no-untyped-def]
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "PostgresSummarizationRepository requires 'psycopg[binary]' to be installed",
            ) from exc

        return psycopg.connect(self._dsn)


def _row_to_cluster(row: Sequence[Any]) -> Cluster:
    return Cluster(
        cluster_id=str(row[0]),
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
        doc_ids=[str(item) for item in row[16]],
    )


def _row_to_document_record(row: Sequence[Any]) -> SummarizationDocumentRecord:
    return SummarizationDocumentRecord(
        doc_id=str(row[0]),
        embedding=_parse_vector(row[1]),
        author_id=str(row[2]),
        source_type=SourceType(row[3]),
        text=str(row[4]),
        created_at=row[5],
        region=str(row[6]) if row[6] is not None else None,
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
