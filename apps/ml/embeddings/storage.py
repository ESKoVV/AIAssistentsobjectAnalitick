from __future__ import annotations

import json
from typing import Iterable, Protocol, Sequence

from apps.ml.embeddings.schema import EmbeddedDocument
from apps.ml.embeddings.serde import serialize_document


CREATE_IVFFLAT_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS embeddings_embedding_ivfflat_idx
ON embeddings USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100)
"""


class EmbeddingRepositoryProtocol(Protocol):
    def ensure_model_compatibility(self, *, model_name: str, model_version: str) -> None:
        ...

    def upsert_embeddings(self, documents: Sequence[EmbeddedDocument]) -> None:
        ...


class EmbeddingCacheProtocol(Protocol):
    def cache_embeddings(self, documents: Sequence[EmbeddedDocument]) -> None:
        ...


class NullEmbeddingCache:
    def cache_embeddings(self, documents: Sequence[EmbeddedDocument]) -> None:
        return None


class InMemoryEmbeddingRepository:
    def __init__(self) -> None:
        self._documents: dict[str, EmbeddedDocument] = {}

    @property
    def documents(self) -> dict[str, EmbeddedDocument]:
        return dict(self._documents)

    def ensure_model_compatibility(self, *, model_name: str, model_version: str) -> None:
        existing_versions = {
            (document.model_name, document.model_version)
            for document in self._documents.values()
        }
        if existing_versions and existing_versions != {(model_name, model_version)}:
            raise RuntimeError("existing embeddings were produced by a different model version")

    def upsert_embeddings(self, documents: Sequence[EmbeddedDocument]) -> None:
        for document in documents:
            self._documents[document.doc_id] = document


class InMemoryEmbeddingCache:
    def __init__(self) -> None:
        self.entries: dict[str, dict[str, object]] = {}

    def cache_embeddings(self, documents: Sequence[EmbeddedDocument]) -> None:
        for document in documents:
            self.entries[f"emb:{document.doc_id}"] = serialize_document(document)


class PostgresEmbeddingRepository:
    def __init__(self, dsn: str, *, embedding_dimension: int) -> None:
        self._dsn = dsn
        self._embedding_dimension = embedding_dimension

    def ensure_model_compatibility(self, *, model_name: str, model_version: str) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(self._create_table_sql())
                cursor.execute(
                    """
                    SELECT model_name, model_version
                    FROM embeddings
                    GROUP BY model_name, model_version
                    LIMIT 2
                    """,
                )
                rows = cursor.fetchall()
            connection.commit()

        if rows and set(rows) != {(model_name, model_version)}:
            raise RuntimeError("existing embeddings use a different model_name/model_version")

    def upsert_embeddings(self, documents: Sequence[EmbeddedDocument]) -> None:
        if not documents:
            return

        vector_dimension = len(documents[0].embedding)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if vector_dimension != self._embedding_dimension:
                    raise ValueError(
                        f"embedding dimension mismatch: expected {self._embedding_dimension}, got {vector_dimension}",
                    )
                cursor.execute(self._create_table_sql())
                cursor.execute(CREATE_IVFFLAT_INDEX_SQL)
                cursor.executemany(
                    """
                    INSERT INTO embeddings (
                        doc_id,
                        embedding,
                        model_name,
                        model_version,
                        embedded_at,
                        truncated
                    )
                    VALUES (%(doc_id)s, %(embedding)s, %(model_name)s, %(model_version)s, %(embedded_at)s, %(truncated)s)
                    ON CONFLICT (doc_id) DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        model_name = EXCLUDED.model_name,
                        model_version = EXCLUDED.model_version,
                        embedded_at = EXCLUDED.embedded_at,
                        truncated = EXCLUDED.truncated
                    """,
                    [
                        {
                            "doc_id": document.doc_id,
                            "embedding": _vector_literal(document.embedding),
                            "model_name": document.model_name,
                            "model_version": document.model_version,
                            "embedded_at": document.embedded_at,
                            "truncated": document.truncated,
                        }
                        for document in documents
                    ],
                )
            connection.commit()

    def _connect(self):  # type: ignore[no-untyped-def]
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "PostgresEmbeddingRepository requires 'psycopg[binary]' to be installed",
            ) from exc

        return psycopg.connect(self._dsn)

    def _create_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS embeddings (
            doc_id TEXT PRIMARY KEY,
            embedding vector({self._embedding_dimension}) NOT NULL,
            model_name TEXT NOT NULL,
            model_version TEXT NOT NULL,
            embedded_at TIMESTAMPTZ NOT NULL,
            truncated BOOLEAN NOT NULL DEFAULT FALSE
        )
        """


class RedisEmbeddingCache:
    def __init__(self, dsn: str, *, ttl_seconds: int) -> None:
        self._dsn = dsn
        self._ttl_seconds = ttl_seconds

    def cache_embeddings(self, documents: Sequence[EmbeddedDocument]) -> None:
        if not documents:
            return

        client = self._client()
        for document in documents:
            client.setex(
                f"emb:{document.doc_id}",
                self._ttl_seconds,
                json.dumps(serialize_document(document), ensure_ascii=False),
            )

    def _client(self):  # type: ignore[no-untyped-def]
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("RedisEmbeddingCache requires 'redis' to be installed") from exc

        return redis.Redis.from_url(self._dsn)


def _vector_literal(values: Iterable[float]) -> str:
    return "[" + ",".join(f"{float(value):.12g}" for value in values) + "]"
