from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Sequence

from apps.ml.embeddings.schema import EmbeddedDocument
from apps.ml.embeddings.serde import deserialize_embedded_document, serialize_document


class SQLiteEmbeddingSpool:
    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def buffer_documents(self, documents: Sequence[EmbeddedDocument]) -> None:
        if not documents:
            return

        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO embedding_spool (doc_id, payload)
                VALUES (?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET payload = excluded.payload
                """,
                [
                    (
                        document.doc_id,
                        json.dumps(serialize_document(document), ensure_ascii=False),
                    )
                    for document in documents
                ],
            )
            connection.commit()

    def peek(self, *, limit: int = 100) -> tuple[EmbeddedDocument, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT payload
                FROM embedding_spool
                ORDER BY created_at, doc_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return tuple(
            deserialize_embedded_document(json.loads(row[0]))
            for row in rows
        )

    def acknowledge(self, doc_ids: Sequence[str]) -> None:
        if not doc_ids:
            return

        with self._connect() as connection:
            connection.executemany(
                "DELETE FROM embedding_spool WHERE doc_id = ?",
                [(doc_id,) for doc_id in doc_ids],
            )
            connection.commit()

    def size(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) FROM embedding_spool").fetchone()
        return int(row[0]) if row else 0

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS embedding_spool (
                    doc_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
            connection.commit()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._path)
