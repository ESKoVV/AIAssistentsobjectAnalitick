from __future__ import annotations

import argparse
from typing import Any

import psycopg

from config import load_config, validate_db_config
from kafka_producer import close_producer, send_document
from schema import MediaType, RawMessage, SourceType


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay canonical RawMessage payloads from raw_messages back into Kafka raw.documents",
    )
    parser.add_argument("--limit", type=int, default=0, help="Replay at most N rows. 0 means all rows.")
    parser.add_argument(
        "--source-type",
        type=str,
        default="",
        help="Replay only one source_type value, for example vk_post or rss_article.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the number of replayable rows without publishing to Kafka.",
    )
    return parser.parse_args()


def _build_message(row: dict[str, Any]) -> RawMessage:
    media_type = row["media_type"]
    return RawMessage(
        source_type=SourceType(str(row["source_type"])),
        source_id=str(row["source_id"]),
        author_id=str(row["author_id"]) if row["author_id"] is not None else None,
        text=str(row["text"] or ""),
        media_type=MediaType(str(media_type)) if media_type else None,
        created_at_utc=row["created_at_utc"],
        collected_at=row["collected_at"],
        raw_payload=dict(row["raw_payload"] or {}),
        is_official=bool(row["is_official"]),
        reach=int(row["reach"] or 0),
        likes=int(row["likes"] or 0),
        reposts=int(row["reposts"] or 0),
        comments_count=int(row["comments_count"] or 0),
        parent_id=str(row["parent_id"]) if row["parent_id"] is not None else None,
    )


def main() -> None:
    args = _parse_args()
    config = load_config()
    validate_db_config(config)

    query = """
        SELECT source_type, source_id, author_id, text, media_type, created_at_utc, collected_at,
               raw_payload, is_official, reach, likes, reposts, comments_count, parent_id
        FROM raw_messages
    """
    params: list[Any] = []
    where_clauses: list[str] = []
    if args.source_type:
        where_clauses.append("source_type = %s")
        params.append(args.source_type)
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY collected_at ASC, created_at_utc ASC, source_type ASC, source_id ASC"
    if args.limit > 0:
        query += " LIMIT %s"
        params.append(args.limit)

    replayed = 0
    with psycopg.connect(config.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        conn.commit()

    if args.dry_run:
        for row in rows:
            _build_message(row)
        print(f"✅ Dry-run successful, replayable rows: {len(rows)}")
        return

    try:
        for row in rows:
            message = _build_message(row)
            send_document(config.kafka_raw_topic, message.model_dump(mode="json"))
            replayed += 1
    finally:
        close_producer()

    print(f"✅ Replayed raw_messages -> {config.kafka_raw_topic}: {replayed}")


if __name__ == "__main__":
    main()
