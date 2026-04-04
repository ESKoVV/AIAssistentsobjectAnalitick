import json
import os
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from dotenv import load_dotenv

from region_extractor import extract_geo, extract_region_hint
from schema import MediaType, NormalizedDocument, SourceType

load_dotenv()

RSS_FEEDS = os.getenv("RSS_FEEDS", "")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw.documents")


def _stable_doc_id(source_type: SourceType, source_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_type.value}:{source_id}"))


def _parse_entry_datetime(entry: dict[str, Any]) -> datetime:
    published_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if published_parsed:
        return datetime(*published_parsed[:6], tzinfo=timezone.utc)

    for field in ("published", "updated"):
        raw_value = entry.get(field)
        if not raw_value:
            continue
        try:
            dt = parsedate_to_datetime(raw_value)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (TypeError, ValueError):
            continue

    return datetime.now(timezone.utc)


def _build_source_id(feed_url: str, entry: dict[str, Any]) -> str:
    preferred_id = entry.get("id") or entry.get("guid") or entry.get("link")
    if preferred_id:
        return str(preferred_id)

    title = entry.get("title", "")
    published = entry.get("published", "")
    fallback_key = f"{feed_url}:{title}:{published}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, fallback_key))


def _extract_text(entry: dict[str, Any]) -> str:
    title = (entry.get("title") or "").strip()
    summary = (entry.get("summary") or entry.get("description") or "").strip()

    if title and summary:
        return f"{title}\n\n{summary}"
    if title:
        return title
    if summary:
        return summary

    content_items = entry.get("content") or []
    for item in content_items:
        value = (item.get("value") or "").strip()
        if value:
            return value

    return ""


def _to_raw_payload(entry: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(entry, ensure_ascii=False, default=str))


def normalize_rss_entry(feed_url: str, entry: dict[str, Any]) -> NormalizedDocument:
    text = _extract_text(entry)
    if not text.strip():
        raise ValueError("Пустая RSS-запись: отсутствуют title/summary/content")

    source_id = _build_source_id(feed_url, entry)
    raw_payload = _to_raw_payload(entry)
    geo_lat, geo_lon = extract_geo(raw_payload)
    region_hint = extract_region_hint(text, raw_payload)

    return NormalizedDocument(
        doc_id=_stable_doc_id(SourceType.RSS_ARTICLE, source_id),
        source_type=SourceType.RSS_ARTICLE,
        source_id=source_id,
        parent_id=None,
        text=text,
        media_type=MediaType.LINK,
        created_at=_parse_entry_datetime(entry),
        collected_at=datetime.now(timezone.utc),
        author_id=str(entry.get("author") or entry.get("source", {}).get("title") or "rss"),
        is_official=False,
        reach=0,
        likes=0,
        reposts=0,
        comments_count=0,
        region_hint=region_hint,
        geo_lat=geo_lat,
        geo_lon=geo_lon,
        raw_payload=raw_payload,
    )


def save_document_jsonl(path: str, doc: NormalizedDocument) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                doc.model_dump(),
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )


def main() -> None:
    if not RSS_FEEDS.strip():
        raise ValueError("Не найден RSS_FEEDS в .env")

    feed_urls = [x.strip() for x in RSS_FEEDS.split(",") if x.strip()]

    print("RSS-ленты для обхода:")
    for feed_url in feed_urls:
        print(f" - {feed_url}")
    print("-" * 80)

    total_sent = 0

    for feed_url in feed_urls:
        try:
            import feedparser

            parsed_feed = feedparser.parse(feed_url)

            if parsed_feed.bozo:
                bozo_exception = getattr(parsed_feed, "bozo_exception", "Unknown feed parsing error")
                raise ValueError(f"Ошибка парсинга RSS: {bozo_exception}")

            entries = parsed_feed.entries or []
            print(f"[{feed_url}] Получено записей: {len(entries)}")

            feed_sent = 0
            for entry in entries:
                try:
                    doc = normalize_rss_entry(feed_url, entry)
                    from kafka_producer import send_document

                    send_document(KAFKA_TOPIC, doc.model_dump())
                    save_document_jsonl("documents.jsonl", doc)

                    short_view = doc.model_dump()
                    short_view.pop("raw_payload", None)

                    print(json.dumps(short_view, indent=2, ensure_ascii=False, default=str))
                    print("-" * 80)

                    total_sent += 1
                    feed_sent += 1
                except Exception as entry_error:
                    entry_id = entry.get("id") or entry.get("link") or "unknown"
                    print(f"[{feed_url}] [entry={entry_id}] Ошибка обработки записи: {entry_error}")
                    print("-" * 80)

            print(f"[{feed_url}] Отправлено в Kafka: {feed_sent}")
            print("-" * 80)

        except Exception as feed_error:
            print(f"[{feed_url}] Ошибка обработки RSS-ленты: {feed_error}")
            print("-" * 80)

    print(f"Всего отправлено в Kafka: {total_sent}")


if __name__ == "__main__":
    main()
