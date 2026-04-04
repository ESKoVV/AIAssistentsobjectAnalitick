from datetime import datetime, timezone

from raw_to_ml_bridge import build_ml_payload
from schema import RawDocument


def test_build_ml_payload_uses_raw_fields_and_none_for_missing_normalized() -> None:
    raw_document = RawDocument(
        source_type="vk_post",
        source_id="123",
        parent_source_id=None,
        text_raw="Сломался светофор на перекрестке",
        author_raw="42",
        media_type="text",
        created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 5, tzinfo=timezone.utc),
        reach=100,
        likes=5,
        reposts=2,
        comments_count=1,
        raw_payload={"media_type": "text"},
    )

    payload = build_ml_payload(raw_document)

    assert payload["doc_id"] == "vk_post:123"
    assert payload["text"] == raw_document.text_raw
    assert payload["normalized_text"] is None
    assert payload["language"] is None
    assert payload["reach"] == 100
    assert payload["engagement"]["likes"] == 5
    assert payload["engagement"]["comments_count"] == 1


def test_build_ml_payload_keeps_empty_text_if_text_empty() -> None:
    raw_document = RawDocument(
        source_type="rss_article",
        source_id="abc",
        parent_source_id=None,
        text_raw=" ",
        author_raw=None,
        media_type=None,
        created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 5, tzinfo=timezone.utc),
        raw_payload={},
    )

    payload = build_ml_payload(raw_document)

    assert payload["text"] == ""
    assert payload["reach"] == 0
