from datetime import datetime, timezone

from raw_to_ml_bridge import build_ml_payload
from schema import RawMessage


def test_build_ml_payload_uses_raw_message_fields_and_none_for_missing_normalized() -> None:
    raw_message = RawMessage(
        source_type="vk_post",
        source_id="123",
        author_id="42",
        text="Сломался светофор на перекрестке",
        media_type="text",
        created_at_utc=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 5, tzinfo=timezone.utc),
        reach=100,
        likes=5,
        reposts=2,
        comments_count=1,
        raw_payload={"media_type": "text"},
    )

    payload = build_ml_payload(raw_message)

    assert payload["doc_id"] == "vk_post:123"
    assert payload["text"] == raw_message.text
    assert payload["normalized_text"] is None
    assert payload["language"] is None
    assert payload["reach"] == 100
    assert payload["engagement"]["likes"] == 5
    assert payload["engagement"]["comments_count"] == 1


def test_build_ml_payload_keeps_empty_text_if_text_empty() -> None:
    raw_message = RawMessage(
        source_type="rss_article",
        source_id="abc",
        author_id=None,
        text=" ",
        media_type=None,
        created_at_utc=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 5, tzinfo=timezone.utc),
        raw_payload={},
    )

    payload = build_ml_payload(raw_message)

    assert payload["text"] == ""
    assert payload["reach"] == 0
