from datetime import datetime, timezone

from raw_to_ml_bridge import build_ml_payload
from schema import RawDocument


def test_build_ml_payload_uses_raw_fields_and_none_for_missing_normalized() -> None:
    raw_document = RawDocument(
        doc_id="vk_post:123",
        source_type="vk_post",
        source_id="123",
        parent_source_id=None,
        text_raw="Сломался светофор на перекрестке",
        title_raw=None,
        author_raw="42",
        created_at_raw="1710000000",
        created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 5, tzinfo=timezone.utc),
        source_url="https://vk.com/wall1_123",
        source_domain="vk.com",
        region_hint_raw="rostov",
        geo_raw={"lat": 47.0, "lon": 39.0},
        engagement_raw={"likes": 5, "reposts": 2, "comments": 1, "views": 100},
        raw_payload={"media_type": "text"},
    )

    payload = build_ml_payload(raw_document)

    assert payload["doc_id"] == raw_document.doc_id
    assert payload["text"] == raw_document.text_raw
    assert payload["normalized_text"] is None
    assert payload["language"] is None
    assert payload["region_id"] == "rostov"
    assert payload["reach"] == 100
    assert payload["engagement"]["likes"] == 5
    assert payload["engagement"]["comments_count"] == 1
    assert payload["raw_equivalents"]["geo_raw"] == {"lat": 47.0, "lon": 39.0}


def test_build_ml_payload_falls_back_to_title_if_text_empty() -> None:
    raw_document = RawDocument(
        doc_id="rss_article:abc",
        source_type="rss_article",
        source_id="abc",
        parent_source_id=None,
        text_raw=" ",
        title_raw="Заголовок без текста",
        author_raw=None,
        created_at_raw=None,
        created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 5, tzinfo=timezone.utc),
        source_url=None,
        source_domain=None,
        region_hint_raw=None,
        geo_raw=None,
        engagement_raw={},
        raw_payload={},
    )

    payload = build_ml_payload(raw_document)

    assert payload["text"] == "Заголовок без текста"
    assert payload["reach"] is None
