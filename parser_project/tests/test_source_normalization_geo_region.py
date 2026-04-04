from collect_rss import build_rss_raw_message
from normalizers.vk import build_vk_post_raw_message


def test_vk_raw_message_keeps_source_payload_without_derived_geo_fields() -> None:
    raw_post = {
        "id": 456,
        "owner_id": 123,
        "from_id": 123,
        "date": 1712150000,
        "text": "Сильный ливень в Ростове",
        "geo": {"coordinates": "47.2221 39.7203"},
        "views": {"count": 1},
        "likes": {"count": 1},
        "reposts": {"count": 0},
        "comments": {"count": 0},
    }

    doc = build_vk_post_raw_message(raw_post)

    assert doc.source_type == "vk_post"
    assert doc.raw_payload["geo"]["coordinates"] == "47.2221 39.7203"
    assert not hasattr(doc, "region_hint")
    assert not hasattr(doc, "geo_lat")


def test_rss_raw_message_keeps_feed_entry_without_derived_geo_fields() -> None:
    entry = {
        "title": "Ситуация в Краснодаре",
        "summary": "Ожидаются осадки",
        "published": "Wed, 03 Apr 2024 10:00:00 +0300",
        "geo": {"lat": "45.035", "lon": "38.975"},
        "author": "Региональные новости",
    }

    doc = build_rss_raw_message("https://example.com/rss", entry)

    assert doc.source_type == "rss_article"
    assert doc.raw_payload["geo"]["lat"] == "45.035"
    assert not hasattr(doc, "region_hint")
