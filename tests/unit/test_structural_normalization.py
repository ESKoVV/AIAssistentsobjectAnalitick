from __future__ import annotations

from datetime import datetime, timezone

from apps.preprocessing.normalization import MediaType, SourceType, normalize_document


def test_vk_comment_sets_parent_id_and_stringifies_identifiers() -> None:
    payload = {
        "id": 991,
        "post_id": 77,
        "owner_id": -55,
        "from_id": 123456,
        "text": "Подтверждаю, проблема сохраняется.",
        "date": 1712120400,
    }

    document = normalize_document(payload, {"source": "vk", "entity_type": "comment"})

    assert document.source_type is SourceType.VK_COMMENT
    assert document.source_id == "-55_77_991"
    assert document.parent_id == "-55_77"
    assert document.author_id == "123456"
    assert document.doc_id == "vk_comment:-55_77_991"


def test_missing_reach_defaults_to_zero() -> None:
    payload = {
        "guid": "rss-no-reach",
        "title": "Короткая заметка",
        "published_at": "2026-04-02T07:00:00+03:00",
        "author": "Редакция",
    }

    document = normalize_document(
        payload,
        {"source": "rss", "feed_id": "city-feed", "collected_at": "2026-04-02T07:05:00+03:00"},
    )

    assert document.source_type is SourceType.RSS_ARTICLE
    assert document.reach == 0
    assert document.likes == 0
    assert document.reposts == 0
    assert document.comments_count == 0


def test_empty_text_is_preserved_as_empty_string() -> None:
    payload = {
        "appeal_id": "portal-empty-text",
        "message": "",
        "created_at": "2026-04-02T12:00:00+03:00",
        "user": {"id": "anon-7"},
    }

    document = normalize_document(
        payload,
        {"source": "portal", "timezone": "+03:00", "collected_at": "2026-04-02T12:02:00+03:00"},
    )

    assert document.text == ""
    assert document.media_type is MediaType.TEXT


def test_local_timestamps_are_normalized_to_utc() -> None:
    payload = {
        "id": "post-55",
        "channel": {"id": "max-channel"},
        "author": {"id": "user-1"},
        "message": "Локальное время должно стать UTC.",
        "created_at": "2026-04-02T10:30:00",
        "collected_at": "2026-04-02T10:45:00",
    }

    document = normalize_document(payload, {"source": "max", "timezone": "+03:00"})

    assert document.created_at == datetime(2026, 4, 2, 7, 30, tzinfo=timezone.utc)
    assert document.collected_at == datetime(2026, 4, 2, 7, 45, tzinfo=timezone.utc)


def test_different_identifier_formats_are_coerced_to_strings() -> None:
    payload = {
        "id": "article-uuid-001",
        "title": "Новость по району",
        "summary": "Текст публикации",
        "published_at": "2026-04-02T09:00:00+03:00",
        "author": 42,
        "link": "https://example.test/article-uuid-001",
    }

    document = normalize_document(
        payload,
        {"source": "rss", "feed_id": 999, "collected_at": "2026-04-02T09:05:00+03:00"},
    )

    assert document.source_id == "article-uuid-001"
    assert document.author_id == "42"
    assert document.doc_id == "rss_article:article-uuid-001"


def test_portal_coordinates_and_region_fallback_are_normalized() -> None:
    payload = {
        "appeal_id": "A-500",
        "subject": "Яма на дороге",
        "message": "Нужен ремонт покрытия.",
        "created_at": "2026-04-02T08:00:00+03:00",
        "location": {"lat": "48.7080", "lon": "44.5133"},
    }

    document = normalize_document(
        payload,
        {
            "source": "portal",
            "collected_at": "2026-04-02T08:10:00+03:00",
            "region_hint": "Волгоградская область",
            "is_official": True,
        },
    )

    assert document.source_type is SourceType.PORTAL_APPEAL
    assert document.region_hint == "Волгоградская область"
    assert document.geo_lat == 48.708
    assert document.geo_lon == 44.5133
    assert document.is_official is True
