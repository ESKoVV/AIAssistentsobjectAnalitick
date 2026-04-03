from __future__ import annotations

from dataclasses import asdict
from datetime import timezone

import pytest

from apps.preprocessing.normalization import (
    MediaType,
    NormalizedDocument,
    SourceType,
    normalize_document,
)


EXPECTED_FIELDS = {
    "doc_id",
    "source_type",
    "source_id",
    "parent_id",
    "text",
    "media_type",
    "created_at",
    "collected_at",
    "author_id",
    "is_official",
    "reach",
    "likes",
    "reposts",
    "comments_count",
    "region_hint",
    "geo_lat",
    "geo_lon",
    "raw_payload",
}


@pytest.mark.parametrize(
    ("payload", "source_config", "expected_source_type", "expected_media_type"),
    [
        (
            {
                "id": 77,
                "owner_id": -55,
                "from_id": -55,
                "text": "Плановое отключение воды завершено.",
                "date": 1712120400,
                "collected_at": "2026-04-02T10:15:00+03:00",
                "views": {"count": 1500},
                "likes": {"count": 41},
                "reposts": {"count": 7},
                "comments": {"count": 9},
                "attachments": [{"type": "photo"}],
            },
            {"source": "vk", "is_official": True, "region_hint": "Волгоград"},
            SourceType.VK_POST,
            MediaType.PHOTO,
        ),
        (
            {
                "id": "post-99",
                "channel": {"id": "community-42"},
                "author": {"id": 5001},
                "message": "Готовим новую схему движения автобусов.",
                "created_at": "2026-04-02T11:30:00+03:00",
                "collected_at": "2026-04-02T11:31:00+03:00",
                "metrics": {"reach": 730, "likes": 14, "reposts": 2, "comments": 5},
                "attachments": [{"kind": "video"}],
            },
            {"source": "max", "source_id": "transport-max"},
            SourceType.MAX_POST,
            MediaType.VIDEO,
        ),
        (
            {
                "guid": "rss-article-1",
                "title": "В городе открыли новый ФАП",
                "summary": "Жители получили доступ к первичной помощи.",
                "published_at": "2026-04-02T09:00:00+03:00",
                "link": "https://example.test/articles/1",
                "author": "Редакция",
            },
            {"source": "rss", "feed_id": "health-feed", "collected_at": "2026-04-02T09:20:00+03:00"},
            SourceType.RSS_ARTICLE,
            MediaType.LINK,
        ),
        (
            {
                "appeal_id": "A-17",
                "subject": "Освещение во дворе",
                "message": "Не работает фонарь у дома 10.",
                "created_at": "2026-04-02T08:00:00+03:00",
                "location": {"region": "Волжский", "lat": "48.7858", "lon": "44.7797"},
                "author": {"id": "citizen-44"},
            },
            {"source": "portal", "collected_at": "2026-04-02T08:05:00+03:00"},
            SourceType.PORTAL_APPEAL,
            MediaType.TEXT,
        ),
    ],
)
def test_structural_normalization_contract_across_sources(
    payload: dict,
    source_config: dict,
    expected_source_type: SourceType,
    expected_media_type: MediaType,
) -> None:
    payload_snapshot = dict(payload)

    document = normalize_document(payload, source_config)

    assert isinstance(document, NormalizedDocument)
    assert set(asdict(document).keys()) == EXPECTED_FIELDS
    assert document.source_type is expected_source_type
    assert document.media_type is expected_media_type
    assert document.created_at.tzinfo == timezone.utc
    assert document.collected_at.tzinfo == timezone.utc
    assert document.raw_payload == payload_snapshot
    assert document.raw_payload is not payload
    assert not hasattr(document, "owner_id")
    assert not hasattr(document, "channel")


def test_structural_normalization_is_deterministic_for_same_input() -> None:
    payload = {
        "id": 101,
        "owner_id": -10,
        "from_id": 22,
        "text": "Стабильный пост",
        "date": 1712120400,
    }
    source_config = {"source": "vk", "timezone": "+03:00"}

    first_document = normalize_document(payload, source_config)
    second_document = normalize_document(payload, source_config)

    assert first_document == second_document
