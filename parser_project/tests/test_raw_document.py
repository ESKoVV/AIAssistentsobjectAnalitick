from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from schema import RawDocument


def test_raw_document_accepts_valid_payload() -> None:
    doc = RawDocument(
        doc_id="vk_post:-123_456",
        source_type="vk_post",
        source_id="-123_456",
        parent_source_id=None,
        text_raw="Тестовый raw текст",
        title_raw=None,
        author_raw="42",
        created_at_raw="1712150000",
        created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        source_url="https://vk.com/wall-123_456",
        source_domain="vk.com",
        region_hint_raw="Ростовская область",
        geo_raw={"coordinates": "47.22 39.72"},
        engagement_raw={"likes": {"count": 5}},
        raw_payload={"id": 456},
    )

    dumped = doc.model_dump(mode="json")

    assert dumped["doc_id"] == "vk_post:-123_456"
    assert dumped["source_type"] == "vk_post"
    assert dumped["text_raw"] == "Тестовый raw текст"
    assert dumped["source_domain"] == "vk.com"


@pytest.mark.parametrize("field", ["doc_id", "source_type", "source_id", "text_raw", "created_at", "collected_at"])
def test_raw_document_requires_mandatory_fields(field: str) -> None:
    payload = {
        "doc_id": "rss_article:abc",
        "source_type": "rss_article",
        "source_id": "abc",
        "parent_source_id": None,
        "text_raw": "Новость",
        "title_raw": "Заголовок",
        "author_raw": None,
        "created_at_raw": None,
        "created_at": datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        "collected_at": datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        "source_url": None,
        "source_domain": None,
        "region_hint_raw": None,
        "geo_raw": None,
        "engagement_raw": {},
        "raw_payload": {},
    }
    payload.pop(field)

    with pytest.raises(ValidationError):
        RawDocument.model_validate(payload)
