from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from schema import RawDocument


def test_raw_document_accepts_valid_payload() -> None:
    doc = RawDocument(
        source_type="vk_post",
        source_id="-123_456",
        parent_source_id=None,
        text_raw="Тестовый raw текст",
        author_raw="42",
        media_type="text",
        created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        raw_payload={"id": 456},
        is_official=False,
        reach=10,
        likes=5,
        reposts=2,
        comments_count=1,
    )

    dumped = doc.model_dump(mode="json")

    assert dumped["source_type"] == "vk_post"
    assert dumped["text_raw"] == "Тестовый raw текст"
    assert dumped["likes"] == 5


@pytest.mark.parametrize("field", ["source_type", "source_id", "text_raw", "created_at", "raw_payload"])
def test_raw_document_requires_mandatory_fields(field: str) -> None:
    payload = {
        "source_type": "rss_article",
        "source_id": "abc",
        "parent_source_id": None,
        "text_raw": "Новость",
        "author_raw": None,
        "media_type": None,
        "created_at": datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        "collected_at": datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        "raw_payload": {},
        "is_official": False,
        "reach": 0,
        "likes": 0,
        "reposts": 0,
        "comments_count": 0,
    }
    payload.pop(field)

    with pytest.raises(ValidationError):
        RawDocument.model_validate(payload)
