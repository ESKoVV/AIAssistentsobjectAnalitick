from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from schema import RawMessage


def test_raw_message_accepts_valid_payload() -> None:
    message = RawMessage(
        source_type="vk_post",
        source_id="-123_456",
        author_id="42",
        text="Тестовый raw текст",
        media_type="text",
        created_at_utc=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        raw_payload={"id": 456},
        is_official=False,
        reach=10,
        likes=5,
        reposts=2,
        comments_count=1,
        parent_id=None,
    )

    dumped = message.model_dump(mode="json")

    assert dumped["source_type"] == "vk_post"
    assert dumped["text"] == "Тестовый raw текст"
    assert dumped["likes"] == 5


@pytest.mark.parametrize("field", ["source_type", "source_id", "text", "created_at_utc", "raw_payload"])
def test_raw_message_requires_mandatory_fields(field: str) -> None:
    payload = {
        "source_type": "rss_article",
        "source_id": "abc",
        "author_id": None,
        "text": "Новость",
        "media_type": None,
        "created_at_utc": datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        "collected_at": datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        "raw_payload": {},
        "is_official": False,
        "reach": 0,
        "likes": 0,
        "reposts": 0,
        "comments_count": 0,
        "parent_id": None,
    }
    payload.pop(field)

    with pytest.raises(ValidationError):
        RawMessage.model_validate(payload)
