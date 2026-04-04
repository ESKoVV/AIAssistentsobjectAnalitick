from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from schema import RawMessage


def test_raw_message_contract_accepts_raw_payload_only() -> None:
    payload = {
        "source_type": "vk_post",
        "source_id": "-123_456",
        "author_id": "42",
        "text": "Жители пишут об аварии на теплосети",
        "media_type": "text",
        "created_at_utc": datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc),
        "collected_at": datetime(2026, 4, 2, 9, 5, tzinfo=timezone.utc),
        "raw_payload": {"id": 456, "owner_id": -123, "from_id": 42},
        "is_official": False,
        "reach": 100,
        "likes": 4,
        "reposts": 1,
        "comments_count": 2,
        "parent_id": None,
    }

    message = RawMessage.model_validate(payload)

    assert message.source_type.value == "vk_post"
    assert message.source_id == "-123_456"
    assert not hasattr(message, "doc_id")


def test_raw_message_contract_rejects_derived_fields() -> None:
    payload = {
        "source_type": "vk_post",
        "source_id": "-123_456",
        "text": "Жители пишут об аварии на теплосети",
        "created_at_utc": datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc),
        "collected_at": datetime(2026, 4, 2, 9, 5, tzinfo=timezone.utc),
        "raw_payload": {"id": 456, "owner_id": -123},
        "doc_id": "vk_post:-123_456",
    }

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        RawMessage.model_validate(payload)
