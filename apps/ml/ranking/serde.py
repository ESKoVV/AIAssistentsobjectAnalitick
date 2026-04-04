from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any


def serialize_payload(payload: Any) -> Any:
    if is_dataclass(payload):
        payload = asdict(payload)

    if isinstance(payload, dict):
        return {str(key): serialize_payload(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [serialize_payload(value) for value in payload]
    if isinstance(payload, tuple):
        return [serialize_payload(value) for value in payload]
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, Enum):
        return payload.value
    return payload
