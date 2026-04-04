from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


class SourceType(str, Enum):
    VK_POST = "vk_post"
    VK_COMMENT = "vk_comment"
    MAX_POST = "max_post"
    MAX_COMMENT = "max_comment"
    RSS_ARTICLE = "rss_article"
    PORTAL_APPEAL = "portal_appeal"


class MediaType(str, Enum):
    TEXT = "text"
    PHOTO = "photo"
    VIDEO = "video"
    LINK = "link"


@dataclass(slots=True)
class NormalizedDocument:
    doc_id: str
    source_type: SourceType
    source_id: str
    parent_id: str | None
    text: str
    media_type: MediaType
    created_at: datetime
    collected_at: datetime
    author_id: str
    is_official: bool
    reach: int
    likes: int
    reposts: int
    comments_count: int
    region_hint: str | None
    geo_lat: float | None
    geo_lon: float | None
    raw_payload: dict[str, Any]
