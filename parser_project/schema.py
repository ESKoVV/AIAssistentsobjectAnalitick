from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


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


class NormalizedDocument(BaseModel):
    doc_id: str
    source_type: SourceType
    source_id: str
    parent_id: Optional[str]

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

    region_hint: Optional[str]
    geo_lat: Optional[float]
    geo_lon: Optional[float]

    raw_payload: dict


class RawDocument(BaseModel):
    # legacy compatibility (not written to public.raw_messages)
    doc_id: Optional[str] = None

    source_type: str
    source_id: str
    parent_source_id: Optional[str] = None

    author_raw: Optional[str] = None
    text_raw: str
    media_type: Optional[str] = None

    created_at: datetime
    collected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    raw_payload: dict[str, Any]

    is_official: bool = False
    reach: int = 0
    likes: int = 0
    reposts: int = 0
    comments_count: int = 0
