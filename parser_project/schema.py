from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict


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


class RawMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: SourceType
    source_id: str
    author_id: Optional[str] = None

    text: str
    media_type: Optional[MediaType] = None

    created_at_utc: datetime
    collected_at: datetime

    raw_payload: dict

    is_official: bool = False
    reach: int = 0
    likes: int = 0
    reposts: int = 0
    comments_count: int = 0

    parent_id: Optional[str] = None


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
