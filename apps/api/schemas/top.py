from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


PeriodLiteral = Literal["6h", "24h", "72h"]
GranularityLiteral = Literal["hourly", "6h", "daily"]


class APIError(BaseModel):
    error_code: str
    message: str
    request_id: str


class UrgencyLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ScoreBreakdown(BaseModel):
    volume: float
    dynamics: float
    sentiment: float
    reach: float
    geo: float
    source: float


class SourceSummary(BaseModel):
    source_type: str
    count: int


class SamplePost(BaseModel):
    doc_id: str
    text_preview: str
    source_type: str
    created_at: datetime
    reach: int
    source_url: str | None = None


class TopItem(BaseModel):
    rank: int
    cluster_id: str
    summary: str
    key_phrases: list[str]
    urgency: UrgencyLevel
    urgency_reason: str
    mention_count: int
    unique_authors: int
    reach_total: int
    growth_rate: float
    is_new: bool
    is_growing: bool
    geo_regions: list[str]
    sources: list[SourceSummary]
    sample_posts: list[SamplePost]
    score: float
    score_breakdown: ScoreBreakdown


class TopResponse(BaseModel):
    computed_at: datetime
    period_start: datetime
    period_end: datetime
    total_clusters: int
    items: list[TopItem]


class TimelinePoint(BaseModel):
    hour: datetime
    count: int
    reach: int
    growth_rate: float


class TimelineResponse(BaseModel):
    cluster_id: str
    points: list[TimelinePoint]


class ClusterDetailResponse(TopItem):
    sample_doc_ids: list[str]
    all_regions: list[str]
    timeline: list[TimelinePoint]


class ClusterDocument(BaseModel):
    doc_id: str
    source_id: str
    source_type: str
    author_id: str
    text: str
    text_preview: str
    created_at: datetime
    collected_at: datetime
    reach: int
    likes: int
    reposts: int
    comments_count: int
    is_official: bool
    parent_id: str | None = None
    region: str | None = None
    source_url: str | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class ClusterDocumentsResponse(BaseModel):
    cluster_id: str
    page: int
    page_size: int
    total: int
    items: list[ClusterDocument]


class HistoryBucket(BaseModel):
    bucket_start: datetime
    bucket_end: datetime
    computed_at: datetime
    items: list[TopItem]


class HistoryResponse(BaseModel):
    from_dt: datetime
    to_dt: datetime
    granularity: GranularityLiteral
    buckets: list[HistoryBucket]


class HealthResponse(BaseModel):
    status: str
    last_ranking_at: datetime
    ranking_age_minutes: int
    documents_last_hour: int
    pipeline_status: dict[str, str]


class TopQueryParams(BaseModel):
    region: str | None = None
    source: str | None = None
    period: PeriodLiteral = "24h"
    limit: int = Field(default=10, ge=1, le=50)
    as_of: datetime | None = None


class ClusterDocumentsQueryParams(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    source_type: str | None = None
    region: str | None = None


class HistoryQueryParams(BaseModel):
    from_dt: datetime
    to_dt: datetime
    granularity: GranularityLiteral = "hourly"
