from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class ScoreBreakdown:
    volume_score: float
    dynamics_score: float
    sentiment_score: float
    reach_score: float
    geo_score: float
    source_score: float
    weights: dict[str, float]


@dataclass(frozen=True, slots=True)
class RankedSourceSummary:
    source_type: str
    count: int


@dataclass(frozen=True, slots=True)
class RankedSamplePost:
    doc_id: str
    text_preview: str
    source_type: str
    created_at: datetime
    reach: int
    source_url: str | None = None


@dataclass(frozen=True, slots=True)
class RankedTimelinePoint:
    hour: datetime
    count: int
    reach: int
    growth_rate: float


@dataclass(frozen=True, slots=True)
class RankingDocumentRecord:
    doc_id: str
    source_id: str
    author_id: str
    source_type: str
    text: str
    created_at: datetime
    reach: int
    region: str | None
    raw_payload: dict[str, Any]
    quality_weight: float = 1.0
    sentiment_score: float | None = None


@dataclass(slots=True)
class RankedCluster:
    cluster_id: str
    rank: int
    score: float
    score_breakdown: ScoreBreakdown
    summary: str
    key_phrases: list[str]
    period_start: datetime
    period_end: datetime
    size: int
    mention_count: int
    growth_rate: float
    reach_total: int
    geo_regions: list[str]
    unique_sources: int
    unique_authors: int
    sentiment_score: float
    is_new: bool
    is_growing: bool
    sample_doc_ids: list[str]
    sources: list[RankedSourceSummary] = field(default_factory=list)
    sample_posts: list[RankedSamplePost] = field(default_factory=list)
    timeline: list[RankedTimelinePoint] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RankingRecord:
    ranking_id: str
    computed_at: datetime
    period_start: datetime
    period_end: datetime
    weights_config: dict[str, Any]
    top_n: int
    period_hours: int


@dataclass(frozen=True, slots=True)
class StoredRankingItem:
    cluster_id: str
    rank: int
    score: float
    score_breakdown: ScoreBreakdown
    sentiment_score: float
    mention_count: int
    reach_total: int
    growth_rate: float
    geo_regions: tuple[str, ...]
    unique_authors: int
    unique_sources: int
    is_new: bool
    is_growing: bool


@dataclass(frozen=True, slots=True)
class StoredRankingSnapshot:
    ranking: RankingRecord
    items: tuple[StoredRankingItem, ...]


@dataclass(frozen=True, slots=True)
class RankingsUpdatedEvent:
    ranking_id: str
    computed_at: datetime
    period_start: datetime
    period_end: datetime
    top_n: int
    mode: str = "descriptions_updated"


@dataclass(frozen=True, slots=True)
class RankingMetrics:
    computed_at: datetime
    candidates_total: int
    candidates_excluded: int
    exclusion_reasons: dict[str, int]
    top10_score_range: tuple[float, float]
    score_gap_1_2: float
    new_entries: int
    dropped_entries: int
    runtime_ms: float


@dataclass(frozen=True, slots=True)
class RankingComputationResult:
    ranking: RankingRecord
    items: tuple[RankedCluster, ...]
    metrics: RankingMetrics
    event: RankingsUpdatedEvent
