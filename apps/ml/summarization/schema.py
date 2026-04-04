from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from apps.preprocessing.normalization import SourceType


@dataclass(frozen=True, slots=True)
class ClusterDescription:
    cluster_id: str
    summary: str
    key_phrases: list[str]
    sample_doc_ids: list[str]
    model_name: str
    prompt_version: str
    generated_at: datetime
    input_token_count: int
    output_token_count: int
    generation_time_ms: int
    fallback_used: bool


@dataclass(frozen=True, slots=True)
class StoredClusterDescription:
    description: ClusterDescription
    needs_review: bool
    cluster_size_at_generation: int


@dataclass(frozen=True, slots=True)
class DescriptionHistoryRecord:
    cluster_id: str
    summary: str
    prompt_version: str
    generated_at: datetime
    superseded_at: datetime


@dataclass(frozen=True, slots=True)
class DescriptionsUpdatedEvent:
    run_at: datetime
    updated_cluster_ids: list[str]
    mode: str = "batch_refresh"


@dataclass(frozen=True, slots=True)
class DescriptionMetrics:
    run_at: datetime
    clusters_processed: int
    clusters_regenerated: int
    clusters_skipped: int
    fallback_used_count: int
    validation_failed: int
    avg_generation_time_ms: float
    total_input_tokens: int
    total_output_tokens: int
    estimated_cost_usd: float


@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class LLMUsage:
    input_tokens: int
    output_tokens: int


@dataclass(frozen=True, slots=True)
class LLMResponse:
    text: str
    usage: LLMUsage
    model_name: str
    fallback_used: bool = False


@dataclass(frozen=True, slots=True)
class SummarizationDocumentRecord:
    doc_id: str
    author_id: str
    source_type: SourceType
    text: str
    created_at: datetime
    region: str | None
    embedding: list[float]


@dataclass(frozen=True, slots=True)
class LLMCostRecord:
    cluster_id: str
    attempt_number: int
    model_name: str
    prompt_version: str
    requested_at: datetime
    input_token_count: int
    output_token_count: int
    estimated_cost_usd: float
    generation_time_ms: int
    fallback_used: bool
    validation_error: str | None = None


@dataclass(frozen=True, slots=True)
class ClusterDescriptionBatchResult:
    updated_cluster_ids: tuple[str, ...]
    metrics: DescriptionMetrics
    event: DescriptionsUpdatedEvent | None
