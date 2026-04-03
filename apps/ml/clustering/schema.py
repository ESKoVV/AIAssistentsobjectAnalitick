from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from apps.preprocessing.normalization import SourceType


@dataclass(frozen=True, slots=True)
class ClusteringParams:
    min_cluster_size: int = 10
    min_samples: int = 5
    assignment_strength_threshold: float = 0.6
    reconcile_similarity_threshold: float = 0.85
    full_recompute_window_hours: int = 72
    growth_recent_hours: int = 6
    growth_previous_hours: int = 6

    def to_dict(self) -> dict[str, Any]:
        return {
            "min_cluster_size": self.min_cluster_size,
            "min_samples": self.min_samples,
            "assignment_strength_threshold": self.assignment_strength_threshold,
            "reconcile_similarity_threshold": self.reconcile_similarity_threshold,
            "full_recompute_window_hours": self.full_recompute_window_hours,
            "growth_recent_hours": self.growth_recent_hours,
            "growth_previous_hours": self.growth_previous_hours,
        }


@dataclass(slots=True)
class Cluster:
    cluster_id: str
    doc_ids: list[str]
    centroid: list[float]
    created_at: datetime
    period_start: datetime
    period_end: datetime
    size: int
    unique_authors: int
    unique_sources: int
    reach_total: int
    earliest_doc_at: datetime
    latest_doc_at: datetime
    growth_rate: float
    geo_regions: list[str]
    noise: bool
    cohesion_score: float
    algorithm_params: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ClusteringMetrics:
    run_at: datetime
    n_documents: int
    n_clusters: int
    n_noise: int
    noise_ratio: float
    avg_cohesion: float
    min_cluster_size: int
    runtime_seconds: float


@dataclass(frozen=True, slots=True)
class ClustersUpdatedEvent:
    run_at: datetime
    period_start: datetime
    period_end: datetime
    changed_cluster_ids: list[str]
    mode: str = "full_recompute"


@dataclass(frozen=True, slots=True)
class ClusterDocumentRecord:
    doc_id: str
    embedding: list[float]
    author_id: str
    source_type: SourceType
    reach: int
    created_at: datetime
    region: str | None


@dataclass(frozen=True, slots=True)
class ClusterAssignment:
    cluster_id: str
    doc_id: str
    assigned_at: datetime
    strength: float = 1.0


@dataclass(frozen=True, slots=True)
class BufferedCandidate:
    doc_id: str
    buffered_at: datetime
    last_strength: float


@dataclass(slots=True)
class ClustererSnapshot:
    snapshot_id: str
    clusterer: Any
    params: ClusteringParams
    period_start: datetime
    period_end: datetime
    label_to_cluster_id: dict[int, str]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ClusterRunRecord:
    run_id: str
    mode: str
    params: dict[str, Any]
    n_documents: int
    n_clusters: int
    n_noise: int
    noise_ratio: float
    avg_cohesion: float
    runtime_seconds: float
    snapshot_id: str | None
    run_at: datetime


@dataclass(frozen=True, slots=True)
class OnlineAssignmentResult:
    assignments: tuple[ClusterAssignment, ...] = ()
    buffered_candidates: tuple[BufferedCandidate, ...] = ()
    updated_clusters: tuple[Cluster, ...] = ()


@dataclass(frozen=True, slots=True)
class FullRecomputeResult:
    clusters: tuple[Cluster, ...]
    snapshot: ClustererSnapshot | None
    metrics: ClusteringMetrics
    event: ClustersUpdatedEvent | None
