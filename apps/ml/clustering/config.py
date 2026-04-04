from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_INPUT_TABLE = "embeddings"
DEFAULT_DOCUMENTS_TABLE = "normalized_messages"
DEFAULT_UPDATED_TOPIC = "clusters.updated"
DEFAULT_FULL_RECOMPUTE_WINDOW_HOURS = 72
DEFAULT_GROWTH_RECENT_HOURS = 6
DEFAULT_GROWTH_PREVIOUS_HOURS = 6
DEFAULT_MIN_CLUSTER_SIZE = 10
DEFAULT_MIN_SAMPLES = 5
DEFAULT_ASSIGNMENT_STRENGTH_THRESHOLD = 0.6
DEFAULT_RECONCILE_SIMILARITY_THRESHOLD = 0.85


@dataclass(frozen=True, slots=True)
class ClusteringServiceConfig:
    postgres_dsn: str | None
    kafka_bootstrap_servers: str | None = None
    embeddings_table: str = DEFAULT_INPUT_TABLE
    documents_table: str = DEFAULT_DOCUMENTS_TABLE
    updated_topic: str = DEFAULT_UPDATED_TOPIC
    min_cluster_size: int = DEFAULT_MIN_CLUSTER_SIZE
    min_samples: int = DEFAULT_MIN_SAMPLES
    assignment_strength_threshold: float = DEFAULT_ASSIGNMENT_STRENGTH_THRESHOLD
    reconcile_similarity_threshold: float = DEFAULT_RECONCILE_SIMILARITY_THRESHOLD
    full_recompute_window_hours: int = DEFAULT_FULL_RECOMPUTE_WINDOW_HOURS
    growth_recent_hours: int = DEFAULT_GROWTH_RECENT_HOURS
    growth_previous_hours: int = DEFAULT_GROWTH_PREVIOUS_HOURS

    def __post_init__(self) -> None:
        if self.min_cluster_size <= 0:
            raise ValueError("min_cluster_size must be positive")
        if self.min_samples <= 0:
            raise ValueError("min_samples must be positive")
        if not 0.0 <= self.assignment_strength_threshold <= 1.0:
            raise ValueError("assignment_strength_threshold must be between 0 and 1")
        if not 0.0 <= self.reconcile_similarity_threshold <= 1.0:
            raise ValueError("reconcile_similarity_threshold must be between 0 and 1")
        if self.full_recompute_window_hours <= 0:
            raise ValueError("full_recompute_window_hours must be positive")
        if self.growth_recent_hours <= 0:
            raise ValueError("growth_recent_hours must be positive")
        if self.growth_previous_hours <= 0:
            raise ValueError("growth_previous_hours must be positive")

    @classmethod
    def from_env(cls) -> "ClusteringServiceConfig":
        return cls(
            postgres_dsn=os.getenv("CLUSTERING_POSTGRES_DSN"),
            kafka_bootstrap_servers=os.getenv("CLUSTERING_KAFKA_BOOTSTRAP_SERVERS"),
            embeddings_table=os.getenv("CLUSTERING_EMBEDDINGS_TABLE", DEFAULT_INPUT_TABLE),
            documents_table=os.getenv("CLUSTERING_DOCUMENTS_TABLE", DEFAULT_DOCUMENTS_TABLE),
            updated_topic=os.getenv("CLUSTERING_UPDATED_TOPIC", DEFAULT_UPDATED_TOPIC),
            min_cluster_size=int(os.getenv("CLUSTERING_MIN_CLUSTER_SIZE", DEFAULT_MIN_CLUSTER_SIZE)),
            min_samples=int(os.getenv("CLUSTERING_MIN_SAMPLES", DEFAULT_MIN_SAMPLES)),
            assignment_strength_threshold=float(
                os.getenv(
                    "CLUSTERING_ASSIGNMENT_STRENGTH_THRESHOLD",
                    DEFAULT_ASSIGNMENT_STRENGTH_THRESHOLD,
                ),
            ),
            reconcile_similarity_threshold=float(
                os.getenv(
                    "CLUSTERING_RECONCILE_SIMILARITY_THRESHOLD",
                    DEFAULT_RECONCILE_SIMILARITY_THRESHOLD,
                ),
            ),
            full_recompute_window_hours=int(
                os.getenv(
                    "CLUSTERING_FULL_RECOMPUTE_WINDOW_HOURS",
                    DEFAULT_FULL_RECOMPUTE_WINDOW_HOURS,
                ),
            ),
            growth_recent_hours=int(
                os.getenv("CLUSTERING_GROWTH_RECENT_HOURS", DEFAULT_GROWTH_RECENT_HOURS),
            ),
            growth_previous_hours=int(
                os.getenv("CLUSTERING_GROWTH_PREVIOUS_HOURS", DEFAULT_GROWTH_PREVIOUS_HOURS),
            ),
        )
