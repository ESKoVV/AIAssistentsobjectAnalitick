from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Callable, Sequence
from uuid import uuid4

from .config import ClusteringServiceConfig
from .engine import (
    assign_online_documents,
    build_metrics,
    cluster_documents,
    enrich_cluster,
    measure_runtime,
    rebuild_cluster,
    reconcile_clusters,
)
from .schema import (
    Cluster,
    ClusterAssignment,
    ClusterDocumentRecord,
    ClusteringParams,
    ClustersUpdatedEvent,
    ClustererSnapshot,
    FullRecomputeResult,
    OnlineAssignmentResult,
)
from .storage import ClusteringRepositoryProtocol


class ClusteringService:
    def __init__(
        self,
        *,
        repository: ClusteringRepositoryProtocol,
        config: ClusteringServiceConfig,
        clusterer_factory: Callable[[ClusteringParams], object] | None = None,
        approximate_predictor: Callable[[object, Sequence[Sequence[float]]], tuple[Sequence[int], Sequence[float]]] | None = None,
    ) -> None:
        self._repository = repository
        self._config = config
        self._clusterer_factory = clusterer_factory
        self._approximate_predictor = approximate_predictor
        self._params = ClusteringParams(
            min_cluster_size=config.min_cluster_size,
            min_samples=config.min_samples,
            assignment_strength_threshold=config.assignment_strength_threshold,
            reconcile_similarity_threshold=config.reconcile_similarity_threshold,
            full_recompute_window_hours=config.full_recompute_window_hours,
            growth_recent_hours=config.growth_recent_hours,
            growth_previous_hours=config.growth_previous_hours,
        )

    def initialize(self) -> None:
        self._repository.ensure_schema()
        self._repository.ensure_upstream_dependencies()

    def run_full_recompute(self, *, now: datetime | None = None) -> FullRecomputeResult:
        now = now or datetime.now(timezone.utc)
        period_end = now
        period_start = now - timedelta(hours=self._params.full_recompute_window_hours)
        documents = self._repository.fetch_documents_for_window(
            period_start=period_start,
            period_end=period_end,
        )

        clusters, clusterer, cluster_labels, runtime_seconds = measure_runtime(
            lambda: cluster_documents(
                documents,
                self._params,
                period_start=period_start,
                period_end=period_end,
                created_at=now,
                clusterer_factory=self._clusterer_factory,
            ),
        )

        enriched_clusters = [
            enrich_cluster(
                cluster,
                documents,
                now=now,
                growth_recent_hours=self._params.growth_recent_hours,
                growth_previous_hours=self._params.growth_previous_hours,
            )
            for cluster in clusters
        ]
        reconciled_clusters = reconcile_clusters(
            self._repository.load_latest_clusters(),
            enriched_clusters,
            similarity_threshold=self._params.reconcile_similarity_threshold,
        )
        assignments = tuple(
            ClusterAssignment(cluster_id=cluster.cluster_id, doc_id=doc_id, assigned_at=now, strength=1.0)
            for cluster in reconciled_clusters
            for doc_id in cluster.doc_ids
        )

        snapshot = self._build_snapshot(
            clusterer=clusterer,
            clusters=reconciled_clusters,
            cluster_labels=cluster_labels,
            period_start=period_start,
            period_end=period_end,
            now=now,
        )
        metrics = build_metrics(
            reconciled_clusters,
            n_documents=len(documents),
            run_at=now,
            min_cluster_size=self._params.min_cluster_size,
            runtime_seconds=runtime_seconds,
        )
        self._repository.save_full_recompute(
            clusters=reconciled_clusters,
            assignments=assignments,
            snapshot=snapshot,
            metrics=metrics,
        )
        event = ClustersUpdatedEvent(
            run_at=now,
            period_start=period_start,
            period_end=period_end,
            changed_cluster_ids=[
                cluster.cluster_id
                for cluster in reconciled_clusters
                if not cluster.noise
            ],
        )
        return FullRecomputeResult(
            clusters=tuple(reconciled_clusters),
            snapshot=snapshot,
            metrics=metrics,
            event=event,
        )

    def run_online_cycle(self, *, now: datetime | None = None) -> OnlineAssignmentResult:
        now = now or datetime.now(timezone.utc)
        snapshot = self._repository.load_latest_snapshot()
        if snapshot is None:
            return OnlineAssignmentResult()

        documents = self._repository.fetch_unassigned_documents_since(since=snapshot.period_end)
        decisions = assign_online_documents(
            snapshot,
            documents,
            self._params,
            assigned_at=now,
            approximate_predictor=self._approximate_predictor,
        )
        if not decisions.assignments and not decisions.buffered_candidates:
            return decisions

        self._repository.save_online_updates(
            assignments=decisions.assignments,
            buffered_candidates=decisions.buffered_candidates,
            updated_clusters=(),
        )
        updated_clusters = self._rebuild_updated_clusters(decisions.assignments, now=now)
        result = OnlineAssignmentResult(
            assignments=decisions.assignments,
            buffered_candidates=decisions.buffered_candidates,
            updated_clusters=tuple(updated_clusters),
        )
        self._repository.save_online_updates(
            assignments=(),
            buffered_candidates=(),
            updated_clusters=result.updated_clusters,
        )
        return result

    def _build_snapshot(
        self,
        *,
        clusterer: object,
        clusters: Sequence[Cluster],
        cluster_labels: Sequence[int],
        period_start: datetime,
        period_end: datetime,
        now: datetime,
    ) -> ClustererSnapshot | None:
        if not clusters:
            return None

        non_noise_clusters = [cluster for cluster in clusters if not cluster.noise]
        label_to_cluster_id = {
            int(label): cluster.cluster_id
            for label, cluster in zip(cluster_labels, non_noise_clusters)
        }

        return ClustererSnapshot(
            snapshot_id=str(uuid4()),
            clusterer=clusterer,
            params=self._params,
            period_start=period_start,
            period_end=period_end,
            label_to_cluster_id=label_to_cluster_id,
            created_at=now,
        )

    def _rebuild_updated_clusters(
        self,
        assignments: Sequence[ClusterAssignment],
        *,
        now: datetime,
    ) -> list[Cluster]:
        cluster_ids = sorted({assignment.cluster_id for assignment in assignments})
        if not cluster_ids:
            return []

        existing_clusters = self._repository.load_clusters_by_ids(cluster_ids)
        documents_by_cluster = self._repository.fetch_documents_for_clusters(cluster_ids)

        updated_clusters: list[Cluster] = []
        for cluster_id in cluster_ids:
            cluster = existing_clusters[cluster_id]
            documents = documents_by_cluster.get(cluster_id, ())
            updated_clusters.append(
                rebuild_cluster(
                    cluster,
                    documents,
                    self._params,
                    now=now,
                ),
            )

        return updated_clusters
