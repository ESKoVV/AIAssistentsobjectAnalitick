from __future__ import annotations

import math
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Sequence
from uuid import uuid4

from .schema import (
    BufferedCandidate,
    Cluster,
    ClusterAssignment,
    ClusterDocumentRecord,
    ClusteringMetrics,
    ClusteringParams,
    ClustererSnapshot,
    OnlineAssignmentResult,
)


def cluster_documents(
    documents: Sequence[ClusterDocumentRecord],
    params: ClusteringParams,
    *,
    period_start: datetime,
    period_end: datetime,
    created_at: datetime | None = None,
    clusterer_factory: Callable[[ClusteringParams], object] | None = None,
    cluster_id_factory: Callable[[], str] | None = None,
) -> tuple[list[Cluster], object, list[int]]:
    created_at = created_at or datetime.now(timezone.utc)
    cluster_id_factory = cluster_id_factory or (lambda: str(uuid4()))

    if not documents:
        return [], _FallbackClusterer(), []

    vectors = normalize_embeddings([document.embedding for document in documents])
    clusterer = (clusterer_factory or _default_clusterer_factory)(params)
    labels = list(clusterer.fit_predict(vectors))

    clusters: list[Cluster] = []
    non_noise_labels: list[int] = []
    for label in sorted(set(labels)):
        indices = [index for index, value in enumerate(labels) if value == label]
        cluster_docs = [documents[index] for index in indices]
        cluster_vectors = [vectors[index] for index in indices]
        raw_centroid = _mean_vector(cluster_vectors)
        centroid = _normalize_vector(raw_centroid)
        cohesion = _mean_cosine_to_centroid(cluster_vectors, centroid)

        cluster_id = cluster_id_factory()
        clusters.append(
            Cluster(
                cluster_id=cluster_id,
                doc_ids=[document.doc_id for document in cluster_docs],
                centroid=centroid,
                created_at=created_at,
                period_start=period_start,
                period_end=period_end,
                size=len(cluster_docs),
                unique_authors=0,
                unique_sources=0,
                reach_total=0,
                earliest_doc_at=min(document.created_at for document in cluster_docs),
                latest_doc_at=max(document.created_at for document in cluster_docs),
                growth_rate=0.0,
                geo_regions=[],
                noise=(label == -1),
                cohesion_score=cohesion,
                algorithm_params=params.to_dict(),
            ),
        )
        if label != -1:
            non_noise_labels.append(int(label))

    return clusters, clusterer, non_noise_labels


def enrich_cluster(
    cluster: Cluster,
    documents: Sequence[ClusterDocumentRecord],
    *,
    now: datetime,
    growth_recent_hours: int,
    growth_previous_hours: int,
) -> Cluster:
    document_map = {document.doc_id: document for document in documents}
    cluster_documents = [document_map[doc_id] for doc_id in cluster.doc_ids if doc_id in document_map]
    if not cluster_documents:
        return cluster

    recent_start = now - timedelta(hours=growth_recent_hours)
    previous_start = recent_start - timedelta(hours=growth_previous_hours)

    cluster.unique_authors = len({document.author_id for document in cluster_documents})
    cluster.unique_sources = len({document.source_type for document in cluster_documents})
    cluster.reach_total = sum(document.reach for document in cluster_documents)
    cluster.earliest_doc_at = min(document.created_at for document in cluster_documents)
    cluster.latest_doc_at = max(document.created_at for document in cluster_documents)
    cluster.geo_regions = sorted({document.region for document in cluster_documents if document.region})

    recent = [
        document
        for document in cluster_documents
        if document.created_at >= recent_start
    ]
    previous = [
        document
        for document in cluster_documents
        if previous_start <= document.created_at < recent_start
    ]
    cluster.growth_rate = len(recent) / max(len(previous), 1)
    cluster.size = len(cluster.doc_ids)
    cluster.period_start = min(cluster.period_start, cluster.earliest_doc_at)
    cluster.period_end = max(cluster.period_end, cluster.latest_doc_at)
    return cluster


def reconcile_clusters(
    old_clusters: Sequence[Cluster],
    new_clusters: Sequence[Cluster],
    *,
    similarity_threshold: float,
) -> list[Cluster]:
    old_non_noise = [cluster for cluster in old_clusters if not cluster.noise]
    new_non_noise = [cluster for cluster in new_clusters if not cluster.noise]
    if not old_non_noise or not new_non_noise:
        return list(new_clusters)

    candidate_pairs: list[tuple[float, int, int]] = []
    for new_index, new_cluster in enumerate(new_non_noise):
        for old_index, old_cluster in enumerate(old_non_noise):
            similarity = cosine_similarity(new_cluster.centroid, old_cluster.centroid)
            if similarity >= similarity_threshold:
                candidate_pairs.append((similarity, new_index, old_index))

    matched_new: set[int] = set()
    matched_old: set[int] = set()
    for _, new_index, old_index in sorted(candidate_pairs, reverse=True):
        if new_index in matched_new or old_index in matched_old:
            continue
        new_non_noise[new_index].cluster_id = old_non_noise[old_index].cluster_id
        new_non_noise[new_index].created_at = old_non_noise[old_index].created_at
        matched_new.add(new_index)
        matched_old.add(old_index)

    return list(new_clusters)


def assign_online_documents(
    snapshot: ClustererSnapshot,
    documents: Sequence[ClusterDocumentRecord],
    params: ClusteringParams,
    *,
    assigned_at: datetime,
    approximate_predictor: Callable[[object, Sequence[Sequence[float]]], tuple[Sequence[int], Sequence[float]]] | None = None,
) -> OnlineAssignmentResult:
    if not documents:
        return OnlineAssignmentResult()

    predictor = approximate_predictor or _default_approximate_predict
    vectors = normalize_embeddings([document.embedding for document in documents])
    labels, strengths = predictor(snapshot.clusterer, vectors)

    assignments: list[ClusterAssignment] = []
    buffered_candidates: list[BufferedCandidate] = []
    grouped_documents: dict[str, list[ClusterDocumentRecord]] = defaultdict(list)

    for document, label, strength in zip(documents, labels, strengths):
        cluster_id = snapshot.label_to_cluster_id.get(int(label))
        normalized_strength = float(strength)
        if label != -1 and cluster_id and normalized_strength > params.assignment_strength_threshold:
            assignments.append(
                ClusterAssignment(
                    cluster_id=cluster_id,
                    doc_id=document.doc_id,
                    assigned_at=assigned_at,
                    strength=normalized_strength,
                ),
            )
            grouped_documents[cluster_id].append(document)
        else:
            buffered_candidates.append(
                BufferedCandidate(
                    doc_id=document.doc_id,
                    buffered_at=assigned_at,
                    last_strength=normalized_strength,
                ),
            )

    return OnlineAssignmentResult(
        assignments=tuple(assignments),
        buffered_candidates=tuple(buffered_candidates),
        updated_clusters=(),
    )


def rebuild_cluster(
    cluster: Cluster,
    documents: Sequence[ClusterDocumentRecord],
    params: ClusteringParams,
    *,
    now: datetime,
) -> Cluster:
    cluster_vectors = normalize_embeddings([document.embedding for document in documents])
    centroid = _normalize_vector(_mean_vector(cluster_vectors))
    cluster.doc_ids = [document.doc_id for document in documents]
    cluster.centroid = centroid
    cluster.cohesion_score = _mean_cosine_to_centroid(cluster_vectors, centroid)
    cluster.algorithm_params = params.to_dict()
    cluster.noise = False
    cluster = enrich_cluster(
        cluster,
        documents,
        now=now,
        growth_recent_hours=params.growth_recent_hours,
        growth_previous_hours=params.growth_previous_hours,
    )
    return cluster


def build_metrics(
    clusters: Sequence[Cluster],
    *,
    n_documents: int,
    run_at: datetime,
    min_cluster_size: int,
    runtime_seconds: float,
) -> ClusteringMetrics:
    non_noise_clusters = [cluster for cluster in clusters if not cluster.noise]
    noise_docs = sum(cluster.size for cluster in clusters if cluster.noise)
    avg_cohesion = (
        sum(cluster.cohesion_score for cluster in non_noise_clusters) / len(non_noise_clusters)
        if non_noise_clusters
        else 0.0
    )
    return ClusteringMetrics(
        run_at=run_at,
        n_documents=n_documents,
        n_clusters=len(non_noise_clusters),
        n_noise=noise_docs,
        noise_ratio=(noise_docs / n_documents) if n_documents else 0.0,
        avg_cohesion=avg_cohesion,
        min_cluster_size=min_cluster_size,
        runtime_seconds=runtime_seconds,
    )


def measure_runtime(
    function: Callable[[], tuple[list[Cluster], object, list[int]]],
) -> tuple[list[Cluster], object, list[int], float]:
    started_at = time.perf_counter()
    clusters, clusterer, cluster_labels = function()
    return clusters, clusterer, cluster_labels, time.perf_counter() - started_at


def normalize_embeddings(vectors: Sequence[Sequence[float]]) -> list[list[float]]:
    normalized = [_normalize_vector(vector) for vector in vectors]
    return normalized


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    left_norm = _l2_norm(left)
    right_norm = _l2_norm(right)
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return sum(left_value * right_value for left_value, right_value in zip(left, right)) / (
        left_norm * right_norm
    )


def _default_clusterer_factory(params: ClusteringParams) -> object:
    try:
        import hdbscan
    except ImportError as exc:
        raise RuntimeError("cluster_documents requires 'hdbscan' to be installed") from exc

    return hdbscan.HDBSCAN(
        min_cluster_size=params.min_cluster_size,
        min_samples=params.min_samples,
        metric="euclidean",
        cluster_selection_method="eom",
        prediction_data=True,
    )


def _default_approximate_predict(clusterer: object, vectors: Sequence[Sequence[float]]) -> tuple[Sequence[int], Sequence[float]]:
    try:
        import hdbscan
    except ImportError as exc:
        raise RuntimeError("online assignment requires 'hdbscan' to be installed") from exc

    return hdbscan.approximate_predict(clusterer, vectors)


def _mean_vector(vectors: Sequence[Sequence[float]]) -> list[float]:
    if not vectors:
        return []

    dimension = len(vectors[0])
    result = [0.0] * dimension
    for vector in vectors:
        for index, value in enumerate(vector):
            result[index] += float(value)
    return [value / len(vectors) for value in result]


def _normalize_vector(vector: Sequence[float]) -> list[float]:
    norm = _l2_norm(vector)
    if norm == 0.0:
        return [0.0 for _ in vector]
    return [float(value) / norm for value in vector]


def _l2_norm(vector: Sequence[float]) -> float:
    return math.sqrt(sum(float(value) * float(value) for value in vector))


def _mean_cosine_to_centroid(vectors: Sequence[Sequence[float]], centroid: Sequence[float]) -> float:
    if not vectors:
        return 0.0
    return sum(cosine_similarity(vector, centroid) for vector in vectors) / len(vectors)


class _FallbackClusterer:
    def fit_predict(self, vectors: Sequence[Sequence[float]]) -> list[int]:
        return [-1 for _ in vectors]
