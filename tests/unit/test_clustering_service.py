from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from apps.ml.clustering.engine import (
    assign_online_documents,
    build_metrics,
    cluster_documents,
    enrich_cluster,
    reconcile_clusters,
)
from apps.ml.clustering.schema import ClusteringParams, ClustererSnapshot
from apps.preprocessing.normalization import SourceType
from tests.helpers import build_cluster, build_cluster_document_record


class FakeClusterer:
    def __init__(self, labels: list[int]) -> None:
        self._labels = labels

    def fit_predict(self, vectors):  # type: ignore[no-untyped-def]
        assert len(vectors) == len(self._labels)
        return list(self._labels)


def test_hdbscan_on_synthetic_embeddings_finds_two_clusters_and_noise() -> None:
    pytest.importorskip("hdbscan")
    documents = [
        build_cluster_document_record(doc_id="a1", embedding=[1.0, 0.0]),
        build_cluster_document_record(doc_id="a2", embedding=[0.98, 0.02]),
        build_cluster_document_record(doc_id="b1", embedding=[0.0, 1.0]),
        build_cluster_document_record(doc_id="b2", embedding=[0.02, 0.98]),
        build_cluster_document_record(doc_id="n1", embedding=[0.7, 0.7]),
    ]

    clusters, _, _ = cluster_documents(
        documents,
        ClusteringParams(min_cluster_size=2, min_samples=1),
        period_start=datetime(2026, 4, 4, 0, 0, tzinfo=timezone.utc),
        period_end=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
    )

    assert len([cluster for cluster in clusters if not cluster.noise]) == 2
    assert any(cluster.noise for cluster in clusters)


def test_reconcile_preserves_cluster_id_only_for_best_match_above_threshold() -> None:
    old_clusters = [
        build_cluster(cluster_id="old-a", centroid=[1.0, 0.0]),
        build_cluster(cluster_id="old-b", centroid=[0.0, 1.0]),
    ]
    new_clusters = [
        build_cluster(cluster_id="new-a", centroid=[0.99, 0.01]),
        build_cluster(cluster_id="new-c", centroid=[-1.0, 0.0]),
    ]

    reconciled = reconcile_clusters(old_clusters, new_clusters, similarity_threshold=0.85)

    assert reconciled[0].cluster_id == "old-a"
    assert reconciled[1].cluster_id == "new-c"


def test_online_assignment_splits_strong_matches_and_buffered_candidates() -> None:
    now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    snapshot = ClustererSnapshot(
        snapshot_id="snapshot-1",
        clusterer=object(),
        params=ClusteringParams(),
        period_start=now - timedelta(hours=72),
        period_end=now - timedelta(minutes=15),
        label_to_cluster_id={0: "cluster-1"},
        created_at=now - timedelta(minutes=1),
    )
    documents = [
        build_cluster_document_record(doc_id="doc-1"),
        build_cluster_document_record(doc_id="doc-2", embedding=[0.0, 1.0]),
    ]

    result = assign_online_documents(
        snapshot,
        documents,
        ClusteringParams(assignment_strength_threshold=0.6),
        assigned_at=now,
        approximate_predictor=lambda clusterer, vectors: ([0, -1], [0.91, 0.2]),
    )

    assert [assignment.doc_id for assignment in result.assignments] == ["doc-1"]
    assert [candidate.doc_id for candidate in result.buffered_candidates] == ["doc-2"]


def test_enrich_cluster_computes_aggregates_and_growth_rate() -> None:
    now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    documents = [
        build_cluster_document_record(
            doc_id="doc-1",
            author_id="author-1",
            source_type=SourceType.VK_POST,
            reach=100,
            created_at=now - timedelta(hours=2),
            region="volgograd-oblast",
        ),
        build_cluster_document_record(
            doc_id="doc-2",
            author_id="author-2",
            source_type=SourceType.RSS_ARTICLE,
            reach=200,
            created_at=now - timedelta(hours=4),
            region="astrakhan-oblast",
        ),
        build_cluster_document_record(
            doc_id="doc-3",
            author_id="author-2",
            source_type=SourceType.RSS_ARTICLE,
            reach=50,
            created_at=now - timedelta(hours=8),
            region="volgograd-oblast",
        ),
    ]
    cluster = build_cluster(doc_ids=["doc-1", "doc-2", "doc-3"], size=3)

    enriched = enrich_cluster(
        cluster,
        documents,
        now=now,
        growth_recent_hours=6,
        growth_previous_hours=6,
    )

    assert enriched.unique_authors == 2
    assert enriched.unique_sources == 2
    assert enriched.reach_total == 350
    assert enriched.geo_regions == ["astrakhan-oblast", "volgograd-oblast"]
    assert enriched.growth_rate == pytest.approx(2.0)


def test_metrics_follow_monitoring_contract() -> None:
    clusters = [
        build_cluster(cluster_id="cluster-1", size=3, cohesion_score=0.8, noise=False),
        build_cluster(cluster_id="noise", doc_ids=["n1", "n2"], size=2, cohesion_score=0.0, noise=True),
    ]

    metrics = build_metrics(
        clusters,
        n_documents=5,
        run_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        min_cluster_size=10,
        runtime_seconds=1.25,
    )

    assert metrics.n_clusters == 1
    assert metrics.n_noise == 2
    assert metrics.noise_ratio == pytest.approx(0.4)
    assert metrics.avg_cohesion == pytest.approx(0.8)
