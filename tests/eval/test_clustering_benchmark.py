from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from apps.ml.clustering.engine import build_metrics, cluster_documents
from apps.ml.clustering.schema import ClusteringParams
from tests.helpers import build_cluster_document_record


@pytest.mark.skipif(
    os.getenv("RUN_CLUSTERING_BENCHMARK") != "1",
    reason="Set RUN_CLUSTERING_BENCHMARK=1 to run the clustering smoke benchmark",
)
def test_clustering_runtime_and_noise_smoke() -> None:
    pytest.importorskip("hdbscan")
    documents = [
        build_cluster_document_record(
            doc_id=f"doc-{index}",
            embedding=[1.0, 0.0] if index < 32 else [0.0, 1.0],
            created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc) - timedelta(minutes=index),
        )
        for index in range(64)
    ]
    params = ClusteringParams(min_cluster_size=8, min_samples=4)

    started_at = time.perf_counter()
    clusters, _, _ = cluster_documents(
        documents,
        params,
        period_start=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        period_end=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
    )
    runtime = time.perf_counter() - started_at
    metrics = build_metrics(
        clusters,
        n_documents=len(documents),
        run_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        min_cluster_size=params.min_cluster_size,
        runtime_seconds=runtime,
    )

    assert runtime < 5.0
    assert metrics.noise_ratio <= 0.4
