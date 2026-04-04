from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from apps.ml.clustering.schema import Cluster, ClustersUpdatedEvent
from tests.helpers import build_cluster


EXPECTED_CLUSTER_FIELDS = {
    "cluster_id",
    "doc_ids",
    "centroid",
    "created_at",
    "period_start",
    "period_end",
    "size",
    "unique_authors",
    "unique_sources",
    "reach_total",
    "earliest_doc_at",
    "latest_doc_at",
    "growth_rate",
    "geo_regions",
    "noise",
    "cohesion_score",
    "algorithm_params",
}

EXPECTED_EVENT_FIELDS = {
    "run_at",
    "period_start",
    "period_end",
    "changed_cluster_ids",
    "mode",
}


def test_cluster_contract_contains_expected_fields() -> None:
    cluster = build_cluster()

    assert isinstance(cluster, Cluster)
    assert set(asdict(cluster).keys()) == EXPECTED_CLUSTER_FIELDS


def test_clusters_updated_event_contains_only_changed_ids_and_period_info() -> None:
    event = ClustersUpdatedEvent(
        run_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        period_start=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        period_end=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        changed_cluster_ids=["cluster-1", "cluster-2"],
    )

    assert set(asdict(event).keys()) == EXPECTED_EVENT_FIELDS
    assert event.changed_cluster_ids == ["cluster-1", "cluster-2"]
    assert event.mode == "full_recompute"
