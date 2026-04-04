from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from apps.ml.clustering.config import ClusteringServiceConfig
from apps.ml.clustering.schema import ClusteringParams
from apps.ml.clustering.service import ClusteringService
from apps.ml.clustering.storage import InMemoryClusteringRepository
from apps.orchestration.schedulers import NullClusterEventPublisher, run_full_recompute, run_online_cycle
from tests.helpers import build_cluster_document_record


class SequenceClustererFactory:
    def __init__(self, labels_per_run: list[list[int]]) -> None:
        self._labels_per_run = labels_per_run
        self._index = 0

    def __call__(self, params: ClusteringParams) -> object:
        del params
        labels = self._labels_per_run[self._index]
        self._index += 1

        class _Clusterer:
            def fit_predict(self, vectors):  # type: ignore[no-untyped-def]
                assert len(vectors) == len(labels)
                return list(labels)

        return _Clusterer()


class FakePublisher(NullClusterEventPublisher):
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, object]]] = []

    async def publish(self, topic: str, value: dict[str, object]) -> None:
        self.messages.append((topic, value))


def test_full_recompute_reads_documents_persists_clusters_snapshot_and_event() -> None:
    repository = InMemoryClusteringRepository()
    now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    repository.documents = {
        "a1": build_cluster_document_record(doc_id="a1", embedding=[1.0, 0.0], created_at=now - timedelta(hours=1)),
        "a2": build_cluster_document_record(doc_id="a2", embedding=[0.98, 0.02], created_at=now - timedelta(hours=2)),
        "b1": build_cluster_document_record(doc_id="b1", embedding=[0.0, 1.0], created_at=now - timedelta(hours=3)),
    }
    service = ClusteringService(
        repository=repository,
        config=ClusteringServiceConfig(postgres_dsn=None),
        clusterer_factory=SequenceClustererFactory([[0, 0, -1]]),
    )
    publisher = FakePublisher()

    service.initialize()
    result = asyncio.run(
        run_full_recompute(
            service=service,
            publisher=publisher,
            topic="clusters.updated",
            now=now,
        ),
    )

    assert len(result.clusters) == 2
    assert repository.snapshot is not None
    assert len(repository.cluster_documents) == 2
    assert publisher.messages[0][0] == "clusters.updated"
    assert sorted(publisher.messages[0][1]["changed_cluster_ids"]) == sorted(
        [cluster.cluster_id for cluster in result.clusters if not cluster.noise],
    )


def test_second_full_recompute_reuses_cluster_id_for_similar_cluster_and_creates_new_one() -> None:
    repository = InMemoryClusteringRepository()
    now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    repository.documents = {
        "a1": build_cluster_document_record(doc_id="a1", embedding=[1.0, 0.0], created_at=now - timedelta(hours=1)),
        "a2": build_cluster_document_record(doc_id="a2", embedding=[0.99, 0.01], created_at=now - timedelta(hours=2)),
        "b1": build_cluster_document_record(doc_id="b1", embedding=[0.0, 1.0], created_at=now - timedelta(hours=3)),
        "b2": build_cluster_document_record(doc_id="b2", embedding=[0.01, 0.99], created_at=now - timedelta(hours=4)),
    }
    service = ClusteringService(
        repository=repository,
        config=ClusteringServiceConfig(postgres_dsn=None),
        clusterer_factory=SequenceClustererFactory([[0, 0, 1, 1], [0, 0, 2, 2]]),
    )
    service.initialize()

    first_result = service.run_full_recompute(now=now)
    first_cluster_ids = {
        cluster.cluster_id
        for cluster in first_result.clusters
        if not cluster.noise
    }

    repository.documents = {
        "a1": repository.documents["a1"],
        "a2": repository.documents["a2"],
    }
    repository.documents["c1"] = build_cluster_document_record(
        doc_id="c1",
        embedding=[-1.0, 0.0],
        created_at=now - timedelta(minutes=10),
    )
    repository.documents["c2"] = build_cluster_document_record(
        doc_id="c2",
        embedding=[-0.99, 0.01],
        created_at=now - timedelta(minutes=5),
    )
    second_result = service.run_full_recompute(now=now + timedelta(minutes=1))
    second_cluster_ids = {
        cluster.cluster_id
        for cluster in second_result.clusters
        if not cluster.noise
    }

    assert first_cluster_ids & second_cluster_ids
    assert second_cluster_ids - first_cluster_ids


def test_online_cycle_assigns_strong_matches_updates_clusters_and_buffers_weak_ones() -> None:
    repository = InMemoryClusteringRepository()
    now = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    repository.documents = {
        "a1": build_cluster_document_record(doc_id="a1", embedding=[1.0, 0.0], created_at=now - timedelta(hours=1)),
        "a2": build_cluster_document_record(doc_id="a2", embedding=[0.98, 0.02], created_at=now - timedelta(hours=2)),
    }
    service = ClusteringService(
        repository=repository,
        config=ClusteringServiceConfig(postgres_dsn=None),
        clusterer_factory=SequenceClustererFactory([[0, 0]]),
        approximate_predictor=lambda clusterer, vectors: ([0, -1], [0.9, 0.4]),
    )
    service.initialize()
    full_result = service.run_full_recompute(now=now)
    strong_cluster_id = next(cluster.cluster_id for cluster in full_result.clusters if not cluster.noise)

    repository.documents["new-strong"] = build_cluster_document_record(
        doc_id="new-strong",
        embedding=[0.99, 0.01],
        created_at=now + timedelta(minutes=1),
    )
    repository.documents["new-weak"] = build_cluster_document_record(
        doc_id="new-weak",
        embedding=[0.0, 1.0],
        created_at=now + timedelta(minutes=2),
    )

    result = run_online_cycle(service=service)

    assert [assignment.doc_id for assignment in result.assignments] == ["new-strong"]
    assert result.updated_clusters[0].cluster_id == strong_cluster_id
    assert "new-strong" in result.updated_clusters[0].doc_ids
    assert "new-weak" in repository.buffered_candidates


def test_startup_preflight_fails_when_upstream_tables_are_missing() -> None:
    repository = InMemoryClusteringRepository()
    repository.upstream_ready = False
    service = ClusteringService(
        repository=repository,
        config=ClusteringServiceConfig(postgres_dsn=None),
    )

    with pytest.raises(RuntimeError, match="upstream tables"):
        service.initialize()
