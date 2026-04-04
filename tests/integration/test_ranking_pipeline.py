from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from apps.ml.ranking import InMemoryRankingRepository, RankingService, RankingServiceConfig
from apps.orchestration.consumers import RankingConsumer, RankingConsumerDependencies
from tests.helpers import build_cluster, build_stored_cluster_description


class FakeMessage:
    def __init__(self, value):
        self.value = value
        self.committed = False

    async def commit(self) -> None:
        self.committed = True


class FakeProducer:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    async def publish(self, topic: str, value: dict[str, object]) -> None:
        self.published.append((topic, value))


def test_consumer_recomputes_ranking_and_publishes_event() -> None:
    now = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    repository = InMemoryRankingRepository()

    cluster_1 = build_cluster(
        cluster_id="cluster-1",
        doc_ids=["c1-doc-1", "c1-doc-2"],
        size=80,
        unique_sources=3,
        reach_total=100000,
        growth_rate=3.2,
        geo_regions=["volgograd-oblast", "astrakhan-oblast"],
        earliest_doc_at=now - timedelta(hours=1),
        latest_doc_at=now - timedelta(minutes=10),
    )
    cluster_2 = build_cluster(
        cluster_id="cluster-2",
        doc_ids=["c2-doc-1", "c2-doc-2"],
        size=120,
        unique_sources=1,
        reach_total=120000,
        growth_rate=1.1,
        geo_regions=["volgograd-oblast"],
        earliest_doc_at=now - timedelta(hours=10),
        latest_doc_at=now - timedelta(minutes=5),
    )
    repository.clusters = {
        cluster_1.cluster_id: cluster_1,
        cluster_2.cluster_id: cluster_2,
    }
    repository.descriptions = {
        cluster_1.cluster_id: build_stored_cluster_description(cluster_id=cluster_1.cluster_id),
        cluster_2.cluster_id: build_stored_cluster_description(cluster_id=cluster_2.cluster_id),
    }
    repository.document_sentiments = {
        "c1-doc-1": -0.9,
        "c1-doc-2": -0.7,
        "c2-doc-1": 0.8,
        "c2-doc-2": 0.5,
    }
    repository.document_reaches = {
        "c1-doc-1": 60000,
        "c1-doc-2": 40000,
        "c2-doc-1": 80000,
        "c2-doc-2": 40000,
    }

    config = RankingServiceConfig(postgres_dsn=None, top_n=2)
    service = RankingService(repository=repository, config=config)
    service.initialize()
    producer = FakeProducer()
    consumer = RankingConsumer(
        config=config,
        dependencies=RankingConsumerDependencies(service=service, producer=producer),
    )

    messages = [
        FakeMessage({"updated_cluster_ids": ["cluster-1"]}),
        FakeMessage(b'{"updated_cluster_ids":["cluster-2"]}'),
    ]

    processed = asyncio.run(consumer.handle_messages(messages))

    assert processed == 2
    assert all(message.committed for message in messages)
    assert len(repository.rankings) == 3
    assert {snapshot.ranking.period_hours for snapshot in repository.rankings} == {6, 24, 72}
    assert len(producer.published) == 1
    assert producer.published[0][0] == "rankings.updated"
    assert producer.published[0][1]["mode"] == "descriptions_updated"
    assert producer.published[0][1]["top_n"] == 2
