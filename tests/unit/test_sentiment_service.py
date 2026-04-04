from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from apps.ml.sentiment import (
    InMemorySentimentRepository,
    SentimentBatchService,
    SentimentPrediction,
    SentimentServiceConfig,
)
from apps.orchestration.consumers.sentiment_consumer import (
    SentimentConsumer,
    SentimentConsumerDependencies,
)
from tests.helpers import build_enriched_document


class FakeBackend:
    def __init__(self, predictions: list[SentimentPrediction]) -> None:
        self._predictions = predictions
        self.calls = 0

    def predict(self, texts):  # type: ignore[no-untyped-def]
        self.calls += 1
        assert all(isinstance(text, str) and text.strip() for text in texts)
        return tuple(self._predictions)


class FakeMessage:
    def __init__(self, value):
        self.value = value
        self.committed = False

    async def commit(self) -> None:
        self.committed = True


def test_sentiment_service_persists_document_sentiments() -> None:
    repository = InMemorySentimentRepository()
    backend = FakeBackend(
        [
            SentimentPrediction(label="NEGATIVE", score=-0.82, raw_result={"label": "NEGATIVE"}),
            SentimentPrediction(label="POSITIVE", score=0.34, raw_result={"label": "POSITIVE"}),
        ],
    )
    service = SentimentBatchService(
        repository=repository,
        backend=backend,
        config=SentimentServiceConfig(
            postgres_dsn=None,
            model_name="blanchefort/rubert-base-cased-sentiment",
            model_version="rubert-v1",
        ),
    )
    service.initialize()
    documents = [
        build_enriched_document(doc_id="doc-1", text="Во дворе нет воды"),
        build_enriched_document(doc_id="doc-2", text="Подача воды восстановлена"),
    ]

    processed = service.process_batch(
        documents,
        now=datetime(2026, 4, 5, 10, 0, tzinfo=timezone.utc),
    )

    assert len(processed) == 2
    assert backend.calls == 1
    assert repository.sentiments["doc-1"].sentiment_score == -0.82
    assert repository.sentiments["doc-2"].sentiment_score == 0.34


def test_sentiment_consumer_commits_processed_messages() -> None:
    repository = InMemorySentimentRepository()
    service = SentimentBatchService(
        repository=repository,
        backend=FakeBackend(
            [SentimentPrediction(label="NEGATIVE", score=-0.6, raw_result={"label": "NEGATIVE"})],
        ),
        config=SentimentServiceConfig(
            postgres_dsn=None,
            model_name="blanchefort/rubert-base-cased-sentiment",
            model_version="rubert-v1",
        ),
    )
    service.initialize()
    consumer = SentimentConsumer(
        config=SentimentServiceConfig(
            postgres_dsn=None,
            model_name="blanchefort/rubert-base-cased-sentiment",
            model_version="rubert-v1",
        ),
        dependencies=SentimentConsumerDependencies(service=service),
    )
    message = FakeMessage(
        {
            "doc_id": "doc-1",
            "source_type": "vk_post",
            "source_id": "source-1",
            "parent_id": None,
            "text": "Во дворе нет воды",
            "media_type": "text",
            "created_at": "2026-04-05T09:00:00+00:00",
            "collected_at": "2026-04-05T09:01:00+00:00",
            "author_id": "author-1",
            "is_official": False,
            "reach": 10,
            "likes": 0,
            "reposts": 0,
            "comments_count": 0,
            "region_hint": "Волгоград",
            "geo_lat": None,
            "geo_lon": None,
            "raw_payload": {},
            "language": "ru",
            "language_confidence": 0.99,
            "is_supported_language": True,
            "filter_status": "pass",
            "filter_reasons": [],
            "quality_weight": 1.0,
            "anomaly_flags": [],
            "anomaly_confidence": 0.0,
            "normalized_text": "Во дворе нет воды",
            "token_count": 4,
            "cleanup_flags": [],
            "text_sha256": "abc",
            "duplicate_group_id": "dup-1",
            "near_duplicate_flag": False,
            "duplicate_cluster_size": 1,
            "canonical_doc_id": "doc-1",
            "region_id": "volgograd-oblast",
            "municipality_id": "volgograd",
            "geo_confidence": 0.8,
            "geo_source": "text_toponym",
            "geo_evidence": [],
            "engagement": 0,
            "metadata_version": "meta-v1",
            "category": "housing",
            "category_label": "ЖКХ",
            "category_confidence": 0.8,
            "secondary_category": None,
        },
    )

    processed = asyncio.run(consumer.handle_messages([message]))

    assert processed == 1
    assert message.committed is True
    assert repository.sentiments["doc-1"].sentiment_score == -0.6
