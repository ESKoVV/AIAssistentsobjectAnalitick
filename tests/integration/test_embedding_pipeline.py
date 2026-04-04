from __future__ import annotations

import asyncio
from dataclasses import asdict

from apps.ml.embeddings import EmbeddingServiceConfig, prepare_document
from apps.ml.embeddings.inference import EmbeddingPipeline
from apps.ml.embeddings.service import EmbeddingBatchService
from apps.ml.embeddings.spool import SQLiteEmbeddingSpool
from apps.ml.embeddings.storage import InMemoryEmbeddingCache, InMemoryEmbeddingRepository
from apps.orchestration.consumers import EmbeddingConsumer, EmbeddingConsumerDependencies
from tests.helpers import build_enriched_document
from tests.unit.test_embedding_generation import FakeEmbeddingBackend, FakeTokenizer


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


class FlakyRepository(InMemoryEmbeddingRepository):
    def __init__(self) -> None:
        super().__init__()
        self.fail_writes = True

    def upsert_embeddings(self, documents) -> None:  # type: ignore[no-untyped-def]
        if self.fail_writes:
            raise ConnectionError("postgres unavailable")
        super().upsert_embeddings(documents)


def test_end_to_end_batch_processes_and_publishes_embedded_documents(tmp_path) -> None:
    tokenizer = FakeTokenizer()
    document = build_enriched_document("one two")
    prepared = prepare_document(document, tokenizer, max_tokens=16, overlap=0)
    service = EmbeddingBatchService(
        pipeline=EmbeddingPipeline(
            config=EmbeddingServiceConfig(
                model_name="intfloat/multilingual-e5-large",
                model_version="hash-1",
                embedding_dimension=2,
            ),
            tokenizer=tokenizer,
            backend=FakeEmbeddingBackend({prepared.chunks[0]: [3.0, 4.0]}),
        ),
        repository=InMemoryEmbeddingRepository(),
        cache=InMemoryEmbeddingCache(),
        spool=SQLiteEmbeddingSpool(str(tmp_path / "spool.sqlite3")),
    )
    producer = FakeProducer()
    consumer = EmbeddingConsumer(
        config=EmbeddingServiceConfig(
            model_name="intfloat/multilingual-e5-large",
            model_version="hash-1",
            embedding_dimension=2,
        ),
        dependencies=EmbeddingConsumerDependencies(service=service, producer=producer),
    )
    message = FakeMessage(asdict(document))

    processed = asyncio.run(consumer.handle_messages([message]))

    assert processed == 1
    assert message.committed is True
    assert len(producer.published) == 1
    assert producer.published[0][0] == "embedded.documents"
    assert producer.published[0][1]["doc_id"] == document.doc_id


def test_spool_replay_backfills_repository_after_outage(tmp_path) -> None:
    tokenizer = FakeTokenizer()
    document = build_enriched_document("one two")
    prepared = prepare_document(document, tokenizer, max_tokens=16, overlap=0)
    repository = FlakyRepository()
    spool = SQLiteEmbeddingSpool(str(tmp_path / "spool.sqlite3"))
    service = EmbeddingBatchService(
        pipeline=EmbeddingPipeline(
            config=EmbeddingServiceConfig(
                model_name="intfloat/multilingual-e5-large",
                model_version="hash-1",
                embedding_dimension=2,
            ),
            tokenizer=tokenizer,
            backend=FakeEmbeddingBackend({prepared.chunks[0]: [3.0, 4.0]}),
        ),
        repository=repository,
        cache=InMemoryEmbeddingCache(),
        spool=spool,
    )
    producer = FakeProducer()
    consumer = EmbeddingConsumer(
        config=EmbeddingServiceConfig(
            model_name="intfloat/multilingual-e5-large",
            model_version="hash-1",
            embedding_dimension=2,
        ),
        dependencies=EmbeddingConsumerDependencies(service=service, producer=producer),
    )
    message = FakeMessage(asdict(document))

    processed = asyncio.run(consumer.handle_messages([message]))

    assert processed == 1
    assert spool.size() == 1
    assert message.committed is True

    repository.fail_writes = False
    replayed = asyncio.run(consumer.replay_spool(limit=10))

    assert replayed == 1
    assert spool.size() == 0
    assert document.doc_id in repository.documents
