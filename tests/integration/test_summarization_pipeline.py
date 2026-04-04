from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import UTC, datetime

from apps.ml.summarization.config import SummarizationServiceConfig
from apps.ml.summarization.schema import LLMResponse, LLMUsage
from apps.ml.summarization.service import ClusterDescriptionService, FallbackLLMClient, RateLimitError
from apps.ml.summarization.storage import InMemorySummarizationRepository
from apps.orchestration.consumers import (
    ClusterDescriptionConsumer,
    ClusterDescriptionConsumerDependencies,
)
from tests.helpers import (
    build_cluster,
    build_stored_cluster_description,
    build_summarization_document_record,
    write_summarization_prompt,
)


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


class SequenceLLMClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    async def complete(self, **kwargs) -> LLMResponse:  # type: ignore[no-untyped-def]
        del kwargs
        response = self._responses[self.calls]
        self.calls += 1
        if isinstance(response, Exception):
            raise response
        return response


def test_consumer_generates_description_publishes_event_and_logs_costs(tmp_path) -> None:
    prompt_path = write_summarization_prompt(tmp_path / "summarization.md")
    repository = InMemorySummarizationRepository()
    cluster = build_cluster(cluster_id="cluster-1", size=5, doc_ids=["doc-1", "doc-2"])
    repository.clusters[cluster.cluster_id] = cluster
    repository.documents_by_cluster[cluster.cluster_id] = (
        build_summarization_document_record(
            doc_id="doc-1",
            text=(
                "Жители обсуждают, что нет горячей воды, называют сроки восстановления подачи, "
                "адреса домов, ремонтные работы и график отключения."
            ),
        ),
        build_summarization_document_record(
            doc_id="doc-2",
            author_id="author-2",
            text=(
                "В сообщениях повторяются адреса домов, сроки восстановления подачи, график отключения "
                "и ход ремонтных работ."
            ),
            embedding=[0.99, 0.01],
        ),
    )
    service = ClusterDescriptionService(
        repository=repository,
        llm_client=SequenceLLMClient(
            [
                LLMResponse(
                    text=(
                        "ОПИСАНИЕ: Жители обсуждают отсутствие горячей воды в домах и сроки восстановления подачи. "
                        "В сообщениях перечисляются адреса домов, график отключения и ремонтные работы.\n"
                        "ФРАЗЫ: нет горячей воды; сроки восстановления подачи; адреса домов; график отключения; ремонтные работы"
                    ),
                    usage=LLMUsage(input_tokens=120, output_tokens=40),
                    model_name="gpt-4o",
                ),
            ],
        ),
        config=SummarizationServiceConfig(
            postgres_dsn=None,
            prompts_path=str(prompt_path),
            documents_table="normalized_documents",
        ),
    )
    service.initialize()
    producer = FakeProducer()
    consumer = ClusterDescriptionConsumer(
        config=SummarizationServiceConfig(
            postgres_dsn=None,
            prompts_path=str(prompt_path),
            documents_table="normalized_documents",
        ),
        dependencies=ClusterDescriptionConsumerDependencies(service=service, producer=producer),
    )
    messages = [
        FakeMessage({"changed_cluster_ids": ["cluster-1", "cluster-1"]}),
        FakeMessage(asdict(build_cluster(cluster_id="ignored")) | {"changed_cluster_ids": ["cluster-1"]}),
    ]

    processed = asyncio.run(consumer.handle_messages(messages))

    assert processed == 1
    assert all(message.committed for message in messages)
    assert len(producer.published) == 1
    assert producer.published[0][0] == "descriptions.updated"
    assert producer.published[0][1]["updated_cluster_ids"] == ["cluster-1"]
    assert repository.descriptions["cluster-1"].description.summary.startswith("Жители обсуждают отсутствие")
    assert len(repository.llm_costs) == 1


def test_service_skips_cached_cluster_and_creates_history_on_regeneration(tmp_path) -> None:
    prompt_path = write_summarization_prompt(tmp_path / "summarization.md")
    repository = InMemorySummarizationRepository()
    cluster = build_cluster(cluster_id="cluster-1", size=10, doc_ids=["doc-1"])
    repository.clusters[cluster.cluster_id] = cluster
    repository.documents_by_cluster[cluster.cluster_id] = (
        build_summarization_document_record(
            doc_id="doc-1",
            text=(
                "Жители обсуждают, что нет горячей воды, называют сроки восстановления подачи, "
                "адреса домов, ремонтные работы и график отключения."
            ),
        ),
    )
    config = SummarizationServiceConfig(
        postgres_dsn=None,
        prompts_path=str(prompt_path),
        documents_table="normalized_documents",
    )
    service = ClusterDescriptionService(
        repository=repository,
        llm_client=SequenceLLMClient(
            [
                LLMResponse(
                    text=(
                        "ОПИСАНИЕ: Жители обсуждают отсутствие горячей воды в домах и сроки восстановления подачи. "
                        "В сообщениях перечисляются адреса домов, график отключения и ремонтные работы.\n"
                        "ФРАЗЫ: нет горячей воды; сроки восстановления подачи; адреса домов; график отключения; ремонтные работы"
                    ),
                    usage=LLMUsage(input_tokens=100, output_tokens=30),
                    model_name="gpt-4o",
                ),
            ],
        ),
        config=config,
    )
    service.initialize()
    from apps.ml.summarization.prompting import hash_prompt_spec, load_prompt_spec

    repository.descriptions[cluster.cluster_id] = build_stored_cluster_description(
        cluster_id="cluster-1",
        cluster_size_at_generation=10,
        prompt_version=hash_prompt_spec(load_prompt_spec(prompt_path)),
        generated_at=datetime(2026, 4, 4, 11, 30, tzinfo=UTC),
    )

    skipped = asyncio.run(service.process_cluster_ids([cluster.cluster_id], now=datetime(2026, 4, 4, 12, 0, tzinfo=UTC)))

    assert skipped.updated_cluster_ids == ()
    assert skipped.metrics.clusters_skipped == 1

    repository.clusters[cluster.cluster_id] = build_cluster(cluster_id="cluster-1", size=14, doc_ids=["doc-1"])
    regenerated = asyncio.run(
        service.process_cluster_ids([cluster.cluster_id], now=datetime(2026, 4, 4, 12, 30, tzinfo=UTC)),
    )

    assert regenerated.updated_cluster_ids == ("cluster-1",)
    assert len(repository.history) == 1
    assert repository.history[0].cluster_id == "cluster-1"


def test_service_marks_needs_review_after_exhausting_retries_and_records_fallback(tmp_path) -> None:
    prompt_path = write_summarization_prompt(tmp_path / "summarization.md")
    repository = InMemorySummarizationRepository()
    cluster = build_cluster(cluster_id="cluster-1", size=5, doc_ids=["doc-1"])
    repository.clusters[cluster.cluster_id] = cluster
    repository.documents_by_cluster[cluster.cluster_id] = (
        build_summarization_document_record(
            doc_id="doc-1",
            text=(
                "Жители обсуждают, что нет горячей воды, называют сроки восстановления подачи, "
                "адреса домов, ремонтные работы и график отключения."
            ),
        ),
    )
    service = ClusterDescriptionService(
        repository=repository,
        llm_client=FallbackLLMClient(
            primary=SequenceLLMClient([RateLimitError("429"), RateLimitError("429"), RateLimitError("429")]),
            fallback=SequenceLLMClient(
                [
                    LLMResponse(
                        text="ОПИСАНИЕ: проблема.\nФРАЗЫ: проблема; сроки",
                        usage=LLMUsage(input_tokens=90, output_tokens=10),
                        model_name="llama3-70b",
                    ),
                    LLMResponse(
                        text="ОПИСАНИЕ: проблема.\nФРАЗЫ: проблема; сроки",
                        usage=LLMUsage(input_tokens=95, output_tokens=10),
                        model_name="llama3-70b",
                    ),
                    LLMResponse(
                        text="ОПИСАНИЕ: проблема.\nФРАЗЫ: проблема; сроки",
                        usage=LLMUsage(input_tokens=95, output_tokens=10),
                        model_name="llama3-70b",
                    ),
                ],
            ),
        ),
        config=SummarizationServiceConfig(
            postgres_dsn=None,
            prompts_path=str(prompt_path),
            documents_table="normalized_documents",
        ),
    )
    service.initialize()

    result = asyncio.run(service.process_cluster_ids([cluster.cluster_id]))

    stored = repository.descriptions["cluster-1"]
    assert result.updated_cluster_ids == ("cluster-1",)
    assert stored.needs_review is True
    assert stored.description.fallback_used is True
    assert len(repository.llm_costs) == 3
    assert result.metrics.validation_failed == 1
    assert result.metrics.fallback_used_count == 1
