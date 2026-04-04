from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from apps.ml.summarization.config import SummarizationServiceConfig
from apps.ml.summarization.schema import LLMResponse, LLMUsage
from apps.ml.summarization.service import (
    ClusterDescriptionService,
    FallbackLLMClient,
    RateLimitError,
    should_regenerate,
)
from apps.ml.summarization.storage import InMemorySummarizationRepository
from tests.helpers import (
    build_cluster,
    build_stored_cluster_description,
    build_summarization_document_record,
    write_summarization_prompt,
)


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


def test_should_regenerate_when_prompt_hash_changes_growth_exceeds_threshold_or_description_is_stale() -> None:
    now = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    cluster = build_cluster(size=14, growth_rate=3.0)
    existing = build_stored_cluster_description(
        cluster_size_at_generation=10,
        prompt_version="old-hash",
        generated_at=now - timedelta(hours=7),
    )

    assert should_regenerate(
        cluster,
        existing,
        prompt_version="new-hash",
        now=now,
        growth_threshold=0.3,
        regeneration_age_hours=6,
        active_growth_rate_threshold=2.0,
    ) is True

    assert should_regenerate(
        build_cluster(size=14, growth_rate=1.0),
        build_stored_cluster_description(
            cluster_size_at_generation=10,
            prompt_version="same-hash",
            generated_at=now - timedelta(hours=1),
        ),
        prompt_version="same-hash",
        now=now,
        growth_threshold=0.3,
        regeneration_age_hours=6,
        active_growth_rate_threshold=2.0,
    ) is True

    assert should_regenerate(
        build_cluster(size=10, growth_rate=3.1),
        build_stored_cluster_description(
            cluster_size_at_generation=10,
            prompt_version="same-hash",
            generated_at=now - timedelta(hours=7),
        ),
        prompt_version="same-hash",
        now=now,
        growth_threshold=0.3,
        regeneration_age_hours=6,
        active_growth_rate_threshold=2.0,
    ) is True


def test_fallback_client_uses_secondary_on_primary_rate_limit() -> None:
    client = FallbackLLMClient(
        primary=SequenceLLMClient([RateLimitError("too many requests")]),
        fallback=SequenceLLMClient(
            [
                LLMResponse(
                    text="ОПИСАНИЕ: тест.\nФРАЗЫ: одна; две; три; четыре; пять",
                    usage=LLMUsage(input_tokens=10, output_tokens=5),
                    model_name="llama3-70b",
                ),
            ],
        ),
    )

    response = asyncio.run(
        client.complete(
            system="system",
            user="user",
            temperature=0.1,
            max_tokens=300,
            timeout_seconds=15,
        ),
    )

    assert response.model_name == "llama3-70b"
    assert response.fallback_used is True


def test_service_retries_after_invalid_output_and_persists_valid_second_attempt(tmp_path) -> None:
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
        llm_client=SequenceLLMClient(
            [
                LLMResponse(
                    text="ОПИСАНИЕ: проблема с водой.\nФРАЗЫ: проблема с водой; адреса домов",
                    usage=LLMUsage(input_tokens=100, output_tokens=20),
                    model_name="gpt-4o",
                ),
                LLMResponse(
                    text=(
                        "ОПИСАНИЕ: Жители обсуждают отсутствие горячей воды в домах и сроки восстановления подачи. "
                        "В сообщениях перечисляются адреса домов, ремонтные работы и график отключения.\n"
                        "ФРАЗЫ: нет горячей воды; сроки восстановления подачи; адреса домов; ремонтные работы; график отключения"
                    ),
                    usage=LLMUsage(input_tokens=110, output_tokens=35),
                    model_name="gpt-4o",
                ),
            ],
        ),
        config=SummarizationServiceConfig(
            postgres_dsn=None,
            prompts_path=str(prompt_path),
            documents_table="normalized_messages",
        ),
    )
    service.initialize()

    result = asyncio.run(service.process_cluster_ids([cluster.cluster_id]))

    stored = repository.descriptions[cluster.cluster_id]
    assert result.updated_cluster_ids == ("cluster-1",)
    assert stored.needs_review is False
    assert stored.description.model_name == "gpt-4o"
    assert len(repository.llm_costs) == 2
    assert repository.llm_costs[0].validation_error == "описание слишком короткое"
