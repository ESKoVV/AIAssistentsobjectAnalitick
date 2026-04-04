from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Protocol, Sequence

from apps.ml.clustering.schema import Cluster

from .config import SummarizationServiceConfig
from .parsing import parse_response
from .prompting import PromptSpec, hash_prompt_spec, load_prompt_spec, render_user_prompt
from .schema import (
    ClusterDescription,
    ClusterDescriptionBatchResult,
    DescriptionMetrics,
    DescriptionsUpdatedEvent,
    LLMCostRecord,
    LLMResponse,
    StoredClusterDescription,
    SummarizationDocumentRecord,
)
from .selection import render_selected_texts, select_representative_docs
from .storage import SummarizationRepositoryProtocol
from .validation import validate_description


class LLMError(RuntimeError):
    pass


class RateLimitError(LLMError):
    pass


class APIError(LLMError):
    pass


class LLMClientProtocol(Protocol):
    async def complete(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        timeout_seconds: int,
    ) -> LLMResponse:
        ...


class FallbackLLMClient:
    def __init__(self, primary: LLMClientProtocol, fallback: LLMClientProtocol) -> None:
        self._primary = primary
        self._fallback = fallback

    async def complete(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        timeout_seconds: int,
    ) -> LLMResponse:
        try:
            response = await self._primary.complete(
                system=system,
                user=user,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout_seconds=timeout_seconds,
            )
        except (TimeoutError, RateLimitError, APIError):
            response = await self._fallback.complete(
                system=system,
                user=user,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout_seconds=timeout_seconds,
            )
            return LLMResponse(
                text=response.text,
                usage=response.usage,
                model_name=response.model_name,
                fallback_used=True,
            )

        return LLMResponse(
            text=response.text,
            usage=response.usage,
            model_name=response.model_name,
            fallback_used=response.fallback_used,
        )


class ClusterDescriptionService:
    def __init__(
        self,
        *,
        repository: SummarizationRepositoryProtocol,
        llm_client: LLMClientProtocol,
        config: SummarizationServiceConfig,
    ) -> None:
        self._repository = repository
        self._llm_client = llm_client
        self._config = config

    def initialize(self) -> None:
        self._repository.ensure_schema()
        self._repository.ensure_upstream_dependencies()

    async def process_cluster_ids(
        self,
        changed_cluster_ids: Sequence[str],
        *,
        now: datetime | None = None,
    ) -> ClusterDescriptionBatchResult:
        now = now or datetime.now(UTC)
        prompt_spec = load_prompt_spec(self._config.prompts_path)
        prompt_version = hash_prompt_spec(prompt_spec)
        cluster_ids = _deduplicate_ids(changed_cluster_ids)
        clusters = self._repository.load_clusters_by_ids(cluster_ids)
        existing_descriptions = self._repository.load_descriptions_by_ids(cluster_ids)

        processed = 0
        skipped = 0
        regenerated = 0
        fallback_used_count = 0
        validation_failed = 0
        generation_time_sum = 0
        total_input_tokens = 0
        total_output_tokens = 0
        estimated_cost_usd = 0.0
        updated_cluster_ids: list[str] = []
        cluster_ids_to_regenerate: list[str] = []

        for cluster_id in cluster_ids:
            cluster = clusters.get(cluster_id)
            if cluster is None or cluster.noise:
                continue

            processed += 1
            existing = existing_descriptions.get(cluster_id)
            if existing is not None and not should_regenerate(
                cluster,
                existing,
                prompt_version=prompt_version,
                now=now,
                growth_threshold=self._config.regeneration_growth_threshold,
                regeneration_age_hours=self._config.regeneration_age_hours,
                active_growth_rate_threshold=self._config.regeneration_active_growth_rate,
            ):
                skipped += 1
                continue
            cluster_ids_to_regenerate.append(cluster_id)

        documents_by_cluster = self._repository.fetch_documents_for_clusters(cluster_ids_to_regenerate)

        for cluster_id in cluster_ids_to_regenerate:
            cluster = clusters[cluster_id]
            cluster_documents = documents_by_cluster.get(cluster_id, ())
            if not cluster_documents:
                continue

            result = await self._generate_description(
                cluster=cluster,
                documents=cluster_documents,
                prompt_spec=prompt_spec,
                prompt_version=prompt_version,
                now=now,
            )
            self._repository.save_description(result.stored_description)

            regenerated += 1
            updated_cluster_ids.append(cluster_id)
            if result.stored_description.description.fallback_used:
                fallback_used_count += 1
            if result.stored_description.needs_review:
                validation_failed += 1
            generation_time_sum += result.stored_description.description.generation_time_ms
            total_input_tokens += result.total_input_tokens
            total_output_tokens += result.total_output_tokens
            estimated_cost_usd += result.estimated_cost_usd

        avg_generation_time_ms = generation_time_sum / regenerated if regenerated else 0.0
        metrics = DescriptionMetrics(
            run_at=now,
            clusters_processed=processed,
            clusters_regenerated=regenerated,
            clusters_skipped=skipped,
            fallback_used_count=fallback_used_count,
            validation_failed=validation_failed,
            avg_generation_time_ms=avg_generation_time_ms,
            total_input_tokens=total_input_tokens,
            total_output_tokens=total_output_tokens,
            estimated_cost_usd=estimated_cost_usd,
        )
        event = (
            DescriptionsUpdatedEvent(run_at=now, updated_cluster_ids=updated_cluster_ids)
            if updated_cluster_ids
            else None
        )
        return ClusterDescriptionBatchResult(
            updated_cluster_ids=tuple(updated_cluster_ids),
            metrics=metrics,
            event=event,
        )

    async def _generate_description(
        self,
        *,
        cluster: Cluster,
        documents: Sequence[SummarizationDocumentRecord],
        prompt_spec: PromptSpec,
        prompt_version: str,
        now: datetime,
    ) -> "_GenerationResult":
        selected_docs = select_representative_docs(
            cluster,
            documents,
            max_docs=self._config.max_prompt_docs,
            max_tokens=self._config.max_prompt_tokens,
            max_docs_per_author=self._config.max_docs_per_author,
            max_doc_chars=self._config.max_doc_chars,
        )
        texts_block = render_selected_texts(selected_docs, max_doc_chars=self._config.max_doc_chars)
        source_types = ", ".join(sorted({document.source_type.value for document in documents}))
        geo_regions = ", ".join(cluster.geo_regions) if cluster.geo_regions else "не определён"

        base_user_prompt = render_user_prompt(
            prompt_spec,
            size=cluster.size,
            period_start=cluster.period_start.astimezone(UTC).strftime("%d.%m.%Y %H:%M"),
            period_end=cluster.period_end.astimezone(UTC).strftime("%d.%m.%Y %H:%M"),
            source_types=source_types,
            geo_regions=geo_regions,
            texts=texts_block,
        )

        total_input_tokens = 0
        total_output_tokens = 0
        total_generation_time_ms = 0
        estimated_cost_usd = 0.0
        any_fallback_used = False
        validation_reason: str | None = None
        last_response: LLMResponse | None = None
        last_summary = ""
        last_key_phrases: list[str] = []

        for attempt_number in range(1, self._config.max_retries + 2):
            user_prompt = render_user_prompt(
                prompt_spec,
                size=cluster.size,
                period_start=cluster.period_start.astimezone(UTC).strftime("%d.%m.%Y %H:%M"),
                period_end=cluster.period_end.astimezone(UTC).strftime("%d.%m.%Y %H:%M"),
                source_types=source_types,
                geo_regions=geo_regions,
                texts=texts_block,
                feedback_reason=validation_reason,
            ) if validation_reason else base_user_prompt

            started = time.monotonic()
            response = await self._llm_client.complete(
                system=prompt_spec.system_prompt,
                user=user_prompt,
                temperature=self._config.temperature,
                max_tokens=self._config.max_output_tokens,
                timeout_seconds=self._config.timeout_seconds,
            )
            elapsed_ms = int((time.monotonic() - started) * 1000)
            summary, key_phrases = parse_response(response.text)
            validation = validate_description(summary, key_phrases, selected_docs)
            last_response = response
            last_summary = summary
            last_key_phrases = key_phrases
            validation_reason = validation.reason
            any_fallback_used = any_fallback_used or response.fallback_used
            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens
            total_generation_time_ms += elapsed_ms
            estimated_cost = estimate_cost_usd(
                response,
                input_price_per_1k=self._config.input_token_price_usd_per_1k,
                output_price_per_1k=self._config.output_token_price_usd_per_1k,
            )
            estimated_cost_usd += estimated_cost
            self._repository.record_llm_cost(
                LLMCostRecord(
                    cluster_id=cluster.cluster_id,
                    attempt_number=attempt_number,
                    model_name=response.model_name,
                    prompt_version=prompt_version,
                    requested_at=now,
                    input_token_count=response.usage.input_tokens,
                    output_token_count=response.usage.output_tokens,
                    estimated_cost_usd=estimated_cost,
                    generation_time_ms=elapsed_ms,
                    fallback_used=response.fallback_used,
                    validation_error=validation.reason,
                ),
            )
            if validation.valid:
                break

        if last_response is None:
            raise RuntimeError("LLM response was not produced")

        stored_description = StoredClusterDescription(
            description=ClusterDescription(
                cluster_id=cluster.cluster_id,
                summary=last_summary,
                key_phrases=last_key_phrases,
                sample_doc_ids=[document.doc_id for document in selected_docs[: self._config.sample_doc_ids_count]],
                model_name=last_response.model_name,
                prompt_version=prompt_version,
                generated_at=now,
                input_token_count=last_response.usage.input_tokens,
                output_token_count=last_response.usage.output_tokens,
                generation_time_ms=total_generation_time_ms,
                fallback_used=any_fallback_used,
            ),
            needs_review=validation_reason is not None,
            cluster_size_at_generation=cluster.size,
        )
        return _GenerationResult(
            stored_description=stored_description,
            total_input_tokens=total_input_tokens,
            total_output_tokens=total_output_tokens,
            estimated_cost_usd=estimated_cost_usd,
        )


def should_regenerate(
    cluster: Cluster,
    existing: StoredClusterDescription,
    *,
    prompt_version: str,
    now: datetime,
    growth_threshold: float,
    regeneration_age_hours: int,
    active_growth_rate_threshold: float,
) -> bool:
    if existing.description.prompt_version != prompt_version:
        return True

    growth = (
        cluster.size - existing.cluster_size_at_generation
    ) / max(existing.cluster_size_at_generation, 1)
    if growth > growth_threshold:
        return True

    age_hours = (now - existing.description.generated_at).total_seconds() / 3600
    if age_hours > regeneration_age_hours and cluster.growth_rate > active_growth_rate_threshold:
        return True

    return False


def estimate_cost_usd(
    response: LLMResponse,
    *,
    input_price_per_1k: float,
    output_price_per_1k: float,
) -> float:
    return (
        (response.usage.input_tokens / 1000.0) * input_price_per_1k
        + (response.usage.output_tokens / 1000.0) * output_price_per_1k
    )


def _deduplicate_ids(cluster_ids: Sequence[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for cluster_id in cluster_ids:
        normalized = str(cluster_id)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


class _GenerationResult:
    def __init__(
        self,
        *,
        stored_description: StoredClusterDescription,
        total_input_tokens: int,
        total_output_tokens: int,
        estimated_cost_usd: float,
    ) -> None:
        self.stored_description = stored_description
        self.total_input_tokens = total_input_tokens
        self.total_output_tokens = total_output_tokens
        self.estimated_cost_usd = estimated_cost_usd
