from __future__ import annotations

import os

import psycopg

from apps.ml.clustering import ClusteringServiceConfig
from apps.ml.embeddings.config import DEFAULT_MODEL_NAME as DEFAULT_EMBEDDING_MODEL_NAME
from apps.ml.embeddings.storage import PostgresEmbeddingRepository
from apps.ml.ranking import RankingServiceConfig
from apps.ml.summarization import LLMResponse, LLMUsage, SummarizationServiceConfig
from apps.orchestration.consumers import build_default_ranking_service, build_default_summarization_service
from apps.orchestration.schedulers.clustering_jobs import build_default_clustering_service
from apps.ml.sentiment.storage import PostgresSentimentRepository
from apply_schema import apply_sql_schema
from config import load_config, validate_db_config


class NullLLMClient:
    async def complete(self, **kwargs) -> LLMResponse:  # type: ignore[no-untyped-def]
        del kwargs
        return LLMResponse(
            text="ОПИСАНИЕ: bootstrap placeholder.\nФРАЗЫ: bootstrap; placeholder; cluster; summary; text",
            usage=LLMUsage(input_tokens=0, output_tokens=0),
            model_name="bootstrap-only",
        )


def _embedding_model_version() -> str:
    return os.getenv("EMBEDDINGS_MODEL_VERSION", "").strip() or "bootstrap-only"


def _require_vector_extension(database_url: str) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT extname FROM pg_extension WHERE extname IN ('vector', 'pgcrypto')")
            installed = {str(row[0]) for row in cur.fetchall()}
        conn.commit()
    required = {"vector", "pgcrypto"}
    if installed != required:
        missing = ", ".join(sorted(required - installed))
        raise RuntimeError(
            "Не удалось подтвердить обязательные PostgreSQL extensions после bootstrap: "
            f"{missing}. Проверьте права пользователя БД и установку pgvector на сервере.",
        )


def main() -> None:
    config = load_config()
    validate_db_config(config)
    apply_sql_schema(database_url=config.database_url)
    _require_vector_extension(config.database_url)

    embedding_repository = PostgresEmbeddingRepository(
        config.database_url,
        embedding_dimension=1024,
    )
    embedding_repository.ensure_model_compatibility(
        model_name=os.getenv("EMBEDDINGS_MODEL_NAME", DEFAULT_EMBEDDING_MODEL_NAME),
        model_version=_embedding_model_version(),
    )

    sentiment_repository = PostgresSentimentRepository(config.database_url)
    sentiment_repository.ensure_schema()

    clustering_config = ClusteringServiceConfig.from_env()
    if clustering_config.postgres_dsn is None:
        clustering_config = ClusteringServiceConfig(
            postgres_dsn=config.database_url,
            kafka_bootstrap_servers=clustering_config.kafka_bootstrap_servers,
            embeddings_table=clustering_config.embeddings_table,
            documents_table=clustering_config.documents_table,
            updated_topic=clustering_config.updated_topic,
            min_cluster_size=clustering_config.min_cluster_size,
            min_samples=clustering_config.min_samples,
            assignment_strength_threshold=clustering_config.assignment_strength_threshold,
            reconcile_similarity_threshold=clustering_config.reconcile_similarity_threshold,
            full_recompute_window_hours=clustering_config.full_recompute_window_hours,
            growth_recent_hours=clustering_config.growth_recent_hours,
            growth_previous_hours=clustering_config.growth_previous_hours,
        )
    build_default_clustering_service(clustering_config)

    summarization_config = SummarizationServiceConfig.from_env()
    if summarization_config.postgres_dsn is None:
        summarization_config = SummarizationServiceConfig(
            postgres_dsn=config.database_url,
            kafka_bootstrap_servers=summarization_config.kafka_bootstrap_servers,
            embeddings_table=summarization_config.embeddings_table,
            documents_table=summarization_config.documents_table,
            input_topic=summarization_config.input_topic,
            output_topic=summarization_config.output_topic,
            prompts_path=summarization_config.prompts_path,
            max_prompt_docs=summarization_config.max_prompt_docs,
            max_prompt_tokens=summarization_config.max_prompt_tokens,
            max_doc_chars=summarization_config.max_doc_chars,
            max_docs_per_author=summarization_config.max_docs_per_author,
            sample_doc_ids_count=summarization_config.sample_doc_ids_count,
            temperature=summarization_config.temperature,
            max_output_tokens=summarization_config.max_output_tokens,
            timeout_seconds=summarization_config.timeout_seconds,
            max_retries=summarization_config.max_retries,
            regeneration_growth_threshold=summarization_config.regeneration_growth_threshold,
            regeneration_age_hours=summarization_config.regeneration_age_hours,
            regeneration_active_growth_rate=summarization_config.regeneration_active_growth_rate,
            input_token_price_usd_per_1k=summarization_config.input_token_price_usd_per_1k,
            output_token_price_usd_per_1k=summarization_config.output_token_price_usd_per_1k,
        )
    build_default_summarization_service(summarization_config, llm_client=NullLLMClient())

    ranking_config = RankingServiceConfig.from_env()
    if ranking_config.postgres_dsn is None:
        ranking_config = RankingServiceConfig(
            postgres_dsn=config.database_url,
            kafka_bootstrap_servers=ranking_config.kafka_bootstrap_servers,
            config_path=ranking_config.config_path,
            documents_table=ranking_config.documents_table,
            sentiments_table=ranking_config.sentiments_table,
            input_topic=ranking_config.input_topic,
            output_topic=ranking_config.output_topic,
            top_n=ranking_config.top_n,
            min_cluster_size_for_ranking=ranking_config.min_cluster_size_for_ranking,
            stale_after_hours=ranking_config.stale_after_hours,
            new_cluster_hours=ranking_config.new_cluster_hours,
            growing_threshold=ranking_config.growing_threshold,
            geo_max_coverage_ratio=ranking_config.geo_max_coverage_ratio,
            source_type_count=ranking_config.source_type_count,
            schedule_interval_minutes=ranking_config.schedule_interval_minutes,
            snapshot_period_hours=ranking_config.snapshot_period_hours,
            active_profile=ranking_config.active_profile,
            weight_profiles=ranking_config.weight_profiles,
        )
    build_default_ranking_service(ranking_config)
    print(
        "✅ Production bootstrap completed: canonical schema, vector extension, "
        "document_sentiments and downstream tables are ready.",
    )


if __name__ == "__main__":
    main()
