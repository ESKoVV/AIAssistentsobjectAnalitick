from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_EMBEDDINGS_TABLE = "embeddings"
DEFAULT_DOCUMENTS_TABLE = "normalized_messages"
DEFAULT_INPUT_TOPIC = "clusters.updated"
DEFAULT_OUTPUT_TOPIC = "descriptions.updated"
DEFAULT_PROMPTS_PATH = "configs/prompts/summarization.md"
DEFAULT_MAX_PROMPT_DOCS = 40
DEFAULT_MAX_PROMPT_TOKENS = 6000
DEFAULT_MAX_DOC_CHARS = 500
DEFAULT_MAX_DOCS_PER_AUTHOR = 3
DEFAULT_SAMPLE_DOC_IDS = 5
DEFAULT_TEMPERATURE = 0.1
DEFAULT_MAX_OUTPUT_TOKENS = 300
DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_MAX_RETRIES = 2
DEFAULT_REGENERATION_GROWTH_THRESHOLD = 0.3
DEFAULT_REGENERATION_AGE_HOURS = 6
DEFAULT_REGENERATION_ACTIVE_GROWTH_RATE = 2.0
DEFAULT_INPUT_TOKEN_PRICE_USD_PER_1K = 0.0
DEFAULT_OUTPUT_TOKEN_PRICE_USD_PER_1K = 0.0


@dataclass(frozen=True, slots=True)
class SummarizationServiceConfig:
    postgres_dsn: str | None
    kafka_bootstrap_servers: str | None = None
    embeddings_table: str = DEFAULT_EMBEDDINGS_TABLE
    documents_table: str = DEFAULT_DOCUMENTS_TABLE
    input_topic: str = DEFAULT_INPUT_TOPIC
    output_topic: str = DEFAULT_OUTPUT_TOPIC
    prompts_path: str = DEFAULT_PROMPTS_PATH
    max_prompt_docs: int = DEFAULT_MAX_PROMPT_DOCS
    max_prompt_tokens: int = DEFAULT_MAX_PROMPT_TOKENS
    max_doc_chars: int = DEFAULT_MAX_DOC_CHARS
    max_docs_per_author: int = DEFAULT_MAX_DOCS_PER_AUTHOR
    sample_doc_ids_count: int = DEFAULT_SAMPLE_DOC_IDS
    temperature: float = DEFAULT_TEMPERATURE
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    regeneration_growth_threshold: float = DEFAULT_REGENERATION_GROWTH_THRESHOLD
    regeneration_age_hours: int = DEFAULT_REGENERATION_AGE_HOURS
    regeneration_active_growth_rate: float = DEFAULT_REGENERATION_ACTIVE_GROWTH_RATE
    input_token_price_usd_per_1k: float = DEFAULT_INPUT_TOKEN_PRICE_USD_PER_1K
    output_token_price_usd_per_1k: float = DEFAULT_OUTPUT_TOKEN_PRICE_USD_PER_1K

    def __post_init__(self) -> None:
        if not self.embeddings_table.strip():
            raise ValueError("embeddings_table must be non-empty")
        if not self.documents_table.strip():
            raise ValueError("documents_table must be non-empty")
        if not self.input_topic.strip():
            raise ValueError("input_topic must be non-empty")
        if not self.output_topic.strip():
            raise ValueError("output_topic must be non-empty")
        if not self.prompts_path.strip():
            raise ValueError("prompts_path must be non-empty")
        if self.max_prompt_docs <= 0:
            raise ValueError("max_prompt_docs must be positive")
        if self.max_prompt_tokens <= 0:
            raise ValueError("max_prompt_tokens must be positive")
        if self.max_doc_chars <= 0:
            raise ValueError("max_doc_chars must be positive")
        if self.max_docs_per_author <= 0:
            raise ValueError("max_docs_per_author must be positive")
        if self.sample_doc_ids_count <= 0:
            raise ValueError("sample_doc_ids_count must be positive")
        if self.sample_doc_ids_count > self.max_prompt_docs:
            raise ValueError("sample_doc_ids_count must not exceed max_prompt_docs")
        if not 0.0 <= self.temperature <= 2.0:
            raise ValueError("temperature must be between 0 and 2")
        if self.max_output_tokens <= 0:
            raise ValueError("max_output_tokens must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.regeneration_growth_threshold < 0.0:
            raise ValueError("regeneration_growth_threshold must be non-negative")
        if self.regeneration_age_hours <= 0:
            raise ValueError("regeneration_age_hours must be positive")
        if self.regeneration_active_growth_rate < 0.0:
            raise ValueError("regeneration_active_growth_rate must be non-negative")
        if self.input_token_price_usd_per_1k < 0.0:
            raise ValueError("input_token_price_usd_per_1k must be non-negative")
        if self.output_token_price_usd_per_1k < 0.0:
            raise ValueError("output_token_price_usd_per_1k must be non-negative")

    @classmethod
    def from_env(cls) -> "SummarizationServiceConfig":
        return cls(
            postgres_dsn=os.getenv("SUMMARIZATION_POSTGRES_DSN") or None,
            kafka_bootstrap_servers=os.getenv("SUMMARIZATION_KAFKA_BOOTSTRAP_SERVERS") or None,
            embeddings_table=os.getenv("SUMMARIZATION_EMBEDDINGS_TABLE", DEFAULT_EMBEDDINGS_TABLE),
            documents_table=os.getenv("SUMMARIZATION_DOCUMENTS_TABLE", DEFAULT_DOCUMENTS_TABLE),
            input_topic=os.getenv("SUMMARIZATION_INPUT_TOPIC", DEFAULT_INPUT_TOPIC),
            output_topic=os.getenv("SUMMARIZATION_OUTPUT_TOPIC", DEFAULT_OUTPUT_TOPIC),
            prompts_path=os.getenv("SUMMARIZATION_PROMPTS_PATH", DEFAULT_PROMPTS_PATH),
            max_prompt_docs=int(os.getenv("SUMMARIZATION_MAX_PROMPT_DOCS", DEFAULT_MAX_PROMPT_DOCS)),
            max_prompt_tokens=int(os.getenv("SUMMARIZATION_MAX_PROMPT_TOKENS", DEFAULT_MAX_PROMPT_TOKENS)),
            max_doc_chars=int(os.getenv("SUMMARIZATION_MAX_DOC_CHARS", DEFAULT_MAX_DOC_CHARS)),
            max_docs_per_author=int(
                os.getenv("SUMMARIZATION_MAX_DOCS_PER_AUTHOR", DEFAULT_MAX_DOCS_PER_AUTHOR),
            ),
            sample_doc_ids_count=int(
                os.getenv("SUMMARIZATION_SAMPLE_DOC_IDS_COUNT", DEFAULT_SAMPLE_DOC_IDS),
            ),
            temperature=float(os.getenv("SUMMARIZATION_TEMPERATURE", DEFAULT_TEMPERATURE)),
            max_output_tokens=int(
                os.getenv("SUMMARIZATION_MAX_OUTPUT_TOKENS", DEFAULT_MAX_OUTPUT_TOKENS),
            ),
            timeout_seconds=int(os.getenv("SUMMARIZATION_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)),
            max_retries=int(os.getenv("SUMMARIZATION_MAX_RETRIES", DEFAULT_MAX_RETRIES)),
            regeneration_growth_threshold=float(
                os.getenv(
                    "SUMMARIZATION_REGENERATION_GROWTH_THRESHOLD",
                    DEFAULT_REGENERATION_GROWTH_THRESHOLD,
                ),
            ),
            regeneration_age_hours=int(
                os.getenv("SUMMARIZATION_REGENERATION_AGE_HOURS", DEFAULT_REGENERATION_AGE_HOURS),
            ),
            regeneration_active_growth_rate=float(
                os.getenv(
                    "SUMMARIZATION_REGENERATION_ACTIVE_GROWTH_RATE",
                    DEFAULT_REGENERATION_ACTIVE_GROWTH_RATE,
                ),
            ),
            input_token_price_usd_per_1k=float(
                os.getenv(
                    "SUMMARIZATION_INPUT_TOKEN_PRICE_USD_PER_1K",
                    DEFAULT_INPUT_TOKEN_PRICE_USD_PER_1K,
                ),
            ),
            output_token_price_usd_per_1k=float(
                os.getenv(
                    "SUMMARIZATION_OUTPUT_TOKEN_PRICE_USD_PER_1K",
                    DEFAULT_OUTPUT_TOKEN_PRICE_USD_PER_1K,
                ),
            ),
        )
