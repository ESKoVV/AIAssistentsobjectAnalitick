from __future__ import annotations

from apps.ml.summarization.http_client import (
    ChatCompletionsLLMClient,
    ChatCompletionsLLMClientConfig,
    build_alicagpt_client_from_env,
)
from apps.ml.summarization.service import FallbackLLMClient


def test_config_reads_alicagpt_env_first(monkeypatch) -> None:
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_BASE_URL", "http://alicagpt:8000")
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_MODEL_NAME", "alicagpt-primary")
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_API_KEY", "token")
    monkeypatch.setenv("SUMMARIZATION_LLM_BASE_URL", "http://legacy:8000")
    monkeypatch.setenv("SUMMARIZATION_LLM_MODEL_NAME", "legacy-model")

    config = ChatCompletionsLLMClientConfig.from_env()

    assert config.base_url == "http://alicagpt:8000"
    assert config.model_name == "alicagpt-primary"
    assert config.api_key == "token"


def test_config_falls_back_to_legacy_llm_env(monkeypatch) -> None:
    monkeypatch.delenv("SUMMARIZATION_ALICAGPT_BASE_URL", raising=False)
    monkeypatch.delenv("SUMMARIZATION_ALICAGPT_MODEL_NAME", raising=False)
    monkeypatch.delenv("SUMMARIZATION_ALICAGPT_API_KEY", raising=False)
    monkeypatch.setenv("SUMMARIZATION_LLM_BASE_URL", "http://legacy:8000")
    monkeypatch.setenv("SUMMARIZATION_LLM_MODEL_NAME", "legacy-model")
    monkeypatch.setenv("SUMMARIZATION_LLM_API_KEY", "legacy-token")

    config = ChatCompletionsLLMClientConfig.from_env()

    assert config.base_url == "http://legacy:8000"
    assert config.model_name == "legacy-model"
    assert config.api_key == "legacy-token"


def test_builder_creates_fallback_client_from_alicagpt_env(monkeypatch) -> None:
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_BASE_URL", "http://alicagpt:8000")
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_MODEL_NAME", "alicagpt-primary")
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_FALLBACK_BASE_URL", "http://alicagpt-fallback:8000")
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_FALLBACK_MODEL_NAME", "alicagpt-fallback")

    client = build_alicagpt_client_from_env()

    assert isinstance(client, FallbackLLMClient)


def test_builder_creates_primary_client_without_fallback(monkeypatch) -> None:
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_BASE_URL", "http://alicagpt:8000")
    monkeypatch.setenv("SUMMARIZATION_ALICAGPT_MODEL_NAME", "alicagpt-primary")
    monkeypatch.delenv("SUMMARIZATION_ALICAGPT_FALLBACK_BASE_URL", raising=False)
    monkeypatch.delenv("SUMMARIZATION_ALICAGPT_FALLBACK_MODEL_NAME", raising=False)

    client = build_alicagpt_client_from_env()

    assert isinstance(client, ChatCompletionsLLMClient)
