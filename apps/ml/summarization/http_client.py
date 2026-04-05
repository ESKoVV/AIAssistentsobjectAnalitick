from __future__ import annotations

import json
import os
from dataclasses import dataclass

import httpx

from .schema import LLMResponse, LLMUsage
from .service import APIError, RateLimitError


def _read_env(primary_name: str, legacy_name: str | None = None) -> str:
    value = os.getenv(primary_name, "").strip()
    if value:
        return value
    if legacy_name:
        return os.getenv(legacy_name, "").strip()
    return ""


@dataclass(frozen=True, slots=True)
class ChatCompletionsLLMClientConfig:
    base_url: str
    model_name: str
    api_key: str | None = None

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "SUMMARIZATION_ALICAGPT_",
        legacy_prefix: str = "SUMMARIZATION_LLM_",
    ) -> "ChatCompletionsLLMClientConfig":
        base_url = _read_env(f"{prefix}BASE_URL", f"{legacy_prefix}BASE_URL")
        model_name = _read_env(f"{prefix}MODEL_NAME", f"{legacy_prefix}MODEL_NAME")
        api_key = _read_env(f"{prefix}API_KEY", f"{legacy_prefix}API_KEY") or None
        if not base_url:
            raise ValueError(f"{prefix}BASE_URL must be configured")
        if not model_name:
            raise ValueError(f"{prefix}MODEL_NAME must be configured")
        return cls(base_url=base_url.rstrip("/"), model_name=model_name, api_key=api_key)


class ChatCompletionsLLMClient:
    def __init__(self, config: ChatCompletionsLLMClientConfig) -> None:
        self._config = config

    async def complete(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        timeout_seconds: int,
    ) -> LLMResponse:
        payload = {
            "model": self._config.model_name,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["Authorization"] = f"Bearer {self._config.api_key}"

        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(
                f"{self._config.base_url}/v1/chat/completions",
                headers=headers,
                json=payload,
            )

        if response.status_code == 429:
            raise RateLimitError(response.text)
        if response.status_code >= 400:
            raise APIError(f"LLM endpoint returned {response.status_code}: {response.text}")

        data = response.json()
        try:
            message = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise APIError(f"Unexpected LLM response schema: {json.dumps(data, ensure_ascii=False)}") from exc

        usage_payload = data.get("usage") or {}
        usage = LLMUsage(
            input_tokens=int(usage_payload.get("prompt_tokens", 0)),
            output_tokens=int(usage_payload.get("completion_tokens", 0)),
        )
        return LLMResponse(
            text=str(message),
            usage=usage,
            model_name=str(data.get("model") or self._config.model_name),
        )


def build_alicagpt_client_from_env():  # type: ignore[no-untyped-def]
    primary = ChatCompletionsLLMClient(ChatCompletionsLLMClientConfig.from_env())
    fallback_base_url = _read_env(
        "SUMMARIZATION_ALICAGPT_FALLBACK_BASE_URL",
        "SUMMARIZATION_LLM_FALLBACK_BASE_URL",
    )
    fallback_model_name = _read_env(
        "SUMMARIZATION_ALICAGPT_FALLBACK_MODEL_NAME",
        "SUMMARIZATION_LLM_FALLBACK_MODEL_NAME",
    )
    if not fallback_base_url or not fallback_model_name:
        return primary

    fallback = ChatCompletionsLLMClient(
        ChatCompletionsLLMClientConfig(
            base_url=fallback_base_url.rstrip("/"),
            model_name=fallback_model_name,
            api_key=_read_env(
                "SUMMARIZATION_ALICAGPT_FALLBACK_API_KEY",
                "SUMMARIZATION_LLM_FALLBACK_API_KEY",
            )
            or None,
        ),
    )
    from .service import FallbackLLMClient

    return FallbackLLMClient(primary=primary, fallback=fallback)


OpenAICompatibleLLMClientConfig = ChatCompletionsLLMClientConfig
OpenAICompatibleLLMClient = ChatCompletionsLLMClient


def build_llm_client_from_env():  # type: ignore[no-untyped-def]
    return build_alicagpt_client_from_env()
