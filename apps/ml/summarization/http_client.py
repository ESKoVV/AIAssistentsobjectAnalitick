from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import httpx

from .schema import LLMResponse, LLMUsage
from .service import APIError, RateLimitError


@dataclass(frozen=True, slots=True)
class OpenAICompatibleLLMClientConfig:
    base_url: str
    model_name: str
    api_key: str | None = None

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "SUMMARIZATION_LLM_",
    ) -> "OpenAICompatibleLLMClientConfig":
        base_url = os.getenv(f"{prefix}BASE_URL", "").strip()
        model_name = os.getenv(f"{prefix}MODEL_NAME", "").strip()
        api_key = os.getenv(f"{prefix}API_KEY", "").strip() or None
        if not base_url:
            raise ValueError(f"{prefix}BASE_URL must be configured")
        if not model_name:
            raise ValueError(f"{prefix}MODEL_NAME must be configured")
        return cls(base_url=base_url.rstrip("/"), model_name=model_name, api_key=api_key)


class OpenAICompatibleLLMClient:
    def __init__(self, config: OpenAICompatibleLLMClientConfig) -> None:
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


def build_llm_client_from_env():  # type: ignore[no-untyped-def]
    primary = OpenAICompatibleLLMClient(OpenAICompatibleLLMClientConfig.from_env())
    fallback_base_url = os.getenv("SUMMARIZATION_LLM_FALLBACK_BASE_URL", "").strip()
    fallback_model_name = os.getenv("SUMMARIZATION_LLM_FALLBACK_MODEL_NAME", "").strip()
    if not fallback_base_url or not fallback_model_name:
        return primary

    fallback = OpenAICompatibleLLMClient(
        OpenAICompatibleLLMClientConfig(
            base_url=fallback_base_url.rstrip("/"),
            model_name=fallback_model_name,
            api_key=os.getenv("SUMMARIZATION_LLM_FALLBACK_API_KEY", "").strip() or None,
        ),
    )
    from .service import FallbackLLMClient

    return FallbackLLMClient(primary=primary, fallback=fallback)
