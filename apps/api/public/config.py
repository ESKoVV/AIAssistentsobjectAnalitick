from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()


DEFAULT_CONFIG_PATH = "configs/api_top.yaml"


@dataclass(frozen=True, slots=True)
class UrgencyConfig:
    critical_growth_rate: float = 5.0
    critical_new_size: int = 100
    critical_geo_spread: int = 5
    high_score_threshold: float = 0.7
    medium_score_threshold: float = 0.4


@dataclass(frozen=True, slots=True)
class AuthConfig:
    disabled: bool = False
    issuer: str | None = None
    audience: str | None = None
    jwks_url: str | None = None
    roles_claim: str = "roles"
    role_claim: str = "role"

    def validate(self) -> None:
        if self.disabled:
            return
        if not self.issuer or not self.audience or not self.jwks_url:
            raise ValueError("issuer, audience and jwks_url must be configured when auth is enabled")


@dataclass(frozen=True, slots=True)
class CacheWarmupQuery:
    period: str
    limit: int
    region: str | None = None
    source: str | None = None


DEFAULT_WARMUP_QUERIES = (
    CacheWarmupQuery(period="24h", limit=10),
    CacheWarmupQuery(period="6h", limit=10),
    CacheWarmupQuery(period="24h", limit=10, region="Ростов-на-Дону"),
)


@dataclass(frozen=True, slots=True)
class APIConfig:
    database_url: str | None
    host: str = "0.0.0.0"
    port: int = 8000
    config_path: str = DEFAULT_CONFIG_PATH
    redis_dsn: str | None = None
    cache_ttl_seconds: int = 300
    freshness_threshold_minutes: int = 30
    kafka_bootstrap_servers: str | None = None
    rankings_updated_topic: str = "rankings.updated"
    api_version: str = "v1"
    auth: AuthConfig = field(default_factory=AuthConfig)
    urgency: UrgencyConfig = field(default_factory=UrgencyConfig)
    warmup_queries: tuple[CacheWarmupQuery, ...] = DEFAULT_WARMUP_QUERIES
    cors_allow_origins: tuple[str, ...] = ("*",)

    def validate(self) -> None:
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")
        if self.port <= 0:
            raise ValueError("port must be positive")
        if self.cache_ttl_seconds <= 0:
            raise ValueError("cache_ttl_seconds must be positive")
        if self.freshness_threshold_minutes <= 0:
            raise ValueError("freshness_threshold_minutes must be positive")
        self.auth.validate()

    @classmethod
    def from_env(cls) -> "APIConfig":
        config_path = os.getenv("API_TOP_CONFIG_PATH", DEFAULT_CONFIG_PATH)
        payload = _load_yaml(config_path)
        auth_payload = dict(payload.get("auth", {}))
        urgency_payload = dict(payload.get("urgency", {}))
        warmup_payload = payload.get("warmup_queries") or []
        cors_payload = payload.get("cors_allow_origins") or ["*"]
        config = cls(
            database_url=os.getenv("DATABASE_URL") or os.getenv("API_DATABASE_URL") or None,
            host=os.getenv("API_HOST", str(payload.get("host", "0.0.0.0"))),
            port=int(os.getenv("API_PORT", payload.get("port", 8000))),
            config_path=config_path,
            redis_dsn=os.getenv("API_REDIS_DSN", payload.get("redis_dsn")) or None,
            cache_ttl_seconds=int(
                os.getenv("API_CACHE_TTL_SECONDS", payload.get("cache_ttl_seconds", 300)),
            ),
            freshness_threshold_minutes=int(
                os.getenv(
                    "API_FRESHNESS_THRESHOLD_MINUTES",
                    payload.get("freshness_threshold_minutes", 30),
                ),
            ),
            kafka_bootstrap_servers=os.getenv(
                "API_KAFKA_BOOTSTRAP_SERVERS",
                payload.get("kafka_bootstrap_servers"),
            )
            or None,
            rankings_updated_topic=os.getenv(
                "API_RANKINGS_UPDATED_TOPIC",
                str(payload.get("rankings_updated_topic", "rankings.updated")),
            ),
            api_version=str(payload.get("api_version", "v1")),
            auth=AuthConfig(
                disabled=_env_bool(
                    os.getenv("API_AUTH_DISABLED"),
                    bool(auth_payload.get("disabled", False)),
                ),
                issuer=os.getenv("API_JWT_ISSUER", auth_payload.get("issuer")) or None,
                audience=os.getenv("API_JWT_AUDIENCE", auth_payload.get("audience")) or None,
                jwks_url=os.getenv("API_JWKS_URL", auth_payload.get("jwks_url")) or None,
                roles_claim=str(auth_payload.get("roles_claim", "roles")),
                role_claim=str(auth_payload.get("role_claim", "role")),
            ),
            urgency=UrgencyConfig(
                critical_growth_rate=float(urgency_payload.get("critical_growth_rate", 5.0)),
                critical_new_size=int(urgency_payload.get("critical_new_size", 100)),
                critical_geo_spread=int(urgency_payload.get("critical_geo_spread", 5)),
                high_score_threshold=float(urgency_payload.get("high_score_threshold", 0.7)),
                medium_score_threshold=float(urgency_payload.get("medium_score_threshold", 0.4)),
            ),
            warmup_queries=tuple(
                CacheWarmupQuery(
                    period=str(item.get("period", "24h")),
                    limit=int(item.get("limit", 10)),
                    region=item.get("region"),
                    source=item.get("source"),
                )
                for item in warmup_payload
            )
            or DEFAULT_WARMUP_QUERIES,
            cors_allow_origins=tuple(str(item) for item in cors_payload),
        )
        config.validate()
        return config


def _env_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _load_yaml(path: str) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return {}
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("api config root must be a mapping")
    return dict(payload)
