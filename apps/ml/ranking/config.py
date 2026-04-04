from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


WEIGHT_KEYS = ("volume", "dynamics", "sentiment", "reach", "geo", "source")
DEFAULT_WEIGHTS = {
    "volume": 0.25,
    "dynamics": 0.25,
    "sentiment": 0.20,
    "reach": 0.15,
    "geo": 0.10,
    "source": 0.05,
}
DEFAULT_ACTIVE_PROFILE = "monitoring"
DEFAULT_CONFIG_PATH = "configs/ranking.yaml"
DEFAULT_DOCUMENTS_TABLE = "normalized_messages"
DEFAULT_SENTIMENTS_TABLE = "document_sentiments"
DEFAULT_INPUT_TOPIC = "descriptions.updated"
DEFAULT_OUTPUT_TOPIC = "rankings.updated"
DEFAULT_TOP_N = 10
DEFAULT_MIN_CLUSTER_SIZE_FOR_RANKING = 10
DEFAULT_STALE_AFTER_HOURS = 48
DEFAULT_NEW_CLUSTER_HOURS = 3
DEFAULT_GROWING_THRESHOLD = 2.0
DEFAULT_GEO_MAX_COVERAGE_RATIO = 0.3
DEFAULT_SOURCE_TYPE_COUNT = 4
DEFAULT_SCHEDULE_INTERVAL_MINUTES = 15
DEFAULT_SNAPSHOT_PERIOD_HOURS = (6, 24, 72)


def _default_weight_profiles() -> dict[str, dict[str, float]]:
    return {DEFAULT_ACTIVE_PROFILE: dict(DEFAULT_WEIGHTS)}


@dataclass(frozen=True, slots=True)
class RankingServiceConfig:
    postgres_dsn: str | None
    kafka_bootstrap_servers: str | None = None
    config_path: str = DEFAULT_CONFIG_PATH
    documents_table: str = DEFAULT_DOCUMENTS_TABLE
    sentiments_table: str = DEFAULT_SENTIMENTS_TABLE
    input_topic: str = DEFAULT_INPUT_TOPIC
    output_topic: str = DEFAULT_OUTPUT_TOPIC
    top_n: int = DEFAULT_TOP_N
    min_cluster_size_for_ranking: int = DEFAULT_MIN_CLUSTER_SIZE_FOR_RANKING
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS
    new_cluster_hours: int = DEFAULT_NEW_CLUSTER_HOURS
    growing_threshold: float = DEFAULT_GROWING_THRESHOLD
    geo_max_coverage_ratio: float = DEFAULT_GEO_MAX_COVERAGE_RATIO
    source_type_count: int = DEFAULT_SOURCE_TYPE_COUNT
    schedule_interval_minutes: int = DEFAULT_SCHEDULE_INTERVAL_MINUTES
    snapshot_period_hours: tuple[int, ...] = DEFAULT_SNAPSHOT_PERIOD_HOURS
    active_profile: str = DEFAULT_ACTIVE_PROFILE
    weight_profiles: dict[str, dict[str, float]] = field(default_factory=_default_weight_profiles)

    def __post_init__(self) -> None:
        normalized_profiles = _normalize_weight_profiles(self.weight_profiles)
        active_profile = self.active_profile.strip()
        if not active_profile:
            raise ValueError("active_profile must be non-empty")
        if active_profile not in normalized_profiles:
            raise ValueError(f"active_profile '{active_profile}' is not defined in weight_profiles")
        if not self.config_path.strip():
            raise ValueError("config_path must be non-empty")
        if not self.documents_table.strip():
            raise ValueError("documents_table must be non-empty")
        if not self.sentiments_table.strip():
            raise ValueError("sentiments_table must be non-empty")
        if not self.input_topic.strip():
            raise ValueError("input_topic must be non-empty")
        if not self.output_topic.strip():
            raise ValueError("output_topic must be non-empty")
        if self.top_n <= 0:
            raise ValueError("top_n must be positive")
        if self.min_cluster_size_for_ranking <= 0:
            raise ValueError("min_cluster_size_for_ranking must be positive")
        if self.stale_after_hours <= 0:
            raise ValueError("stale_after_hours must be positive")
        if self.new_cluster_hours <= 0:
            raise ValueError("new_cluster_hours must be positive")
        if self.growing_threshold < 0.0:
            raise ValueError("growing_threshold must be non-negative")
        if not 0.0 < self.geo_max_coverage_ratio <= 1.0:
            raise ValueError("geo_max_coverage_ratio must be between 0 and 1")
        if self.source_type_count <= 0:
            raise ValueError("source_type_count must be positive")
        if self.schedule_interval_minutes <= 0:
            raise ValueError("schedule_interval_minutes must be positive")
        if not self.snapshot_period_hours:
            raise ValueError("snapshot_period_hours must contain at least one period")
        if any(period <= 0 for period in self.snapshot_period_hours):
            raise ValueError("snapshot_period_hours values must be positive")

        object.__setattr__(self, "active_profile", active_profile)
        object.__setattr__(self, "weight_profiles", normalized_profiles)
        object.__setattr__(
            self,
            "snapshot_period_hours",
            tuple(sorted({int(period) for period in self.snapshot_period_hours})),
        )

    @property
    def weights(self) -> dict[str, float]:
        return dict(self.weight_profiles[self.active_profile])

    def weights_config_payload(self) -> dict[str, Any]:
        return {
            "active_profile": self.active_profile,
            "weights": self.weights,
        }

    @classmethod
    def from_env(cls) -> "RankingServiceConfig":
        config_path = os.getenv("RANKING_CONFIG_PATH", DEFAULT_CONFIG_PATH)
        payload = _load_config_payload(config_path)
        return cls(
            postgres_dsn=os.getenv("RANKING_POSTGRES_DSN") or None,
            kafka_bootstrap_servers=os.getenv("RANKING_KAFKA_BOOTSTRAP_SERVERS") or None,
            config_path=config_path,
            documents_table=os.getenv(
                "RANKING_DOCUMENTS_TABLE",
                str(payload.get("documents_table", DEFAULT_DOCUMENTS_TABLE)),
            ),
            sentiments_table=os.getenv(
                "RANKING_SENTIMENTS_TABLE",
                str(payload.get("sentiments_table", DEFAULT_SENTIMENTS_TABLE)),
            ),
            input_topic=os.getenv(
                "RANKING_INPUT_TOPIC",
                str(payload.get("input_topic", DEFAULT_INPUT_TOPIC)),
            ),
            output_topic=os.getenv(
                "RANKING_OUTPUT_TOPIC",
                str(payload.get("output_topic", DEFAULT_OUTPUT_TOPIC)),
            ),
            top_n=int(os.getenv("RANKING_TOP_N", payload.get("top_n", DEFAULT_TOP_N))),
            min_cluster_size_for_ranking=int(
                os.getenv(
                    "RANKING_MIN_CLUSTER_SIZE_FOR_RANKING",
                    payload.get("min_cluster_size_for_ranking", DEFAULT_MIN_CLUSTER_SIZE_FOR_RANKING),
                ),
            ),
            stale_after_hours=int(
                os.getenv(
                    "RANKING_STALE_AFTER_HOURS",
                    payload.get("stale_after_hours", DEFAULT_STALE_AFTER_HOURS),
                ),
            ),
            new_cluster_hours=int(
                os.getenv(
                    "RANKING_NEW_CLUSTER_HOURS",
                    payload.get("new_cluster_hours", DEFAULT_NEW_CLUSTER_HOURS),
                ),
            ),
            growing_threshold=float(
                os.getenv(
                    "RANKING_GROWING_THRESHOLD",
                    payload.get("growing_threshold", DEFAULT_GROWING_THRESHOLD),
                ),
            ),
            geo_max_coverage_ratio=float(
                os.getenv(
                    "RANKING_GEO_MAX_COVERAGE_RATIO",
                    payload.get("geo_max_coverage_ratio", DEFAULT_GEO_MAX_COVERAGE_RATIO),
                ),
            ),
            source_type_count=int(
                os.getenv(
                    "RANKING_SOURCE_TYPE_COUNT",
                    payload.get("source_type_count", DEFAULT_SOURCE_TYPE_COUNT),
                ),
            ),
            schedule_interval_minutes=int(
                os.getenv(
                    "RANKING_SCHEDULE_INTERVAL_MINUTES",
                    payload.get("schedule_interval_minutes", DEFAULT_SCHEDULE_INTERVAL_MINUTES),
                ),
            ),
            snapshot_period_hours=_coerce_snapshot_period_hours(
                os.getenv("RANKING_SNAPSHOT_PERIOD_HOURS"),
                payload.get("snapshot_period_hours"),
            ),
            active_profile=os.getenv(
                "RANKING_ACTIVE_PROFILE",
                str(payload.get("active_profile", DEFAULT_ACTIVE_PROFILE)),
            ),
            weight_profiles=_coerce_weight_profiles(payload.get("weights")),
        )


def _coerce_weight_profiles(payload: Any) -> dict[str, dict[str, float]]:
    if payload is None:
        return _default_weight_profiles()
    if not isinstance(payload, dict):
        raise ValueError("weights must be a mapping of profile_name -> weight map")
    return {
        str(profile_name): {str(key): float(value) for key, value in dict(profile_weights).items()}
        for profile_name, profile_weights in payload.items()
    }


def _coerce_snapshot_period_hours(
    env_value: str | None,
    payload: Any,
) -> tuple[int, ...]:
    if env_value:
        return tuple(int(item.strip()) for item in env_value.split(",") if item.strip())
    if payload is None:
        return DEFAULT_SNAPSHOT_PERIOD_HOURS
    if isinstance(payload, (list, tuple)):
        return tuple(int(item) for item in payload)
    if isinstance(payload, str):
        return tuple(int(item.strip()) for item in payload.split(",") if item.strip())
    raise ValueError("snapshot_period_hours must be a sequence or comma-separated string")


def _normalize_weight_profiles(payload: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    if not payload:
        raise ValueError("weight_profiles must contain at least one profile")

    normalized: dict[str, dict[str, float]] = {}
    for profile_name, weights in payload.items():
        name = str(profile_name).strip()
        if not name:
            raise ValueError("weight profile names must be non-empty")
        weight_keys = set(weights)
        expected_keys = set(WEIGHT_KEYS)
        if weight_keys != expected_keys:
            missing = sorted(expected_keys - weight_keys)
            extra = sorted(weight_keys - expected_keys)
            details = []
            if missing:
                details.append(f"missing={missing}")
            if extra:
                details.append(f"extra={extra}")
            raise ValueError(f"weight profile '{name}' has invalid keys: {', '.join(details)}")
        if any(float(value) < 0.0 for value in weights.values()):
            raise ValueError(f"weight profile '{name}' contains negative values")
        total = sum(float(value) for value in weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"weight profile '{name}' must sum to 1.0, got {total:.6f}")
        normalized[name] = {key: float(weights[key]) for key in WEIGHT_KEYS}
    return normalized


def _load_config_payload(path: str) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return {}

    raw_text = config_path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore[import-not-found]
    except ImportError:
        payload = _parse_simple_yaml(raw_text)
    else:
        loaded = yaml.safe_load(raw_text) or {}
        if not isinstance(loaded, dict):
            raise ValueError("ranking config root must be a mapping")
        payload = dict(loaded)

    if not isinstance(payload, dict):
        raise ValueError("ranking config root must be a mapping")
    return payload


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = line.lstrip()
        key, separator, value = stripped.partition(":")
        if separator != ":":
            raise ValueError(f"invalid YAML line {line_number}: expected 'key: value'")

        while indent <= stack[-1][0]:
            stack.pop()
        current = stack[-1][1]
        normalized_key = key.strip()
        if not normalized_key:
            raise ValueError(f"invalid YAML line {line_number}: empty key")

        if value.strip():
            current[normalized_key] = _parse_scalar(value.strip())
            continue

        child: dict[str, Any] = {}
        current[normalized_key] = child
        stack.append((indent, child))

    return root


def _parse_scalar(value: str) -> Any:
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
        return value[1:-1]

    try:
        if any(marker in value for marker in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value
