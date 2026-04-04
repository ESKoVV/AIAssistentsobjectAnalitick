from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Mapping

import yaml


class SourceRegistryError(RuntimeError):
    """Raised when the source registry config is missing or invalid."""


def load_source_registry(path: str | Path) -> dict[str, dict[str, Any]]:
    config_path = Path(path)
    if not config_path.exists():
        raise SourceRegistryError(f"Source registry config not found: {config_path}")

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise SourceRegistryError("Source registry root must be a mapping")

    defaults = dict(payload.get("defaults") or {})
    sources_payload = payload.get("sources", payload)
    if not isinstance(sources_payload, Mapping):
        raise SourceRegistryError("Source registry must define a 'sources' mapping")

    registry: dict[str, dict[str, Any]] = {}
    for source_key, source_config in sources_payload.items():
        if not isinstance(source_config, Mapping):
            continue
        merged = dict(defaults)
        merged.update(dict(source_config))
        registry[str(source_key)] = merged

    if not registry:
        raise SourceRegistryError("Source registry is empty")
    return registry


def resolve_source_config(
    registry: Mapping[str, Mapping[str, Any]],
    source_type: str | Enum,
) -> dict[str, Any]:
    normalized_source_type = _enum_value(source_type)
    direct_match = registry.get(normalized_source_type)
    if direct_match is not None:
        return dict(direct_match)

    generic_key = _generic_source_key(normalized_source_type)
    generic_match = registry.get(generic_key)
    if generic_match is not None:
        resolved = dict(generic_match)
        resolved.setdefault("entity_type", _entity_type_from_source_type(normalized_source_type))
        resolved.setdefault("source", generic_key)
        return resolved

    raise SourceRegistryError(
        f"Source registry does not contain config for source_type={normalized_source_type!r}",
    )


def _generic_source_key(source_type: str) -> str:
    if source_type.startswith("vk_"):
        return "vk"
    if source_type.startswith("max_"):
        return "max"
    if source_type.startswith("rss_"):
        return "rss"
    if source_type.startswith("portal_"):
        return "portal"
    return source_type


def _entity_type_from_source_type(source_type: str) -> str:
    if source_type.endswith("_comment"):
        return "comment"
    if source_type.endswith("_article"):
        return "article"
    return "post"


def _enum_value(value: str | Enum) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)
