from __future__ import annotations

from dataclasses import asdict
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from apps.ml.classification import TaxonomyConfig, classify_document, load_taxonomy_config
from apps.preprocessing.geo_enrichment import GeoEnrichedDocument

from .schema import EnrichedDocument


DEFAULT_METADATA_VERSION = "meta-v1"


def enrich_metadata(
    document: GeoEnrichedDocument,
    source_config: Mapping[str, Any] | None = None,
    *,
    official_registry: Iterable[tuple[str, str]] | None = None,
    taxonomy_config: TaxonomyConfig | None = None,
    metadata_version: str = DEFAULT_METADATA_VERSION,
) -> EnrichedDocument:
    config = dict(source_config or {})
    payload = asdict(document)
    classification = classify_document(
        document.normalized_text or document.text,
        config=taxonomy_config or load_taxonomy_config(),
    )
    payload["is_official"] = _resolve_official_status(
        document=document,
        source_config=config,
        official_registry=official_registry,
    )
    return EnrichedDocument(
        **payload,
        engagement=max(0, int(document.likes) + int(document.reposts) + int(document.comments_count)),
        metadata_version=metadata_version,
        category=classification.category,
        category_label=classification.category_label,
        category_confidence=classification.confidence,
        secondary_category=classification.secondary_category,
    )


def _resolve_official_status(
    *,
    document: GeoEnrichedDocument,
    source_config: Mapping[str, Any],
    official_registry: Iterable[tuple[str, str]] | None,
) -> bool:
    if document.is_official or bool(source_config.get("is_official", False)):
        return True

    official_pairs = {
        (str(source_type), str(source_id))
        for source_type, source_id in (official_registry or ())
    }
    if (_enum_value(document.source_type), str(document.source_id)) in official_pairs:
        return True

    official_source_ids = {str(item) for item in _sequence_value(source_config.get("official_source_ids"))}
    if str(document.source_id) in official_source_ids:
        return True

    official_author_ids = {str(item) for item in _sequence_value(source_config.get("official_author_ids"))}
    return str(document.author_id) in official_author_ids


def _sequence_value(value: Any) -> Sequence[Any]:
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(value)
    return ()


def _enum_value(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)
