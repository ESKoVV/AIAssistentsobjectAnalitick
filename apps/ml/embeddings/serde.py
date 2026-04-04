from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from enum import Enum
from typing import Any, Mapping

from apps.ml.embeddings.schema import EmbeddedDocument
from apps.preprocessing.enrichment import EnrichedDocument
from apps.preprocessing.filtering.schema import FilterStatus
from apps.preprocessing.normalization import MediaType, SourceType


def serialize_document(document: EnrichedDocument | EmbeddedDocument) -> dict[str, Any]:
    payload = asdict(document)
    return _normalize_value(payload)


def deserialize_enriched_document(payload: Mapping[str, Any]) -> EnrichedDocument:
    normalized = dict(payload)
    return EnrichedDocument(
        doc_id=str(normalized["doc_id"]),
        source_type=SourceType(normalized["source_type"]),
        source_id=str(normalized["source_id"]),
        parent_id=_optional_str(normalized.get("parent_id")),
        text=str(normalized["text"]),
        media_type=MediaType(normalized["media_type"]),
        created_at=_parse_datetime(normalized["created_at"]),
        collected_at=_parse_datetime(normalized["collected_at"]),
        author_id=str(normalized["author_id"]),
        is_official=bool(normalized["is_official"]),
        reach=int(normalized["reach"]),
        likes=int(normalized["likes"]),
        reposts=int(normalized["reposts"]),
        comments_count=int(normalized["comments_count"]),
        region_hint=_optional_str(normalized.get("region_hint")),
        geo_lat=_optional_float(normalized.get("geo_lat")),
        geo_lon=_optional_float(normalized.get("geo_lon")),
        raw_payload=dict(normalized.get("raw_payload") or {}),
        language=str(normalized["language"]),
        language_confidence=float(normalized["language_confidence"]),
        is_supported_language=bool(normalized["is_supported_language"]),
        filter_status=FilterStatus(normalized["filter_status"]),
        filter_reasons=tuple(str(reason) for reason in normalized.get("filter_reasons", ())),
        quality_weight=float(normalized["quality_weight"]),
        anomaly_flags=tuple(str(flag) for flag in normalized.get("anomaly_flags", ())),
        anomaly_confidence=float(normalized.get("anomaly_confidence", 0.0)),
        normalized_text=str(normalized["normalized_text"]),
        token_count=int(normalized["token_count"]),
        cleanup_flags=tuple(str(flag) for flag in normalized.get("cleanup_flags", ())),
        text_sha256=str(normalized["text_sha256"]),
        duplicate_group_id=str(normalized["duplicate_group_id"]),
        near_duplicate_flag=bool(normalized["near_duplicate_flag"]),
        duplicate_cluster_size=int(normalized["duplicate_cluster_size"]),
        canonical_doc_id=str(normalized["canonical_doc_id"]),
        region_id=_optional_str(normalized.get("region_id")),
        municipality_id=_optional_str(normalized.get("municipality_id")),
        geo_confidence=float(normalized["geo_confidence"]),
        geo_source=str(normalized["geo_source"]),
        geo_evidence=tuple(str(item) for item in normalized.get("geo_evidence", ())),
        engagement=int(normalized["engagement"]),
        metadata_version=str(normalized["metadata_version"]),
    )


def deserialize_embedded_document(payload: Mapping[str, Any]) -> EmbeddedDocument:
    enriched = deserialize_enriched_document(payload)
    normalized = dict(payload)
    base_kwargs = asdict(enriched)
    base_kwargs.pop("token_count", None)
    return EmbeddedDocument(
        **base_kwargs,
        embedding=[float(value) for value in normalized["embedding"]],
        model_name=str(normalized["model_name"]),
        model_version=str(normalized["model_version"]),
        embedded_at=_parse_datetime(normalized["embedded_at"]),
        text_used=str(normalized["text_used"]),
        token_count=int(normalized["token_count"]),
        truncated=bool(normalized["truncated"]),
    )


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_value(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value)
    return normalized if normalized else None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
