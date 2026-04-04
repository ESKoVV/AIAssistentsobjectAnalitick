from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from datetime import timedelta
from typing import Any, Mapping, Sequence

from apps.preprocessing.cleaning import clean_text
from apps.preprocessing.deduplication import deduplicate_documents
from apps.preprocessing.enrichment import DEFAULT_METADATA_VERSION, EnrichedDocument, enrich_metadata
from apps.preprocessing.filtering import (
    ContentFilterConfig,
    DEFAULT_CONTENT_FILTER_CONFIG,
    FilterStatus,
    apply_anomaly_detection,
    filter_content,
)
from apps.preprocessing.filtering.schema import FilteredDocument
from apps.preprocessing.geo_enrichment import UNRESOLVED_GEO_SOURCE, enrich_geo
from apps.preprocessing.language import annotate_language
from apps.preprocessing.normalization import normalize_document
from schema import RawMessage


def preprocess_raw_message(
    raw_message: RawMessage,
    source_config: Mapping[str, Any],
    *,
    recent_cleaned_documents,
    content_filter_config: ContentFilterConfig = DEFAULT_CONTENT_FILTER_CONFIG,
    metadata_version: str = DEFAULT_METADATA_VERSION,
) -> tuple[EnrichedDocument, list]:
    normalization_payload = build_normalization_payload(raw_message)
    normalized = normalize_document(normalization_payload, source_config)
    annotated = annotate_language(normalized)
    filtered = filter_content(annotated, content_filter_config)

    if filtered.filter_status is FilterStatus.DROP:
        return build_drop_enriched_document(
            filtered,
            metadata_version=metadata_version,
        ), []

    cleaned = clean_text(filtered)
    deduplication_context = [*recent_cleaned_documents, cleaned]
    deduplicated_documents = deduplicate_documents(deduplication_context)
    deduplicated_documents = _apply_windowed_anomalies(
        deduplicated_documents,
        current_doc_id=cleaned.doc_id,
        config=content_filter_config,
    )

    deduplicated_by_doc_id = {
        document.doc_id: document
        for document in deduplicated_documents
    }
    current_document = deduplicated_by_doc_id[cleaned.doc_id]
    geo_enriched = enrich_geo(current_document, source_config)
    enriched = enrich_metadata(
        geo_enriched,
        source_config,
        metadata_version=metadata_version,
    )
    projection_updates = [
        document
        for document in deduplicated_documents
        if document.doc_id != cleaned.doc_id
    ]
    return enriched, projection_updates


def build_normalization_payload(raw_message: RawMessage) -> dict[str, Any]:
    payload = dict(raw_message.raw_payload or {})
    payload.setdefault("source_id", raw_message.source_id)
    payload.setdefault("author_id", raw_message.author_id)
    payload.setdefault("user_id", raw_message.author_id)
    payload.setdefault("text", raw_message.text)
    payload.setdefault("body", raw_message.text)
    payload.setdefault("message", raw_message.text)
    payload.setdefault("parent_id", raw_message.parent_id)
    payload.setdefault("created_at", raw_message.created_at_utc.isoformat())
    payload.setdefault("published_at", raw_message.created_at_utc.isoformat())
    payload.setdefault("collected_at", raw_message.collected_at.isoformat())
    payload.setdefault("media_type", raw_message.media_type.value if raw_message.media_type else None)
    payload.setdefault("reach", raw_message.reach)
    payload.setdefault("likes", raw_message.likes)
    payload.setdefault("reposts", raw_message.reposts)
    payload.setdefault("comments_count", raw_message.comments_count)
    payload.setdefault("is_official", raw_message.is_official)
    return payload


def build_drop_enriched_document(
    document: FilteredDocument,
    *,
    metadata_version: str = DEFAULT_METADATA_VERSION,
) -> EnrichedDocument:
    payload = asdict(document)
    normalized_text = " ".join(document.text.split())
    text_sha256 = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()
    payload.update(
        {
            "normalized_text": normalized_text,
            "token_count": len(normalized_text.split()) if normalized_text else 0,
            "cleanup_flags": (),
            "text_sha256": text_sha256,
            "duplicate_group_id": f"dup:{document.doc_id}",
            "near_duplicate_flag": False,
            "duplicate_cluster_size": 1,
            "canonical_doc_id": document.doc_id,
            "region_id": None,
            "municipality_id": None,
            "geo_confidence": 0.0,
            "geo_source": UNRESOLVED_GEO_SOURCE,
            "geo_evidence": ("selected_source:unresolved", "geo_unresolved:drop_document"),
            "engagement": max(0, int(document.likes) + int(document.reposts) + int(document.comments_count)),
            "metadata_version": metadata_version,
        },
    )
    return EnrichedDocument(**payload)


def _apply_windowed_anomalies(
    documents,
    *,
    current_doc_id: str,
    config: ContentFilterConfig,
):
    max_window_minutes = max(
        int(config.velocity_window_minutes),
        int(config.author_burst_window_minutes),
    )
    current_document = next(
        document
        for document in documents
        if document.doc_id == current_doc_id
    )
    cutoff = current_document.created_at - timedelta(minutes=max_window_minutes)
    documents_in_window = [
        document
        for document in documents
        if document.created_at >= cutoff
    ]
    updated_window_documents = apply_anomaly_detection(
        documents_in_window,
        config=config,
    )
    updated_by_doc_id = {
        document.doc_id: document
        for document in updated_window_documents
    }
    return [
        updated_by_doc_id.get(document.doc_id, document)
        for document in documents
    ]
