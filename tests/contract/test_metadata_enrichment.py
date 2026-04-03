from __future__ import annotations

from dataclasses import asdict

from apps.preprocessing.enrichment import EnrichedDocument
from tests.helpers import build_enriched_document


EXPECTED_FIELDS = {
    "doc_id",
    "source_type",
    "source_id",
    "parent_id",
    "text",
    "media_type",
    "created_at",
    "collected_at",
    "author_id",
    "is_official",
    "reach",
    "likes",
    "reposts",
    "comments_count",
    "region_hint",
    "geo_lat",
    "geo_lon",
    "raw_payload",
    "language",
    "language_confidence",
    "is_supported_language",
    "filter_status",
    "filter_reasons",
    "quality_weight",
    "normalized_text",
    "token_count",
    "cleanup_flags",
    "text_sha256",
    "duplicate_group_id",
    "near_duplicate_flag",
    "duplicate_cluster_size",
    "canonical_doc_id",
    "region_id",
    "municipality_id",
    "geo_confidence",
    "geo_source",
    "geo_evidence",
    "engagement",
    "metadata_version",
}


def test_enriched_document_preserves_stage_fields_and_adds_metadata_fields() -> None:
    document = build_enriched_document()

    assert isinstance(document, EnrichedDocument)
    assert set(asdict(document).keys()) == EXPECTED_FIELDS
    assert document.engagement == document.likes + document.reposts + document.comments_count
    assert document.metadata_version == "meta-v1"
