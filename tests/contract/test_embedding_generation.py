from __future__ import annotations

from dataclasses import asdict

from apps.ml.embeddings.schema import EmbeddedDocument
from tests.helpers import build_embedded_document, build_enriched_document


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
    "embedding",
    "model_name",
    "model_version",
    "embedded_at",
    "text_used",
    "truncated",
}


def test_embedded_document_preserves_enriched_input_and_adds_embedding_fields() -> None:
    enriched = build_enriched_document()
    embedded = build_embedded_document()

    assert isinstance(embedded, EmbeddedDocument)
    assert set(asdict(embedded).keys()) == EXPECTED_FIELDS
    assert embedded.doc_id == enriched.doc_id
    assert embedded.text == enriched.text
    assert embedded.embedding == [0.6, 0.8]
    assert embedded.text_used.startswith("passage: [vk_post]")
