from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from apps.preprocessing.cleaning import CleanedDocument
from apps.preprocessing.deduplication import DeduplicatedDocument, deduplicate_documents
from apps.preprocessing.filtering import FilterStatus
from apps.preprocessing.normalization import MediaType, SourceType


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
}


def test_duplicate_record_remains_separate_and_cluster_size_increases() -> None:
    original = _build_cleaned_document(doc_id="vk_post:dedup-1", source_id="dedup-1")
    duplicate = _build_cleaned_document(doc_id="vk_post:dedup-2", source_id="dedup-2")

    deduplicated = deduplicate_documents([original, duplicate])

    assert len(deduplicated) == 2
    assert all(isinstance(document, DeduplicatedDocument) for document in deduplicated)
    assert set(asdict(deduplicated[0]).keys()) == EXPECTED_FIELDS
    assert deduplicated[0].doc_id == "vk_post:dedup-1"
    assert deduplicated[1].doc_id == "vk_post:dedup-2"
    assert deduplicated[0].duplicate_group_id == deduplicated[1].duplicate_group_id
    assert deduplicated[0].duplicate_cluster_size == deduplicated[1].duplicate_cluster_size == 2
    assert deduplicated[0].canonical_doc_id == deduplicated[1].canonical_doc_id == original.doc_id
    assert asdict(original) == {
        key: value
        for key, value in asdict(deduplicated[0]).items()
        if key in asdict(original)
    }
    assert asdict(duplicate) == {
        key: value
        for key, value in asdict(deduplicated[1]).items()
        if key in asdict(duplicate)
    }


def _build_cleaned_document(
    *,
    doc_id: str,
    source_id: str,
    text: str = "На улице Мира восстановили освещение после обращения жителей.",
    normalized_text: str = "На улице Мира восстановили освещение после обращения жителей.",
) -> CleanedDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return CleanedDocument(
        doc_id=doc_id,
        source_type=SourceType.VK_POST,
        source_id=source_id,
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-1",
        is_official=False,
        reach=250,
        likes=8,
        reposts=1,
        comments_count=2,
        region_hint="Волгоград",
        geo_lat=None,
        geo_lon=None,
        raw_payload={"text": text},
        language="ru",
        language_confidence=0.99,
        is_supported_language=True,
        filter_status=FilterStatus.PASS,
        filter_reasons=(),
        quality_weight=1.0,
        normalized_text=normalized_text,
        token_count=len(normalized_text.split()),
        cleanup_flags=("whitespace_normalized",),
    )
