from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from apps.preprocessing.cleaning import CleanedDocument, clean_text
from apps.preprocessing.filtering import FilterStatus, FilteredDocument
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
}


def test_text_cleaning_preserves_source_text_and_writes_cleaned_output_to_normalized_text() -> None:
    document = _build_filtered_document(
        text="Смотрите https://example.test/path @city_admin 🚑",
        filter_status=FilterStatus.REVIEW,
    )

    cleaned = clean_text(document)

    assert isinstance(cleaned, CleanedDocument)
    assert set(asdict(cleaned).keys()) == EXPECTED_FIELDS
    assert cleaned.text == document.text
    assert "https://example.test/path" in cleaned.text
    assert cleaned.normalized_text == "Смотрите USER ambulance"
    assert asdict(document) == {
        key: value
        for key, value in asdict(cleaned).items()
        if key in asdict(document)
    }


def _build_filtered_document(
    *,
    text: str,
    filter_status: FilterStatus = FilterStatus.PASS,
) -> FilteredDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return FilteredDocument(
        doc_id="vk_post:cleaning-contract",
        source_type=SourceType.VK_POST,
        source_id="cleaning-contract",
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-1",
        is_official=False,
        reach=420,
        likes=12,
        reposts=3,
        comments_count=5,
        region_hint="Волгоград",
        geo_lat=None,
        geo_lon=None,
        raw_payload={"text": text},
        language="ru",
        language_confidence=0.99,
        is_supported_language=True,
        filter_status=filter_status,
        filter_reasons=("complaint_like",) if filter_status is FilterStatus.REVIEW else (),
        quality_weight=0.6 if filter_status is FilterStatus.REVIEW else 1.0,
    )
