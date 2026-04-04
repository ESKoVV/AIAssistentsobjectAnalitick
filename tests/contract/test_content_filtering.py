from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from apps.preprocessing.filtering import FilterStatus, FilteredDocument, filter_content
from apps.preprocessing.language import LanguageAnnotatedDocument
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
    "anomaly_flags",
    "anomaly_confidence",
}


def test_drop_status_preserves_document_for_audit() -> None:
    document = _build_language_document(text="ok")

    filtered = filter_content(document)

    assert isinstance(filtered, FilteredDocument)
    assert set(asdict(filtered).keys()) == EXPECTED_FIELDS
    assert filtered.doc_id == document.doc_id
    assert filtered.raw_payload == document.raw_payload
    assert filtered.filter_status is FilterStatus.DROP
    assert filtered.filter_reasons == ("short_noise",)
    assert filtered.quality_weight == 0.0
    assert filtered.anomaly_flags == ()
    assert filtered.anomaly_confidence == 0.0


def test_review_status_keeps_document_in_pipeline_with_reason() -> None:
    document = _build_language_document(
        text="Сколько можно, никто не чинит освещение во дворе, вечером снова темно.",
    )

    filtered = filter_content(document)

    assert filtered.doc_id == document.doc_id
    assert filtered.filter_status is FilterStatus.REVIEW
    assert filtered.filter_reasons == ("complaint_like",)
    assert filtered.quality_weight > 0.0
    assert filtered.quality_weight < 1.0
    assert filtered.anomaly_flags == ()
    assert filtered.anomaly_confidence == 0.0


def test_filter_reasons_are_present_for_review_and_drop_statuses() -> None:
    review_document = filter_content(
        _build_language_document(text="Почему до сих пор не работает лифт в доме, прошу разобраться."),
    )
    drop_document = filter_content(
        _build_language_document(text="Подпишись и забери скидку 50% #реклама"),
    )

    assert review_document.filter_status is FilterStatus.REVIEW
    assert review_document.filter_reasons
    assert drop_document.filter_status is FilterStatus.DROP
    assert drop_document.filter_reasons


def _build_language_document(
    *,
    text: str,
    is_official: bool = False,
) -> LanguageAnnotatedDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return LanguageAnnotatedDocument(
        doc_id="vk_post:filter-contract",
        source_type=SourceType.VK_POST,
        source_id="filter-contract",
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-1",
        is_official=is_official,
        reach=250,
        likes=5,
        reposts=1,
        comments_count=2,
        region_hint="Волгоград",
        geo_lat=None,
        geo_lon=None,
        raw_payload={"text": text},
        language="ru",
        language_confidence=0.99,
        is_supported_language=True,
    )
