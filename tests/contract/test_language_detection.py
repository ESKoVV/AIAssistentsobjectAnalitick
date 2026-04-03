from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from apps.preprocessing.language import LanguageAnnotatedDocument, annotate_language
from apps.preprocessing.normalization import MediaType, NormalizedDocument, SourceType


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
}


def test_language_detection_preserves_normalized_document_and_adds_language_fields() -> None:
    document = _build_normalized_document(
        text="На улице Мира восстановили освещение после обращения жителей.",
    )

    annotated = annotate_language(document)

    assert isinstance(annotated, LanguageAnnotatedDocument)
    assert set(asdict(annotated).keys()) == EXPECTED_FIELDS
    assert asdict(document) == {key: value for key, value in asdict(annotated).items() if key in asdict(document)}
    assert annotated.language == "ru"
    assert annotated.is_supported_language is True


def test_language_detection_marks_unsupported_language_without_dropping_document() -> None:
    document = _build_normalized_document(
        doc_id="rss_article:kk-1",
        source_id="kk-1",
        source_type=SourceType.RSS_ARTICLE,
        text="Сәлеметсіз бе, қалада жаңа емхана ашылды.",
    )

    annotated = annotate_language(document)

    assert annotated.doc_id == document.doc_id
    assert annotated.raw_payload == document.raw_payload
    assert annotated.language == "kk"
    assert annotated.is_supported_language is False


def _build_normalized_document(
    *,
    doc_id: str = "vk_post:1",
    source_id: str = "1",
    source_type: SourceType = SourceType.VK_POST,
    text: str,
) -> NormalizedDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return NormalizedDocument(
        doc_id=doc_id,
        source_type=source_type,
        source_id=source_id,
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-1",
        is_official=False,
        reach=120,
        likes=7,
        reposts=1,
        comments_count=2,
        region_hint="Волгоград",
        geo_lat=None,
        geo_lon=None,
        raw_payload={"text": text},
    )
