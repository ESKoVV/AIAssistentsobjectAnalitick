from __future__ import annotations

from datetime import datetime, timezone

from apps.preprocessing.language import UNKNOWN_LANGUAGE, annotate_language
from apps.preprocessing.normalization import MediaType, NormalizedDocument, SourceType


def test_russian_text_is_marked_as_supported() -> None:
    annotated = annotate_language(_build_normalized_document("В городе открыли новую поликлинику для жителей района."))

    assert annotated.language == "ru"
    assert annotated.language_confidence > 0.8
    assert annotated.is_supported_language is True


def test_kazakh_text_is_marked_as_unsupported() -> None:
    annotated = annotate_language(_build_normalized_document("Сәлеметсіз бе, қала тұрғындары үшін жаңа емхана ашылды."))

    assert annotated.language == "kk"
    assert annotated.language_confidence > 0.9
    assert annotated.is_supported_language is False


def test_mixed_text_uses_fasttext_lid_prediction() -> None:
    annotated = annotate_language(
        _build_normalized_document("Привет, город готовится к празднику. Қалада жаңа мектеп ашылды."),
    )

    assert annotated.language == "ru"
    assert annotated.language_confidence > 0.5
    assert annotated.is_supported_language is True


def test_empty_text_is_marked_as_unknown_without_model_call() -> None:
    annotated = annotate_language(_build_normalized_document(""))

    assert annotated.language == UNKNOWN_LANGUAGE
    assert annotated.language_confidence == 0.0
    assert annotated.is_supported_language is False


def test_emoji_only_text_is_marked_as_unknown() -> None:
    annotated = annotate_language(_build_normalized_document("🙂 🚍 🔧"))

    assert annotated.language == UNKNOWN_LANGUAGE
    assert annotated.language_confidence == 0.0
    assert annotated.is_supported_language is False


def _build_normalized_document(text: str) -> NormalizedDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return NormalizedDocument(
        doc_id="vk_post:language-test",
        source_type=SourceType.VK_POST,
        source_id="language-test",
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-1",
        is_official=False,
        reach=0,
        likes=0,
        reposts=0,
        comments_count=0,
        region_hint=None,
        geo_lat=None,
        geo_lon=None,
        raw_payload={"text": text},
    )
