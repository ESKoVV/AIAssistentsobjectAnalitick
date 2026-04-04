from __future__ import annotations

from datetime import datetime, timezone

from apps.preprocessing.filtering import FilterStatus, filter_content
from apps.preprocessing.language import LanguageAnnotatedDocument
from apps.preprocessing.normalization import MediaType, SourceType


def test_short_noise_is_dropped() -> None:
    filtered = filter_content(_build_language_document("ага"))

    assert filtered.filter_status is FilterStatus.DROP
    assert filtered.filter_reasons == ("short_noise",)
    assert filtered.quality_weight == 0.0


def test_explicit_advertising_is_dropped() -> None:
    filtered = filter_content(_build_language_document("Подпишись и получи промокод на скидку 70% #реклама"))

    assert filtered.filter_status is FilterStatus.DROP
    assert filtered.filter_reasons == ("spam_signature",)
    assert filtered.quality_weight == 0.0


def test_ambiguous_complaint_is_sent_to_review() -> None:
    filtered = filter_content(
        _build_language_document("Сколько можно, никто не убирает мусор у остановки, прошу разобраться."),
    )

    assert filtered.filter_status is FilterStatus.REVIEW
    assert filtered.filter_reasons == ("complaint_like",)
    assert filtered.quality_weight == 0.6


def test_official_post_is_not_dropped_by_complaint_like_language() -> None:
    filtered = filter_content(
        _build_language_document(
            "Почему до сих пор перекрыта улица, объясняем: идет плановый ремонт теплосети до вечера.",
            is_official=True,
        ),
    )

    assert filtered.filter_status is FilterStatus.PASS
    assert filtered.filter_reasons == ()
    assert filtered.quality_weight == 1.0


def test_short_but_relevant_message_passes() -> None:
    filtered = filter_content(_build_language_document("Пожар в школе"))

    assert filtered.filter_status is FilterStatus.PASS
    assert filtered.filter_reasons == ()
    assert filtered.quality_weight == 1.0


def _build_language_document(
    text: str,
    *,
    is_official: bool = False,
) -> LanguageAnnotatedDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return LanguageAnnotatedDocument(
        doc_id="vk_post:filter-unit",
        source_type=SourceType.VK_POST,
        source_id="filter-unit",
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-1",
        is_official=is_official,
        reach=100,
        likes=2,
        reposts=0,
        comments_count=1,
        region_hint="Волгоград",
        geo_lat=None,
        geo_lon=None,
        raw_payload={"text": text},
        language="ru",
        language_confidence=0.98,
        is_supported_language=True,
    )
