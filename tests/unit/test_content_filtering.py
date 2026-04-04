from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

from apps.preprocessing.deduplication import DeduplicatedDocument
from apps.preprocessing.filtering import FilterStatus, apply_anomaly_detection, filter_content
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


def test_anomaly_review_reduces_quality_weight_without_dropping_document() -> None:
    base_time = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    documents = [
        _build_deduplicated_document(
            doc_id=f"vk_post:flood-{index}",
            source_id=f"flood-{index}",
            author_id=f"author-{index}",
            created_at=base_time + timedelta(seconds=20 * index),
            text_sha256="same-text",
            duplicate_group_id="dup:flood",
        )
        for index in range(20)
    ]

    flagged = apply_anomaly_detection(documents)

    assert all(document.filter_status is FilterStatus.REVIEW for document in flagged)
    assert all(document.anomaly_flags == ("coordinated_flood",) for document in flagged)
    assert all(document.anomaly_confidence >= 0.8 for document in flagged)
    assert all(document.quality_weight < 1.0 for document in flagged)


def test_normal_distribution_keeps_quality_weight_unchanged() -> None:
    base_time = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    documents = [
        _build_deduplicated_document(
            doc_id=f"vk_post:normal-{index}",
            source_id=f"normal-{index}",
            author_id=f"author-{index}",
            created_at=base_time + timedelta(hours=index),
            text_sha256=f"sha-{index}",
            duplicate_group_id=f"dup:normal-{index}",
        )
        for index in range(5)
    ]

    flagged = apply_anomaly_detection(documents)

    assert all(document.anomaly_flags == () for document in flagged)
    assert all(document.anomaly_confidence == 0.0 for document in flagged)
    assert all(document.filter_status is FilterStatus.PASS for document in flagged)
    assert all(document.quality_weight == 1.0 for document in flagged)


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


def _build_deduplicated_document(
    *,
    doc_id: str,
    source_id: str,
    author_id: str,
    created_at: datetime,
    text_sha256: str,
    duplicate_group_id: str,
) -> DeduplicatedDocument:
    text = "Жители обсуждают отключение воды"
    return DeduplicatedDocument(
        doc_id=doc_id,
        source_type=SourceType.VK_POST,
        source_id=source_id,
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=created_at,
        collected_at=created_at,
        author_id=author_id,
        is_official=False,
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
        filter_status=FilterStatus.PASS,
        filter_reasons=(),
        quality_weight=1.0,
        normalized_text=text,
        token_count=len(text.split()),
        cleanup_flags=("whitespace_normalized",),
        text_sha256=text_sha256,
        duplicate_group_id=duplicate_group_id,
        near_duplicate_flag=False,
        duplicate_cluster_size=1,
        canonical_doc_id=doc_id,
    )
