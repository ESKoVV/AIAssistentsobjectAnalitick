from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.preprocessing.deduplication import DeduplicatedDocument
from apps.preprocessing.filtering import FilterStatus
from apps.preprocessing.filtering.anomaly import check_author_burst, check_velocity
from apps.preprocessing.normalization import MediaType, SourceType


def test_velocity_detects_exact_duplicate_flood_within_window() -> None:
    start = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    documents = [
        _build_document(
            doc_id=f"doc-{index}",
            source_id=f"source-{index}",
            author_id=f"author-{index}",
            created_at=start + timedelta(seconds=25 * index),
            text_sha256="same-sha",
            duplicate_group_id=f"dup:{index}",
        )
        for index in range(20)
    ]

    flags = check_velocity(documents, window_minutes=30)
    target_flag = next(flag for flag in flags if flag.doc_id == "doc-0")

    assert len(flags) == 20
    assert target_flag.anomaly_type == "coordinated_flood"
    assert target_flag.group_size == 20
    assert target_flag.confidence >= 0.8


def test_velocity_ignores_exact_duplicate_flood_outside_window() -> None:
    start = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    documents = [
        _build_document(
            doc_id=f"doc-{index}",
            source_id=f"source-{index}",
            author_id=f"author-{index}",
            created_at=start + timedelta(minutes=7 * index),
            text_sha256="same-sha",
            duplicate_group_id=f"dup:{index}",
        )
        for index in range(20)
    ]

    flags = check_velocity(documents, window_minutes=30)

    assert flags == []


def test_velocity_detects_near_duplicate_flood() -> None:
    start = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    documents = [
        _build_document(
            doc_id=f"doc-{index}",
            source_id=f"source-{index}",
            author_id=f"author-{index}",
            created_at=start + timedelta(seconds=30 * index),
            text_sha256=f"sha-{index}",
            duplicate_group_id="dup:near-flood",
            near_duplicate_flag=True,
        )
        for index in range(15)
    ]

    flags = check_velocity(documents, window_minutes=30)
    target_flag = next(flag for flag in flags if flag.doc_id == "doc-0")

    assert len(flags) == 15
    assert target_flag.anomaly_type == "near_duplicate_flood"
    assert target_flag.group_size == 15
    assert target_flag.confidence >= 0.8


def test_author_burst_detects_single_author() -> None:
    start = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    documents = [
        _build_document(
            doc_id=f"doc-{index}",
            source_id=f"source-{index}",
            author_id="burst-author",
            created_at=start + timedelta(minutes=2 * index + 1),
            text_sha256=f"sha-{index}",
            duplicate_group_id=f"dup:{index}",
        )
        for index in range(12)
    ]

    flags = check_author_burst(documents, window_minutes=60)
    burst_flags = [flag for flag in flags if flag.anomaly_type == "author_burst"]

    assert len(burst_flags) == 12
    assert burst_flags[0].group_size == 12


def test_author_burst_adds_bot_timing_for_tightly_spaced_posts() -> None:
    start = datetime(2026, 4, 2, 9, 0, tzinfo=UTC)
    documents = [
        _build_document(
            doc_id=f"doc-{index}",
            source_id=f"source-{index}",
            author_id="bot-author",
            created_at=start + timedelta(seconds=20 * index),
            text_sha256=f"sha-{index}",
            duplicate_group_id=f"dup:{index}",
        )
        for index in range(12)
    ]

    flags = check_author_burst(documents, window_minutes=60)
    anomaly_types = {flag.anomaly_type for flag in flags if flag.doc_id == "doc-0"}

    assert "author_burst" in anomaly_types
    assert "bot_timing" in anomaly_types


def _build_document(
    *,
    doc_id: str,
    source_id: str,
    author_id: str,
    created_at: datetime,
    text_sha256: str,
    duplicate_group_id: str,
    near_duplicate_flag: bool = False,
) -> DeduplicatedDocument:
    text = "Отключение воды на районе"
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
        likes=1,
        reposts=0,
        comments_count=0,
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
        normalized_text=text,
        token_count=len(text.split()),
        cleanup_flags=("whitespace_normalized",),
        text_sha256=text_sha256,
        duplicate_group_id=duplicate_group_id,
        near_duplicate_flag=near_duplicate_flag,
        duplicate_cluster_size=1,
        canonical_doc_id=doc_id,
    )
