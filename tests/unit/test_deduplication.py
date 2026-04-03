from __future__ import annotations

from datetime import datetime, timezone

from apps.preprocessing.cleaning import CleanedDocument
from apps.preprocessing.deduplication import deduplicate_documents
from apps.preprocessing.filtering import FilterStatus
from apps.preprocessing.normalization import MediaType, SourceType


def test_exact_duplicates_are_clustered_by_sha256() -> None:
    first = _build_cleaned_document(
        doc_id="vk_post:exact-1",
        source_id="exact-1",
        normalized_text="На улице Мира отключили воду до вечера",
    )
    second = _build_cleaned_document(
        doc_id="vk_post:exact-2",
        source_id="exact-2",
        normalized_text="На улице Мира отключили воду до вечера",
    )

    deduplicated = deduplicate_documents([first, second])

    assert deduplicated[0].text_sha256 == deduplicated[1].text_sha256
    assert deduplicated[0].duplicate_group_id == deduplicated[1].duplicate_group_id
    assert deduplicated[0].duplicate_cluster_size == deduplicated[1].duplicate_cluster_size == 2
    assert deduplicated[0].near_duplicate_flag is False
    assert deduplicated[1].near_duplicate_flag is False
    assert deduplicated[0].canonical_doc_id == deduplicated[1].canonical_doc_id == first.doc_id


def test_near_duplicates_with_different_links_are_clustered() -> None:
    first = _build_cleaned_document(
        doc_id="vk_post:near-link-1",
        source_id="near-link-1",
        text="На улице Гагарина отключили воду, подробности: https://example.test/a",
        normalized_text="На улице Гагарина отключили воду подробности в группе района",
    )
    second = _build_cleaned_document(
        doc_id="vk_post:near-link-2",
        source_id="near-link-2",
        text="На улице Гагарина отключили воду, источник: https://example.test/b",
        normalized_text="На улице Гагарина отключили воду подробности в канале района",
    )

    deduplicated = deduplicate_documents([first, second])

    assert deduplicated[0].text_sha256 != deduplicated[1].text_sha256
    assert deduplicated[0].duplicate_group_id == deduplicated[1].duplicate_group_id
    assert deduplicated[0].near_duplicate_flag is True
    assert deduplicated[1].near_duplicate_flag is True
    assert deduplicated[0].duplicate_cluster_size == deduplicated[1].duplicate_cluster_size == 2


def test_near_duplicates_with_typo_are_clustered() -> None:
    first = _build_cleaned_document(
        doc_id="vk_post:near-typo-1",
        source_id="near-typo-1",
        normalized_text="На улице Гагарина отключили отопление до вечера",
    )
    second = _build_cleaned_document(
        doc_id="vk_post:near-typo-2",
        source_id="near-typo-2",
        normalized_text="На улице Гагарина отключили отополение до вечера",
    )

    deduplicated = deduplicate_documents([first, second])

    assert deduplicated[0].text_sha256 != deduplicated[1].text_sha256
    assert deduplicated[0].duplicate_group_id == deduplicated[1].duplicate_group_id
    assert deduplicated[0].near_duplicate_flag is True
    assert deduplicated[1].near_duplicate_flag is True
    assert deduplicated[0].canonical_doc_id == deduplicated[1].canonical_doc_id == first.doc_id


def test_similar_but_not_duplicate_documents_stay_in_separate_clusters() -> None:
    first = _build_cleaned_document(
        doc_id="vk_post:distinct-1",
        source_id="distinct-1",
        normalized_text="На улице Ленина отключили воду из-за аварии на трубе",
    )
    second = _build_cleaned_document(
        doc_id="vk_post:distinct-2",
        source_id="distinct-2",
        normalized_text="На улице Ленина завершили ремонт тротуара возле школы",
    )

    deduplicated = deduplicate_documents([first, second])

    assert deduplicated[0].duplicate_group_id != deduplicated[1].duplicate_group_id
    assert deduplicated[0].duplicate_cluster_size == 1
    assert deduplicated[1].duplicate_cluster_size == 1
    assert deduplicated[0].near_duplicate_flag is False
    assert deduplicated[1].near_duplicate_flag is False
    assert deduplicated[0].canonical_doc_id == first.doc_id
    assert deduplicated[1].canonical_doc_id == second.doc_id


def _build_cleaned_document(
    *,
    doc_id: str,
    source_id: str,
    normalized_text: str,
    text: str | None = None,
) -> CleanedDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    source_text = text or normalized_text
    return CleanedDocument(
        doc_id=doc_id,
        source_type=SourceType.VK_POST,
        source_id=source_id,
        parent_id=None,
        text=source_text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-1",
        is_official=False,
        reach=100,
        likes=2,
        reposts=0,
        comments_count=1,
        region_hint="Волгоград",
        geo_lat=None,
        geo_lon=None,
        raw_payload={"text": source_text},
        language="ru",
        language_confidence=0.98,
        is_supported_language=True,
        filter_status=FilterStatus.PASS,
        filter_reasons=(),
        quality_weight=1.0,
        normalized_text=normalized_text,
        token_count=len(normalized_text.split()),
        cleanup_flags=("whitespace_normalized",),
    )
