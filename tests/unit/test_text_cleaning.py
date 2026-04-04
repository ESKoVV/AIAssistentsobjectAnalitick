from __future__ import annotations

from datetime import datetime, timezone

import pytest

from apps.preprocessing.cleaning import clean_text
from apps.preprocessing.filtering import FilterStatus, FilteredDocument
from apps.preprocessing.normalization import MediaType, SourceType


def test_urls_are_removed_from_normalized_text() -> None:
    cleaned = clean_text(
        _build_filtered_document("Подробности на https://example.test/news/1 и http://example.test/news/2"),
    )

    assert cleaned.normalized_text == "Подробности на и"
    assert cleaned.cleanup_flags == ("url_removed", "whitespace_normalized")


def test_mentions_are_replaced_with_user_token() -> None:
    cleaned = clean_text(_build_filtered_document("@city_admin ответил @resident_24 по обращению"))

    assert cleaned.normalized_text == "USER ответил USER по обращению"
    assert "mention_normalized" in cleaned.cleanup_flags


def test_emoji_are_converted_to_text() -> None:
    cleaned = clean_text(_build_filtered_document("На месте уже работает 🚒"))

    assert cleaned.normalized_text == "На месте уже работает fire engine"
    assert "emoji_demojized" in cleaned.cleanup_flags


def test_whitespace_is_normalized() -> None:
    cleaned = clean_text(_build_filtered_document("  Мусор   вывезли \n утром\tсегодня  "))

    assert cleaned.normalized_text == "Мусор вывезли утром сегодня"
    assert cleaned.cleanup_flags == ("whitespace_normalized",)
    assert cleaned.token_count == 4


def test_different_links_produce_the_same_normalized_text() -> None:
    first = clean_text(_build_filtered_document("Свет вернули, детали: https://example.test/a"))
    second = clean_text(_build_filtered_document("Свет вернули, детали: https://another.test/b"))

    assert first.normalized_text == second.normalized_text == "Свет вернули, детали:"
    assert first.token_count == second.token_count


def test_drop_documents_are_rejected() -> None:
    with pytest.raises(ValueError, match="pass/review"):
        clean_text(_build_filtered_document("Слишком коротко", filter_status=FilterStatus.DROP))


def _build_filtered_document(
    text: str,
    *,
    filter_status: FilterStatus = FilterStatus.PASS,
) -> FilteredDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return FilteredDocument(
        doc_id="vk_post:cleaning-unit",
        source_type=SourceType.VK_POST,
        source_id="cleaning-unit",
        parent_id=None,
        text=text,
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
        raw_payload={"text": text},
        language="ru",
        language_confidence=0.98,
        is_supported_language=True,
        filter_status=filter_status,
        filter_reasons=("short_noise",) if filter_status is FilterStatus.DROP else (),
        quality_weight=0.0 if filter_status is FilterStatus.DROP else 1.0,
    )
