from __future__ import annotations

from datetime import datetime, timezone

from apps.preprocessing.deduplication import DeduplicatedDocument
from apps.preprocessing.filtering import FilterStatus
from apps.preprocessing.geo_enrichment import (
    EXPLICIT_GEO_SOURCE,
    SOURCE_DEFAULT_GEO_SOURCE,
    SOURCE_METADATA_GEO_SOURCE,
    TEXT_TOPONYM_SOURCE,
    enrich_geo,
)
from apps.preprocessing.normalization import MediaType, SourceType


def test_explicit_geotag_has_highest_priority() -> None:
    document = _build_deduplicated_document(
        text="Волгоград снова пишет про отключение воды",
        normalized_text="Волгоград снова пишет про отключение воды",
        geo_lat=48.795,
        geo_lon=44.799,
    )

    enriched = enrich_geo(document, _build_source_config())

    assert enriched.region_id == "ru-vgg"
    assert enriched.municipality_id == "volzhsky"
    assert enriched.geo_source == EXPLICIT_GEO_SOURCE


def test_text_toponym_is_used_when_explicit_geotag_is_missing() -> None:
    document = _build_deduplicated_document(
        text="В Волжский вернули освещение во дворах",
        normalized_text="В Волжский вернули освещение во дворах",
    )

    enriched = enrich_geo(document, _build_source_config())

    assert enriched.region_id == "ru-vgg"
    assert enriched.municipality_id == "volzhsky"
    assert enriched.geo_source == TEXT_TOPONYM_SOURCE
    assert "matched_toponym:волжский" in enriched.geo_evidence


def test_channel_metadata_is_used_when_text_has_no_toponym() -> None:
    document = _build_deduplicated_document(
        text="Опубликовали график подвоза воды на сегодня",
        normalized_text="Опубликовали график подвоза воды на сегодня",
    )

    enriched = enrich_geo(document, _build_source_config())

    assert enriched.region_id == "ru-ast"
    assert enriched.municipality_id == "astrakhan"
    assert enriched.geo_source == SOURCE_METADATA_GEO_SOURCE
    assert "metadata_label:channel_profile" in enriched.geo_evidence


def test_default_region_is_used_as_last_fallback() -> None:
    document = _build_deduplicated_document(
        text="Без адреса и без локальных подсказок",
        normalized_text="Без адреса и без локальных подсказок",
    )

    enriched = enrich_geo(
        document,
        {
            "default_geo": {
                "region_id": "ru-ros",
                "municipality_id": "rostov-na-donu",
                "label": "source_default",
            },
        },
    )

    assert enriched.region_id == "ru-ros"
    assert enriched.municipality_id == "rostov-na-donu"
    assert enriched.geo_source == SOURCE_DEFAULT_GEO_SOURCE
    assert "default_label:source_default" in enriched.geo_evidence


def test_non_ru_document_skips_text_ner_and_falls_back_to_metadata() -> None:
    document = _build_deduplicated_document(
        text="Волгоградта су беру қалпына келтірілді",
        normalized_text="Волгоградта су беру қалпына келтірілді",
        language="kk",
        is_supported_language=False,
    )

    enriched = enrich_geo(document, _build_source_config())

    assert enriched.region_id == "ru-ast"
    assert enriched.municipality_id == "astrakhan"
    assert enriched.geo_source == SOURCE_METADATA_GEO_SOURCE
    assert "text_toponym_skipped:unsupported_language:kk" in enriched.geo_evidence
    assert all(not item.startswith("matched_toponym:") for item in enriched.geo_evidence)


def _build_source_config() -> dict[str, object]:
    return {
        "explicit_geo_regions": [
            {
                "label": "Волжский",
                "region_id": "ru-vgg",
                "municipality_id": "volzhsky",
                "lat_min": 48.700,
                "lat_max": 48.850,
                "lon_min": 44.700,
                "lon_max": 44.900,
            },
        ],
        "toponym_index": {
            "волжский": {
                "region_id": "ru-vgg",
                "municipality_id": "volzhsky",
            },
            "волгоград": {
                "region_id": "ru-vgg",
                "municipality_id": "volgograd",
            },
        },
        "metadata_geo": {
            "region_id": "ru-ast",
            "municipality_id": "astrakhan",
            "label": "channel_profile",
        },
        "default_geo": {
            "region_id": "ru-ros",
            "municipality_id": "rostov-na-donu",
            "label": "source_default",
        },
    }


def _build_deduplicated_document(
    *,
    text: str,
    normalized_text: str,
    geo_lat: float | None = None,
    geo_lon: float | None = None,
    language: str = "ru",
    is_supported_language: bool = True,
) -> DeduplicatedDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return DeduplicatedDocument(
        doc_id="vk_post:geo-unit",
        source_type=SourceType.VK_POST,
        source_id="geo-unit",
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-geo",
        is_official=False,
        reach=145,
        likes=5,
        reposts=1,
        comments_count=2,
        region_hint="Волгоградская область",
        geo_lat=geo_lat,
        geo_lon=geo_lon,
        raw_payload={"text": text},
        language=language,
        language_confidence=0.99 if language == "ru" else 0.95,
        is_supported_language=is_supported_language,
        filter_status=FilterStatus.PASS,
        filter_reasons=(),
        quality_weight=1.0,
        normalized_text=normalized_text,
        token_count=len(normalized_text.split()),
        cleanup_flags=("whitespace_normalized",),
        text_sha256="sha256-geo-unit",
        duplicate_group_id="dup:vk_post:geo-unit",
        near_duplicate_flag=False,
        duplicate_cluster_size=1,
        canonical_doc_id="vk_post:geo-unit",
    )
