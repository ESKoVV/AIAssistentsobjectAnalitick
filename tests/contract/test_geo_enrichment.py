from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from apps.preprocessing.deduplication import DeduplicatedDocument
from apps.preprocessing.filtering import FilterStatus
from apps.preprocessing.geo_enrichment import (
    EXPLICIT_GEO_SOURCE,
    GeoEnrichedDocument,
    enrich_geo,
)
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
    "normalized_text",
    "token_count",
    "cleanup_flags",
    "text_sha256",
    "duplicate_group_id",
    "near_duplicate_flag",
    "duplicate_cluster_size",
    "canonical_doc_id",
    "region_id",
    "municipality_id",
    "geo_confidence",
    "geo_source",
    "geo_evidence",
}


def test_final_geo_uses_source_priority_and_geo_evidence_explains_choice() -> None:
    document = _build_deduplicated_document(
        text="Жители Волгограда жалуются на перебои с водой",
        normalized_text="Жители Волгограда жалуются на перебои с водой",
        geo_lat=48.795,
        geo_lon=44.799,
    )
    source_config = {
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
            "волгограда": {
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

    enriched = enrich_geo(document, source_config)

    assert isinstance(enriched, GeoEnrichedDocument)
    assert set(asdict(enriched).keys()) == EXPECTED_FIELDS
    assert enriched.region_id == "ru-vgg"
    assert enriched.municipality_id == "volzhsky"
    assert enriched.geo_source == EXPLICIT_GEO_SOURCE
    assert enriched.geo_confidence == 1.0
    assert "selected_source:explicit_geo" in enriched.geo_evidence
    assert "matched_area:Волжский" in enriched.geo_evidence
    assert any(item.startswith("coordinates:48.79500,44.79900") for item in enriched.geo_evidence)
    assert asdict(document) == {
        key: value
        for key, value in asdict(enriched).items()
        if key in asdict(document)
    }


def _build_deduplicated_document(
    *,
    text: str,
    normalized_text: str,
    geo_lat: float | None = None,
    geo_lon: float | None = None,
) -> DeduplicatedDocument:
    timestamp = datetime(2026, 4, 2, 9, 0, tzinfo=timezone.utc)
    return DeduplicatedDocument(
        doc_id="vk_post:geo-contract",
        source_type=SourceType.VK_POST,
        source_id="geo-contract",
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=timestamp,
        collected_at=timestamp,
        author_id="author-geo",
        is_official=False,
        reach=380,
        likes=11,
        reposts=2,
        comments_count=4,
        region_hint="Волгоградская область",
        geo_lat=geo_lat,
        geo_lon=geo_lon,
        raw_payload={"text": text},
        language="ru",
        language_confidence=0.99,
        is_supported_language=True,
        filter_status=FilterStatus.PASS,
        filter_reasons=(),
        quality_weight=1.0,
        normalized_text=normalized_text,
        token_count=len(normalized_text.split()),
        cleanup_flags=("whitespace_normalized",),
        text_sha256="sha256-geo-contract",
        duplicate_group_id="dup:vk_post:geo-contract",
        near_duplicate_flag=False,
        duplicate_cluster_size=1,
        canonical_doc_id="vk_post:geo-contract",
    )
