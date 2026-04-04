from __future__ import annotations

from apps.preprocessing.cleaning import clean_text
from apps.preprocessing.deduplication import deduplicate_documents
from apps.preprocessing.enrichment import enrich_metadata
from apps.preprocessing.filtering import FilterStatus, filter_content
from apps.preprocessing.geo_enrichment import enrich_geo
from apps.preprocessing.language import annotate_language
from apps.preprocessing.normalization import SourceType, normalize_document


def _detector_ru(_: str) -> tuple[str, float]:
    return "ru", 0.99


def _source_config() -> dict[str, object]:
    return {
        "source": "vk",
        "source_id": "wall-1",
        "region_hint": "Волгоград",
        "toponym_index": {
            "волгоград": {
                "region_id": "volgograd-oblast",
                "municipality_id": "volgograd",
                "confidence": 0.88,
            }
        },
        "default_geo": {"region_id": "fallback-region"},
    }


def test_pipeline_1_to_7_returns_valid_enriched_document() -> None:
    raw = {
        "id": 1001,
        "owner_id": -10,
        "from_id": -10,
        "text": "Волгоград: во дворе нет горячей воды",
        "date": 1_712_212_800,
        "likes": {"count": 7},
        "reposts": {"count": 2},
        "comments": {"count": 3},
    }

    normalized = normalize_document(raw, _source_config())
    annotated = annotate_language(normalized, detector=_detector_ru)
    filtered = filter_content(annotated)
    cleaned = clean_text(filtered)
    deduplicated = deduplicate_documents([cleaned])[0]
    geo = enrich_geo(deduplicated, _source_config())
    enriched = enrich_metadata(
        geo,
        official_registry={(SourceType.VK_POST.value, normalized.source_id)},
    )

    assert enriched.doc_id == normalized.doc_id
    assert enriched.filter_status is FilterStatus.PASS
    assert enriched.region_id == "volgograd-oblast"
    assert enriched.engagement == enriched.likes + enriched.reposts + enriched.comments_count
    assert enriched.metadata_version


def test_drop_document_is_not_sent_to_downstream_semantic_stages() -> None:
    raw = {
        "id": 1002,
        "owner_id": -10,
        "from_id": 77,
        "text": "купи подписчиков #реклама",
        "date": 1_712_212_900,
    }

    normalized = normalize_document(raw, _source_config())
    annotated = annotate_language(normalized, detector=_detector_ru)
    filtered = filter_content(annotated)

    assert filtered.filter_status is FilterStatus.DROP

    forwarded_for_semantic = []
    if filtered.filter_status is not FilterStatus.DROP:
        cleaned = clean_text(filtered)
        deduplicated = deduplicate_documents([cleaned])[0]
        geo = enrich_geo(deduplicated, _source_config())
        forwarded_for_semantic.append(enrich_metadata(geo))

    assert forwarded_for_semantic == []
