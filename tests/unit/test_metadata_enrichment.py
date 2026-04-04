from __future__ import annotations

from dataclasses import asdict

from apps.preprocessing.enrichment import DEFAULT_METADATA_VERSION, enrich_metadata
from apps.preprocessing.geo_enrichment import GeoEnrichedDocument
from apps.preprocessing.normalization import SourceType
from tests.helpers import build_enriched_document


def _build_geo_document(*, doc_id: str) -> GeoEnrichedDocument:
    enriched = build_enriched_document(doc_id=doc_id)
    payload = asdict(enriched)
    payload.pop("engagement")
    payload.pop("metadata_version")
    return GeoEnrichedDocument(**payload)


def test_enrich_metadata_uses_official_registry_and_engagement_formula() -> None:
    base = _build_geo_document(doc_id="vk_post:meta-1")
    base.is_official = False
    base.likes = 12
    base.reposts = 3
    base.comments_count = 4

    enriched = enrich_metadata(
        base,
        official_registry={(SourceType.VK_POST.value, base.source_id)},
    )

    assert enriched.is_official is True
    assert enriched.engagement == 19
    assert enriched.metadata_version == DEFAULT_METADATA_VERSION


def test_enrich_metadata_preserves_reach_snapshot_and_media_type() -> None:
    base = _build_geo_document(doc_id="vk_post:meta-2")
    original_reach = base.reach
    original_media_type = base.media_type

    enriched = enrich_metadata(base, source_config={"is_official": False})

    assert enriched.reach == original_reach
    assert enriched.media_type == original_media_type
