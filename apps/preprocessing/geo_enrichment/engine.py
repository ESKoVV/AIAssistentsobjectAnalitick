from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from apps.preprocessing.deduplication import DeduplicatedDocument

from .schema import GeoEnrichedDocument


SUPPORTED_TEXT_NER_LANGUAGE = "ru"
EXPLICIT_GEO_SOURCE = "explicit_geo"
TEXT_TOPONYM_SOURCE = "text_toponym"
SOURCE_METADATA_GEO_SOURCE = "source_metadata"
SOURCE_DEFAULT_GEO_SOURCE = "source_default"
UNRESOLVED_GEO_SOURCE = "unresolved"

EXPLICIT_GEO_CONFIDENCE = 1.0
TEXT_TOPONYM_CONFIDENCE = 0.85
SOURCE_METADATA_CONFIDENCE = 0.7
SOURCE_DEFAULT_CONFIDENCE = 0.4
UNRESOLVED_GEO_CONFIDENCE = 0.0


@dataclass(frozen=True, slots=True)
class _GeoCandidate:
    region_id: str | None
    municipality_id: str | None
    geo_confidence: float
    geo_source: str
    geo_evidence: tuple[str, ...]


def enrich_geo(
    document: DeduplicatedDocument,
    source_config: Mapping[str, Any] | None = None,
) -> GeoEnrichedDocument:
    config = dict(source_config or {})
    skipped_text_ner_evidence = _build_text_ner_skip_evidence(document)

    candidate = _resolve_explicit_geo(document, config)
    if candidate is None and not skipped_text_ner_evidence:
        candidate = _resolve_text_toponym(document, config)
    if candidate is None:
        candidate = _resolve_source_metadata(config)
    if candidate is None:
        candidate = _resolve_source_default(config)
    if candidate is None:
        candidate = _build_unresolved_candidate()

    return GeoEnrichedDocument(
        **asdict(document),
        region_id=candidate.region_id,
        municipality_id=candidate.municipality_id,
        geo_confidence=candidate.geo_confidence,
        geo_source=candidate.geo_source,
        geo_evidence=skipped_text_ner_evidence + candidate.geo_evidence,
    )


def _build_text_ner_skip_evidence(document: DeduplicatedDocument) -> tuple[str, ...]:
    normalized_language = (document.language or "").strip().lower()
    if document.is_supported_language and normalized_language == SUPPORTED_TEXT_NER_LANGUAGE:
        return ()

    return (f"text_toponym_skipped:unsupported_language:{normalized_language or 'unknown'}",)


def _resolve_explicit_geo(
    document: DeduplicatedDocument,
    source_config: Mapping[str, Any],
) -> _GeoCandidate | None:
    if document.geo_lat is None or document.geo_lon is None:
        return None

    for geo_entry in _iter_explicit_geo_entries(source_config):
        if _coordinates_match_entry(document.geo_lat, document.geo_lon, geo_entry):
            label = _string_value(geo_entry, "label") or _string_value(geo_entry, "name") or "unnamed_area"
            return _GeoCandidate(
                region_id=_string_value(geo_entry, "region_id"),
                municipality_id=_string_value(geo_entry, "municipality_id"),
                geo_confidence=EXPLICIT_GEO_CONFIDENCE,
                geo_source=EXPLICIT_GEO_SOURCE,
                geo_evidence=(
                    f"selected_source:{EXPLICIT_GEO_SOURCE}",
                    f"coordinates:{document.geo_lat:.5f},{document.geo_lon:.5f}",
                    f"matched_area:{label}",
                ),
            )

    return None


def _resolve_text_toponym(
    document: DeduplicatedDocument,
    source_config: Mapping[str, Any],
) -> _GeoCandidate | None:
    text = " ".join((document.normalized_text or document.text or "").casefold().split())
    if not text:
        return None

    for alias, entry in _iter_text_toponym_entries(source_config):
        if _contains_alias(text, alias.casefold()):
            return _GeoCandidate(
                region_id=_string_value(entry, "region_id"),
                municipality_id=_string_value(entry, "municipality_id"),
                geo_confidence=_float_value(entry, "confidence", default=TEXT_TOPONYM_CONFIDENCE),
                geo_source=TEXT_TOPONYM_SOURCE,
                geo_evidence=(
                    f"selected_source:{TEXT_TOPONYM_SOURCE}",
                    f"matched_toponym:{alias}",
                    f"resolved_region:{_string_value(entry, 'region_id')}",
                ),
            )

    return None


def _resolve_source_metadata(source_config: Mapping[str, Any]) -> _GeoCandidate | None:
    metadata_geo = _mapping_value(source_config, "metadata_geo", "channel_geo", "source_metadata_geo")
    if metadata_geo is None:
        return None

    region_id = _string_value(metadata_geo, "region_id")
    municipality_id = _string_value(metadata_geo, "municipality_id")
    if region_id is None and municipality_id is None:
        return None

    label = _string_value(metadata_geo, "label") or _string_value(metadata_geo, "source") or "channel_metadata"
    return _GeoCandidate(
        region_id=region_id,
        municipality_id=municipality_id,
        geo_confidence=_float_value(metadata_geo, "confidence", default=SOURCE_METADATA_CONFIDENCE),
        geo_source=SOURCE_METADATA_GEO_SOURCE,
        geo_evidence=(
            f"selected_source:{SOURCE_METADATA_GEO_SOURCE}",
            f"metadata_label:{label}",
            f"resolved_region:{region_id}",
        ),
    )


def _resolve_source_default(source_config: Mapping[str, Any]) -> _GeoCandidate | None:
    default_geo = _mapping_value(source_config, "default_geo")
    if default_geo is not None:
        region_id = _string_value(default_geo, "region_id")
        municipality_id = _string_value(default_geo, "municipality_id")
        confidence = _float_value(default_geo, "confidence", default=SOURCE_DEFAULT_CONFIDENCE)
        label = _string_value(default_geo, "label") or "source_default"
    else:
        region_id = _string_value(source_config, "default_region_id")
        municipality_id = _string_value(source_config, "default_municipality_id")
        confidence = _float_value(source_config, "default_geo_confidence", default=SOURCE_DEFAULT_CONFIDENCE)
        label = _string_value(source_config, "default_region_label") or "source_default"

    if region_id is None and municipality_id is None:
        return None

    return _GeoCandidate(
        region_id=region_id,
        municipality_id=municipality_id,
        geo_confidence=confidence,
        geo_source=SOURCE_DEFAULT_GEO_SOURCE,
        geo_evidence=(
            f"selected_source:{SOURCE_DEFAULT_GEO_SOURCE}",
            f"default_label:{label}",
            f"resolved_region:{region_id}",
        ),
    )


def _build_unresolved_candidate() -> _GeoCandidate:
    return _GeoCandidate(
        region_id=None,
        municipality_id=None,
        geo_confidence=UNRESOLVED_GEO_CONFIDENCE,
        geo_source=UNRESOLVED_GEO_SOURCE,
        geo_evidence=("selected_source:unresolved", "geo_unresolved:no_matching_source"),
    )


def _iter_explicit_geo_entries(source_config: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    entries = _sequence_value(source_config, "explicit_geo_regions", "geotag_regions", "geo_regions")
    normalized_entries: list[Mapping[str, Any]] = []
    for entry in entries:
        if isinstance(entry, Mapping):
            normalized_entries.append(entry)
    return tuple(normalized_entries)


def _iter_text_toponym_entries(
    source_config: Mapping[str, Any],
) -> tuple[tuple[str, Mapping[str, Any]], ...]:
    toponym_index = source_config.get("toponym_index")
    entries: list[tuple[str, Mapping[str, Any]]] = []

    if isinstance(toponym_index, Mapping):
        for alias, entry in toponym_index.items():
            if isinstance(alias, str) and isinstance(entry, Mapping):
                entries.append((alias, entry))

    for item in _sequence_value(source_config, "toponyms", "text_toponyms"):
        if not isinstance(item, Mapping):
            continue
        aliases = item.get("aliases")
        if isinstance(aliases, str):
            aliases = [aliases]
        if not isinstance(aliases, (list, tuple)):
            continue
        for alias in aliases:
            if isinstance(alias, str):
                entries.append((alias, item))

    entries.sort(key=lambda pair: len(pair[0]), reverse=True)
    return tuple(entries)


def _coordinates_match_entry(latitude: float, longitude: float, geo_entry: Mapping[str, Any]) -> bool:
    lat_min = _float_value(geo_entry, "lat_min")
    lat_max = _float_value(geo_entry, "lat_max")
    lon_min = _float_value(geo_entry, "lon_min")
    lon_max = _float_value(geo_entry, "lon_max")
    if None in {lat_min, lat_max, lon_min, lon_max}:
        return False

    return lat_min <= latitude <= lat_max and lon_min <= longitude <= lon_max


def _contains_alias(text: str, alias: str) -> bool:
    escaped_alias = re.escape(alias)
    pattern = rf"(?<!\w){escaped_alias}(?!\w)"
    return re.search(pattern, text) is not None


def _mapping_value(mapping: Mapping[str, Any], *keys: str) -> Mapping[str, Any] | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _sequence_value(mapping: Mapping[str, Any], *keys: str) -> tuple[Any, ...]:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, (list, tuple)):
            return tuple(value)
    return ()


def _string_value(mapping: Mapping[str, Any], key: str) -> str | None:
    value = mapping.get(key)
    if value is None:
        return None
    normalized_value = str(value).strip()
    return normalized_value or None


def _float_value(mapping: Mapping[str, Any], key: str, default: float | None = None) -> float | None:
    value = mapping.get(key, default)
    if value is None:
        return None
    return float(value)
