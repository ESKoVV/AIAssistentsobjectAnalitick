from __future__ import annotations

import re
from typing import Any

_DEFAULT_REGION_PATTERNS: dict[str, tuple[str, ...]] = {
    "Москва": (r"\bмоскв[а-яё-]*\b", r"\bмосковск(?:ая|ой|ую)\s+област[ьи]\b"),
    "Санкт-Петербург": (r"\bсанкт[-\s]?петербург[а-яё-]*\b", r"\bпитер[аеуы]?\b"),
    "Ростовская область": (r"\bростов(?:-на-дону)?[а-яё-]*\b", r"\bростовск(?:ая|ой|ую)\s+област[ьи]\b"),
    "Волгоградская область": (r"\bволгоград[а-яё-]*\b", r"\bволгоградск(?:ая|ой|ую)\s+област[ьи]\b"),
    "Краснодарский край": (r"\bкраснодар[а-яё-]*\b", r"\bкраснодарск(?:ий|ого|ом)\s+кра[йяе]\b"),
    "Нижегородская область": (r"\bнижн(?:ий|его)\s+новгород[аеуы]?\b", r"\bнижегородск(?:ая|ой|ую)\s+област[ьи]\b"),
}


def _safe_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _deep_get(payload: dict[str, Any], path: str) -> Any:
    current: Any = payload
    for chunk in path.split("."):
        if not isinstance(current, dict) or chunk not in current:
            return None
        current = current[chunk]
    return current


def _iter_text_candidates(text: str, raw_payload: dict[str, Any]) -> list[str]:
    candidates = [text or ""]

    metadata_paths = (
        "geo.place.title",
        "geo.place.city",
        "geo.place.country",
        "place.title",
        "location.title",
        "location.region",
        "location.city",
        "group.name",
        "group.screen_name",
        "author",
        "source.title",
        "title",
        "summary",
        "description",
    )

    for path in metadata_paths:
        value = _deep_get(raw_payload, path)
        if isinstance(value, (str, int, float)):
            candidates.append(_safe_to_text(value))

    return [item for item in candidates if item]


def extract_region_hint(text: str, raw_payload: dict[str, Any]) -> str | None:
    explicit_region = _deep_get(raw_payload, "region_hint") or _deep_get(raw_payload, "region")
    if isinstance(explicit_region, str) and explicit_region.strip():
        return explicit_region.strip()

    for candidate in _iter_text_candidates(text, raw_payload):
        normalized_candidate = candidate.lower()
        for region_name, patterns in _DEFAULT_REGION_PATTERNS.items():
            if any(re.search(pattern, normalized_candidate, flags=re.IGNORECASE) for pattern in patterns):
                return region_name

    return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None

    return None


def _valid_geo(lat: float | None, lon: float | None) -> bool:
    return lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180


def extract_geo(raw_payload: dict[str, Any]) -> tuple[float | None, float | None]:
    path_pairs = (
        ("geo.coordinates.latitude", "geo.coordinates.longitude"),
        ("geo.lat", "geo.lon"),
        ("geo.latitude", "geo.longitude"),
        ("location.lat", "location.lon"),
        ("location.latitude", "location.longitude"),
        ("lat", "lon"),
        ("latitude", "longitude"),
    )

    for lat_path, lon_path in path_pairs:
        lat = _coerce_float(_deep_get(raw_payload, lat_path))
        lon = _coerce_float(_deep_get(raw_payload, lon_path))
        if _valid_geo(lat, lon):
            return lat, lon

    coordinates = _deep_get(raw_payload, "geo.coordinates")
    if isinstance(coordinates, str):
        parts = [part for part in coordinates.replace(";", " ").replace(",", " ").split() if part]
        if len(parts) >= 2:
            lat = _coerce_float(parts[0])
            lon = _coerce_float(parts[1])
            if _valid_geo(lat, lon):
                return lat, lon

    return None, None
