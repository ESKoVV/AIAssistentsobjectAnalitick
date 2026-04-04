from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Mapping

from .schema import MediaType, NormalizedDocument, SourceType


SOURCE_NAME_ALIASES = {
    "vk": "vk",
    "vkontakte": "vk",
    "max": "max",
    "mailru_max": "max",
    "rss": "rss",
    "feed": "rss",
    "portal": "portal",
    "portals": "portal",
}


def normalize_document(
    raw_payload: Mapping[str, Any],
    source_config: Mapping[str, Any],
) -> NormalizedDocument:
    payload_snapshot = deepcopy(dict(raw_payload))
    source_name = _resolve_source_name(source_config)

    if source_name == "vk":
        return _normalize_vk(payload_snapshot, source_config)
    if source_name == "max":
        return _normalize_max(payload_snapshot, source_config)
    if source_name == "rss":
        return _normalize_rss(payload_snapshot, source_config)
    if source_name == "portal":
        return _normalize_portal(payload_snapshot, source_config)

    raise ValueError(f"Unsupported source kind: {source_name}")


def _normalize_vk(
    raw_payload: dict[str, Any],
    source_config: Mapping[str, Any],
) -> NormalizedDocument:
    is_comment = _resolve_entity_type(
        raw_payload,
        source_config,
        comment_markers=("post_id", "parents_stack", "reply_to_comment", "thread"),
    )
    owner_id = _stringify_id(
        _pick(raw_payload, "owner_id", "group_id", "peer_id", default=_config_value(source_config, "source_id")),
        fallback=f"vk:{_payload_fingerprint(raw_payload)}",
    )
    item_id = _stringify_id(
        _pick(raw_payload, "id", "comment_id", default=_payload_fingerprint(raw_payload)),
        fallback=_payload_fingerprint(raw_payload),
    )
    post_id_value = _pick(raw_payload, "post_id", "thread.post_id")
    post_id = _stringify_optional_id(post_id_value)
    if is_comment and post_id:
        source_id = _compose_identifier(owner_id, post_id, item_id)
        parent_id = _compose_identifier(owner_id, post_id)
        source_type = SourceType.VK_COMMENT
    else:
        source_id = _compose_identifier(owner_id, item_id)
        parent_id = _stringify_optional_id(_pick(raw_payload, "parent_id"))
        source_type = SourceType.VK_POST

    created_at = _resolve_datetime(
        raw_payload,
        source_config,
        payload_keys=("created_at", "published_at", "date"),
    )
    collected_at = _resolve_collected_at(raw_payload, source_config, created_at)

    return NormalizedDocument(
        doc_id=_build_doc_id(source_type, source_id),
        source_type=source_type,
        source_id=source_id,
        parent_id=parent_id,
        text=_normalize_text(_pick(raw_payload, "text")),
        media_type=_detect_media_type(
            attachments=_pick(raw_payload, "attachments", default=[]),
            fallback_link=_pick(raw_payload, "short_url", "url"),
        ),
        created_at=created_at,
        collected_at=collected_at,
        author_id=_stringify_id(
            _pick(raw_payload, "from_id", "author_id", "user_id", default=owner_id),
            fallback=owner_id,
        ),
        is_official=bool(_config_value(source_config, "is_official", default=False)),
        reach=_coerce_int(_pick(raw_payload, "views.count", "reach", default=0)),
        likes=_coerce_int(_pick(raw_payload, "likes.count", "likes", default=0)),
        reposts=_coerce_int(_pick(raw_payload, "reposts.count", "reposts", default=0)),
        comments_count=_coerce_int(_pick(raw_payload, "comments.count", "comments_count", default=0)),
        region_hint=_stringify_optional_id(
            _pick(raw_payload, "geo.place.title", "region_hint", default=_config_value(source_config, "region_hint", "default_region"))
        ),
        geo_lat=_coerce_float(_pick(raw_payload, "geo.coordinates.latitude", "geo.latitude", "lat", default=None)),
        geo_lon=_coerce_float(_pick(raw_payload, "geo.coordinates.longitude", "geo.longitude", "lon", default=None)),
        raw_payload=raw_payload,
    )


def _normalize_max(
    raw_payload: dict[str, Any],
    source_config: Mapping[str, Any],
) -> NormalizedDocument:
    is_comment = _resolve_entity_type(
        raw_payload,
        source_config,
        comment_markers=("post_id", "parent_post_id", "reply_to_id"),
    )
    container_id = _stringify_id(
        _pick(
            raw_payload,
            "channel.id",
            "community.id",
            "chat.id",
            "source.id",
            default=_config_value(source_config, "source_id"),
        ),
        fallback=f"max:{_payload_fingerprint(raw_payload)}",
    )
    item_id = _stringify_id(
        _pick(raw_payload, "id", "message_id", default=_payload_fingerprint(raw_payload)),
        fallback=_payload_fingerprint(raw_payload),
    )
    post_id = _stringify_optional_id(_pick(raw_payload, "post_id", "parent_post_id"))
    if is_comment and post_id:
        source_type = SourceType.MAX_COMMENT
        source_id = _compose_identifier(container_id, post_id, item_id)
        parent_id = _compose_identifier(container_id, post_id)
    else:
        source_type = SourceType.MAX_POST
        source_id = _compose_identifier(container_id, item_id)
        parent_id = _stringify_optional_id(_pick(raw_payload, "parent_id"))

    created_at = _resolve_datetime(
        raw_payload,
        source_config,
        payload_keys=("created_at", "published_at", "timestamp"),
    )
    collected_at = _resolve_collected_at(raw_payload, source_config, created_at)

    return NormalizedDocument(
        doc_id=_build_doc_id(source_type, source_id),
        source_type=source_type,
        source_id=source_id,
        parent_id=parent_id,
        text=_normalize_text(_pick(raw_payload, "message", "text", "body")),
        media_type=_detect_media_type(
            attachments=_pick(raw_payload, "attachments", "media", default=[]),
            fallback_link=_pick(raw_payload, "url", "link"),
        ),
        created_at=created_at,
        collected_at=collected_at,
        author_id=_stringify_id(
            _pick(raw_payload, "author.id", "author_id", "user.id", default=container_id),
            fallback=container_id,
        ),
        is_official=bool(_config_value(source_config, "is_official", default=False)),
        reach=_coerce_int(_pick(raw_payload, "metrics.reach", "reach", default=0)),
        likes=_coerce_int(_pick(raw_payload, "metrics.likes", "likes", default=0)),
        reposts=_coerce_int(_pick(raw_payload, "metrics.reposts", "reposts", default=0)),
        comments_count=_coerce_int(_pick(raw_payload, "metrics.comments", "comments_count", default=0)),
        region_hint=_stringify_optional_id(
            _pick(raw_payload, "channel.region", "region_hint", default=_config_value(source_config, "region_hint", "default_region"))
        ),
        geo_lat=_coerce_float(_pick(raw_payload, "location.lat", "geo.lat", "lat", default=None)),
        geo_lon=_coerce_float(_pick(raw_payload, "location.lon", "geo.lon", "lon", default=None)),
        raw_payload=raw_payload,
    )


def _normalize_rss(
    raw_payload: dict[str, Any],
    source_config: Mapping[str, Any],
) -> NormalizedDocument:
    source_id = _stringify_id(
        _pick(raw_payload, "guid", "id", "link", "source_id", default=_payload_fingerprint(raw_payload)),
        fallback=_payload_fingerprint(raw_payload),
    )
    created_at = _resolve_datetime(
        raw_payload,
        source_config,
        payload_keys=("published_at", "published", "created_at", "pub_date", "pubDate"),
    )
    collected_at = _resolve_collected_at(raw_payload, source_config, created_at)
    text = _compose_text(
        _pick(raw_payload, "title"),
        _pick(raw_payload, "summary", "description"),
        _pick(raw_payload, "content", "content_text"),
    )

    return NormalizedDocument(
        doc_id=_build_doc_id(SourceType.RSS_ARTICLE, source_id),
        source_type=SourceType.RSS_ARTICLE,
        source_id=source_id,
        parent_id=None,
        text=text,
        media_type=_detect_media_type(
            attachments=_pick(raw_payload, "enclosures", "media", default=[]),
            fallback_link=_pick(raw_payload, "link"),
        ),
        created_at=created_at,
        collected_at=collected_at,
        author_id=_stringify_id(
            _pick(raw_payload, "author.id", "author", "creator", default=_config_value(source_config, "feed_id", "source_id")),
            fallback=f"rss:{_config_value(source_config, 'feed_id', 'source_id', default='unknown')}",
        ),
        is_official=bool(_config_value(source_config, "is_official", default=False)),
        reach=_coerce_int(_pick(raw_payload, "metrics.reach", "reach", default=0)),
        likes=_coerce_int(_pick(raw_payload, "metrics.likes", "likes", default=0)),
        reposts=_coerce_int(_pick(raw_payload, "metrics.reposts", "reposts", default=0)),
        comments_count=_coerce_int(_pick(raw_payload, "metrics.comments", "comments_count", default=0)),
        region_hint=_stringify_optional_id(
            _pick(raw_payload, "region", "region_hint", default=_config_value(source_config, "region_hint", "default_region"))
        ),
        geo_lat=_coerce_float(_pick(raw_payload, "geo.lat", "latitude", default=None)),
        geo_lon=_coerce_float(_pick(raw_payload, "geo.lon", "longitude", default=None)),
        raw_payload=raw_payload,
    )


def _normalize_portal(
    raw_payload: dict[str, Any],
    source_config: Mapping[str, Any],
) -> NormalizedDocument:
    source_id = _stringify_id(
        _pick(raw_payload, "appeal_id", "request_number", "id", "source_id", default=_payload_fingerprint(raw_payload)),
        fallback=_payload_fingerprint(raw_payload),
    )
    created_at = _resolve_datetime(
        raw_payload,
        source_config,
        payload_keys=("created_at", "submitted_at", "published_at"),
    )
    collected_at = _resolve_collected_at(raw_payload, source_config, created_at)

    return NormalizedDocument(
        doc_id=_build_doc_id(SourceType.PORTAL_APPEAL, source_id),
        source_type=SourceType.PORTAL_APPEAL,
        source_id=source_id,
        parent_id=_stringify_optional_id(_pick(raw_payload, "parent_id")),
        text=_compose_text(
            _pick(raw_payload, "subject", "title"),
            _pick(raw_payload, "message", "text", "body"),
        ),
        media_type=_detect_media_type(
            attachments=_pick(raw_payload, "attachments", default=[]),
            fallback_link=_pick(raw_payload, "url", "link"),
        ),
        created_at=created_at,
        collected_at=collected_at,
        author_id=_stringify_id(
            _pick(raw_payload, "author.id", "user.id", "author_id", default="anonymous"),
            fallback="anonymous",
        ),
        is_official=bool(_config_value(source_config, "is_official", default=False)),
        reach=_coerce_int(_pick(raw_payload, "metrics.reach", "reach", default=0)),
        likes=_coerce_int(_pick(raw_payload, "metrics.likes", "likes", default=0)),
        reposts=_coerce_int(_pick(raw_payload, "metrics.reposts", "reposts", default=0)),
        comments_count=_coerce_int(_pick(raw_payload, "metrics.comments", "comments_count", default=0)),
        region_hint=_stringify_optional_id(
            _pick(raw_payload, "location.region", "region_hint", default=_config_value(source_config, "region_hint", "default_region"))
        ),
        geo_lat=_coerce_float(_pick(raw_payload, "location.lat", "geo.lat", "latitude", default=None)),
        geo_lon=_coerce_float(_pick(raw_payload, "location.lon", "geo.lon", "longitude", default=None)),
        raw_payload=raw_payload,
    )


def _resolve_source_name(source_config: Mapping[str, Any]) -> str:
    source_name = _config_value(source_config, "source", "source_name", "kind", "type")
    if source_name is None:
        raise ValueError("source_config must define source kind")
    normalized = str(source_name).strip().lower()
    return SOURCE_NAME_ALIASES.get(normalized, normalized)


def _resolve_entity_type(
    raw_payload: Mapping[str, Any],
    source_config: Mapping[str, Any],
    comment_markers: tuple[str, ...],
) -> bool:
    explicit_type = _config_value(source_config, "entity_type", "record_type", "object_type")
    if explicit_type is not None:
        return str(explicit_type).strip().lower() == "comment"
    return any(_pick(raw_payload, marker) is not None for marker in comment_markers)


def _resolve_datetime(
    raw_payload: Mapping[str, Any],
    source_config: Mapping[str, Any],
    payload_keys: tuple[str, ...],
) -> datetime:
    value = _pick(raw_payload, *payload_keys, default=_config_value(source_config, *payload_keys))
    if value is None:
        raise ValueError("Source payload must provide a creation timestamp")
    return _parse_datetime(value, source_config)


def _resolve_collected_at(
    raw_payload: Mapping[str, Any],
    source_config: Mapping[str, Any],
    created_at: datetime,
) -> datetime:
    value = _pick(
        raw_payload,
        "collected_at",
        "fetched_at",
        "ingested_at",
        default=_config_value(source_config, "collected_at", "fetched_at", "ingested_at"),
    )
    if value is None:
        return created_at
    return _parse_datetime(value, source_config)


def _parse_datetime(value: Any, source_config: Mapping[str, Any]) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(value, tz=timezone.utc)
    else:
        text = str(value).strip()
        if text.isdigit():
            parsed = datetime.fromtimestamp(int(text), tz=timezone.utc)
        else:
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            parsed = datetime.fromisoformat(text)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_timezone_from_config(source_config))
    return parsed.astimezone(timezone.utc)


def _timezone_from_config(source_config: Mapping[str, Any]) -> timezone:
    raw_timezone = _config_value(source_config, "timezone", "tz", "utc_offset", default="+00:00")
    text = str(raw_timezone).strip()
    if text.upper() == "UTC":
        return timezone.utc

    sign = -1 if text.startswith("-") else 1
    cleaned = text[1:] if text[:1] in {"+", "-"} else text
    hours_text, _, minutes_text = cleaned.partition(":")
    hours = int(hours_text or "0")
    minutes = int(minutes_text or "0")
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def _pick(data: Mapping[str, Any], *paths: str, default: Any = None) -> Any:
    for path in paths:
        current: Any = data
        found = True
        for part in path.split("."):
            if not isinstance(current, Mapping) or part not in current:
                found = False
                break
            current = current[part]
        if found:
            return current
    return default


def _config_value(source_config: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in source_config:
            return source_config[key]
    return default


def _build_doc_id(source_type: SourceType, source_id: str) -> str:
    return f"{source_type.value}:{source_id}"


def _compose_identifier(*parts: str) -> str:
    return "_".join(part for part in parts if part)


def _compose_text(*parts: Any) -> str:
    normalized_parts = [_normalize_text(part) for part in parts]
    return "\n\n".join(part for part in normalized_parts if part)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _stringify_id(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip()
    return text or fallback


def _stringify_optional_id(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip()
    return text or None


def _coerce_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return int(str(value).strip())


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).strip())


def _detect_media_type(attachments: Any, fallback_link: Any = None) -> MediaType:
    if isinstance(attachments, Mapping):
        attachments = [attachments]
    if isinstance(attachments, list):
        kinds = []
        for attachment in attachments:
            if not isinstance(attachment, Mapping):
                continue
            kind = _pick(attachment, "type", "kind", "media_type")
            if kind is not None:
                kinds.append(str(kind).strip().lower())
        if any("video" in kind for kind in kinds):
            return MediaType.VIDEO
        if any("photo" in kind or "image" in kind for kind in kinds):
            return MediaType.PHOTO
        if any("link" in kind for kind in kinds):
            return MediaType.LINK
    if fallback_link not in (None, ""):
        return MediaType.LINK
    return MediaType.TEXT


def _payload_fingerprint(raw_payload: Mapping[str, Any]) -> str:
    serialized = json.dumps(raw_payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
