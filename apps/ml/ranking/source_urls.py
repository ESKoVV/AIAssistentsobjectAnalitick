from __future__ import annotations

from typing import Any


def extract_source_url(*, source_type: str, source_id: str, raw_payload: dict[str, Any]) -> str | None:
    payload_url = _find_url_candidate(raw_payload)
    if payload_url:
        return payload_url

    if source_type == "vk_post":
        owner_id, post_id = _split_source_id(source_id, expected_parts=2)
        if owner_id and post_id:
            return f"https://vk.com/wall{owner_id}_{post_id}"
    if source_type == "vk_comment":
        owner_id, post_id, comment_id = _split_source_id(source_id, expected_parts=3)
        if owner_id and post_id and comment_id:
            return f"https://vk.com/wall{owner_id}_{post_id}?reply={comment_id}"

    return None


def _split_source_id(source_id: str, *, expected_parts: int) -> tuple[str | None, ...]:
    parts = str(source_id).split("_")
    if len(parts) != expected_parts:
        return tuple(None for _ in range(expected_parts))
    return tuple(parts)


def _find_url_candidate(payload: dict[str, Any]) -> str | None:
    candidates = (
        "url",
        "link",
        "short_url",
        "source_url",
        "permalink",
        "canonical_url",
        "full_url",
    )
    for key in candidates:
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value

    attachment_link = _nested_lookup(payload, ("attachments", "0", "link", "url"))
    if isinstance(attachment_link, str) and attachment_link.startswith(("http://", "https://")):
        return attachment_link

    research_url = _nested_lookup(payload, ("research", "urls", "0"))
    if isinstance(research_url, str) and research_url.startswith(("http://", "https://")):
        return research_url

    return None


def _nested_lookup(payload: Any, path: tuple[str, ...]) -> Any:
    current = payload
    for key in path:
        if isinstance(current, list):
            if not key.isdigit():
                return None
            index = int(key)
            if index >= len(current):
                return None
            current = current[index]
            continue
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
