import uuid
from datetime import datetime, UTC

from schema import NormalizedDocument, SourceType, MediaType


def normalize_vk_post(raw_post: dict) -> NormalizedDocument:
    text = raw_post.get("text", "")
    if not text.strip():
        raise ValueError("Пустой text: документ не создаём")

    media_type = MediaType.TEXT

    attachments = raw_post.get("attachments", [])
    for attachment in attachments:
        attachment_type = attachment.get("type")

        if attachment_type == "photo":
            media_type = MediaType.PHOTO
            break
        if attachment_type == "video":
            media_type = MediaType.VIDEO
            break
        if attachment_type == "link":
            media_type = MediaType.LINK
            break

    geo_lat = None
    geo_lon = None
    region_hint = None

    geo = raw_post.get("geo")
    if geo:
        place = geo.get("place")
        if place:
            region_hint = place.get("title")

        coordinates = geo.get("coordinates")
        if isinstance(coordinates, str):
            parts = coordinates.replace(",", " ").split()
            if len(parts) >= 2:
                try:
                    geo_lat = float(parts[0])
                    geo_lon = float(parts[1])
                except ValueError:
                    pass

    return NormalizedDocument(
        doc_id=str(uuid.uuid4()),
        source_type=SourceType.VK_POST,
        source_id=f"{raw_post['owner_id']}_{raw_post['id']}",
        parent_id=None,
        text=text,
        media_type=media_type,
        created_at=datetime.fromtimestamp(raw_post["date"], UTC),
        collected_at=datetime.now(UTC),
        author_id=str(raw_post["from_id"]),
        is_official=False,
        reach=raw_post.get("views", {}).get("count", 0),
        likes=raw_post.get("likes", {}).get("count", 0),
        reposts=raw_post.get("reposts", {}).get("count", 0),
        comments_count=raw_post.get("comments", {}).get("count", 0),
        region_hint=region_hint,
        geo_lat=geo_lat,
        geo_lon=geo_lon,
        raw_payload=raw_post,
    )