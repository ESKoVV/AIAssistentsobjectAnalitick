import uuid
from datetime import datetime, timezone

from region_extractor import extract_geo, extract_region_hint
from schema import MediaType, NormalizedDocument, SourceType


def _stable_doc_id(source_type: SourceType, source_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_type.value}:{source_id}"))


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

    geo_lat, geo_lon = extract_geo(raw_post)

    group_name = raw_post.get("signer_name") or raw_post.get("group", {}).get("name") or ""
    text_for_region = "\n".join(part for part in (text, group_name) if part)
    region_hint = extract_region_hint(text_for_region, raw_post)

    return NormalizedDocument(
        doc_id=str(uuid.uuid4()),
        source_type=SourceType.VK_POST,
        source_id=f"{raw_post['owner_id']}_{raw_post['id']}",
        parent_id=None,
        text=text,
        media_type=media_type,
        created_at=datetime.fromtimestamp(raw_post["date"], timezone.utc),
        collected_at=datetime.now(timezone.utc),
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


def normalize_vk_comment(raw_comment: dict, parent_post: dict) -> NormalizedDocument:
    text = raw_comment.get("text", "")
    if not text.strip():
        raise ValueError("Пустой text комментария: документ не создаём")

    owner_id = parent_post["owner_id"]
    post_id = parent_post["id"]
    comment_id = raw_comment["id"]

    source_id = f"{owner_id}_{post_id}_{comment_id}"
    parent_source_id = f"{owner_id}_{post_id}"
    geo_lat, geo_lon = extract_geo(raw_comment)

    return NormalizedDocument(
        doc_id=_stable_doc_id(SourceType.VK_COMMENT, source_id),
        source_type=SourceType.VK_COMMENT,
        source_id=source_id,
        parent_id=parent_source_id,
        text=text,
        media_type=MediaType.TEXT,
        created_at=datetime.fromtimestamp(raw_comment["date"], timezone.utc),
        collected_at=datetime.now(timezone.utc),
        author_id=str(raw_comment.get("from_id", "")),
        is_official=False,
        reach=0,
        likes=raw_comment.get("likes", {}).get("count", 0),
        reposts=0,
        comments_count=raw_comment.get("thread", {}).get("count", 0),
        region_hint=extract_region_hint(text, raw_comment),
        geo_lat=geo_lat,
        geo_lon=geo_lon,
        raw_payload=raw_comment,
    )
