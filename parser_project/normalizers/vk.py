from datetime import datetime, timezone

from schema import MediaType, RawMessage, SourceType


def build_vk_post_raw_message(raw_post: dict) -> RawMessage:
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

    source_id = f"{raw_post['owner_id']}_{raw_post['id']}"

    return RawMessage(
        source_type=SourceType.VK_POST,
        source_id=source_id,
        parent_id=None,
        text=text,
        media_type=media_type,
        created_at_utc=datetime.fromtimestamp(raw_post["date"], timezone.utc),
        collected_at=datetime.now(timezone.utc),
        author_id=str(raw_post["from_id"]),
        is_official=False,
        reach=raw_post.get("views", {}).get("count", 0),
        likes=raw_post.get("likes", {}).get("count", 0),
        reposts=raw_post.get("reposts", {}).get("count", 0),
        comments_count=raw_post.get("comments", {}).get("count", 0),
        raw_payload=raw_post,
    )


def build_vk_comment_raw_message(raw_comment: dict, parent_post: dict) -> RawMessage:
    text = raw_comment.get("text", "")
    if not text.strip():
        raise ValueError("Пустой text комментария: документ не создаём")

    owner_id = parent_post["owner_id"]
    post_id = parent_post["id"]
    comment_id = raw_comment["id"]

    source_id = f"{owner_id}_{post_id}_{comment_id}"
    parent_source_id = f"{owner_id}_{post_id}"

    return RawMessage(
        source_type=SourceType.VK_COMMENT,
        source_id=source_id,
        parent_id=parent_source_id,
        text=text,
        media_type=MediaType.TEXT,
        created_at_utc=datetime.fromtimestamp(raw_comment["date"], timezone.utc),
        collected_at=datetime.now(timezone.utc),
        author_id=str(raw_comment.get("from_id", "")),
        is_official=False,
        reach=0,
        likes=raw_comment.get("likes", {}).get("count", 0),
        reposts=0,
        comments_count=raw_comment.get("thread", {}).get("count", 0),
        raw_payload=raw_comment,
    )


normalize_vk_post = build_vk_post_raw_message
normalize_vk_comment = build_vk_comment_raw_message
