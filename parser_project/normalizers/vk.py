import uuid
from datetime import datetime, timezone
from schema import NormalizedDocument, SourceType, MediaType


def normalize_vk_post(raw_post: dict) -> NormalizedDocument:
    return NormalizedDocument(
        doc_id=str(uuid.uuid4()),
        source_type=SourceType.VK_POST,
        source_id=f"{raw_post['owner_id']}_{raw_post['id']}",
        parent_id=None,

        text=raw_post.get("text", ""),
        media_type=MediaType.TEXT,

        created_at=datetime.fromtimestamp(raw_post["date"], tz=timezone.utc),
        collected_at=datetime.now(timezone.utc),

        author_id=str(raw_post["from_id"]),
        is_official=False,

        reach=raw_post.get("views", {}).get("count", 0),
        likes=raw_post.get("likes", {}).get("count", 0),
        reposts=raw_post.get("reposts", {}).get("count", 0),
        comments_count=raw_post.get("comments", {}).get("count", 0),

        region_hint=None,
        geo_lat=None,
        geo_lon=None,

        raw_payload=raw_post
    )