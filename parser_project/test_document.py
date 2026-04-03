from schema import NormalizedDocument
from datetime import datetime

doc = NormalizedDocument(
    doc_id="test-id",
    source_type="vk_post",
    source_id="123_456",
    parent_id=None,

    text="Тестовый пост",
    media_type="text",

    created_at=datetime.utcnow(),
    collected_at=datetime.utcnow(),

    author_id="user_1",
    is_official=False,

    reach=0,
    likes=0,
    reposts=0,
    comments_count=0,

    region_hint=None,
    geo_lat=None,
    geo_lon=None,

    raw_payload={}
)

print(doc)