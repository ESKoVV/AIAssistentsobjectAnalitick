import sys
import types
from datetime import datetime, timezone

sys.modules.setdefault("kafka_producer", types.SimpleNamespace(send_document=lambda *args, **kwargs: None))

from collect_max_messages import normalize_max_comment, normalize_max_post
from collect_portal_appeals import RawPortalAppeal, normalize_portal_appeal
from collect_rss import normalize_rss_entry
from id_builders import build_max_post_doc_id, build_vk_post_doc_id
from normalizers.vk import normalize_vk_comment, normalize_vk_post


def test_doc_id_is_deterministic_for_same_payload_across_sources() -> None:
    raw_vk_post = {
        "id": 101,
        "owner_id": 42,
        "from_id": 42,
        "date": 1712150000,
        "text": "Повторный запуск должен дать тот же ID",
        "views": {"count": 10},
        "likes": {"count": 1},
        "reposts": {"count": 0},
        "comments": {"count": 0},
    }
    vk_doc_first = normalize_vk_post(raw_vk_post)
    vk_doc_second = normalize_vk_post(raw_vk_post)
    assert vk_doc_first.doc_id == vk_doc_second.doc_id

    rss_entry = {
        "id": "article-1",
        "title": "Новость",
        "summary": "Краткий текст",
        "published": "Wed, 03 Apr 2024 10:00:00 +0300",
    }
    rss_doc_first = normalize_rss_entry("https://example.com/rss", rss_entry)
    rss_doc_second = normalize_rss_entry("https://example.com/rss", rss_entry)
    assert rss_doc_first.doc_id == rss_doc_second.doc_id

    appeal = RawPortalAppeal(
        appeal_id="appeal-77",
        text="Не работает освещение",
        created_at=datetime(2024, 4, 3, 8, 0, tzinfo=timezone.utc),
        region_hint="Ростовская область",
        author_id="citizen-1",
        raw_payload={"id": "appeal-77", "text": "Не работает освещение"},
    )
    portal_doc_first = normalize_portal_appeal(appeal)
    portal_doc_second = normalize_portal_appeal(appeal)
    assert portal_doc_first.doc_id == portal_doc_second.doc_id

    channel = {"id": "channel-1", "owner_id": "owner-1"}
    max_post = {
        "id": "post-1",
        "channel_id": "channel-1",
        "text": "MAX пост",
        "created_at": "2024-04-03T10:00:00+00:00",
    }
    max_post_first = normalize_max_post(max_post, channel)
    max_post_second = normalize_max_post(max_post, channel)
    assert max_post_first.doc_id == max_post_second.doc_id

    max_comment = {
        "id": "comment-1",
        "text": "MAX комментарий",
        "created_at": "2024-04-03T10:05:00+00:00",
    }
    max_comment_first = normalize_max_comment(max_comment, max_post)
    max_comment_second = normalize_max_comment(max_comment, max_post)
    assert max_comment_first.doc_id == max_comment_second.doc_id

    vk_comment = {
        "id": 555,
        "from_id": 101,
        "date": 1712150300,
        "text": "Комментарий VK",
        "likes": {"count": 2},
        "thread": {"count": 0},
    }
    vk_comment_first = normalize_vk_comment(vk_comment, raw_vk_post)
    vk_comment_second = normalize_vk_comment(vk_comment, raw_vk_post)
    assert vk_comment_first.doc_id == vk_comment_second.doc_id


def test_comment_parent_id_points_to_parent_doc_id() -> None:
    raw_vk_post = {
        "id": 202,
        "owner_id": 33,
        "from_id": 33,
        "date": 1712150000,
        "text": "Родительский VK пост",
        "views": {"count": 3},
        "likes": {"count": 1},
        "reposts": {"count": 0},
        "comments": {"count": 1},
    }
    vk_comment = {
        "id": 900,
        "from_id": 34,
        "date": 1712150400,
        "text": "Дочерний VK комментарий",
        "likes": {"count": 0},
        "thread": {"count": 0},
    }
    vk_comment_doc = normalize_vk_comment(vk_comment, raw_vk_post)
    assert vk_comment_doc.parent_id == build_vk_post_doc_id("33_202")

    max_parent_post = {
        "id": "post-77",
        "channel_id": "channel-55",
        "text": "Родительский MAX пост",
        "created_at": "2024-04-03T10:00:00+00:00",
    }
    max_comment = {
        "id": "comment-77",
        "text": "Дочерний MAX комментарий",
        "created_at": "2024-04-03T10:15:00+00:00",
    }
    max_comment_doc = normalize_max_comment(max_comment, max_parent_post)
    assert max_comment_doc.parent_id == build_max_post_doc_id("channel-55_post-77")
