from datetime import datetime, timezone
import sys
import types

sys.modules.setdefault("kafka_producer", types.SimpleNamespace(send_document=lambda *args, **kwargs: None))

from collect_max_messages import build_raw_max_comment, build_raw_max_post
from collect_rss import build_raw_rss_entry
from collect_vk_posts import build_raw_vk_comment_document, build_raw_vk_post_document


def test_source_id_is_deterministic_for_same_payload_across_collectors() -> None:
    collected_at = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)

    raw_vk_post = {
        "id": 101,
        "owner_id": 42,
        "from_id": 42,
        "date": 1712150000,
        "text": "Повторный запуск должен дать тот же source_id",
    }
    vk_doc_first = build_raw_vk_post_document(raw_vk_post, collected_at)
    vk_doc_second = build_raw_vk_post_document(raw_vk_post, collected_at)
    assert vk_doc_first.source_id == vk_doc_second.source_id

    rss_entry = {
        "id": "article-1",
        "title": "Новость",
        "summary": "Краткий текст",
        "published": "Wed, 03 Apr 2024 10:00:00 +0300",
    }
    rss_doc_first = build_raw_rss_entry("https://example.com/rss", rss_entry)
    rss_doc_second = build_raw_rss_entry("https://example.com/rss", rss_entry)
    assert rss_doc_first.source_id == rss_doc_second.source_id

    channel = {"id": "channel-1", "owner_id": "owner-1"}
    max_post = {
        "id": "post-1",
        "channel_id": "channel-1",
        "text": "MAX пост",
        "created_at": "2024-04-03T10:00:00+00:00",
    }
    max_post_first = build_raw_max_post(max_post, channel)
    max_post_second = build_raw_max_post(max_post, channel)
    assert max_post_first.source_id == max_post_second.source_id

    max_comment = {
        "id": "comment-1",
        "text": "MAX комментарий",
        "created_at": "2024-04-03T10:05:00+00:00",
    }
    max_comment_first = build_raw_max_comment(max_comment, max_post)
    max_comment_second = build_raw_max_comment(max_comment, max_post)
    assert max_comment_first.source_id == max_comment_second.source_id


def test_comment_parent_source_id_points_to_parent_source_id() -> None:
    collected_at = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    raw_vk_post = {
        "id": 202,
        "owner_id": 33,
        "from_id": 33,
        "date": 1712150000,
        "text": "Родительский VK пост",
    }
    vk_comment = {
        "id": 900,
        "from_id": 34,
        "date": 1712150400,
        "text": "Дочерний VK комментарий",
    }
    vk_comment_doc = build_raw_vk_comment_document(vk_comment, raw_vk_post, collected_at)
    assert vk_comment_doc.parent_source_id == "33_202"

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
    max_comment_doc = build_raw_max_comment(max_comment, max_parent_post)
    assert max_comment_doc.parent_source_id == "channel-55_post-77"


def test_collectors_keep_source_specific_fields_inside_raw_payload() -> None:
    collected_at = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    raw_vk_post = {
        "id": 456,
        "owner_id": 123,
        "from_id": 123,
        "date": 1712150000,
        "text": "Сильный ливень в Ростове",
        "geo": {"coordinates": "47.2221 39.7203"},
    }

    doc = build_raw_vk_post_document(raw_vk_post, collected_at)

    assert doc.raw_payload["geo"] == {"coordinates": "47.2221 39.7203"}
    assert doc.text_raw == "Сильный ливень в Ростове"
