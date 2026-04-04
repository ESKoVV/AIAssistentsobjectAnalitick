import json
from datetime import datetime, timedelta, timezone

from config import load_config, validate_vk_config
from kafka_producer import send_document
from schema import RawDocument, SourceType
from vk_client import get_post_comments, get_wall_posts, resolve_screen_name

CONFIG = load_config()


def _cutoff_datetime(days_back: int) -> datetime:
    if days_back < 0:
        raise ValueError("DAYS_BACK должен быть >= 0")
    return datetime.now(timezone.utc) - timedelta(days=days_back)


def _vk_created_at(raw_item: dict) -> datetime:
    raw_ts = raw_item.get("date")
    if raw_ts is None:
        raise ValueError("В VK-объекте отсутствует поле date")
    return datetime.fromtimestamp(raw_ts, timezone.utc)


def _author_raw(raw_item: dict) -> str | None:
    author_id = raw_item.get("from_id")
    if author_id is None:
        author_id = raw_item.get("owner_id")
    if author_id is None:
        return None
    return str(author_id)


def build_raw_vk_post_document(raw_post: dict, collected_at: datetime) -> RawDocument:
    source_id = f"{raw_post['owner_id']}_{raw_post['id']}"
    return RawDocument(
        source_type=SourceType.VK_POST.value,
        source_id=source_id,
        parent_source_id=None,
        text_raw=raw_post.get("text", ""),
        author_raw=_author_raw(raw_post),
        created_at=_vk_created_at(raw_post),
        collected_at=collected_at,
        media_type=raw_post.get("post_type"),
        raw_payload=raw_post,
        is_official=False,
        reach=int(raw_post.get("views", {}).get("count", 0) or 0),
        likes=int(raw_post.get("likes", {}).get("count", 0) or 0),
        reposts=int(raw_post.get("reposts", {}).get("count", 0) or 0),
        comments_count=int(raw_post.get("comments", {}).get("count", 0) or 0),
    )


def build_raw_vk_comment_document(raw_comment: dict, raw_post: dict, collected_at: datetime) -> RawDocument:
    parent_source_id = f"{raw_post['owner_id']}_{raw_post['id']}"
    source_id = f"{parent_source_id}_{raw_comment['id']}"
    return RawDocument(
        source_type=SourceType.VK_COMMENT.value,
        source_id=source_id,
        parent_source_id=parent_source_id,
        text_raw=raw_comment.get("text", ""),
        author_raw=_author_raw(raw_comment),
        created_at=_vk_created_at(raw_comment),
        collected_at=collected_at,
        media_type=raw_comment.get("media_type"),
        raw_payload=raw_comment,
        is_official=False,
        reach=0,
        likes=int(raw_comment.get("likes", {}).get("count", 0) or 0),
        reposts=int(raw_comment.get("reposts", {}).get("count", 0) or 0),
        comments_count=int(raw_comment.get("thread", {}).get("count", 0) or 0),
    )


def get_group_owner_id(screen_name: str) -> int:
    resolved = resolve_screen_name(screen_name)

    obj_type = resolved.get("type")
    object_id = resolved.get("object_id")

    if obj_type not in ("group", "page", "event"):
        raise ValueError(f"{screen_name}: это не группа/паблик/ивент, а {obj_type}")

    if object_id is None:
        raise ValueError(f"{screen_name}: не удалось получить object_id")

    return -int(object_id)


def save_document_jsonl(path: str, doc) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                doc.model_dump(mode="json"),
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )


def main():
    validate_vk_config(CONFIG)
    group_domains = CONFIG.vk_group_domains
    cutoff_dt = _cutoff_datetime(CONFIG.days_back)

    print("Группы для обхода:")
    for domain in group_domains:
        print(f" - {domain}")
    print(
        f"Фильтрация по created_at: только за последние {CONFIG.days_back} дней "
        f"(cutoff={cutoff_dt.isoformat()})"
    )
    print("-" * 80)

    total_sent = 0

    for domain in group_domains:
        try:
            owner_id = get_group_owner_id(domain)
            print(f"[{domain}] owner_id = {owner_id}")

            group_posts_sent = 0
            group_comments_sent = 0
            group_posts_dropped_old = 0
            group_comments_dropped_old = 0
            offset = 0

            while group_posts_sent < CONFIG.vk_posts_per_group:
                remaining_posts = CONFIG.vk_posts_per_group - group_posts_sent
                page_size = min(CONFIG.vk_page_size, remaining_posts)

                raw_posts = get_wall_posts(owner_id, count=page_size, offset=offset)
                page_posts_received = len(raw_posts)
                page_posts_sent = 0

                print(
                    f"[{domain}] offset={offset} | получено постов: {page_posts_received} | "
                    f"отправлено постов: {page_posts_sent}"
                )

                if not raw_posts:
                    print(f"[{domain}] Посты закончились (offset={offset}).")
                    break

                for raw_post in raw_posts:
                    if group_posts_sent >= CONFIG.vk_posts_per_group:
                        break

                    post_created_at = _vk_created_at(raw_post)
                    if post_created_at < cutoff_dt:
                        group_posts_dropped_old += 1
                        continue

                    text = raw_post.get("text", "")
                    if not text.strip():
                        continue

                    collected_at = datetime.now(timezone.utc)
                    doc = build_raw_vk_post_document(raw_post, collected_at)

                    send_document(CONFIG.kafka_raw_topic, doc.model_dump(mode="json"))
                    save_document_jsonl("documents.jsonl", doc)

                    short_view = doc.model_dump(mode="json")
                    short_view.pop("raw_payload", None)

                    print(json.dumps(short_view, indent=2, ensure_ascii=False, default=str))
                    print("-" * 80)

                    total_sent += 1
                    group_posts_sent += 1
                    page_posts_sent += 1

                    post_source_id = f"{raw_post['owner_id']}_{raw_post['id']}"
                    try:
                        raw_comments = get_post_comments(
                            owner_id=raw_post["owner_id"],
                            post_id=raw_post["id"],
                        )
                        print(f"[{domain}] [{post_source_id}] Получено комментариев: {len(raw_comments)}")

                        post_comments_sent = 0
                        for raw_comment in raw_comments:
                            comment_created_at = _vk_created_at(raw_comment)
                            if comment_created_at < cutoff_dt:
                                group_comments_dropped_old += 1
                                continue

                            comment_text = raw_comment.get("text", "")
                            if not comment_text.strip():
                                continue

                            comment_collected_at = datetime.now(timezone.utc)
                            comment_doc = build_raw_vk_comment_document(
                                raw_comment,
                                raw_post,
                                comment_collected_at,
                            )
                            send_document(CONFIG.kafka_raw_topic, comment_doc.model_dump(mode="json"))
                            save_document_jsonl("documents.jsonl", comment_doc)
                            total_sent += 1
                            post_comments_sent += 1
                            group_comments_sent += 1

                        print(f"[{domain}] [{post_source_id}] Отправлено комментариев: {post_comments_sent}")
                        print("-" * 80)
                    except Exception as e:
                        print(f"[{domain}] [{post_source_id}] Ошибка сбора комментариев: {e}")
                        print("-" * 80)

                print(
                    f"[{domain}] offset={offset} | получено постов: {page_posts_received} | "
                    f"отправлено постов: {page_posts_sent}"
                )
                print("-" * 80)

                offset += page_posts_received

                if page_posts_received < page_size:
                    print(f"[{domain}] Достигнут конец стены (offset={offset}).")
                    break

            print(
                f"[{domain}] Итого отправлено: постов={group_posts_sent}, "
                f"комментариев={group_comments_sent}, всего={group_posts_sent + group_comments_sent}"
            )
            print(
                f"[{domain}] Отфильтровано как слишком старые: "
                f"постов={group_posts_dropped_old}, комментариев={group_comments_dropped_old}, "
                f"всего={group_posts_dropped_old + group_comments_dropped_old}"
            )
            print("-" * 80)

        except Exception as e:
            print(f"[{domain}] Ошибка: {e}")
            print("-" * 80)

    print(f"Всего отправлено в Kafka: {total_sent}")


if __name__ == "__main__":
    main()
