import os
import json
from dotenv import load_dotenv

from vk_client import resolve_screen_name, get_wall_posts
from normalizers.vk import normalize_vk_post

load_dotenv()

VK_GROUP_DOMAINS = os.getenv("VK_GROUP_DOMAINS", "")


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
                doc.model_dump(),
                ensure_ascii=False,
                default=str,
            ) + "\n"
        )


def main():
    if not VK_GROUP_DOMAINS.strip():
        raise ValueError("Не найден VK_GROUP_DOMAINS в .env")

    group_domains = [x.strip() for x in VK_GROUP_DOMAINS.split(",") if x.strip()]

    print("Группы для обхода:")
    for domain in group_domains:
        print(f" - {domain}")
    print("-" * 80)

    for domain in group_domains:
        try:
            owner_id = get_group_owner_id(domain)
            print(f"[{domain}] owner_id = {owner_id}")

            raw_posts = get_wall_posts(owner_id, count=5)
            print(f"[{domain}] Получено постов: {len(raw_posts)}")

            for raw_post in raw_posts:
                text = raw_post.get("text", "")
                if not text.strip():
                    continue

                doc = normalize_vk_post(raw_post)

                short_view = doc.model_dump()
                short_view.pop("raw_payload", None)

                print(json.dumps(short_view, indent=2, ensure_ascii=False, default=str))
                print("-" * 80)

                save_document_jsonl("documents.jsonl", doc)

        except Exception as e:
            print(f"[{domain}] Ошибка: {e}")
            print("-" * 80)


if __name__ == "__main__":
    main()