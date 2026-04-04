import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from dotenv import load_dotenv

from id_builders import build_max_comment_doc_id, build_max_post_doc_id
from kafka_producer import send_document
from schema import MediaType, NormalizedDocument, SourceType

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw.documents")
MAX_MOCK_DATA_PATH = os.getenv("MAX_MOCK_DATA_PATH", "parser_project/mock/max_messages.json")
MAX_OUTPUT_JSONL_PATH = os.getenv("MAX_OUTPUT_JSONL_PATH", "documents.jsonl")

logger = logging.getLogger("collect_max_messages")


class MaxClient(Protocol):
    """Интерфейс клиента MAX.

    TODO: при подключении реального API вынести контракт в отдельный модуль и
    добавить реализацию с auth/retry/rate-limit.
    """

    def get_channels(self) -> list[dict[str, Any]]:
        """Возвращает список каналов."""

    def get_posts(self, channel_id: str) -> list[dict[str, Any]]:
        """Возвращает посты по каналу."""

    def get_comments(self, post_id: str) -> list[dict[str, Any]]:
        """Возвращает комментарии/ответы по посту."""


class MockMaxClient:
    """Mock-реализация MAX-клиента поверх локального JSON.

    Поддерживаемые форматы файла:
    1) Плоский:
       {
         "channels": [...],
         "posts": [...],
         "comments": [...]
       }
    2) Вложенный:
       {
         "channels": [
           {"id": "...", "posts": [{..., "comments": [...]}]}
         ]
       }
    """

    def __init__(self, json_path: str) -> None:
        self._json_path = Path(json_path)
        self._data = self._load_data(self._json_path)

    @staticmethod
    def _load_data(path: Path) -> dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"MAX mock file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            raise ValueError("MAX mock JSON должен быть объектом")
        return payload

    def get_channels(self) -> list[dict[str, Any]]:
        channels = self._data.get("channels", [])
        if not isinstance(channels, list):
            raise ValueError("MAX mock JSON: поле channels должно быть списком")
        return channels

    def get_posts(self, channel_id: str) -> list[dict[str, Any]]:
        if isinstance(self._data.get("posts"), list):
            return [p for p in self._data["posts"] if str(p.get("channel_id")) == str(channel_id)]

        posts: list[dict[str, Any]] = []
        for channel in self.get_channels():
            if str(channel.get("id")) != str(channel_id):
                continue
            for post in channel.get("posts", []):
                post_copy = dict(post)
                post_copy.setdefault("channel_id", channel_id)
                posts.append(post_copy)
        return posts

    def get_comments(self, post_id: str) -> list[dict[str, Any]]:
        if isinstance(self._data.get("comments"), list):
            return [c for c in self._data["comments"] if str(c.get("post_id")) == str(post_id)]

        comments: list[dict[str, Any]] = []
        for channel in self.get_channels():
            for post in channel.get("posts", []):
                if str(post.get("id")) != str(post_id):
                    continue
                for comment in post.get("comments", []):
                    comment_copy = dict(comment)
                    comment_copy.setdefault("post_id", post_id)
                    comments.append(comment_copy)
        return comments


# TODO: подключить здесь реальный MAX HTTP/SDK клиент и оставить Mock как fallback.
# class RealMaxClient(MaxClient):
#     ...


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc)
    if isinstance(value, str):
        candidate = value.strip()
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            logger.warning("Не удалось распарсить created_at=%s, используем текущее timezone.utc", value)
    return datetime.now(timezone.utc)


def normalize_max_post(raw_post: dict[str, Any], channel: dict[str, Any]) -> NormalizedDocument:
    text = str(raw_post.get("text", "")).strip()
    if not text:
        raise ValueError("Пустой MAX-пост: text обязателен")

    channel_id = str(raw_post.get("channel_id") or channel.get("id"))
    post_id = str(raw_post["id"])
    source_id = f"{channel_id}_{post_id}"

    return NormalizedDocument(
        doc_id=build_max_post_doc_id(source_id),
        source_type=SourceType.MAX_POST,
        source_id=source_id,
        parent_id=None,
        text=text,
        media_type=MediaType.TEXT,
        created_at=_parse_datetime(raw_post.get("created_at")),
        collected_at=datetime.now(timezone.utc),
        author_id=str(raw_post.get("author_id") or channel.get("owner_id") or "unknown"),
        is_official=bool(raw_post.get("is_official", channel.get("is_official", False))),
        reach=int(raw_post.get("reach", 0) or 0),
        likes=int(raw_post.get("likes", 0) or 0),
        reposts=int(raw_post.get("reposts", 0) or 0),
        comments_count=int(raw_post.get("comments_count", 0) or 0),
        region_hint=raw_post.get("region_hint") or channel.get("region_hint"),
        geo_lat=raw_post.get("geo_lat"),
        geo_lon=raw_post.get("geo_lon"),
        raw_payload=raw_post,
    )


def normalize_max_comment(raw_comment: dict[str, Any], parent_post: dict[str, Any]) -> NormalizedDocument:
    text = str(raw_comment.get("text", "")).strip()
    if not text:
        raise ValueError("Пустой MAX-комментарий: text обязателен")

    parent_channel_id = str(parent_post.get("channel_id") or "")
    parent_post_id = str(parent_post.get("id"))
    post_source_id = f"{parent_channel_id}_{parent_post_id}" if parent_channel_id else parent_post_id
    comment_id = str(raw_comment["id"])
    source_id = f"{post_source_id}_{comment_id}"

    return NormalizedDocument(
        doc_id=build_max_comment_doc_id(source_id),
        source_type=SourceType.MAX_COMMENT,
        source_id=source_id,
        parent_id=build_max_post_doc_id(post_source_id),
        text=text,
        media_type=MediaType.TEXT,
        created_at=_parse_datetime(raw_comment.get("created_at")),
        collected_at=datetime.now(timezone.utc),
        author_id=str(raw_comment.get("author_id", "unknown")),
        is_official=bool(raw_comment.get("is_official", False)),
        reach=int(raw_comment.get("reach", 0) or 0),
        likes=int(raw_comment.get("likes", 0) or 0),
        reposts=0,
        comments_count=int(raw_comment.get("replies_count", 0) or 0),
        region_hint=raw_comment.get("region_hint") or parent_post.get("region_hint"),
        geo_lat=raw_comment.get("geo_lat"),
        geo_lon=raw_comment.get("geo_lon"),
        raw_payload=raw_comment,
    )


def save_document_jsonl(path: str, doc: NormalizedDocument) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(doc.model_dump(), ensure_ascii=False, default=str) + "\n")


def process_max_messages(client: MaxClient, kafka_topic: str) -> int:
    total_sent = 0

    channels = client.get_channels()
    logger.info("Получено каналов MAX: %d", len(channels))

    for channel in channels:
        channel_id = str(channel.get("id"))
        if not channel_id:
            logger.warning("Пропускаем канал без id: %s", channel)
            continue

        logger.info("Обрабатываем канал=%s", channel_id)
        posts = client.get_posts(channel_id)
        logger.info("Канал=%s, постов=%d", channel_id, len(posts))

        for post in posts:
            try:
                post_doc = normalize_max_post(post, channel)
                send_document(kafka_topic, post_doc.model_dump())
                save_document_jsonl(MAX_OUTPUT_JSONL_PATH, post_doc)
                total_sent += 1
            except Exception as post_error:
                logger.exception("Ошибка нормализации/отправки поста channel=%s: %s", channel_id, post_error)
                continue

            post_id = str(post.get("id"))
            if not post_id:
                continue

            comments = client.get_comments(post_id)
            logger.info("Канал=%s, пост=%s, комментариев=%d", channel_id, post_id, len(comments))

            for comment in comments:
                try:
                    comment_doc = normalize_max_comment(comment, post)
                    send_document(kafka_topic, comment_doc.model_dump())
                    save_document_jsonl(MAX_OUTPUT_JSONL_PATH, comment_doc)
                    total_sent += 1
                except Exception as comment_error:
                    logger.exception(
                        "Ошибка нормализации/отправки комментария channel=%s post=%s: %s",
                        channel_id,
                        post_id,
                        comment_error,
                    )

    return total_sent


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    logger.info("Запуск сборщика MAX (mock mode)")
    logger.info("Kafka topic=%s", KAFKA_TOPIC)
    logger.info("Mock data path=%s", MAX_MOCK_DATA_PATH)

    # TODO: заменить фабрику клиента на переключение mock/real через env-флаг.
    client: MaxClient = MockMaxClient(MAX_MOCK_DATA_PATH)

    total = process_max_messages(client, KAFKA_TOPIC)
    logger.info("MAX ingest завершён, отправлено сообщений=%d", total)


if __name__ == "__main__":
    main()
