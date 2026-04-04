import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from config import load_config, validate_max_config
from kafka_producer import send_document
from schema import RawDocument, SourceType

CONFIG = load_config()

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


def build_raw_max_post(raw_post: dict[str, Any], channel: dict[str, Any]) -> RawDocument:
    text = str(raw_post.get("text", "")).strip()
    if not text:
        raise ValueError("Пустой MAX-пост: text обязателен")

    channel_id = str(raw_post.get("channel_id") or channel.get("id"))
    post_id = str(raw_post["id"])
    source_id = f"{channel_id}_{post_id}"

    return RawDocument(
        source_type=SourceType.MAX_POST.value,
        source_id=source_id,
        parent_source_id=None,
        text_raw=text,
        author_raw=str(raw_post.get("author_id")) if raw_post.get("author_id") is not None else None,
        created_at=_parse_datetime(raw_post.get("created_at")),
        collected_at=datetime.now(timezone.utc),
        media_type=raw_post.get("media_type"),
        raw_payload=raw_post,
        is_official=bool(raw_post.get("is_official", False)),
        reach=int(raw_post.get("reach", 0) or 0),
        likes=int(raw_post.get("likes", 0) or 0),
        reposts=int(raw_post.get("reposts", 0) or 0),
        comments_count=int(raw_post.get("comments_count", 0) or 0),
    )


def build_raw_max_comment(raw_comment: dict[str, Any], parent_post: dict[str, Any]) -> RawDocument:
    text = str(raw_comment.get("text", "")).strip()
    if not text:
        raise ValueError("Пустой MAX-комментарий: text обязателен")

    parent_channel_id = str(parent_post.get("channel_id") or "")
    parent_post_id = str(parent_post.get("id"))
    post_source_id = f"{parent_channel_id}_{parent_post_id}" if parent_channel_id else parent_post_id
    comment_id = str(raw_comment["id"])
    source_id = f"{post_source_id}_{comment_id}"

    return RawDocument(
        source_type=SourceType.MAX_COMMENT.value,
        source_id=source_id,
        parent_source_id=post_source_id,
        text_raw=text,
        author_raw=str(raw_comment.get("author_id")) if raw_comment.get("author_id") is not None else None,
        created_at=_parse_datetime(raw_comment.get("created_at")),
        collected_at=datetime.now(timezone.utc),
        media_type=raw_comment.get("media_type"),
        raw_payload=raw_comment,
        is_official=bool(raw_comment.get("is_official", False)),
        reach=int(raw_comment.get("reach", 0) or 0),
        likes=int(raw_comment.get("likes", 0) or 0),
        reposts=int(raw_comment.get("reposts", 0) or 0),
        comments_count=int(raw_comment.get("comments_count", 0) or 0),
    )


def save_document_jsonl(path: str, doc: RawDocument) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(doc.model_dump(mode="json"), ensure_ascii=False, default=str) + "\n")


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
                post_doc = build_raw_max_post(post, channel)
                send_document(kafka_topic, post_doc.model_dump(mode="json"))
                save_document_jsonl(CONFIG.max_output_jsonl_path, post_doc)
                total_sent += 1
            except Exception as post_error:
                logger.exception("Ошибка raw-обработки/отправки поста channel=%s: %s", channel_id, post_error)
                continue

            post_id = str(post.get("id"))
            if not post_id:
                continue

            comments = client.get_comments(post_id)
            logger.info("Канал=%s, пост=%s, комментариев=%d", channel_id, post_id, len(comments))

            for comment in comments:
                try:
                    comment_doc = build_raw_max_comment(comment, post)
                    send_document(kafka_topic, comment_doc.model_dump(mode="json"))
                    save_document_jsonl(CONFIG.max_output_jsonl_path, comment_doc)
                    total_sent += 1
                except Exception as comment_error:
                    logger.exception(
                        "Ошибка raw-обработки/отправки комментария channel=%s post=%s: %s",
                        channel_id,
                        post_id,
                        comment_error,
                    )

    return total_sent


def main() -> None:
    validate_max_config(CONFIG)
    logging.basicConfig(
        level=CONFIG.log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    logger.info("Запуск сборщика MAX (mock mode)")
    logger.info("Kafka topic=%s", CONFIG.kafka_raw_topic)
    logger.info("Mock data path=%s", CONFIG.max_mock_data_path)

    # TODO: заменить фабрику клиента на переключение mock/real через env-флаг.
    client: MaxClient = MockMaxClient(CONFIG.max_mock_data_path)

    total = process_max_messages(client, CONFIG.kafka_raw_topic)
    logger.info("MAX ingest завершён, отправлено сообщений=%d", total)


if __name__ == "__main__":
    main()
