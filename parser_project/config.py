from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class ConfigError(RuntimeError):
    """Понятная ошибка конфигурации ingestion-проекта."""


@dataclass(frozen=True)
class AppConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    failed_messages_path: Path

    database_url: str | None

    vk_token: str | None
    vk_api_version: str
    vk_group_domains: list[str]
    vk_posts_per_group: int
    vk_page_size: int

    days_back: int
    rss_feeds: list[str]

    portal_source: str
    portal_name: str
    portal_appeals_file: str

    max_mock_data_path: str
    max_output_jsonl_path: str

    log_level: str


def _csv_env(name: str, default: str = "") -> list[str]:
    raw = (os.getenv(name, default) or "").strip()
    if not raw:
        return []
    return [value.strip() for value in raw.split(",") if value.strip()]


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as error:
        raise ConfigError(f"Некорректное значение {name}={raw!r}: ожидается целое число.") from error


def _text_env(name: str, default: str | None = None) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip()
    return value if value else default


def load_config() -> AppConfig:
    return AppConfig(
        kafka_bootstrap_servers=_text_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "localhost:9092",
        kafka_topic=_text_env("KAFKA_TOPIC", "raw.documents") or "raw.documents",
        kafka_group_id=_text_env("KAFKA_GROUP_ID", "documents-consumer-group") or "documents-consumer-group",
        failed_messages_path=Path(_text_env("FAILED_MESSAGES_PATH", "failed_messages.jsonl") or "failed_messages.jsonl"),
        database_url=_text_env("DATABASE_URL"),
        vk_token=_text_env("VK_TOKEN"),
        vk_api_version=_text_env("VK_API_VERSION", "5.131") or "5.131",
        vk_group_domains=_csv_env("VK_GROUP_DOMAINS"),
        vk_posts_per_group=_int_env("VK_POSTS_PER_GROUP", 100),
        vk_page_size=_int_env("VK_PAGE_SIZE", 20),
        days_back=_int_env("DAYS_BACK", 7),
        rss_feeds=_csv_env("RSS_FEEDS"),
        portal_source=_text_env("PORTAL_SOURCE", "mock_local_file") or "mock_local_file",
        portal_name=_text_env("PORTAL_NAME", "mock_portal") or "mock_portal",
        portal_appeals_file=_text_env("PORTAL_APPEALS_FILE", "portal_appeals.jsonl") or "portal_appeals.jsonl",
        max_mock_data_path=_text_env("MAX_MOCK_DATA_PATH", "parser_project/mock/max_messages.json")
        or "parser_project/mock/max_messages.json",
        max_output_jsonl_path=_text_env("MAX_OUTPUT_JSONL_PATH", "documents.jsonl") or "documents.jsonl",
        log_level=_text_env("LOG_LEVEL", "INFO") or "INFO",
    )


def _raise_missing(component: str, missing_names: list[str]) -> None:
    if not missing_names:
        return
    required_list = ", ".join(missing_names)
    raise ConfigError(
        f"Не хватает обязательных переменных окружения для {component}: {required_list}. "
        "Добавьте их в .env и повторите запуск."
    )


def validate_common(config: AppConfig) -> None:
    if config.days_back < 0:
        raise ConfigError("DAYS_BACK должен быть >= 0.")
    if config.vk_posts_per_group <= 0:
        raise ConfigError("VK_POSTS_PER_GROUP должен быть > 0.")
    if config.vk_page_size <= 0:
        raise ConfigError("VK_PAGE_SIZE должен быть > 0.")


def validate_vk_config(config: AppConfig) -> None:
    validate_common(config)
    missing: list[str] = []
    if not config.vk_token:
        missing.append("VK_TOKEN")
    if not config.vk_group_domains:
        missing.append("VK_GROUP_DOMAINS")
    _raise_missing("VK-сборщика", missing)


def validate_rss_config(config: AppConfig) -> None:
    validate_common(config)
    missing: list[str] = []
    if not config.rss_feeds:
        missing.append("RSS_FEEDS")
    _raise_missing("RSS-сборщика", missing)


def validate_portal_config(config: AppConfig) -> None:
    missing: list[str] = []
    if not config.portal_source:
        missing.append("PORTAL_SOURCE")
    if config.portal_source == "mock_local_file" and not config.portal_appeals_file:
        missing.append("PORTAL_APPEALS_FILE")
    _raise_missing("сборщика обращений портала", missing)


def validate_max_config(config: AppConfig) -> None:
    missing: list[str] = []
    if not config.max_mock_data_path:
        missing.append("MAX_MOCK_DATA_PATH")
    _raise_missing("MAX-сборщика", missing)


def validate_consumer_config(config: AppConfig) -> None:
    missing: list[str] = []
    if not config.kafka_bootstrap_servers:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    if not config.kafka_topic:
        missing.append("KAFKA_TOPIC")
    if not config.kafka_group_id:
        missing.append("KAFKA_GROUP_ID")
    _raise_missing("consumer", missing)


def validate_db_config(config: AppConfig) -> None:
    missing: list[str] = []
    if not config.database_url:
        missing.append("DATABASE_URL")
    _raise_missing("подключения к PostgreSQL", missing)
