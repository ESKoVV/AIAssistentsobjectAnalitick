from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent

# Сначала загружаем переменные из parser_project/.env независимо от текущей директории запуска,
# затем даем возможность дополнить их .env из cwd (если он есть).
load_dotenv(PROJECT_DIR / ".env")
load_dotenv()


class ConfigError(RuntimeError):
    """Понятная ошибка конфигурации ingestion-проекта."""


@dataclass(frozen=True)
class AppConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_raw_topic: str
    kafka_raw_dlq_topic: str
    kafka_preprocessed_topic: str
    kafka_ml_topic: str
    kafka_ml_results_topic: str
    kafka_group_id: str
    kafka_preprocessing_group_id: str
    failed_messages_path: Path

    database_url: str | None
    sources_config_path: Path

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
        raw = _read_multiline_env_value(name)
    if not raw:
        return []
    return [value.strip() for value in raw.split(",") if value.strip()]


_ENV_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")


def _read_multiline_env_value(name: str) -> str:
    """
    Поддержка формата:
      VK_GROUP_DOMAINS=
      group_1,
      group_2,
      group_3
    в parser_project/.env.
    """
    env_path = PROJECT_DIR / ".env"
    if not env_path.exists():
        return ""

    lines = env_path.read_text(encoding="utf-8").splitlines()
    prefix = f"{name}="
    for idx, line in enumerate(lines):
        if not line.startswith(prefix):
            continue

        first_value = line[len(prefix) :].strip()
        if first_value:
            return first_value

        collected: list[str] = []
        for next_line in lines[idx + 1 :]:
            stripped = next_line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if _ENV_KEY_PATTERN.match(stripped):
                break
            collected.append(stripped)
        return "".join(collected)

    return ""


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


def _normalize_bootstrap_servers(raw_value: str) -> str:
    """
    Нормализует bootstrap servers для локального запуска.

    На Windows/WSL `localhost` иногда резолвится в IPv6 (`::1`), а Docker-порт
    Kafka проброшен только на IPv4. В результате kafka-python получает
    NoBrokersAvailable. Замена `localhost` -> `127.0.0.1` устраняет этот класс
    ошибок и не затрагивает контейнерные адреса (`kafka:29092`) или внешние
    хосты.
    """
    normalized: list[str] = []
    for server in raw_value.split(","):
        item = server.strip()
        if not item:
            continue
        if item.startswith("localhost:"):
            item = item.replace("localhost:", "127.0.0.1:", 1)
        normalized.append(item)
    return ",".join(normalized)


def load_config() -> AppConfig:
    legacy_kafka_topic = _text_env("KAFKA_TOPIC")
    kafka_raw_topic = _text_env("KAFKA_RAW_TOPIC", legacy_kafka_topic or "raw.documents") or "raw.documents"
    kafka_bootstrap_servers = _normalize_bootstrap_servers(
        _text_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "localhost:9092",
    )

    return AppConfig(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_raw_topic,
        kafka_raw_topic=kafka_raw_topic,
        kafka_raw_dlq_topic=_text_env("KAFKA_RAW_DLQ_TOPIC", f"{kafka_raw_topic}.dlq") or f"{kafka_raw_topic}.dlq",
        kafka_preprocessed_topic=(
            _text_env("KAFKA_PREPROCESSED_TOPIC", "preprocessed.documents") or "preprocessed.documents"
        ),
        kafka_ml_topic=_text_env("KAFKA_ML_TOPIC", "ml.documents") or "ml.documents",
        kafka_ml_results_topic=_text_env("KAFKA_ML_RESULTS_TOPIC", "ml.results") or "ml.results",
        kafka_group_id=_text_env("KAFKA_GROUP_ID", "documents-consumer-group") or "documents-consumer-group",
        kafka_preprocessing_group_id=(
            _text_env(
                "KAFKA_PREPROCESSING_GROUP_ID",
                f"{_text_env('KAFKA_GROUP_ID', 'documents-consumer-group') or 'documents-consumer-group'}-preprocessing",
            )
            or f"{_text_env('KAFKA_GROUP_ID', 'documents-consumer-group') or 'documents-consumer-group'}-preprocessing"
        ),
        failed_messages_path=Path(_text_env("FAILED_MESSAGES_PATH", "failed_messages.jsonl") or "failed_messages.jsonl"),
        database_url=_text_env("DATABASE_URL"),
        sources_config_path=Path(
            _text_env("SOURCES_CONFIG_PATH", str((PROJECT_DIR.parent / "configs" / "sources.yaml")))  # type: ignore[arg-type]
            or str(PROJECT_DIR.parent / "configs" / "sources.yaml")
        ),
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
    validate_preprocessing_consumer_config(config)


def validate_raw_consumer_config(config: AppConfig) -> None:
    missing: list[str] = []
    if not config.kafka_bootstrap_servers:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    if not config.kafka_raw_topic:
        missing.append("KAFKA_RAW_TOPIC")
    if not config.kafka_group_id:
        missing.append("KAFKA_GROUP_ID")
    if not config.database_url:
        missing.append("DATABASE_URL")
    _raise_missing("raw consumer", missing)


def validate_preprocessing_consumer_config(config: AppConfig) -> None:
    missing: list[str] = []
    if not config.kafka_bootstrap_servers:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    if not config.kafka_raw_topic:
        missing.append("KAFKA_RAW_TOPIC")
    if not config.kafka_preprocessed_topic:
        missing.append("KAFKA_PREPROCESSED_TOPIC")
    if not config.kafka_preprocessing_group_id:
        missing.append("KAFKA_PREPROCESSING_GROUP_ID")
    if not config.database_url:
        missing.append("DATABASE_URL")
    if not str(config.sources_config_path).strip():
        missing.append("SOURCES_CONFIG_PATH")
    _raise_missing("preprocessing consumer", missing)


def validate_db_config(config: AppConfig) -> None:
    missing: list[str] = []
    if not config.database_url:
        missing.append("DATABASE_URL")
    _raise_missing("подключения к PostgreSQL", missing)
