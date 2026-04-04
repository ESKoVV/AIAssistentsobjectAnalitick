import json
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv

from kafka_producer import send_document
from schema import MediaType, NormalizedDocument, SourceType

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw.documents")
PORTAL_SOURCE = os.getenv("PORTAL_SOURCE", "mock_local_file")
PORTAL_NAME = os.getenv("PORTAL_NAME", "mock_portal")
PORTAL_APPEALS_FILE = os.getenv("PORTAL_APPEALS_FILE", "portal_appeals.jsonl")


@dataclass(frozen=True)
class RawPortalAppeal:
    """Единый контракт обращения гражданина, полученного из любого портала."""

    appeal_id: str
    text: str
    created_at: datetime
    region_hint: str
    author_id: str
    raw_payload: dict[str, Any]


class PortalAppealLoader(ABC):
    """Базовый интерфейс загрузчика обращений с порталов."""

    @abstractmethod
    def iter_appeals(self) -> Iterable[RawPortalAppeal]:
        """Возвращает поток сырых обращений портала."""


class MockLocalFilePortalLoader(PortalAppealLoader):
    """Демо-источник: читает обращения из локального json/jsonl файла."""

    def __init__(self, file_path: str):
        self._file_path = Path(file_path)

    def iter_appeals(self) -> Iterable[RawPortalAppeal]:
        if not self._file_path.exists():
            raise FileNotFoundError(f"Файл обращений не найден: {self._file_path}")

        rows = self._read_rows(self._file_path)
        for row in rows:
            yield self._to_raw_appeal(row)

    @staticmethod
    def _read_rows(file_path: Path) -> Iterable[dict[str, Any]]:
        ext = file_path.suffix.lower()

        if ext == ".jsonl":
            with file_path.open("r", encoding="utf-8") as f:
                for line_number, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as error:
                        raise ValueError(f"Некорректный JSONL в строке {line_number}: {error}") from error
            return

        if ext == ".json":
            with file_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)

            if isinstance(payload, dict):
                items = payload.get("items")
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            yield item
                        else:
                            raise ValueError("Поле items должно содержать объекты обращений")
                    return
                raise ValueError("JSON-объект должен содержать список обращений в поле 'items'")

            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        yield item
                    else:
                        raise ValueError("JSON-массив должен содержать объекты обращений")
                return

            raise ValueError("JSON должен быть массивом обращений или объектом с полем 'items'")

        raise ValueError("Поддерживаются только файлы .json и .jsonl")

    @staticmethod
    def _to_raw_appeal(row: dict[str, Any]) -> RawPortalAppeal:
        appeal_id = str(row.get("id") or "").strip()
        if not appeal_id:
            raise ValueError("У обращения отсутствует обязательное поле id")

        text = str(row.get("text") or "").strip()
        if not text:
            raise ValueError(f"У обращения {appeal_id} отсутствует обязательное поле text")

        created_at = parse_datetime_or_now(row.get("created_at"))
        region_hint = str(row.get("region_hint") or "unknown").strip() or "unknown"
        author_id = str(row.get("author_id") or "anonymous").strip() or "anonymous"

        return RawPortalAppeal(
            appeal_id=appeal_id,
            text=text,
            created_at=created_at,
            region_hint=region_hint,
            author_id=author_id,
            raw_payload=row,
        )


def parse_datetime_or_now(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    if isinstance(value, str) and value.strip():
        raw = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            pass

    return datetime.now(UTC)


def stable_doc_id(source_type: SourceType, source_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_type.value}:{source_id}"))


def normalize_portal_appeal(appeal: RawPortalAppeal) -> NormalizedDocument:
    """Нормализует обращение гражданина в единый контракт документа."""

    collected_at = datetime.now(UTC)

    return NormalizedDocument(
        doc_id=stable_doc_id(SourceType.PORTAL_APPEAL, appeal.appeal_id),
        source_type=SourceType.PORTAL_APPEAL,
        source_id=appeal.appeal_id,
        parent_id=None,
        text=appeal.text,
        media_type=MediaType.TEXT,
        created_at=appeal.created_at,
        collected_at=collected_at,
        author_id=appeal.author_id or "anonymous",
        is_official=False,
        reach=0,
        likes=0,
        reposts=0,
        comments_count=0,
        region_hint=appeal.region_hint,
        geo_lat=None,
        geo_lon=None,
        raw_payload=appeal.raw_payload,
    )


def save_document_jsonl(path: str, doc: NormalizedDocument) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(doc.model_dump(), ensure_ascii=False, default=str) + "\n")


def build_loader(source_name: str, **kwargs: Any) -> PortalAppealLoader:
    """
    Фабрика загрузчиков по типу источника.

    TODO: Добавить реализации для production-порталов (API, auth, pagination, retries).
    """

    if source_name == "mock_local_file":
        file_path = kwargs.get("file_path")
        if not file_path:
            raise ValueError("Для mock_local_file требуется file_path")
        return MockLocalFilePortalLoader(file_path=file_path)

    raise ValueError(f"Неизвестный тип источника PORTAL_SOURCE: {source_name}")


def main() -> None:
    loader = build_loader(PORTAL_SOURCE, file_path=PORTAL_APPEALS_FILE)

    print(f"Источник обращений: {PORTAL_SOURCE}")
    print(f"Портал: {PORTAL_NAME}")
    if PORTAL_SOURCE == "mock_local_file":
        print(f"Файл: {PORTAL_APPEALS_FILE}")
    print("-" * 80)

    sent = 0

    for appeal in loader.iter_appeals():
        try:
            doc = normalize_portal_appeal(appeal)
            send_document(KAFKA_TOPIC, doc.model_dump())
            save_document_jsonl("documents.jsonl", doc)

            short_view = doc.model_dump()
            short_view.pop("raw_payload", None)

            print(json.dumps(short_view, ensure_ascii=False, indent=2, default=str))
            print("-" * 80)
            sent += 1
        except Exception as error:
            print(f"[portal={PORTAL_NAME}] [appeal_id={appeal.appeal_id}] Ошибка обработки: {error}")
            print("-" * 80)

    print(f"Всего отправлено в Kafka: {sent}")


if __name__ == "__main__":
    main()
