import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config import _csv_env, load_config, validate_portal_config
from kafka_producer import send_document
from schema import MediaType, RawMessage, SourceType

CONFIG = load_config()
PORTAL_URLS = _csv_env("PORTAL_URLS")
PORTAL_KEYWORDS = [keyword.lower() for keyword in _csv_env("PORTAL_KEYWORDS")]

DEFAULT_TIMEOUT_SECONDS = 10
DEFAULT_ARTICLES_PER_SITE = 15

logging.basicConfig(
    level=getattr(logging, (CONFIG.log_level or "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


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


class HtmlPortalLoader(PortalAppealLoader):
    """Собирает обращения с обычных HTML-страниц порталов."""

    def __init__(
        self,
        portal_urls: list[str],
        keywords: list[str],
        max_articles_per_site: int = DEFAULT_ARTICLES_PER_SITE,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ):
        self._portal_urls = portal_urls
        self._keywords = [k.lower() for k in keywords if k.strip()]
        self._max_articles_per_site = max_articles_per_site
        self._timeout_seconds = timeout_seconds
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; PortalAppealsBot/1.0; +https://example.local/bot)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

    def iter_appeals(self) -> Iterable[RawPortalAppeal]:
        if not self._portal_urls:
            logger.warning("PORTAL_URLS пустой: нет источников для HTML scraping.")
            return

        if not self._keywords:
            logger.warning("PORTAL_KEYWORDS пустой: фильтрация по ключевым словам отключена.")

        for portal_url in self._portal_urls:
            logger.info("Сканирование портала: %s", portal_url)

            try:
                root_soup = self._fetch_soup(portal_url)
            except Exception as error:
                logger.warning("Не удалось загрузить портал %s: %s", portal_url, error)
                continue

            article_urls = self._extract_article_links(portal_url, root_soup)
            limited_urls = article_urls[: self._max_articles_per_site]
            logger.info(
                "Портал %s: найдено %s ссылок, обработаем %s",
                portal_url,
                len(article_urls),
                len(limited_urls),
            )

            for article_url in limited_urls:
                try:
                    article = self._parse_article(article_url)
                    if not article:
                        continue

                    title, text, created_at_raw = article
                    if not self._matches_keywords(text):
                        logger.debug("Статья не прошла фильтр ключевых слов: %s", article_url)
                        continue

                    appeal_id = hashlib.sha1(article_url.encode("utf-8")).hexdigest()
                    payload = {
                        "portal_url": portal_url,
                        "article_url": article_url,
                        "title": title,
                        "created_at": created_at_raw,
                        "keywords": self._keywords,
                    }

                    yield RawPortalAppeal(
                        appeal_id=appeal_id,
                        text=f"{title}\n\n{text}".strip(),
                        created_at=parse_datetime_or_now(created_at_raw),
                        region_hint="rostov",
                        author_id=urlparse(portal_url).netloc,
                        raw_payload=payload,
                    )
                except Exception as error:
                    logger.warning("Ошибка обработки статьи %s: %s", article_url, error)

    def _fetch_soup(self, url: str) -> BeautifulSoup:
        response = self._session.get(url, timeout=self._timeout_seconds)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _extract_article_links(self, base_url: str, soup: BeautifulSoup) -> list[str]:
        base_domain = urlparse(base_url).netloc
        candidates: list[str] = []
        seen: set[str] = set()

        for link in soup.select("a[href]"):
            href = (link.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue

            absolute_url = urljoin(base_url, href)
            parsed = urlparse(absolute_url)
            if parsed.scheme not in {"http", "https"}:
                continue
            if base_domain not in parsed.netloc:
                continue
            if any(parsed.path.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf")):
                continue

            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
            if normalized in seen:
                continue

            seen.add(normalized)
            candidates.append(normalized)

        return candidates

    def _parse_article(self, article_url: str) -> tuple[str, str, str | None] | None:
        soup = self._fetch_soup(article_url)

        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)

        paragraphs = [p.get_text(" ", strip=True) for p in soup.select("p")]
        paragraphs = [p for p in paragraphs if p]
        text = "\n".join(paragraphs)

        if not title and not text:
            logger.debug("Пустая статья (без h1 и p): %s", article_url)
            return None

        created_at = self._extract_date(soup)
        return title, text, created_at

    @staticmethod
    def _extract_date(soup: BeautifulSoup) -> str | None:
        time_tag = soup.find("time")
        if time_tag:
            datetime_attr = time_tag.get("datetime")
            if datetime_attr:
                return datetime_attr
            time_text = time_tag.get_text(" ", strip=True)
            if time_text:
                return time_text

        for selector, attr in (
            ('meta[property="article:published_time"]', "content"),
            ('meta[name="pubdate"]', "content"),
            ('meta[name="publish_date"]', "content"),
            ('meta[name="date"]', "content"),
        ):
            tag = soup.select_one(selector)
            if tag and tag.get(attr):
                return str(tag.get(attr))

        return None

    def _matches_keywords(self, text: str) -> bool:
        if not self._keywords:
            return True
        lowered = text.lower()
        return any(keyword in lowered for keyword in self._keywords)


def parse_datetime_or_now(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, str) and value.strip():
        raw = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass

    return datetime.now(timezone.utc)


def build_portal_raw_message(appeal: RawPortalAppeal) -> RawMessage:
    collected_at = datetime.now(timezone.utc)

    return RawMessage(
        source_type=SourceType.PORTAL_APPEAL,
        source_id=appeal.appeal_id,
        parent_id=None,
        text=appeal.text,
        media_type=MediaType.TEXT,
        created_at_utc=appeal.created_at,
        collected_at=collected_at,
        author_id=appeal.author_id or "anonymous",
        is_official=False,
        reach=0,
        likes=0,
        reposts=0,
        comments_count=0,
        raw_payload=appeal.raw_payload,
    )


def save_document_jsonl(path: str, doc: RawMessage) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(doc.model_dump(), ensure_ascii=False, default=str) + "\n")


def build_loader(source_name: str, **kwargs: Any) -> PortalAppealLoader:
    """
    Фабрика загрузчиков по типу источника.

    Поддерживаемые источники:
      - mock_local_file
      - html_portals
    """

    if source_name == "mock_local_file":
        file_path = kwargs.get("file_path")
        if not file_path:
            raise ValueError("Для mock_local_file требуется file_path")
        return MockLocalFilePortalLoader(file_path=file_path)

    if source_name == "html_portals":
        portal_urls = kwargs.get("portal_urls") or []
        keywords = kwargs.get("keywords") or []
        max_articles_per_site = int(kwargs.get("max_articles_per_site") or DEFAULT_ARTICLES_PER_SITE)
        timeout_seconds = int(kwargs.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)

        return HtmlPortalLoader(
            portal_urls=portal_urls,
            keywords=keywords,
            max_articles_per_site=max_articles_per_site,
            timeout_seconds=timeout_seconds,
        )

    raise ValueError(f"Неизвестный тип источника PORTAL_SOURCE: {source_name}")


def main() -> None:
    validate_portal_config(CONFIG)
    loader = build_loader(
        CONFIG.portal_source,
        file_path=CONFIG.portal_appeals_file,
        portal_urls=PORTAL_URLS,
        keywords=PORTAL_KEYWORDS,
    )

    print(f"Источник обращений: {CONFIG.portal_source}")
    print(f"Портал: {CONFIG.portal_name}")
    if CONFIG.portal_source == "mock_local_file":
        print(f"Файл: {CONFIG.portal_appeals_file}")
    if CONFIG.portal_source == "html_portals":
        print(f"Сайтов для обхода: {len(PORTAL_URLS)}")
        print(f"Ключевых слов: {len(PORTAL_KEYWORDS)}")
    print("-" * 80)

    sent = 0

    for appeal in loader.iter_appeals():
        try:
            doc = build_portal_raw_message(appeal)
            send_document(CONFIG.kafka_topic, doc.model_dump())
            save_document_jsonl("documents.jsonl", doc)

            short_view = doc.model_dump()
            short_view.pop("raw_payload", None)

            print(json.dumps(short_view, ensure_ascii=False, indent=2, default=str))
            print("-" * 80)
            sent += 1
        except Exception as error:
            print(f"[portal={CONFIG.portal_name}] [appeal_id={appeal.appeal_id}] Ошибка обработки: {error}")
            print("-" * 80)

    print(f"Всего отправлено в Kafka: {sent}")


if __name__ == "__main__":
    main()


normalize_portal_appeal = build_portal_raw_message
