from datetime import datetime, timezone
from types import SimpleNamespace

import consumer
from schema import RawDocument


def _raw_payload() -> dict:
    return RawDocument(
        doc_id="rss_article:ok-1",
        source_type="rss_article",
        source_id="ok-1",
        parent_source_id=None,
        text_raw="raw text",
        title_raw="title",
        author_raw=None,
        created_at_raw="Wed, 03 Apr 2024 10:00:00 +0300",
        created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        source_url="https://example.com/item",
        source_domain="example.com",
        region_hint_raw=None,
        geo_raw=None,
        engagement_raw={},
        raw_payload={"id": "ok-1"},
    ).model_dump(mode="json")


class FakeKafkaConsumer:
    def __init__(self, messages: list[dict]) -> None:
        self._messages = messages
        self.commit_calls = 0
        self.closed = False

    def __iter__(self):
        for payload in self._messages:
            yield SimpleNamespace(value=payload, topic="raw.documents", offset=1, partition=0)

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.closed = True


def test_consumer_commits_after_successful_upsert(monkeypatch) -> None:
    fake_consumer = FakeKafkaConsumer([_raw_payload()])
    upserted_doc_ids: list[str] = []

    monkeypatch.setattr(consumer, "KafkaConsumer", lambda *args, **kwargs: fake_consumer)
    monkeypatch.setattr(consumer, "upsert_raw_document", lambda doc: upserted_doc_ids.append(doc.doc_id))
    monkeypatch.setattr(consumer, "save_failed_message", lambda raw, error: None)

    consumer.main()

    assert upserted_doc_ids == ["rss_article:ok-1"]
    assert fake_consumer.commit_calls == 1
    assert fake_consumer.closed is True


def test_consumer_does_not_commit_when_upsert_fails(monkeypatch) -> None:
    fake_consumer = FakeKafkaConsumer([_raw_payload()])
    failures: list[str] = []

    monkeypatch.setattr(consumer, "KafkaConsumer", lambda *args, **kwargs: fake_consumer)

    def _raise_on_upsert(_doc):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(consumer, "upsert_raw_document", _raise_on_upsert)
    monkeypatch.setattr(consumer, "save_failed_message", lambda raw, error: failures.append(error))

    consumer.main()

    assert fake_consumer.commit_calls == 0
    assert any("db unavailable" in error for error in failures)
    assert fake_consumer.closed is True
