from datetime import datetime, timezone
from types import SimpleNamespace

import consumer
from schema import RawMessage


def _raw_payload() -> dict:
    return RawMessage(
        source_type="rss_article",
        source_id="ok-1",
        author_id=None,
        text="raw text",
        media_type="link",
        created_at_utc=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
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


class FakeKafkaProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []
        self.flush_calls = 0
        self.closed = False

    def send(self, topic: str, payload: dict) -> None:
        self.sent.append((topic, payload))

    def flush(self) -> None:
        self.flush_calls += 1

    def close(self) -> None:
        self.closed = True


def test_consumer_commits_after_successful_raw_persistence(monkeypatch) -> None:
    fake_consumer = FakeKafkaConsumer([_raw_payload()])
    fake_dlq_producer = FakeKafkaProducer()
    upserted_ids: list[str] = []

    monkeypatch.setattr(consumer, "KafkaConsumer", lambda *args, **kwargs: fake_consumer)
    monkeypatch.setattr(consumer, "KafkaProducer", lambda *args, **kwargs: fake_dlq_producer)
    monkeypatch.setattr(consumer, "validate_raw_consumer_config", lambda *_: None)
    monkeypatch.setattr(consumer, "upsert_raw_message", lambda msg: upserted_ids.append(msg.source_id))

    consumer.main()

    assert upserted_ids == ["ok-1"]
    assert fake_consumer.commit_calls == 1
    assert fake_dlq_producer.sent == []
    assert fake_consumer.closed is True
    assert fake_dlq_producer.closed is True


def test_consumer_does_not_commit_when_raw_persistence_fails_and_sends_to_dlq(monkeypatch) -> None:
    fake_consumer = FakeKafkaConsumer([_raw_payload()])
    fake_dlq_producer = FakeKafkaProducer()

    monkeypatch.setattr(consumer, "KafkaConsumer", lambda *args, **kwargs: fake_consumer)
    monkeypatch.setattr(consumer, "KafkaProducer", lambda *args, **kwargs: fake_dlq_producer)
    monkeypatch.setattr(consumer, "validate_raw_consumer_config", lambda *_: None)

    def _raise_on_upsert(_msg):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(consumer, "upsert_raw_message", _raise_on_upsert)

    consumer.main()

    assert fake_consumer.commit_calls == 0
    assert len(fake_dlq_producer.sent) == 1
    topic, payload = fake_dlq_producer.sent[0]
    assert topic == consumer.CONFIG.kafka_raw_dlq_topic
    assert payload["original_offset"] == 1
    assert payload["original_partition"] == 0
    assert "db unavailable" in payload["error"]
    assert payload["message"]["source_id"] == "ok-1"
    assert fake_dlq_producer.flush_calls == 1
    assert fake_consumer.closed is True
    assert fake_dlq_producer.closed is True
