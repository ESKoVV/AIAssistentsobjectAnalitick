from datetime import datetime, timezone

import db
from schema import RawDocument


def test_compression_roundtrip() -> None:
    text = "Очень длинный текст"
    payload = {"k": "v", "n": 1}

    text_compressed = db.compress_text(text)
    payload_compressed = db.compress_raw_payload(payload)

    assert text_compressed
    assert payload_compressed
    assert db.decompress_text(text_compressed) == text
    assert db.decompress_raw_payload(payload_compressed) == payload


def test_upsert_raw_document_uses_conflict_by_source_type_source_id(monkeypatch) -> None:
    executed = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, payload):
            executed["query"] = query
            executed["payload"] = payload

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    class FakeCtx:
        def __enter__(self):
            return FakeConn()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(db, "get_raw_connection", lambda: FakeCtx())

    doc = RawDocument(
        source_type="rss_article",
        source_id="abc",
        parent_source_id=None,
        author_raw="u1",
        text_raw="text",
        media_type="link",
        created_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        raw_payload={"id": "abc"},
    )

    db.upsert_raw_document(doc)

    assert "ON CONFLICT (source_type, source_id)" in executed["query"]
    assert executed["payload"]["source_id"] == "abc"
    assert executed["payload"]["text_compressed"]
    assert executed["payload"]["raw_payload_compressed"]
