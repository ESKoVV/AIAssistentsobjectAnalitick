from datetime import datetime, timezone

import db
from schema import RawMessage


def test_upsert_raw_message_uses_conflict_by_source_type_source_id(monkeypatch) -> None:
    executed = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, payload):
            executed["query"] = query
            executed["payload"] = payload

        def fetchone(self):
            return ("11111111-1111-1111-1111-111111111111",)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    class FakeCtx:
        def __enter__(self):
            return FakeConn()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(db, "get_connection", lambda: FakeCtx())

    message = RawMessage(
        source_type="rss_article",
        source_id="abc",
        author_id="u1",
        text="text",
        media_type="link",
        created_at_utc=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        collected_at=datetime(2026, 4, 4, 12, 1, tzinfo=timezone.utc),
        raw_payload={"id": "abc"},
    )

    raw_message_id = db.upsert_raw_message(message)

    assert str(raw_message_id) == "11111111-1111-1111-1111-111111111111"
    assert "ON CONFLICT (source_type, source_id)" in executed["query"]
    assert executed["payload"]["source_id"] == "abc"
    assert executed["payload"]["raw_payload"]


def test_find_raw_message_id_returns_uuid_when_row_exists(monkeypatch) -> None:
    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, payload):
            self.query = query
            self.payload = payload

        def fetchone(self):
            return ("22222222-2222-2222-2222-222222222222",)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    class FakeCtx:
        def __enter__(self):
            return FakeConn()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(db, "get_connection", lambda: FakeCtx())

    raw_message_id = db.find_raw_message_id(source_type="vk_post", source_id="abc")

    assert str(raw_message_id) == "22222222-2222-2222-2222-222222222222"
