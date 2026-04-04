from kafka.errors import NodeNotReadyError, NoBrokersAvailable

import create_topic


def test_create_admin_client_retries_after_node_not_ready(monkeypatch) -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []
    expected_client = object()

    def _fake_admin_client(**_kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise NodeNotReadyError(1)
        return expected_client

    monkeypatch.setattr(create_topic, "KafkaAdminClient", _fake_admin_client)
    monkeypatch.setattr(create_topic.time, "sleep", lambda delay: sleeps.append(delay))

    client = create_topic._create_admin_client(max_attempts=5, retry_delay_seconds=0.25)

    assert client is expected_client
    assert attempts["count"] == 3
    assert sleeps == [0.25, 0.25]


def test_create_admin_client_raises_runtime_error_after_exhausted_retries(monkeypatch) -> None:
    monkeypatch.setattr(create_topic, "KafkaAdminClient", lambda **_kwargs: (_ for _ in ()).throw(NoBrokersAvailable()))
    monkeypatch.setattr(create_topic.time, "sleep", lambda _delay: None)

    try:
        create_topic._create_admin_client(max_attempts=2, retry_delay_seconds=0)
    except RuntimeError as exc:
        assert "после повторных попыток" in str(exc)
    else:
        raise AssertionError("RuntimeError was expected when retries are exhausted.")
