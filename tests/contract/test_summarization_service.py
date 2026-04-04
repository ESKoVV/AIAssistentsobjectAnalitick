from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from apps.ml.summarization.schema import ClusterDescription, DescriptionsUpdatedEvent
from tests.helpers import build_cluster_description


EXPECTED_DESCRIPTION_FIELDS = {
    "cluster_id",
    "summary",
    "key_phrases",
    "sample_doc_ids",
    "model_name",
    "prompt_version",
    "generated_at",
    "input_token_count",
    "output_token_count",
    "generation_time_ms",
    "fallback_used",
}

EXPECTED_EVENT_FIELDS = {
    "run_at",
    "updated_cluster_ids",
    "mode",
}


def test_cluster_description_contract_contains_expected_fields() -> None:
    description = build_cluster_description()

    assert isinstance(description, ClusterDescription)
    assert set(asdict(description).keys()) == EXPECTED_DESCRIPTION_FIELDS


def test_descriptions_updated_event_contains_updated_ids_and_mode() -> None:
    event = DescriptionsUpdatedEvent(
        run_at=datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc),
        updated_cluster_ids=["cluster-1", "cluster-2"],
    )

    assert set(asdict(event).keys()) == EXPECTED_EVENT_FIELDS
    assert event.updated_cluster_ids == ["cluster-1", "cluster-2"]
    assert event.mode == "batch_refresh"
