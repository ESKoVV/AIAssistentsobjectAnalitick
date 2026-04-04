from __future__ import annotations

from dataclasses import replace

from apps.ml.embeddings.serde import deserialize_enriched_document, serialize_document
from apps.preprocessing.filtering import FilterStatus
from tests.helpers import build_enriched_document


def test_serde_round_trip_preserves_anomaly_fields() -> None:
    document = replace(
        build_enriched_document(),
        filter_status=FilterStatus.REVIEW,
        quality_weight=0.05,
        anomaly_flags=("author_burst", "bot_timing"),
        anomaly_confidence=0.95,
    )

    restored = deserialize_enriched_document(serialize_document(document))

    assert restored.filter_status is FilterStatus.REVIEW
    assert restored.quality_weight == 0.05
    assert restored.anomaly_flags == ("author_burst", "bot_timing")
    assert restored.anomaly_confidence == 0.95
