from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from apps.ml.ranking.schema import RankingMetrics, RankingsUpdatedEvent
from tests.helpers import build_ranked_cluster, build_score_breakdown


EXPECTED_SCORE_BREAKDOWN_FIELDS = {
    "volume_score",
    "dynamics_score",
    "sentiment_score",
    "reach_score",
    "geo_score",
    "source_score",
    "weights",
}

EXPECTED_RANKED_CLUSTER_FIELDS = {
    "cluster_id",
    "rank",
    "score",
    "score_breakdown",
    "summary",
    "key_phrases",
    "period_start",
    "period_end",
    "size",
    "mention_count",
    "growth_rate",
    "reach_total",
    "geo_regions",
    "unique_authors",
    "unique_sources",
    "sentiment_score",
    "is_new",
    "is_growing",
    "sample_doc_ids",
    "sources",
    "sample_posts",
    "timeline",
}

EXPECTED_EVENT_FIELDS = {
    "ranking_id",
    "computed_at",
    "period_start",
    "period_end",
    "top_n",
    "mode",
}

EXPECTED_METRICS_FIELDS = {
    "computed_at",
    "candidates_total",
    "candidates_excluded",
    "exclusion_reasons",
    "top10_score_range",
    "score_gap_1_2",
    "new_entries",
    "dropped_entries",
    "runtime_ms",
}


def test_score_breakdown_contract_contains_expected_fields() -> None:
    assert set(asdict(build_score_breakdown()).keys()) == EXPECTED_SCORE_BREAKDOWN_FIELDS


def test_ranked_cluster_contract_contains_expected_fields() -> None:
    assert set(asdict(build_ranked_cluster()).keys()) == EXPECTED_RANKED_CLUSTER_FIELDS


def test_rankings_updated_event_contract_contains_expected_fields() -> None:
    event = RankingsUpdatedEvent(
        ranking_id="ranking-1",
        computed_at=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        period_start=datetime(2026, 4, 4, 0, 0, tzinfo=UTC),
        period_end=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        top_n=10,
    )

    assert set(asdict(event).keys()) == EXPECTED_EVENT_FIELDS
    assert event.mode == "descriptions_updated"


def test_ranking_metrics_contract_contains_expected_fields() -> None:
    metrics = RankingMetrics(
        computed_at=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        candidates_total=12,
        candidates_excluded=2,
        exclusion_reasons={"too_small": 1, "stale": 1},
        top10_score_range=(0.55, 0.92),
        score_gap_1_2=0.14,
        new_entries=3,
        dropped_entries=1,
        runtime_ms=42.0,
    )

    assert set(asdict(metrics).keys()) == EXPECTED_METRICS_FIELDS
