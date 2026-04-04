from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from apps.ml.ranking import (
    InMemoryRankingRepository,
    RankingDocumentRecord,
    RankingService,
    RankingServiceConfig,
    explain_rank,
    normalize_dynamics,
)
from tests.helpers import build_cluster, build_ranked_cluster, build_stored_cluster_description


def test_service_ranks_clusters_and_tracks_exclusions() -> None:
    now = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    repository = InMemoryRankingRepository()

    cluster_1 = build_cluster(
        cluster_id="cluster-1",
        doc_ids=["c1-doc-1", "c1-doc-2"],
        size=80,
        unique_sources=3,
        reach_total=100000,
        growth_rate=3.2,
        geo_regions=["volgograd-oblast", "astrakhan-oblast", "rostov-oblast"],
        earliest_doc_at=now - timedelta(hours=1),
        latest_doc_at=now - timedelta(minutes=10),
    )
    cluster_2 = build_cluster(
        cluster_id="cluster-2",
        doc_ids=["c2-doc-1", "c2-doc-2"],
        size=120,
        unique_sources=1,
        reach_total=120000,
        growth_rate=1.1,
        geo_regions=["volgograd-oblast"],
        earliest_doc_at=now - timedelta(hours=10),
        latest_doc_at=now - timedelta(minutes=5),
    )
    cluster_3 = build_cluster(
        cluster_id="cluster-3",
        doc_ids=["c3-doc-1"],
        size=4,
        unique_sources=1,
        reach_total=5000,
        growth_rate=2.8,
        geo_regions=["astrakhan-oblast"],
        earliest_doc_at=now - timedelta(hours=2),
        latest_doc_at=now - timedelta(minutes=15),
    )
    repository.clusters = {
        cluster_1.cluster_id: cluster_1,
        cluster_2.cluster_id: cluster_2,
        cluster_3.cluster_id: cluster_3,
    }
    repository.descriptions = {
        cluster_1.cluster_id: build_stored_cluster_description(cluster_id=cluster_1.cluster_id),
        cluster_2.cluster_id: build_stored_cluster_description(cluster_id=cluster_2.cluster_id),
        cluster_3.cluster_id: build_stored_cluster_description(cluster_id=cluster_3.cluster_id),
    }
    repository.document_sentiments = {
        "c1-doc-1": -0.9,
        "c1-doc-2": -0.7,
        "c2-doc-1": 0.8,
        "c2-doc-2": 0.5,
        "c3-doc-1": -0.6,
    }
    repository.document_reaches = {
        "c1-doc-1": 60000,
        "c1-doc-2": 40000,
        "c2-doc-1": 80000,
        "c2-doc-2": 40000,
        "c3-doc-1": 5000,
    }

    service = RankingService(
        repository=repository,
        config=RankingServiceConfig(postgres_dsn=None, top_n=2),
    )
    service.initialize()

    result = service.refresh_current_window(now=now)

    assert [item.cluster_id for item in result.items] == ["cluster-1", "cluster-2"]
    assert result.items[0].rank == 1
    assert result.items[0].is_new is True
    assert result.items[0].is_growing is True
    assert result.items[0].score > result.items[1].score
    assert result.metrics.candidates_total == 3
    assert result.metrics.candidates_excluded == 1
    assert result.metrics.exclusion_reasons == {"too_small": 1}
    assert result.metrics.new_entries == 2
    assert result.metrics.dropped_entries == 0
    assert repository.rankings[-1].ranking.ranking_id == result.ranking.ranking_id


def test_normalize_dynamics_penalizes_stagnation_and_caps_extreme_growth() -> None:
    assert normalize_dynamics(0.5) == 0.0
    assert normalize_dynamics(1.0) == pytest.approx(0.3)
    assert normalize_dynamics(5.0) == pytest.approx(1.0)
    assert normalize_dynamics(10.0) == pytest.approx(1.0)


def test_explain_rank_mentions_reasons_and_main_factors() -> None:
    explanation = explain_rank(build_ranked_cluster())

    assert explanation.startswith("Ранг 1:")
    assert "рост в 3.2x за последние 6 часов" in explanation
    assert "охватывает 2 районов" in explanation
    assert "Основные факторы:" in explanation


def test_quality_weight_downweights_cluster_aggregation() -> None:
    now = datetime(2026, 4, 4, 12, 0, tzinfo=UTC)
    repository = InMemoryRankingRepository()
    low_weight_cluster = build_cluster(
        cluster_id="cluster-low",
        doc_ids=["low-1", "low-2"],
        size=2,
        unique_sources=1,
        unique_authors=2,
        reach_total=2000,
        growth_rate=2.0,
        earliest_doc_at=now - timedelta(hours=2),
        latest_doc_at=now - timedelta(minutes=10),
    )
    healthy_cluster = build_cluster(
        cluster_id="cluster-high",
        doc_ids=["high-1", "high-2"],
        size=2,
        unique_sources=1,
        unique_authors=2,
        reach_total=2000,
        growth_rate=2.0,
        earliest_doc_at=now - timedelta(hours=2),
        latest_doc_at=now - timedelta(minutes=10),
    )
    repository.clusters = {
        low_weight_cluster.cluster_id: low_weight_cluster,
        healthy_cluster.cluster_id: healthy_cluster,
    }
    repository.descriptions = {
        low_weight_cluster.cluster_id: build_stored_cluster_description(cluster_id=low_weight_cluster.cluster_id),
        healthy_cluster.cluster_id: build_stored_cluster_description(cluster_id=healthy_cluster.cluster_id),
    }
    repository.documents = {
        "low-1": RankingDocumentRecord(
            doc_id="low-1",
            source_id="low-1",
            author_id="author-1",
            source_type="vk_post",
            text="Жители пишут об отключении воды",
            created_at=now - timedelta(minutes=30),
            reach=1000,
            region="volgograd-oblast",
            raw_payload={},
            quality_weight=0.1,
            sentiment_score=-0.5,
        ),
        "low-2": RankingDocumentRecord(
            doc_id="low-2",
            source_id="low-2",
            author_id="author-2",
            source_type="vk_post",
            text="Жители пишут об отключении воды",
            created_at=now - timedelta(minutes=20),
            reach=1000,
            region="volgograd-oblast",
            raw_payload={},
            quality_weight=0.1,
            sentiment_score=-0.5,
        ),
        "high-1": RankingDocumentRecord(
            doc_id="high-1",
            source_id="high-1",
            author_id="author-3",
            source_type="vk_post",
            text="Жители пишут об отключении воды",
            created_at=now - timedelta(minutes=30),
            reach=1000,
            region="volgograd-oblast",
            raw_payload={},
            quality_weight=1.0,
            sentiment_score=-0.5,
        ),
        "high-2": RankingDocumentRecord(
            doc_id="high-2",
            source_id="high-2",
            author_id="author-4",
            source_type="vk_post",
            text="Жители пишут об отключении воды",
            created_at=now - timedelta(minutes=20),
            reach=1000,
            region="volgograd-oblast",
            raw_payload={},
            quality_weight=1.0,
            sentiment_score=-0.5,
        ),
    }

    service = RankingService(
        repository=repository,
        config=RankingServiceConfig(
            postgres_dsn=None,
            top_n=2,
            min_cluster_size_for_ranking=1,
            source_type_count=1,
        ),
    )
    service.initialize()

    result = service.refresh_current_window(now=now)
    ranked_ids = [item.cluster_id for item in result.items]
    low_item = next(item for item in result.items if item.cluster_id == "cluster-low")
    high_item = next(item for item in result.items if item.cluster_id == "cluster-high")

    assert ranked_ids == ["cluster-high", "cluster-low"]
    assert low_item.mention_count == 2
    assert low_item.reach_total < high_item.reach_total
    assert low_item.score < high_item.score
