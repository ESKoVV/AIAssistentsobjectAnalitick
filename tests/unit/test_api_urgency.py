from apps.api.public.config import UrgencyConfig
from apps.api.public.service import compute_urgency
from apps.api.public.repository import SnapshotItemRecord


def build_snapshot_item(**overrides) -> SnapshotItemRecord:
    payload = {
        "cluster_id": "cluster-1",
        "rank": 1,
        "score": 0.82,
        "summary": "summary",
        "category": "housing",
        "category_label": "ЖКХ",
        "key_phrases": ["phrase"],
        "mention_count": 140,
        "unique_authors": 90,
        "unique_sources": 3,
        "reach_total": 100000,
        "growth_rate": 1.0,
        "geo_regions": ["Ростов-на-Дону"],
        "score_breakdown": {
            "volume": 0.8,
            "dynamics": 0.7,
            "sentiment": 0.9,
            "reach": 0.6,
            "geo": 0.5,
            "source": 0.4,
        },
        "sample_doc_ids": ["doc-1"],
        "sentiment_score": -0.2,
        "is_new": False,
        "is_growing": False,
        "sources": [],
        "sample_posts": [],
        "timeline": [],
    }
    payload.update(overrides)
    return SnapshotItemRecord(**payload)


def test_compute_urgency_returns_critical_for_extreme_growth() -> None:
    urgency, reason = compute_urgency(
        build_snapshot_item(growth_rate=5.4, is_growing=True),
        UrgencyConfig(),
    )

    assert urgency.value == "critical"
    assert "рост" in reason


def test_compute_urgency_returns_medium_for_stable_mentions() -> None:
    urgency, reason = compute_urgency(
        build_snapshot_item(score=0.45, growth_rate=1.3),
        UrgencyConfig(),
    )

    assert urgency.value == "medium"
    assert reason == "устойчивое упоминание"
