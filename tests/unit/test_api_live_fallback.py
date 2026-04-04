from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.api.public.repository import (
    LiveSourceRow,
    _build_live_cluster_id,
    _build_live_snapshot_payload,
    _parse_live_cluster_id,
)


def _build_row(
    *,
    doc_id: str,
    created_at: datetime,
    category: str = "housing",
    category_label: str = "ЖКХ",
    region: str | None = "Ростов-на-Дону",
    source_type: str = "vk_post",
    author_id: str = "author-1",
    reach: int = 100,
    summary: str | None = None,
) -> LiveSourceRow:
    return LiveSourceRow(
        doc_id=doc_id,
        source_id=doc_id,
        source_type=source_type,
        author_id=author_id,
        text="Во дворе нет горячей воды и жители ждут восстановления подачи.",
        created_at=created_at,
        collected_at=created_at,
        reach=reach,
        likes=10,
        reposts=1,
        comments_count=2,
        is_official=False,
        parent_id=None,
        region=region,
        raw_payload={},
        category=category,
        category_label=category_label,
        ml_summary=summary,
        ml_score=0.8,
        ml_processed_at=created_at + timedelta(minutes=5),
    )


def test_live_cluster_id_round_trip_supports_cyrillic_region() -> None:
    cluster_id = _build_live_cluster_id("housing", "Ростов-на-Дону")

    assert _parse_live_cluster_id(cluster_id) == ("housing", "Ростов-на-Дону")


def test_build_live_snapshot_payload_groups_documents_into_live_items() -> None:
    now = datetime(2026, 4, 5, 12, 0, tzinfo=UTC)
    rows = [
        _build_row(doc_id="doc-1", created_at=now - timedelta(hours=1), author_id="author-1", reach=150),
        _build_row(doc_id="doc-2", created_at=now - timedelta(hours=2), author_id="author-2", reach=120),
        _build_row(
            doc_id="doc-3",
            created_at=now - timedelta(hours=3),
            category="roads",
            category_label="Дороги и транспорт",
            region="Батайск",
            source_type="portal_appeal",
            summary="Жители жалуются на длительные задержки автобусов.",
        ),
    ]

    snapshot, items = _build_live_snapshot_payload(
        rows,
        period_hours=24,
        period_start=now - timedelta(hours=24),
        period_end=now,
        category=None,
        limit=10,
    )

    assert snapshot.period_hours == 24
    assert len(items) == 2

    top_item = items[0]
    assert top_item.category == "housing"
    assert top_item.category_label == "ЖКХ"
    assert top_item.mention_count == 2
    assert top_item.unique_authors == 2
    assert top_item.geo_regions == ["Ростов-на-Дону"]
    assert len(top_item.sample_posts) >= 1
