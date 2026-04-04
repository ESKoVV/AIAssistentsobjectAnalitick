from datetime import UTC, datetime

from apps.api.public.cache import TopCache
from apps.api.schemas.top import TopQueryParams, TopResponse


def test_cache_key_is_stable_for_equivalent_query_params() -> None:
    cache = TopCache(redis_dsn=None, ttl_seconds=300)
    params_a = TopQueryParams(period="24h", limit=10, region="Ростов-на-Дону")
    params_b = TopQueryParams(limit=10, region="Ростов-на-Дону", period="24h")

    assert cache.cache_key(params_a) == cache.cache_key(params_b)


def test_cache_round_trip_uses_pydantic_json_serialization() -> None:
    cache = TopCache(redis_dsn=None, ttl_seconds=300)
    params = TopQueryParams(period="24h", limit=5)
    payload = TopResponse(
        computed_at=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        period_start=datetime(2026, 4, 3, 12, 0, tzinfo=UTC),
        period_end=datetime(2026, 4, 4, 12, 0, tzinfo=UTC),
        total_clusters=0,
        items=[],
    )

    cache.set(params, payload)

    assert cache.get(params) == payload
