from __future__ import annotations

import json
from hashlib import md5
from typing import Any

from apps.api.schemas.top import TopQueryParams, TopResponse


class TopCache:
    def __init__(self, redis_dsn: str | None, *, ttl_seconds: int, namespace: str = "top:v1") -> None:
        self._ttl_seconds = ttl_seconds
        self._namespace = namespace
        self._memory_store: dict[str, str] = {}
        self._redis = self._build_client(redis_dsn)

    def cache_key(self, params: TopQueryParams) -> str:
        normalized = params.model_dump(mode="json", exclude_none=True)
        payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
        return f"{self._namespace}:{md5(payload.encode('utf-8')).hexdigest()}"

    def get(self, params: TopQueryParams) -> TopResponse | None:
        key = self.cache_key(params)
        raw = self._redis.get(key) if self._redis else self._memory_store.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return TopResponse.model_validate_json(str(raw))

    def set(self, params: TopQueryParams, response: TopResponse) -> None:
        key = self.cache_key(params)
        payload = response.model_dump_json()
        if self._redis:
            self._redis.setex(key, self._ttl_seconds, payload)
            return
        self._memory_store[key] = payload

    def invalidate_all(self) -> None:
        if self._redis:
            cursor = 0
            keys: list[str] = []
            pattern = f"{self._namespace}:*"
            while True:
                cursor, batch = self._redis.scan(cursor=cursor, match=pattern, count=100)
                keys.extend(batch)
                if cursor == 0:
                    break
            if keys:
                self._redis.delete(*keys)
            return
        self._memory_store.clear()

    def _build_client(self, redis_dsn: str | None):  # type: ignore[no-untyped-def]
        if not redis_dsn:
            return None
        try:
            import redis
        except ImportError:
            return None
        return redis.Redis.from_url(redis_dsn)
