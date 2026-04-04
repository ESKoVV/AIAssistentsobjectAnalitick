# Public API

Versioned public API for top-ranked clusters, history, verification documents, and health.

## Run

```bash
python -m uvicorn apps.api.public.server:app --host 0.0.0.0 --port 8000
```

Swagger is available at `http://localhost:8000/api/docs`.

## Main Environment Variables

- `DATABASE_URL` or `API_DATABASE_URL`
- `API_HOST`
- `API_PORT`
- `API_TOP_CONFIG_PATH`
- `API_REDIS_DSN`
- `API_KAFKA_BOOTSTRAP_SERVERS`
- `API_RANKINGS_UPDATED_TOPIC`
- `API_AUTH_DISABLED`
- `API_JWT_ISSUER`
- `API_JWT_AUDIENCE`
- `API_JWKS_URL`

## Main Endpoints

- `GET /api/v1/top`
- `GET /api/v1/top/{cluster_id}`
- `GET /api/v1/top/{cluster_id}/documents`
- `GET /api/v1/top/{cluster_id}/timeline`
- `GET /api/v1/history`
- `GET /api/v1/health`
- `GET /metrics`

Every response includes `X-API-Version` and `X-Request-ID`.
