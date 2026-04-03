# regional-analytics-assistant

Monorepo-style structure:

- `apps/ui` — frontend dashboard (React + Vite + TS).
- `apps/api` — API services.
- `apps/ingestion`, `apps/preprocessing`, `apps/ml` — data pipeline and ML services.
- `domain` — domain model and business services.
- `storage` — infrastructure adapters and schemas.
- `configs`, `prompts`, `docs`, `tests`, `notebooks` — configuration, prompts, docs, QA, research.

## UI quick start

```bash
cd apps/ui
npm install
npm run dev
```
