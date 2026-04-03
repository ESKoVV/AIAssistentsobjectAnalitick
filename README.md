# regional-analytics-assistant

Целевая структура репозитория (как в ТЗ):

```text
regional-analytics-assistant/
│
├── AGENT.md
├── README.md
├── pyproject.toml
├── .env.example
├── docker-compose.yml
│
├── configs/
│   ├── sources.yaml
│   ├── regions.yaml
│   ├── taxonomy.yaml
│   ├── ranking.yaml
│   └── prompts/
│       ├── summarization.md
│       ├── topic_labeling.md
│       └── relevance_check.md
│
├── apps/
│   ├── ingestion/
│   │   ├── rss/
│   │   ├── telegram/
│   │   ├── vk/
│   │   └── portals/
│   ├── preprocessing/
│   │   ├── normalization/
│   │   ├── language/
│   │   ├── cleaning/
│   │   ├── deduplication/
│   │   ├── geo_enrichment/
│   │   └── enrichment/
│   ├── ml/
│   │   ├── embeddings/
│   │   ├── clustering/
│   │   ├── classification/
│   │   ├── summarization/
│   │   ├── ranking/
│   │   └── evaluation/
│   ├── orchestration/
│   │   ├── consumers/
│   │   ├── pipelines/
│   │   └── schedulers/
│   ├── api/
│   │   ├── public/
│   │   ├── internal/
│   │   └── schemas/
│   └── ui/
│
├── domain/
│   ├── models/
│   ├── entities/
│   ├── value_objects/
│   └── services/
│
├── storage/
│   ├── postgres/
│   ├── redis/
│   └── kafka/
│
├── prompts/
│   ├── system/
│   ├── task/
│   └── validators/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── contract/
│   └── eval/
│
├── docs/
│   ├── architecture/
│   ├── data_contracts/
│   ├── decisions/
│   ├── runbooks/
│   └── metrics/
│
└── notebooks/
    ├── exploration/
    └── experiments/
```

## UI запуск

```bash
cd apps/ui
npm install
npm run dev
```


## Миграция из legacy-раскладки

Если в корне всё ещё остались `src/`, `index.html`, `package.json` или `parser_project/`, запусти:

```bash
./scripts/rehome_legacy_layout.sh
./scripts/verify_structure.sh
```
