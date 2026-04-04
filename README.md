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


## Локальный запуск ingestion-пайплайна (Docker Compose)

Добавлен полноценный стенд в `parser_project/docker-compose.yml`:
- Kafka + Zookeeper + Kafka UI
- PostgreSQL (с автоинициализацией схемы из `parser_project/sql/*.sql`)
- `consumer`
- `vk-collector`
- `rss-collector`

### 1) Подготовка окружения

```bash
cd parser_project
cp .env.example .env
```

Проверьте/заполните минимум:
- `VK_TOKEN`
- `VK_GROUP_DOMAINS`
- `RSS_FEEDS`

Для Postgres можно оставить дефолты, либо задать:
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

### 2) Поднять весь стенд

```bash
docker compose up -d --build
```

### 3) Проверить состояние сервисов

```bash
docker compose ps
```

### 4) Смотреть логи

```bash
docker compose logs -f consumer
docker compose logs -f vk-collector
docker compose logs -f rss-collector
```

### 5) Остановить стенд

```bash
docker compose down
```

Если нужно удалить volume с БД:

```bash
docker compose down -v
```
