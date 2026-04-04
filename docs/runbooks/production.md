# Production Runbook

## Scope

Этот runbook описывает production-контур для единственного канонического pipeline:

`raw.documents` -> `raw_messages` -> preprocessing -> `normalized_messages` -> embeddings -> clustering -> summarization -> ranking -> API/UI`

Поддерживаемые источники в первом production-cut:

- `VK`
- `RSS`

Отключённые источники в production:

- `MAX`
- `portal_appeal`
- любые legacy/raw-to-ml обходные пути

## 1. Prerequisites

- Kubernetes-кластер с Ingress и TLS.
- Внешний PostgreSQL: `postgresql://hack_user:hack_pass@192.168.0.103:5432/hack_db`
- На сервер PostgreSQL установлен `pgvector`, чтобы `CREATE EXTENSION vector` проходил успешно.
- Docker-образы собраны из:
  - [python-worker.Dockerfile](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/python-worker.Dockerfile)
  - [api.Dockerfile](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/api.Dockerfile)
  - [ui.Dockerfile](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/ui.Dockerfile)

Для локального production-like запуска через Docker Compose используйте:

- compose file: [compose.production.yml](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/compose.production.yml)
- env template: [.env.production.example](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/.env.production.example)

## 2. Backup Before Schema Rebuild

Сначала сохраните бэкап:

```bash
python parser_project/backup_database.py
```

Если `pg_dump` недоступен в окружении, выполните эквивалентную команду вручную с тем же `DATABASE_URL`.

## 3. Bootstrap

1. Создайте namespace и базовые объекты:

```bash
kubectl apply -f deploy/k8s/production/namespace.yaml
kubectl apply -f deploy/k8s/production/configmap.yaml
kubectl apply -f deploy/k8s/production/secret.example.yaml
kubectl apply -f deploy/k8s/production/kafka.yaml
```

2. Запустите bootstrap схемы:

```bash
kubectl apply -f deploy/k8s/production/bootstrap-jobs.yaml
kubectl logs -n regional-analytics job/production-bootstrap
```

Bootstrap делает следующее:

- применяет SQL-миграции из `parser_project/sql`
- создаёт `document_sentiments`
- требует `vector` и `pgcrypto`
- инициирует downstream-таблицы `embeddings`, `clusters`, `cluster_descriptions`, `rankings` и связанные snapshot-таблицы

## 4. Backfill Existing raw_messages

После bootstrap выполните одноразовый replay:

```bash
kubectl logs -n regional-analytics job/replay-raw-messages
```

Если нужен dry-run вне Kubernetes:

```bash
python parser_project/replay_raw_messages.py --dry-run
```

## 5. Rollout Runtime

```bash
kubectl apply -f deploy/k8s/production/workers.yaml
kubectl apply -f deploy/k8s/production/api-ui.yaml
kubectl apply -f deploy/k8s/production/ingress.yaml
kubectl apply -f deploy/k8s/production/monitoring.yaml
```

## Docker Compose Shortcut

Подготовьте env:

```bash
cp deploy/docker/.env.production.example deploy/docker/.env.production
```

Потом:

```bash
docker compose --env-file deploy/docker/.env.production -f deploy/docker/compose.production.yml --profile bootstrap run --rm db-backup
docker compose --env-file deploy/docker/.env.production -f deploy/docker/compose.production.yml --profile bootstrap run --rm bootstrap
docker compose --env-file deploy/docker/.env.production -f deploy/docker/compose.production.yml --profile bootstrap run --rm create-topics
docker compose --env-file deploy/docker/.env.production -f deploy/docker/compose.production.yml up -d kafka kafka-ui consumer preprocessing-consumer vk-collector rss-collector sentiment-consumer embedding-consumer clustering-full clustering-incremental summarization-consumer ranking-consumer api ui db-metrics-exporter
```

Если нужен локальный self-hosted LLM внутри compose, добавьте профиль `llm`:

```bash
docker compose --env-file deploy/docker/.env.production -f deploy/docker/compose.production.yml --profile llm up -d vllm
```

## 6. Acceptance Checks

Проверить таблицы:

```sql
SELECT COUNT(*) FROM normalized_messages;
SELECT COUNT(*) FROM document_sentiments;
SELECT COUNT(*) FROM embeddings;
SELECT COUNT(*) FROM clusters;
SELECT COUNT(*) FROM cluster_descriptions;
SELECT COUNT(*) FROM rankings;
```

Ожидается, что после replay ни один downstream-этап не остаётся пустым.

Проверить API:

- `GET /api/v1/health`
- `GET /api/v1/top?period=24h&limit=10`
- `GET /metrics`

## 7. Rollback

- Остановить rollout новых worker deployment/CronJob.
- Восстановить PostgreSQL из backup, созданного перед bootstrap.
- Повторно выполнить bootstrap только после выяснения причины сбоя.

## 8. Incident Checks

Сначала проверьте:

- freshness метрик в `db-metrics-exporter`
- consumer lag в Kafka UI
- наличие свежих snapshot'ов ranking и cluster descriptions
- ошибки JWT/JWKS в API-логах
