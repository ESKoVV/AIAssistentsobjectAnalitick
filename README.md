# AIAssistentsobjectAnalitick

Платформа объективной аналитики региональной повестки на основе публичных сигналов: новости, соцсети, комментарии и обращения граждан.

Проект не про «чат-бота», а про **аудируемый data/ML pipeline** для поддержки решений: от сырых сообщений до объяснимого топа ключевых проблем региона.

---

## 1) Зачем этот проект

Органам управления и аналитическим центрам нужен единый контур, который:

- собирает сигналы из разных источников в реальном времени;
- нормализует данные в общий контракт;
- выделяет устойчивые темы/проблемы;
- генерирует нейтральные сводки без эмоциональной окраски;
- ранжирует проблемы прозрачной формулой (а не «черным ящиком»);
- показывает итог в удобном UI и API.

**Ключевая ценность:** объяснимость, трассируемость до первичных сообщений, воспроизводимость.

---

## 2) Что уже есть в репозитории

- Монорепо с разделением по слоям: ingestion, preprocessing, ml, ranking, api, ui.
- UI-дашборд (`apps/ui`) для обзора ленты, карточек и аналитики.
- Контракты и тестовые каркасы (`tests/unit`, `tests/integration`, `tests/contract`, `tests/eval`).
- Конфиги регионов, источников, таксономии и промптов (`configs/`).
- Инструменты ingestion/нормализации в `parser_project`.
- Snapshot-only public API для выдачи ranked top/issues из PostgreSQL в UI: `apps/api/public/server.py`.

---

## 3) Архитектурные принципы

1. **Детерминизм > эвристики LLM**, где это возможно.
2. LLM используется только там, где нужна семантика (summary/labeling и т.п.).
3. Ранжирование — максимально объяснимое и проверяемое.
4. Строгие границы ответственности между слоями.
5. Явные контракты данных между этапами пайплайна.

---

## 4) Канонический pipeline

1. Ingestion
2. Structural normalization
3. Language detection
4. Content filtering
5. Text cleaning
6. Deduplication
7. Geo enrichment
8. Metadata enrichment
9. Embedding generation
10. Semantic clustering
11. Optional classification
12. Cluster summarization
13. Importance ranking
14. Top-10 issue generation
15. API/UI delivery

Идея: каждая стадия делает **одну** понятную задачу, без скрытой логики из соседних слоев.

---

## 5) Production Status

Репозиторий приведён к одному production-контуру:

- каноническая storage-пара: `raw_messages` + `normalized_messages`
- downstream таблицы: `document_sentiments`, `embeddings`, `clusters`, `cluster_descriptions`, `rankings`
- поддерживаемые production-источники: только `VK` и `RSS`
- локальные production-модели:
  - embeddings: `intfloat/multilingual-e5-large`
  - sentiment: `blanchefort/rubert-base-cased-sentiment`
  - summarization: `Qwen/Qwen2.5-7B-Instruct` через self-hosted OpenAI-compatible endpoint (`vLLM`)

Старый `raw -> ml.documents -> ml_consumer -> ml_results` путь удалён из runtime topology.

## 6) Данные и контракт

Базовая сущность хранения сигналов — `normalized_messages` (PostgreSQL).
Типичные поля:

- идентификация: `doc_id`, `source_type`, `source_id`, `parent_id`
- контент: `text`, `media_type`, `raw_payload`
- время: `created_at`, `collected_at`, `inserted_at`
- автор/официальность: `author_id`, `is_official`
- метрики: `reach`, `likes`, `reposts`, `comments_count`
- гео: `region_hint`, `geo_lat`, `geo_lon`

Именно эта таблица используется как канонический источник данных для API и UI.

---

## 7) Структура репозитория

```text
.
├── apps/
│   ├── api/
│   │   └── public/                # локальный read-only API для UI
│   └── ui/                        # фронтенд-дашборд (Vite + React + TS)
├── configs/                       # источники, регионы, таксономия, промпты
├── docs/                          # архитектура, runbooks, ADR и т.д.
├── domain/                        # доменные модели/сервисы
├── parser_project/                # ingestion/normalization утилиты
├── storage/                       # postgres/kafka/redis артефакты
└── tests/                         # unit/integration/contract/eval
```

---

## 8) Локальный запуск

### 8.1 Требования

- Python 3.11+
- Node.js 20+
- PostgreSQL 14+
- Kafka
- `pgvector` на сервере PostgreSQL для production-контура

### 8.2 Переменные окружения

Минимально нужно задать:

- `DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME`

Для API (опционально):

- `API_HOST=0.0.0.0`
- `API_PORT=8000`

Для UI (`apps/ui/.env`):

- `VITE_API_BASE_URL=http://localhost:8000`

### 8.3 Production Bootstrap

Базовая последовательность для production-ready окружения:

```bash
python parser_project/backup_database.py
python parser_project/bootstrap_production.py
python parser_project/replay_raw_messages.py --dry-run
```

Полный k8s runbook находится в [production.md](/home/ilya/Project/AIAssistentsobjectAnalitick/docs/runbooks/production.md).

### 8.4 Запуск API

```bash
python apps/api/public/server.py
```

API поднимется на `http://localhost:8000`.

Основные production endpoint'ы:

- `GET /api/v1/top`
- `GET /api/v1/top/{cluster_id}`
- `GET /api/v1/top/{cluster_id}/documents`
- `GET /api/v1/top/{cluster_id}/timeline`
- `GET /api/v1/history`
- `GET /api/v1/health`
- `GET /metrics`

### 8.5 Запуск UI

```bash
cd apps/ui
npm install
npm run dev
```

Открыть в браузере адрес, который покажет Vite (обычно `http://localhost:5173`).

---

## 9) Что важно про UI

UI ориентирован на аналитический сценарий:

- фильтрация ленты по региону/периоду/тегам;
- просмотр карточки документа и метрик вовлеченности;
- работа с production-источниками `vk_*` и `rss_article`;
- локальный fallback на mock-данные при отсутствии API.

## 10) Оркестрация и деплой

В репозитории добавлены production-assets:

- Dockerfiles: [python-worker.Dockerfile](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/python-worker.Dockerfile), [api.Dockerfile](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/api.Dockerfile), [ui.Dockerfile](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/ui.Dockerfile)
- Docker Compose: [compose.production.yml](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/compose.production.yml) и [.env.production.example](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/docker/.env.production.example)
- Kubernetes manifests: [deploy/k8s/production](/home/ilya/Project/AIAssistentsobjectAnalitick/deploy/k8s/production)
- DB freshness exporter: [db_metrics_exporter.py](/home/ilya/Project/AIAssistentsobjectAnalitick/apps/ops/db_metrics_exporter.py)

## 11) Тестирование и качество

В репозитории есть каркас тестов разных уровней:

- `tests/unit` — модульная логика;
- `tests/integration` — связность пайплайна;
- `tests/contract` — контракты данных;
- `tests/eval` — quality/eval сценарии.

Рекомендуемый базовый цикл перед merge:

1. Проверка, что API отвечает корректно на фильтры и пагинацию.
2. Сборка UI (`npm run build`) без ошибок TS.
3. Валидация контрактов и отсутствие «тихих» schema-breaking изменений.

---

## 12) Acceptance Gate

Финальный production-cut считается собранным, если:

1. `bootstrap_production.py` успешно применяет каноническую схему.
2. `replay_raw_messages.py` подаёт в Kafka существующие `raw_messages`.
3. После steady-state не пусты `normalized_messages`, `document_sentiments`, `embeddings`, `clusters`, `cluster_descriptions`, `rankings`.
4. `/api/v1/health` не возвращает `ranking_unavailable`.
5. В critical runtime path нет legacy fallback и stub-only ML маршрута.

## 13) Дорожная карта (high-level)

- Поднять production-grade API слой (`apps/api`) с версионированием и схемами.
- Перенести ранжирование в формализованный explainable scoring модуль.
- Добавить полноценную оркестрацию realtime-контура ingestion → preprocessing → clustering.
- Расширить observability: freshness, latency, source coverage, drift-метрики.

---

## 14) Нефункциональные требования

- **Объяснимость:** любой issue должен быть расшифрован до первичных сигналов.
- **Надежность:** graceful degradation при недоступности LLM-компонентов.
- **Повторяемость:** одинаковый вход → одинаковый результат (где нет семантической генерации).
- **Безопасность:** API read-only по умолчанию для контура витрины.

---

## 15) Для разработчиков

- Не смешивайте бизнес-правила ранжирования с UI/API слоем.
- Не прячьте детерминированную логику в LLM-промптах.
- Любые изменения контрактов данных — только явно и с миграционными заметками.
- Если меняете архитектурно чувствительную часть, обновляйте `docs/` и тесты контрактов.

---

## 16) Лицензирование и использование

На текущем этапе репозиторий используется как рабочая кодовая база проекта.
Перед публичным распространением добавьте явную лицензию и политику использования данных.
