# Сервис объективной аналитики региональной повестки

Платформа собирает публичные сигналы из новостей, социальных сетей и обращений граждан, нормализует их в единый контракт, выделяет устойчивые темы и формирует объяснимый рейтинг ключевых проблем региона.

## Реализованная функциональность

- сбор сообщений из источников `VK` и `RSS`;
- публикация сырых сообщений в Kafka и сохранение в PostgreSQL;
- структурная нормализация, фильтрация, очистка и дедупликация документов;
- enrichment по географии и метаданным;
- генерация эмбеддингов и определение тональности документов;
- кластеризация сообщений по смыслу;
- генерация кратких описаний кластеров;
- ранжирование проблем по прозрачным метрикам;
- read-only API для выдачи топа проблем, истории, таймлайна и документов кластера;
- web-интерфейс для аналитика с картой, графиками, фильтрами и карточками тем;
- dev-only mock-режим UI через `VITE_USE_MOCKS=true`.

## Особенность проекта в следующем

- решение построено как auditable data/ML pipeline, а не как чат-бот с непрозрачной логикой;
- основная сущность хранения и обмена между слоями стандартизирована через канонический контракт сообщений;
- ранжирование и выдача опираются на объяснимые признаки: объем, динамика, охват, география, источники и тональность;
- фронтенд и API разделены с ingestion/ML-контуром, поэтому витрину можно разворачивать независимо;
- предусмотрен локальный и контейнерный запуск, включая production-compose для полного пайплайна.

## Основной стек технологий

- Python 3.11+/3.12;
- FastAPI, Uvicorn, Pydantic;
- PostgreSQL, `psycopg`, `pgvector`;
- Apache Kafka, Kafka UI, `kafka-python`, `aiokafka`;
- React 18, TypeScript, Vite;
- Tailwind CSS, React Query, Recharts, Leaflet;
- Hugging Face Transformers, Torch;
- self-hosted `AlicaGPT` для summarization в российском контуре;
- Docker, Docker Compose, Nginx.

## Демо

- локальный UI доступен по адресу `http://localhost:8081` после контейнерного запуска;
- локальный API Swagger доступен по адресу `http://localhost:8000/api/docs`;
- по умолчанию в production-compose авторизация для API отключена через `API_AUTH_DISABLED=true`, отдельный тестовый пользователь не требуется;
- для локальной разработки можно явно включить mock-режим UI через `VITE_USE_MOCKS=true`.

## Среда запуска

1. Windows 10/11, Linux или macOS с установленным Docker Desktop или Docker Engine.
2. Для локального запуска без Docker: Python 3.11+, Node.js 20+, PostgreSQL 14+ и Kafka.
3. Для production-контура требуется PostgreSQL с доступной базой данных; в текущем compose-файле база поднимается вне docker-compose.
4. Для полного ML-контура желательно достаточно RAM/CPU; профиль `llm` дополнительно поднимает self-hosted `AlicaGPT`.

## Установка и запуск

### 1. Клонирование репозитория

```bash
git clone <repo_url>
cd AIAssistentsobjectAnalitick
```

### 2. Подготовка переменных окружения для Docker

PowerShell:

```powershell
Copy-Item deploy/docker/.env.production.example deploy/docker/.env
```

Bash:

```bash
cp deploy/docker/.env.production.example deploy/docker/.env
```

Минимально нужно задать `DATABASE_URL` в файле `deploy/docker/.env`.

Пример:

```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
```

Если PostgreSQL запущен на хост-машине и к нему должны подключаться контейнеры, обычно используется адрес вида:

```env
DATABASE_URL=postgresql://USER:PASSWORD@host.docker.internal:5432/DBNAME
```

### 3. Контейнерный запуск

#### Быстрый запуск витрины и API

```bash
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.production.yml --profile llm up --build -d kafka kafka-ui api ui
```

После запуска будут доступны:

- UI: `http://localhost:8081`
- API: `http://localhost:8000`
- Swagger: `http://localhost:8000/api/docs`
- Kafka UI: `http://localhost:8080`

#### Полный запуск пайплайна

```bash
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.production.yml --profile llm --profile sources up --build -d
```

#### Инициализация production-схемы

```bash
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.production.yml --profile bootstrap up --build bootstrap create-topics
```

### 4. Быстрое демо только UI на mock-данных

```bash
docker build -f deploy/docker/ui.Dockerfile --build-arg VITE_USE_MOCKS=true -t regional-ui .
docker run --rm -p 8081:80 regional-ui
```

После этого интерфейс будет доступен по адресу `http://localhost:8081`.

## Локальный запуск без Docker

### Backend API

Установить зависимости:

```bash
pip install -e .
```

Запустить API:

```bash
python apps/api/public/server.py
```

### Frontend

```bash
cd apps/ui
npm install
npm run dev
```

### Production bootstrap scripts

```bash
python parser_project/backup_database.py
python parser_project/bootstrap_production.py
python parser_project/replay_raw_messages.py --dry-run
```

## Основные API endpoints

- `GET /api/v1/top`
- `GET /api/v1/top/export`
- `GET /api/v1/top/geo`
- `GET /api/v1/top/{cluster_id}`
- `GET /api/v1/top/{cluster_id}/documents`
- `GET /api/v1/top/{cluster_id}/timeline`
- `GET /api/v1/history`
- `GET /api/v1/health`
- `GET /metrics`

## Структура репозитория

```text
.
├── apps/
│   ├── api/
│   │   └── public/
│   └── ui/
├── configs/
├── deploy/
│   ├── docker/
│   └── k8s/
├── docs/
├── domain/
├── parser_project/
├── storage/
└── tests/
```

## Разработчики

Клименко Илья витальевич - data analyst, ML @ler0yyyy.
Еськов Владимир Сергеевич - data science, парсинг. @Koksaralya
Вовченко Артем Алексеевич - Full-Stack, UI-UX
