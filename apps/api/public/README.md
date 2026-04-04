# Public API (local)

Локальный read-only API для выгрузки новостей из PostgreSQL в UI.

## Запуск

```bash
cd /workspace/AIAssistentsobjectAnalitick
python apps/api/public/server.py
```

Сервер поднимается на `http://localhost:8000` по умолчанию.

## Переменные окружения

- `DATABASE_URL` — строка подключения к PostgreSQL (обязательная).
- `API_HOST` — хост API (по умолчанию `0.0.0.0`).
- `API_PORT` — порт API (по умолчанию `8000`).

## Методы

- `GET /api/documents?page=1&limit=20&region=...&date_from=YYYY-MM-DD&date_to=YYYY-MM-DD`
- `GET /api/documents/{doc_id}`
- `GET /api/regions`

Все методы read-only.
