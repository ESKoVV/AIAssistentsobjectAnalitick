# Региональный аналитический дашборд

> Фронтенд-витрина для системы мониторинга публичных сообщений о жизни региона — соцсети, новости, обращения граждан.

---

## О проекте

Сайт отображает результаты работы бэкенд-системы: аналитику публичных сообщений из ВКонтакте, Telegram, RSS-лент и порталов обращений граждан. Предназначен для сотрудников органов власти регионального уровня — строгий, информативный интерфейс без лишнего шума.

Фронтенд разработан на хакатоне как независимый слой, подключаемый к REST API (Python/FastAPI). На старте работает на мок-данных; переключение на боевой бэкенд — одной переменной окружения.

---

## Стек

| Слой | Технология |
|---|---|
| Фронтенд | React + Vite |
| Стили | Tailwind CSS |
| Роутинг | React Router v6 |
| Запросы к API | TanStack Query (React Query) |
| Графики | Recharts |
| Иконки | lucide-react |
| Язык | TypeScript |
| Бэкенд API | REST (Python/FastAPI) |

---

## Быстрый старт

```bash
# Установка зависимостей
npm install

# Запуск в режиме разработки (на мок-данных)
npm run dev

# Сборка для продакшна
npm run build
```

По умолчанию сайт работает на мок-данных из `src/mocks/`. Чтобы подключить реальный бэкенд:

```bash
# .env
VITE_API_BASE_URL=http://localhost:8000
```

После этого все запросы уйдут на реальный API без изменений в коде компонентов.

---

## Структура проекта

```
src/
├── api/                  # Обёртки над fetch/axios
│   ├── documents.ts
│   ├── topics.ts
│   └── stats.ts
├── mocks/                # JSON-файлы с мок-данными
│   ├── documents.json
│   ├── topics.json
│   └── stats.json
├── components/
│   ├── layout/
│   │   ├── Header.tsx
│   │   ├── Sidebar.tsx
│   │   └── PageWrapper.tsx
│   ├── ui/
│   │   ├── KpiCard.tsx
│   │   ├── DocumentCard.tsx
│   │   ├── TopicCard.tsx
│   │   ├── SourceBadge.tsx
│   │   ├── UrgencyIndicator.tsx
│   │   ├── FilterPanel.tsx
│   │   ├── Pagination.tsx
│   │   ├── LoadingState.tsx
│   │   ├── ErrorState.tsx
│   │   └── EmptyState.tsx
│   └── charts/
│       ├── ActivityBarChart.tsx
│       ├── TimelineLineChart.tsx
│       ├── SourcePieChart.tsx
│       └── RegionBarChart.tsx
├── pages/
│   ├── Dashboard.tsx
│   ├── Feed.tsx
│   ├── Topics.tsx
│   ├── DocumentDetail.tsx
│   └── Analytics.tsx
├── hooks/
│   ├── useDocuments.ts
│   ├── useTopics.ts
│   └── useStats.ts
├── types/
│   └── index.ts
├── utils/
│   ├── formatDate.ts
│   ├── sourceLabels.ts
│   └── urgencyColor.ts
├── App.tsx
└── main.tsx
```

---

## Страницы

| Маршрут | Страница | Описание |
|---|---|---|
| `/` | Dashboard | Быстрый обзор за текущие сутки: KPI-плашки, топ-5 тем, график активности по часам, последние 10 документов |
| `/feed` | Лента документов | Полный поток с фильтрацией по источнику, региону, дате; пагинация по 20 записей |
| `/topics` | Топ тем | Top-10 тем за выбранный период с индикатором срочности и примерами документов |
| `/document/:id` | Карточка документа | Полный просмотр: мета-данные, текст, метрики охвата, ссылка на родительский документ, raw JSON |
| `/analytics` | Аналитика | Графики: динамика по дням (30 дней), распределение по источникам, топ-10 регионов, сводная таблица |

---

## API-контракт

Фронтенд ожидает следующие эндпоинты:

### `GET /api/documents`
Query params: `page`, `limit`, `source_type`, `region`, `date_from`, `date_to`

```ts
{
  items: NormalizedDocument[],
  total: number,
  page: number,
  limit: number
}
```

### `GET /api/documents/:id`
Возвращает один `NormalizedDocument`.

### `GET /api/topics`
Query params: `limit` (default 10), `date_from`, `date_to`

```ts
{
  items: {
    rank: number,
    title: string,
    summary: string,
    doc_count: number,
    source_types: string[],
    urgency_score: number,   // 0.0–1.0
    sample_doc_ids: string[]
  }[]
}
```

### `GET /api/stats`
```ts
{
  total_docs: number,
  docs_last_24h: number,
  by_source: { source_type: string, count: number }[],
  by_region: { region: string, count: number }[],
  timeline: { date: string, count: number }[]   // последние 30 дней
}
```

### Тип `NormalizedDocument`

```ts
interface NormalizedDocument {
  doc_id: string
  source_type: 'vk_post' | 'vk_comment' | 'telegram_post' | 'telegram_comment' | 'rss_article' | 'portal_appeal'
  source_id: string
  parent_id: string | null
  text: string
  media_type: 'text' | 'photo' | 'video' | 'link'
  created_at: string       // ISO 8601 UTC
  collected_at: string
  author_id: string
  is_official: boolean
  reach: number
  likes: number
  reposts: number
  comments_count: number
  region_hint: string | null
  geo_lat: number | null
  geo_lon: number | null
  raw_payload: object
}
```

---

## Визуальный стиль

- **Тема:** тёмная, строгая, государственная
- **Фон:** `#0f172a` (тёмно-синий)
- **Акцент:** `#3b82f6` (холодный синий)
- **Срочность:** зелёный / жёлтый / красный по порогам `0–0.4` / `0.4–0.7` / `0.7–1.0`
- **Типографика:** моноширинные акценты для цифр и ID

---

## Архитектурные решения

**Мок-режим vs реальный API** — все запросы проходят через функции в `src/api/`. При отсутствии `VITE_API_BASE_URL` они возвращают данные из `src/mocks/*.json`. Логика компонентов не меняется.

**React Query** — кеширование, состояния загрузки/ошибки и фоновое обновление данных вынесены из компонентов в кастомные хуки (`useDocuments`, `useTopics`, `useStats`).

**Фильтры** на странице `/feed` синхронизированы с URL-параметрами через React Router, что позволяет сохранять и шарить ссылки на отфильтрованные выборки.

---

## Что не входит в фронтенд

Парсинг и сбор данных, ML-обработка текстов, база данных и бэкенд API, авторизация — ответственность бэкенд-команды.

---

## Лицензия

MIT
