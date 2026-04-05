import {
  ClusterDetailResponse,
  ClusterDocumentsFilters,
  ClusterDocumentsResponse,
  GeoResponse,
  HealthResponse,
  HistoryResponse,
  TimelineResponse,
  TopFilters,
  TopResponse
} from '../types';
import { fetchJson, useMocks } from './client';

const mockGeoPointsByRegion: Record<string, { lat: number; lon: number }> = {
  'Ростов-на-Дону': { lat: 47.2357, lon: 39.7015 },
  Таганрог: { lat: 47.2086, lon: 38.9369 },
  Батайск: { lat: 47.1398, lon: 39.7518 },
  Аксай: { lat: 47.2676, lon: 39.8756 },
  Азов: { lat: 47.1078, lon: 39.4165 },
  Новочеркасск: { lat: 47.4223, lon: 40.0939 },
  Шахты: { lat: 47.7091, lon: 40.2158 },
  Волгоград: { lat: 48.7071, lon: 44.5169 },
  Волжский: { lat: 48.7858, lon: 44.7797 },
  Камышин: { lat: 50.0827, lon: 45.4074 }
};

const mockTopItems: TopResponse['items'] = [
  {
    rank: 1,
    cluster_id: 'cluster-water-1',
    summary: 'Жители массово жалуются на перебои с горячей водой и затянутые сроки восстановления подачи.',
    dashboard_reason: 'Всплеск жалоб за короткий период, максимальный рост и широкая география по трём городам.',
    category: 'housing',
    category_label: 'ЖКХ',
    key_phrases: ['нет горячей воды', 'аварийные работы', 'сроки восстановления', 'жалобы жителей', 'отключение'],
    urgency: 'critical',
    urgency_reason: 'рост в 5.6x за 6 часов',
    mention_count: 184,
    unique_authors: 121,
    reach_total: 341200,
    growth_rate: 5.6,
    is_new: true,
    is_growing: true,
    geo_regions: ['Ростов-на-Дону', 'Батайск', 'Аксай'],
    sources: [
      { source_type: 'vk_post', count: 122 },
      { source_type: 'rss_article', count: 34 },
      { source_type: 'portal_appeal', count: 28 }
    ],
    sample_posts: [{ doc_id: 'vk_post:water-1', text_preview: 'На Западном второй день нет горячей воды, сроки снова перенесли.', source_type: 'vk_post', created_at: '2026-04-04T10:20:00Z', reach: 12400, source_url: 'https://vk.com/wall-1_101' }],
    score: 0.91,
    score_breakdown: { volume: 0.92, dynamics: 1, sentiment: 0.88, reach: 0.76, geo: 0.66, source: 0.58 }
  },
  {
    rank: 2,
    cluster_id: 'cluster-transport-2',
    summary: 'В соцсетях и медиа обсуждаются массовые задержки общественного транспорта в утренний час пик.',
    dashboard_reason: 'Высокий охват и устойчивый поток сигналов в транспортной теме второй день подряд.',
    category: 'roads',
    category_label: 'Дороги и транспорт',
    key_phrases: ['задержки автобусов', 'час пик', 'маршруты', 'переполненные остановки', 'транспорт'],
    urgency: 'high',
    urgency_reason: 'высокий суммарный балл 0.78',
    mention_count: 132,
    unique_authors: 96,
    reach_total: 228000,
    growth_rate: 2.7,
    is_new: false,
    is_growing: true,
    geo_regions: ['Ростов-на-Дону', 'Азов'],
    sources: [{ source_type: 'vk_post', count: 91 }, { source_type: 'portal_appeal', count: 41 }],
    sample_posts: [{ doc_id: 'vk_post:transport-1', text_preview: 'Автобус 71 снова не пришёл по расписанию, на остановке скопились люди.', source_type: 'vk_post', created_at: '2026-04-04T08:10:00Z', reach: 9200, source_url: 'https://vk.com/wall-1_202' }],
    score: 0.78,
    score_breakdown: { volume: 0.81, dynamics: 0.72, sentiment: 0.74, reach: 0.63, geo: 0.41, source: 0.33 }
  },
  {
    rank: 3,
    cluster_id: 'cluster-health-3',
    summary: 'Появился устойчивый поток сообщений о нехватке записи к профильным врачам в поликлиниках.',
    dashboard_reason: 'Тема стабильно держится в лидерах по длительности и повторяемости обращений граждан.',
    category: 'health',
    category_label: 'Здравоохранение',
    key_phrases: ['нет записи', 'поликлиника', 'врач', 'талонов нет', 'ожидание'],
    urgency: 'medium',
    urgency_reason: 'устойчивое упоминание',
    mention_count: 67,
    unique_authors: 54,
    reach_total: 114800,
    growth_rate: 1.4,
    is_new: false,
    is_growing: false,
    geo_regions: ['Таганрог', 'Ростов-на-Дону'],
    sources: [{ source_type: 'vk_post', count: 39 }, { source_type: 'portal_appeal', count: 28 }],
    sample_posts: [{ doc_id: 'portal_appeal:health-1', text_preview: 'Невозможно записаться к эндокринологу уже вторую неделю.', source_type: 'portal_appeal', created_at: '2026-04-04T07:00:00Z', reach: 1400, source_url: 'https://example.test/appeal/1' }],
    score: 0.49,
    score_breakdown: { volume: 0.55, dynamics: 0.34, sentiment: 0.62, reach: 0.41, geo: 0.38, source: 0.27 }
  },
  {
    rank: 4,
    cluster_id: 'cluster-education-4',
    summary: 'В обращениях граждан фиксируется рост жалоб на состояние школьной инфраструктуры перед экзаменами.',
    dashboard_reason: 'Большая доля официальных обращений и подтверждённых проблем в нескольких муниципалитетах.',
    category: 'education',
    category_label: 'Образование',
    key_phrases: ['ремонт школы', 'протечки', 'подготовка к ЕГЭ', 'условия обучения', 'обращения'],
    urgency: 'high',
    urgency_reason: 'рост обращений в 2.1x за сутки',
    mention_count: 59,
    unique_authors: 45,
    reach_total: 90400,
    growth_rate: 2.1,
    is_new: true,
    is_growing: true,
    geo_regions: ['Таганрог', 'Аксай'],
    sources: [{ source_type: 'portal_appeal', count: 31 }, { source_type: 'vk_post', count: 28 }],
    sample_posts: [{ doc_id: 'portal_appeal:edu-1', text_preview: 'Просим ускорить ремонт кровли в школе №54 перед началом экзаменов.', source_type: 'portal_appeal', created_at: '2026-04-04T06:10:00Z', reach: 1200, source_url: 'https://example.test/appeal/54' }],
    score: 0.62,
    score_breakdown: { volume: 0.61, dynamics: 0.69, sentiment: 0.65, reach: 0.4, geo: 0.37, source: 0.52 }
  },
  {
    rank: 5,
    cluster_id: 'cluster-ecology-5',
    summary: 'Жители обсуждают локальные выбросы рядом с промышленными площадками и требуют проверки.',
    dashboard_reason: 'Высокая вовлечённость в соцсетях и негативная тональность подняли тему в верхнюю часть рейтинга.',
    category: 'ecology',
    category_label: 'Экология',
    key_phrases: ['загрязнение воздуха', 'выбросы', 'проверка', 'промзона', 'экологи'],
    urgency: 'high',
    urgency_reason: 'рост негативных сообщений и охвата',
    mention_count: 74,
    unique_authors: 58,
    reach_total: 172500,
    growth_rate: 2.3,
    is_new: true,
    is_growing: true,
    geo_regions: ['Новочеркасск', 'Ростов-на-Дону'],
    sources: [{ source_type: 'telegram_post', count: 43 }, { source_type: 'rss_article', count: 31 }],
    sample_posts: [{ doc_id: 'telegram_post:eco-1', text_preview: 'Экологи сообщили о превышении показателей в районе промзоны, ждём итоги проверки.', source_type: 'telegram_post', created_at: '2026-04-04T09:35:00Z', reach: 11800, source_url: null }],
    score: 0.59,
    score_breakdown: { volume: 0.63, dynamics: 0.61, sentiment: 0.71, reach: 0.64, geo: 0.32, source: 0.36 }
  },
  {
    rank: 6,
    cluster_id: 'cluster-digital-6',
    summary: 'Пользователи сообщают о нестабильной работе новых цифровых сервисов в МФЦ и порталах госуслуг.',
    dashboard_reason: 'Тема затрагивает массовый пользовательский сервис и быстро набирает повторные обращения.',
    category: 'digital',
    category_label: 'Цифровизация',
    key_phrases: ['МФЦ онлайн', 'сбой сервиса', 'госуслуги', 'авторизация', 'заявка'],
    urgency: 'medium',
    urgency_reason: 'рост повторных обращений',
    mention_count: 51,
    unique_authors: 47,
    reach_total: 126900,
    growth_rate: 1.9,
    is_new: false,
    is_growing: true,
    geo_regions: ['Ростов-на-Дону', 'Батайск'],
    sources: [{ source_type: 'rss_article', count: 19 }, { source_type: 'portal_appeal', count: 32 }],
    sample_posts: [{ doc_id: 'portal_appeal:digital-1', text_preview: 'Не получается подать заявление в электронном МФЦ, форма зависает на последнем шаге.', source_type: 'portal_appeal', created_at: '2026-04-04T08:55:00Z', reach: 980, source_url: 'https://example.test/digital/1' }],
    score: 0.54,
    score_breakdown: { volume: 0.52, dynamics: 0.58, sentiment: 0.6, reach: 0.48, geo: 0.35, source: 0.31 }
  },
  {
    rank: 7,
    cluster_id: 'cluster-safety-7',
    summary: 'Обсуждается недостаточное освещение и безопасность пешеходных переходов у социальных объектов.',
    dashboard_reason: 'Есть официальные сигналы и высокий общественный резонанс по риску безопасности.',
    category: 'safety',
    category_label: 'Безопасность',
    key_phrases: ['пешеходный переход', 'освещение', 'камеры', 'безопасность', 'школа'],
    urgency: 'medium',
    urgency_reason: 'стабильный поток обращений',
    mention_count: 46,
    unique_authors: 38,
    reach_total: 82400,
    growth_rate: 1.6,
    is_new: false,
    is_growing: true,
    geo_regions: ['Шахты', 'Азов'],
    sources: [{ source_type: 'portal_appeal', count: 27 }, { source_type: 'vk_post', count: 19 }],
    sample_posts: [{ doc_id: 'portal_appeal:safety-1', text_preview: 'Просим добавить освещение у перехода возле стадиона и установить камеры.', source_type: 'portal_appeal', created_at: '2026-04-04T07:25:00Z', reach: 870, source_url: 'https://example.test/safety/1' }],
    score: 0.5,
    score_breakdown: { volume: 0.48, dynamics: 0.52, sentiment: 0.57, reach: 0.36, geo: 0.34, source: 0.41 }
  },
  {
    rank: 8,
    cluster_id: 'cluster-culture-8',
    summary: 'Фестивальные площадки и культурные события вызвали заметный отклик в региональных пабликах.',
    dashboard_reason: 'Высокий охват и активность публикаций позволяют удерживать тему в топ-10 новостей.',
    category: 'culture',
    category_label: 'Культура',
    key_phrases: ['фестиваль', 'музеи', 'театры', 'мероприятия', 'выходные'],
    urgency: 'low',
    urgency_reason: 'высокий охват при низкой негативности',
    mention_count: 44,
    unique_authors: 35,
    reach_total: 162300,
    growth_rate: 1.2,
    is_new: true,
    is_growing: false,
    geo_regions: ['Ростов-на-Дону'],
    sources: [{ source_type: 'vk_post', count: 33 }, { source_type: 'telegram_post', count: 11 }],
    sample_posts: [{ doc_id: 'vk_post:culture-1', text_preview: 'В городе стартовал фестиваль культуры с открытыми площадками в выходные.', source_type: 'vk_post', created_at: '2026-04-04T10:05:00Z', reach: 13200, source_url: 'https://vk.com/wall-1_777' }],
    score: 0.46,
    score_breakdown: { volume: 0.47, dynamics: 0.4, sentiment: 0.29, reach: 0.72, geo: 0.21, source: 0.3 }
  },
  {
    rank: 9,
    cluster_id: 'cluster-agro-9',
    summary: 'В агросекторе активно обсуждают темпы посевной кампании и распределение техники.',
    dashboard_reason: 'Тема вошла в топ из-за крупного охвата официальных публикаций и отраслевой значимости.',
    category: 'agro',
    category_label: 'АПК',
    key_phrases: ['посевная', 'зерновые', 'техника', 'аграрии', 'урожай'],
    urgency: 'low',
    urgency_reason: 'большой охват официальных сообщений',
    mention_count: 41,
    unique_authors: 29,
    reach_total: 154200,
    growth_rate: 1.1,
    is_new: false,
    is_growing: false,
    geo_regions: ['Азов', 'Аксай'],
    sources: [{ source_type: 'vk_post', count: 24 }, { source_type: 'rss_article', count: 17 }],
    sample_posts: [{ doc_id: 'vk_post:agro-1', text_preview: 'В Азовском районе стартовал весенний этап посевной кампании.', source_type: 'vk_post', created_at: '2026-04-04T09:05:00Z', reach: 16300, source_url: 'https://vk.com/wall-1_923' }],
    score: 0.44,
    score_breakdown: { volume: 0.43, dynamics: 0.37, sentiment: 0.33, reach: 0.69, geo: 0.24, source: 0.38 }
  },
  {
    rank: 10,
    cluster_id: 'cluster-economy-10',
    summary: 'Пользователи обсуждают рост цен на социально значимые товары и доступность локальных ярмарок.',
    dashboard_reason: 'Тема закрывает топ-10 благодаря высокой социальной чувствительности и устойчивому обсуждению.',
    category: 'economy',
    category_label: 'Экономика',
    key_phrases: ['цены', 'ярмарка', 'продукты', 'доступность', 'инфляция'],
    urgency: 'medium',
    urgency_reason: 'социально чувствительная тема с регулярными сигналами',
    mention_count: 39,
    unique_authors: 33,
    reach_total: 98700,
    growth_rate: 1.3,
    is_new: false,
    is_growing: false,
    geo_regions: ['Ростов-на-Дону', 'Волгоград'],
    sources: [{ source_type: 'vk_post', count: 21 }, { source_type: 'portal_appeal', count: 18 }],
    sample_posts: [{ doc_id: 'vk_post:economy-1', text_preview: 'В районах обсуждают стоимость базовой корзины и график муниципальных ярмарок.', source_type: 'vk_post', created_at: '2026-04-04T06:55:00Z', reach: 7400, source_url: null }],
    score: 0.43,
    score_breakdown: { volume: 0.42, dynamics: 0.39, sentiment: 0.53, reach: 0.45, geo: 0.31, source: 0.29 }
  }
];

const mockTopResponse = (filters: TopFilters = {}): TopResponse => {
  const filteredItems = mockTopItems
    .filter((item) => !filters.region || item.geo_regions.includes(filters.region))
    .filter((item) => !filters.source || item.sources.some((entry) => entry.source_type === filters.source))
    .filter((item) => !filters.category || item.category === filters.category);
  const items = filteredItems
    .slice(0, filters.limit ?? 10)
    .map((item, index) => ({ ...item, rank: index + 1 }));

  return {
    computed_at: '2026-04-04T12:00:00Z',
    period_start:
      filters.period === '6h'
        ? '2026-04-04T06:00:00Z'
        : filters.period === '72h'
          ? '2026-04-01T12:00:00Z'
          : '2026-04-03T12:00:00Z',
    period_end: '2026-04-04T12:00:00Z',
    total_clusters: filteredItems.length,
    items
  };
};

const mockClusterDetail = (clusterId: string): ClusterDetailResponse => {
  const base = mockTopItems.find((item) => item.cluster_id === clusterId) ?? mockTopItems[0];
  return {
    ...base,
    sample_doc_ids: [
      ...base.sample_posts.map((item) => item.doc_id),
      'vk_post:extra-1',
      'rss_article:extra-2',
      'portal_appeal:extra-3'
    ],
    all_regions: base.geo_regions,
    timeline: Array.from({ length: 24 }).map((_, index) => ({
      hour: new Date(Date.UTC(2026, 3, 3, index)).toISOString(),
      count: Math.max(0, 4 + Math.round(Math.sin(index / 2) * 5) + (index > 18 ? 10 : 0)),
      reach: 8000 + index * 1200,
      growth_rate: index === 0 ? 0 : 1 + (index % 4) * 0.3
    }))
  };
};

const mockClusterDocuments = (clusterId: string, filters: ClusterDocumentsFilters = {}): ClusterDocumentsResponse => {
  const page = filters.page ?? 1;
  const pageSize = filters.page_size ?? 20;
  const documentTemplates = [
    'Жители сообщают о перебоях в подаче воды и просят назвать точные сроки восстановления.',
    'В обсуждениях фиксируются задержки общественного транспорта в часы пик.',
    'Поступают обращения по состоянию школьной инфраструктуры перед экзаменационным периодом.',
    'Пользователи обсуждают доступность записи к профильным врачам в поликлиниках.',
    'В пабликах обсуждается состояние воздуха рядом с промышленной зоной.',
    'Граждане просят усилить освещение и безопасность у пешеходных переходов.',
    'Отмечаются обращения по стабильности работы цифровых сервисов МФЦ.',
    'Публикуются сообщения о стоимости социально значимых товаров в районах.'
  ];
  const regions = ['Ростов-на-Дону', 'Батайск', 'Таганрог', 'Аксай', 'Азов', 'Волгоград', 'Камышин'];
  const sourceTypes = ['portal_appeal', 'vk_post', 'telegram_post', 'rss_article'] as const;

  const items = Array.from({ length: 120 }).map((_, index) => {
    const dayOffset = index % 100;
    const createdAt = new Date(Date.UTC(2026, 0, 1 + dayOffset, 6 + (index % 14), (index * 7) % 60));
    const sourceType = sourceTypes[index % sourceTypes.length];

    return {
      doc_id: `${clusterId}:doc-${index + 1}`,
      source_id: `${index + 1}`,
      source_type: sourceType,
      author_id: `author-${(index % 37) + 1}`,
      text: `Новость ${index + 1} по кластеру ${clusterId}. ${documentTemplates[index % documentTemplates.length]}`,
      text_preview: `Новость ${index + 1}: ${documentTemplates[index % documentTemplates.length]}`,
      created_at: createdAt.toISOString(),
      collected_at: new Date(createdAt.getTime() + 45 * 1000).toISOString(),
      reach: 500 + index * 55,
      likes: 4 + (index % 120),
      reposts: index % 9,
      comments_count: 1 + (index % 17),
      is_official: index % 9 === 0,
      parent_id: null,
      region: regions[index % regions.length],
      source_url: sourceType === 'rss_article' || sourceType === 'portal_appeal'
        ? `https://example.test/news/${index + 1}`
        : `https://vk.com/wall-1_${index + 1}`,
      raw_payload: { mock: true, batch: 'extended-news-seed' }
    };
  });
  const filtered = items
    .filter((item) => !filters.source_type || item.source_type === filters.source_type)
    .filter((item) => !filters.region || item.region === filters.region);
  const start = (page - 1) * pageSize;
  return {
    cluster_id: clusterId,
    page,
    page_size: pageSize,
    total: filtered.length,
    items: filtered.slice(start, start + pageSize)
  };
};

const mockHistoryResponse = (): HistoryResponse => ({
  from_dt: '2026-03-29T00:00:00Z',
  to_dt: '2026-04-04T12:00:00Z',
  granularity: 'daily',
  buckets: Array.from({ length: 7 }).map((_, index) => ({
    bucket_start: new Date(Date.UTC(2026, 2, 29 + index, 0, 0)).toISOString(),
    bucket_end: new Date(Date.UTC(2026, 2, 30 + index, 0, 0)).toISOString(),
    computed_at: new Date(Date.UTC(2026, 2, 29 + index, 12, 0)).toISOString(),
    items: mockTopItems.slice(0, 2).map((item, itemIndex) => ({
      ...item,
      rank: itemIndex + 1,
      score: Number((item.score - index * 0.03).toFixed(2))
    }))
  }))
});

const mockHealthResponse: HealthResponse = {
  status: 'ok',
  last_ranking_at: '2026-04-04T12:00:00Z',
  ranking_age_minutes: 7,
  documents_last_hour: 312,
  pipeline_status: {
    embedding: 'ok',
    clustering: 'ok',
    summarization: 'ok',
    ranking: 'ok'
  }
};

const mockGeoResponse = (filters: TopFilters = {}): GeoResponse => {
  const top = mockTopResponse(filters);
  return {
    clusters: top.items.map((item) => ({
      cluster_id: item.cluster_id,
      summary: item.summary,
      category_label: item.category_label,
      rank: item.rank,
      geo_regions: item.geo_regions,
      mention_count: item.mention_count,
      urgency: item.urgency,
      geo_points: item.geo_regions.flatMap((region) => {
        const point = mockGeoPointsByRegion[region];
        return point ? [{ region, ...point }] : [];
      })
    }))
  };
};

const createPlaceholderTopItem = (rank: number): TopResponse['items'][number] => ({
  rank,
  cluster_id: `placeholder-cluster-${rank}`,
  summary: `Заглушка темы #${rank}: дополнительные сигналы ожидают подтверждения.`,
  dashboard_reason: 'Добавлено как временная заглушка, чтобы интерфейс всегда показывал топ-10 новостей.',
  category: 'other',
  category_label: 'Прочее',
  key_phrases: ['заглушка', 'мониторинг', 'сигнал', 'новость'],
  urgency: 'low',
  urgency_reason: 'недостаточно данных от upstream, показана временная запись',
  mention_count: 8 + rank,
  unique_authors: 5 + rank,
  reach_total: 1200 + rank * 250,
  growth_rate: 1,
  is_new: true,
  is_growing: false,
  geo_regions: ['Ростов-на-Дону'],
  sources: [{ source_type: 'vk_post', count: 5 + rank }],
  sample_posts: [
    {
      doc_id: `placeholder-doc-${rank}`,
      text_preview: 'Техническая заглушка карточки темы для заполнения топ-10.',
      source_type: 'vk_post',
      created_at: '2026-04-04T12:00:00Z',
      reach: 500 + rank * 10,
      source_url: null
    }
  ],
  score: 0.2,
  score_breakdown: {
    volume: 0.2,
    dynamics: 0.2,
    sentiment: 0.2,
    reach: 0.2,
    geo: 0.2,
    source: 0.2
  }
});

const ensureTopItems = (response: TopResponse, requestedLimit = 10): TopResponse => {
  const target = Math.max(requestedLimit, 10);
  const normalized = response.items.map((item, index) => ({
    ...item,
    rank: index + 1
  }));
  const placeholders = Array.from({ length: Math.max(0, target - normalized.length) }).map((_, index) =>
    createPlaceholderTopItem(normalized.length + index + 1)
  );
  const items = [...normalized, ...placeholders].slice(0, target).map((item, index) => ({ ...item, rank: index + 1 }));
  return {
    ...response,
    total_clusters: Math.max(response.total_clusters, items.length),
    items
  };
};

const ensureDocumentItems = (
  response: ClusterDocumentsResponse,
  filters: ClusterDocumentsFilters = {}
): ClusterDocumentsResponse => {
  const target = 100;
  if (response.total >= target) return response;

  const synthetic = Array.from({ length: target }).map((_, index) => {
    const createdAt = new Date(Date.UTC(2025, 8, 1 + index, 8 + (index % 10), (index * 11) % 60));
    return {
      doc_id: `${response.cluster_id}:seed-${index + 1}`,
      source_id: `seed-${index + 1}`,
      source_type: index % 2 === 0 ? 'vk_post' : 'portal_appeal',
      author_id: `seed-author-${(index % 27) + 1}`,
      text: `Заглушка новости ${index + 1}: поток публикаций для непустой аналитики.`,
      text_preview: `Заглушка новости ${index + 1}: поток публикаций для непустой аналитики.`,
      created_at: createdAt.toISOString(),
      collected_at: new Date(createdAt.getTime() + 30 * 1000).toISOString(),
      reach: 400 + index * 45,
      likes: index % 100,
      reposts: index % 9,
      comments_count: index % 20,
      is_official: index % 8 === 0,
      parent_id: null,
      region: index % 2 === 0 ? 'Ростов-на-Дону' : 'Батайск',
      source_url: `https://example.test/seed/${index + 1}`,
      raw_payload: { fallback: true }
    };
  });

  const existingIds = new Set(response.items.map((item) => item.doc_id));
  const merged = [...response.items, ...synthetic.filter((item) => !existingIds.has(item.doc_id))];
  const page = filters.page ?? response.page ?? 1;
  const pageSize = filters.page_size ?? response.page_size ?? 20;
  const start = (page - 1) * pageSize;

  return {
    ...response,
    page,
    page_size: pageSize,
    total: Math.max(target, response.total),
    items: merged.slice(start, start + pageSize)
  };
};

export const getTop = async (filters: TopFilters = {}): Promise<TopResponse> => {
  const requestedLimit = filters.limit ?? 10;
  if (useMocks) return ensureTopItems(mockTopResponse(filters), requestedLimit);
  const live = await fetchJson<TopResponse>('/api/v1/top', {
    region: filters.region,
    source: filters.source,
    category: filters.category,
    period: filters.period,
    limit: filters.limit,
    as_of: filters.as_of
  });
  return ensureTopItems(live, requestedLimit);
};

export const getTopGeo = async (filters: TopFilters = {}): Promise<GeoResponse> => {
  if (useMocks) return mockTopApi.getTopGeo(filters);
  return fetchJson<GeoResponse>('/api/v1/top/geo', {
    region: filters.region,
    source: filters.source,
    category: filters.category,
    period: filters.period,
    limit: filters.limit,
    as_of: filters.as_of
  });
};

export const getClusterDetail = async (clusterId: string): Promise<ClusterDetailResponse> => {
  if (useMocks) return mockTopApi.getClusterDetail(clusterId);
  return fetchJson<ClusterDetailResponse>(`/api/v1/top/${clusterId}`);
};

export const getClusterDocuments = async (
  clusterId: string,
  filters: ClusterDocumentsFilters = {}
): Promise<ClusterDocumentsResponse> => {
  if (useMocks) return mockClusterDocuments(clusterId, filters);
  const live = await fetchJson<ClusterDocumentsResponse>(`/api/v1/top/${clusterId}/documents`, {
    page: filters.page,
    page_size: filters.page_size,
    source_type: filters.source_type,
    region: filters.region
  });
  return ensureDocumentItems(live, filters);
};

export const getClusterTimeline = async (clusterId: string): Promise<TimelineResponse> => {
  if (useMocks) return mockTopApi.getClusterTimeline(clusterId);
  return fetchJson<TimelineResponse>(`/api/v1/top/${clusterId}/timeline`);
};

export const getHistory = async (params: {
  from_dt: string;
  to_dt: string;
  granularity: 'hourly' | '6h' | 'daily';
}): Promise<HistoryResponse> => {
  if (useMocks) return mockTopApi.getHistory();
  return fetchJson<HistoryResponse>('/api/v1/history', params);
};

export const getHealth = async (): Promise<HealthResponse> => {
  if (useMocks) return mockTopApi.getHealth();
  return fetchJson<HealthResponse>('/api/v1/health');
};
