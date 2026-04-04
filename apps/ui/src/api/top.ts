import {
  ClusterDetailResponse,
  ClusterDocumentsFilters,
  ClusterDocumentsResponse,
  HealthResponse,
  HistoryResponse,
  TimelineResponse,
  TopFilters,
  TopResponse
} from '../types';
import { fetchJson, useMocks } from './client';

const mockTopItems: TopResponse['items'] = [
  {
    rank: 1,
    cluster_id: 'cluster-water-1',
    summary: 'Жители массово жалуются на перебои с горячей водой и затянутые сроки восстановления подачи.',
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
    sample_posts: [
      {
        doc_id: 'vk_post:water-1',
        text_preview: 'На Западном второй день нет горячей воды, сроки снова перенесли.',
        source_type: 'vk_post',
        created_at: '2026-04-04T10:20:00Z',
        reach: 12400,
        source_url: 'https://vk.com/wall-1_101'
      },
      {
        doc_id: 'rss_article:water-2',
        text_preview: 'Коммунальная авария оставила несколько кварталов без горячего водоснабжения.',
        source_type: 'rss_article',
        created_at: '2026-04-04T09:40:00Z',
        reach: 8300,
        source_url: 'https://example.test/water-2'
      }
    ],
    score: 0.91,
    score_breakdown: {
      volume: 0.92,
      dynamics: 1,
      sentiment: 0.88,
      reach: 0.76,
      geo: 0.66,
      source: 0.58
    }
  },
  {
    rank: 2,
    cluster_id: 'cluster-transport-2',
    summary: 'В соцсетях и медиа обсуждаются массовые задержки общественного транспорта в утренний час пик.',
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
    sources: [
      { source_type: 'vk_post', count: 91 },
      { source_type: 'portal_appeal', count: 41 }
    ],
    sample_posts: [
      {
        doc_id: 'vk_post:transport-1',
        text_preview: 'Автобус 71 снова не пришёл по расписанию, на остановке скопились люди.',
        source_type: 'vk_post',
        created_at: '2026-04-04T08:10:00Z',
        reach: 9200,
        source_url: 'https://vk.com/wall-1_202'
      }
    ],
    score: 0.78,
    score_breakdown: {
      volume: 0.81,
      dynamics: 0.72,
      sentiment: 0.74,
      reach: 0.63,
      geo: 0.41,
      source: 0.33
    }
  },
  {
    rank: 3,
    cluster_id: 'cluster-health-3',
    summary: 'Появился устойчивый поток сообщений о нехватке записи к профильным врачам в поликлиниках.',
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
    sources: [
      { source_type: 'vk_post', count: 39 },
      { source_type: 'portal_appeal', count: 28 }
    ],
    sample_posts: [
      {
        doc_id: 'portal_appeal:health-1',
        text_preview: 'Невозможно записаться к эндокринологу уже вторую неделю.',
        source_type: 'portal_appeal',
        created_at: '2026-04-04T07:00:00Z',
        reach: 1400,
        source_url: 'https://example.test/appeal/1'
      }
    ],
    score: 0.49,
    score_breakdown: {
      volume: 0.55,
      dynamics: 0.34,
      sentiment: 0.62,
      reach: 0.41,
      geo: 0.38,
      source: 0.27
    }
  }
];

const mockTopResponse = (filters: TopFilters = {}): TopResponse => {
  const filteredItems = mockTopItems
    .filter((item) => !filters.region || item.geo_regions.includes(filters.region))
    .filter((item) => !filters.source || item.sources.some((entry) => entry.source_type === filters.source));
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
  const items = Array.from({ length: 18 }).map((_, index) => ({
    doc_id: `${clusterId}:doc-${index + 1}`,
    source_id: `${index + 1}`,
    source_type: index % 3 === 0 ? 'portal_appeal' : 'vk_post',
    author_id: `author-${index + 1}`,
    text: `Сообщение ${index + 1} по кластеру ${clusterId}: жители описывают проблему и просят ускорить реакцию ведомств.`,
    text_preview: `Сообщение ${index + 1} по кластеру ${clusterId}: жители описывают проблему и просят ускорить реакцию ведомств.`,
    created_at: new Date(Date.UTC(2026, 3, 4, 11, index)).toISOString(),
    collected_at: new Date(Date.UTC(2026, 3, 4, 11, index)).toISOString(),
    reach: 600 + index * 40,
    likes: 5 + index,
    reposts: index % 4,
    comments_count: 2 + (index % 6),
    is_official: index % 7 === 0,
    parent_id: null,
    region: index % 2 === 0 ? 'Ростов-на-Дону' : 'Батайск',
    source_url: index % 3 === 0 ? `https://example.test/appeal/${index + 1}` : `https://vk.com/wall-1_${index + 1}`,
    raw_payload: {}
  }));
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

export const getTop = async (filters: TopFilters = {}): Promise<TopResponse> => {
  if (useMocks) return mockTopResponse(filters);
  return fetchJson<TopResponse>('/api/v1/top', {
    region: filters.region,
    source: filters.source,
    period: filters.period,
    limit: filters.limit,
    as_of: filters.as_of
  });
};

export const getClusterDetail = async (clusterId: string): Promise<ClusterDetailResponse> => {
  if (useMocks) return mockClusterDetail(clusterId);
  return fetchJson<ClusterDetailResponse>(`/api/v1/top/${clusterId}`);
};

export const getClusterDocuments = async (
  clusterId: string,
  filters: ClusterDocumentsFilters = {}
): Promise<ClusterDocumentsResponse> => {
  if (useMocks) return mockClusterDocuments(clusterId, filters);
  return fetchJson<ClusterDocumentsResponse>(`/api/v1/top/${clusterId}/documents`, {
    page: filters.page,
    page_size: filters.page_size,
    source_type: filters.source_type,
    region: filters.region
  });
};

export const getClusterTimeline = async (clusterId: string): Promise<TimelineResponse> => {
  if (useMocks) {
    const detail = mockClusterDetail(clusterId);
    return {
      cluster_id: clusterId,
      points: detail.timeline
    };
  }
  return fetchJson<TimelineResponse>(`/api/v1/top/${clusterId}/timeline`);
};

export const getHistory = async (params: {
  from_dt: string;
  to_dt: string;
  granularity: 'hourly' | '6h' | 'daily';
}): Promise<HistoryResponse> => {
  if (useMocks) return mockHistoryResponse();
  return fetchJson<HistoryResponse>('/api/v1/history', params);
};

export const getHealth = async (): Promise<HealthResponse> => {
  if (useMocks) return mockHealthResponse;
  return fetchJson<HealthResponse>('/api/v1/health');
};
