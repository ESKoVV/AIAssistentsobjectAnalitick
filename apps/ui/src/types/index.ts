export type SourceType =
  | 'vk_post'
  | 'vk_comment'
  | 'telegram_post'
  | 'telegram_comment'
  | 'rss_article'
  | 'portal_appeal';

export type TopicTag =
  | 'СВО'
  | 'ЖКХ'
  | 'Транспорт'
  | 'Медицина'
  | 'Образование'
  | 'Безопасность'
  | 'Экология'
  | 'Экономика'
  | 'АПК'
  | 'Цифровизация'
  | 'Культура'
  | 'Спорт';

export interface NormalizedDocument {
  doc_id: string;
  source_type: SourceType;
  source_id: string;
  parent_id: string | null;
  text: string;
  media_type: 'text' | 'photo' | 'video' | 'link';
  created_at: string;
  collected_at: string;
  author_id: string;
  is_official: boolean;
  reach: number;
  likes: number;
  reposts: number;
  comments_count: number;
  region_hint: string | null;
  geo_lat: number | null;
  geo_lon: number | null;
  raw_payload: object;
}

export interface DocumentsResponse {
  items: NormalizedDocument[];
  total: number;
  page: number;
  limit: number;
}

export interface TopicItem {
  rank: number;
  title: string;
  summary: string;
  doc_count: number;
  tags: TopicTag[];
  urgency_score: number;
  sample_doc_ids: string[];
}

export interface TopicsResponse {
  items: TopicItem[];
}

export interface StatsResponse {
  total_docs: number;
  docs_last_24h: number;
  by_source: { source_type: SourceType; count: number }[];
  by_region: { region: string; count: number }[];
  timeline: { date: string; count: number }[];
}

export interface DocumentFilters {
  page?: number;
  limit?: number;
  region?: string;
  date_from?: string;
  date_to?: string;
  tags?: TopicTag[];
}
