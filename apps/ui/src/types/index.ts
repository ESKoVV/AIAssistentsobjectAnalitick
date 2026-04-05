export type SourceType =
  | 'vk_post'
  | 'vk_comment'
  | 'telegram_post'
  | 'telegram_comment'
  | 'max_post'
  | 'max_comment'
  | 'rss_article'
  | 'portal_appeal';

export type UrgencyLevel = 'low' | 'medium' | 'high' | 'critical';

export interface ScoreBreakdown {
  volume: number;
  dynamics: number;
  sentiment: number;
  reach: number;
  geo: number;
  source: number;
}

export interface SourceSummary {
  source_type: string;
  count: number;
}

export interface SamplePost {
  doc_id: string;
  text_preview: string;
  source_type: string;
  created_at: string;
  reach: number;
  source_url: string | null;
}

export interface TopItem {
  rank: number;
  cluster_id: string;
  summary: string;
  category: string;
  category_label: string;
  importance_reason: string;
  key_phrases: string[];
  urgency: UrgencyLevel;
  urgency_reason: string;
  mention_count: number;
  unique_authors: number;
  reach_total: number;
  growth_rate: number;
  is_new: boolean;
  is_growing: boolean;
  geo_regions: string[];
  sources: SourceSummary[];
  sample_posts: SamplePost[];
  score: number;
  score_breakdown: ScoreBreakdown;
  period_start: string;
  period_end: string;
  computed_at: string;
}

export interface TopResponse {
  computed_at: string;
  period_start: string;
  period_end: string;
  total_clusters: number;
  items: TopItem[];
}

export interface GeoPoint {
  region: string;
  lat: number;
  lon: number;
}

export interface GeoCluster {
  cluster_id: string;
  summary: string;
  category_label: string;
  rank: number;
  geo_regions: string[];
  mention_count: number;
  urgency: UrgencyLevel;
  geo_points: GeoPoint[];
}

export interface GeoResponse {
  clusters: GeoCluster[];
}

export interface TimelinePoint {
  hour: string;
  count: number;
  reach: number;
  growth_rate: number;
}

export interface TimelineResponse {
  cluster_id: string;
  points: TimelinePoint[];
}

export interface ClusterDetailResponse extends TopItem {
  sample_doc_ids: string[];
  all_regions: string[];
  timeline: TimelinePoint[];
}

export interface ClusterDocument {
  doc_id: string;
  source_id: string;
  source_type: string;
  author_id: string;
  text: string;
  text_preview: string;
  created_at: string;
  collected_at: string;
  reach: number;
  likes: number;
  reposts: number;
  comments_count: number;
  is_official: boolean;
  parent_id: string | null;
  region: string | null;
  source_url: string | null;
  raw_payload: Record<string, unknown>;
}

export interface ClusterDocumentsResponse {
  cluster_id: string;
  page: number;
  page_size: number;
  total: number;
  items: ClusterDocument[];
}

export interface HistoryBucket {
  bucket_start: string;
  bucket_end: string;
  computed_at: string;
  items: TopItem[];
}

export interface HistoryResponse {
  from_dt: string;
  to_dt: string;
  granularity: 'hourly' | '6h' | 'daily';
  buckets: HistoryBucket[];
}

export interface HealthResponse {
  status: 'ok' | 'degraded' | 'down';
  last_ranking_at: string;
  ranking_age_minutes: number;
  documents_last_hour: number;
  pipeline_status: Record<string, string>;
}

export interface TopFilters {
  region?: string;
  source?: string;
  category?: string;
  period?: '6h' | '24h' | '72h';
  limit?: number;
  as_of?: string;
}

export interface ClusterDocumentsFilters {
  page?: number;
  page_size?: number;
  source_type?: string;
  region?: string;
}
