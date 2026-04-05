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
import { mockTopApi } from './top.mocks';

export const getTop = async (filters: TopFilters = {}): Promise<TopResponse> => {
  if (useMocks) return mockTopApi.getTop(filters);
  return fetchJson<TopResponse>('/api/v1/top', {
    region: filters.region,
    source: filters.source,
    category: filters.category,
    period: filters.period,
    limit: filters.limit,
    as_of: filters.as_of
  });
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
  if (useMocks) return mockTopApi.getClusterDocuments(clusterId, filters);
  return fetchJson<ClusterDocumentsResponse>(`/api/v1/top/${clusterId}/documents`, {
    page: filters.page,
    page_size: filters.page_size,
    source_type: filters.source_type,
    region: filters.region
  });
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
