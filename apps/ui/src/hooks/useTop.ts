import { useQuery } from '@tanstack/react-query';
import {
  getClusterDetail,
  getClusterDocuments,
  getClusterTimeline,
  getHealth,
  getHistory,
  getTop,
  getTopGeo
} from '../api/top';
import { ClusterDocumentsFilters, TopFilters } from '../types';

export const useTop = (filters: TopFilters) =>
  useQuery({
    queryKey: ['top', filters],
    queryFn: () => getTop(filters)
  });

export const useTopGeo = (filters: TopFilters) =>
  useQuery({
    queryKey: ['top-geo', filters],
    queryFn: () => getTopGeo(filters)
  });

export const useClusterDetail = (clusterId?: string) =>
  useQuery({
    queryKey: ['cluster-detail', clusterId],
    queryFn: () => getClusterDetail(clusterId ?? ''),
    enabled: Boolean(clusterId)
  });

export const useClusterDocuments = (clusterId?: string, filters: ClusterDocumentsFilters = {}) =>
  useQuery({
    queryKey: ['cluster-documents', clusterId, filters],
    queryFn: () => getClusterDocuments(clusterId ?? '', filters),
    enabled: Boolean(clusterId)
  });

export const useClusterTimeline = (clusterId?: string) =>
  useQuery({
    queryKey: ['cluster-timeline', clusterId],
    queryFn: () => getClusterTimeline(clusterId ?? ''),
    enabled: Boolean(clusterId)
  });

export const useHistory = (params: { from_dt: string; to_dt: string; granularity: 'hourly' | '6h' | 'daily' }) =>
  useQuery({
    queryKey: ['history', params],
    queryFn: () => getHistory(params)
  });

export const useHealth = () =>
  useQuery({
    queryKey: ['health'],
    queryFn: getHealth
  });
