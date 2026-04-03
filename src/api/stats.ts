import statsMock from '../mocks/stats.json';
import { StatsResponse } from '../types';
import { buildUrl, useMocks } from './client';

const mockData = statsMock as StatsResponse;

export const getStats = async (): Promise<StatsResponse> => {
  if (useMocks) return mockData;
  const response = await fetch(buildUrl('/api/stats'));
  if (!response.ok) throw new Error('Failed to load stats');
  return response.json();
};
