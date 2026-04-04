import { buildUrl, useMocks } from './client';
import documentsMock from '../mocks/documents.json';
import { NormalizedDocument } from '../types';

const mockData = documentsMock as NormalizedDocument[];

export const getRegions = async (): Promise<string[]> => {
  if (useMocks) {
    return Array.from(new Set(mockData.map((doc) => doc.region_hint).filter(Boolean))) as string[];
  }

  const response = await fetch(buildUrl('/api/regions'));
  if (!response.ok) {
    throw new Error('Failed to load regions');
  }

  const payload = (await response.json()) as { items: string[] };
  return payload.items;
};
