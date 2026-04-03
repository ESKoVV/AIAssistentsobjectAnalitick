import documentsMock from '../mocks/documents.json';
import { NormalizedDocument, StatsResponse, TopicTag } from '../types';
import { inferDocumentTags } from '../utils/documentTags';
import { buildUrl, useMocks } from './client';

const mockDocuments = documentsMock as NormalizedDocument[];

const toDay = (date: Date) => date.toISOString().slice(0, 10);

const buildMockStats = (): StatsResponse => {
  const totalDocs = mockDocuments.length;
  const now = new Date();
  const from24h = now.getTime() - 24 * 60 * 60 * 1000;
  const docsLast24h = mockDocuments.filter((doc) => new Date(doc.created_at).getTime() >= from24h).length;

  const bySource = Object.entries(
    mockDocuments.reduce<Record<string, number>>((acc, doc) => {
      acc[doc.source_type] = (acc[doc.source_type] ?? 0) + 1;
      return acc;
    }, {})
  ).map(([source_type, count]) => ({ source_type, count }));

  const byRegion = Object.entries(
    mockDocuments.reduce<Record<string, number>>((acc, doc) => {
      const region = doc.region_hint ?? 'Не указан';
      acc[region] = (acc[region] ?? 0) + 1;
      return acc;
    }, {})
  ).map(([region, count]) => ({ region, count }));

  const tagAgg = mockDocuments.reduce<Record<string, { count: number; reach: number; official: number }>>((acc, doc) => {
    const tags = inferDocumentTags(doc);
    tags.forEach((tag) => {
      acc[tag] ??= { count: 0, reach: 0, official: 0 };
      acc[tag].count += 1;
      acc[tag].reach += doc.reach;
      if (doc.is_official) acc[tag].official += 1;
    });
    return acc;
  }, {});

  const byTag = Object.entries(tagAgg).map(([tag, values]) => ({
    tag: tag as TopicTag,
    count: values.count,
    avg_reach: Math.round(values.reach / values.count),
    official_share: values.count ? Math.round((values.official / values.count) * 100) : 0
  }));

  const timelineAgg = mockDocuments.reduce<Record<string, number>>((acc, doc) => {
    const date = doc.created_at.slice(0, 10);
    acc[date] = (acc[date] ?? 0) + 1;
    return acc;
  }, {});

  const timeline = Array.from({ length: 7 }).map((_, index) => {
    const date = new Date(now);
    date.setDate(now.getDate() - (6 - index));
    const key = toDay(date);
    return { date: key, count: timelineAgg[key] ?? 0 };
  });

  return {
    total_docs: totalDocs,
    docs_last_24h: docsLast24h,
    by_source: bySource as StatsResponse['by_source'],
    by_tag: byTag.sort((a, b) => b.count - a.count),
    by_region: byRegion.sort((a, b) => b.count - a.count),
    timeline
  };
};

export const getStats = async (): Promise<StatsResponse> => {
  if (useMocks) return buildMockStats();
  const response = await fetch(buildUrl('/api/stats'));
  if (!response.ok) throw new Error('Failed to load stats');
  return response.json();
};
