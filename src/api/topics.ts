import topicsMock from '../mocks/topics.json';
import { TopicItem, TopicsResponse } from '../types';
import { buildUrl, useMocks } from './client';

const mockData = topicsMock as TopicsResponse;

export const getTopics = async (params: {
  limit?: number;
  date_from?: string;
  date_to?: string;
}): Promise<TopicsResponse> => {
  if (useMocks) {
    const items = [...mockData.items].slice(0, params.limit ?? 10) as TopicItem[];
    return { items };
  }

  const response = await fetch(
    buildUrl('/api/topics', {
      limit: params.limit,
      date_from: params.date_from,
      date_to: params.date_to
    })
  );
  if (!response.ok) throw new Error('Failed to load topics');
  return response.json();
};
