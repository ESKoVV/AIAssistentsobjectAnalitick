import { useQuery } from '@tanstack/react-query';
import { getTopics } from '../api/topics';

export const useTopics = (params: { limit?: number; date_from?: string; date_to?: string }) =>
  useQuery({
    queryKey: ['topics', params],
    queryFn: () => getTopics(params)
  });
