import { useQuery } from '@tanstack/react-query';
import { getStats } from '../api/stats';

export const useStats = () =>
  useQuery({
    queryKey: ['stats'],
    queryFn: getStats
  });
