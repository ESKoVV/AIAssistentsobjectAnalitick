import { useQuery } from '@tanstack/react-query';
import { getRegions } from '../api/regions';

export const useRegions = () =>
  useQuery({
    queryKey: ['regions'],
    queryFn: getRegions
  });
