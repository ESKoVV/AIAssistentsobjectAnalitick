import { useMemo, useState } from 'react';
import { ErrorState } from '../components/ui/ErrorState';
import { LoadingState } from '../components/ui/LoadingState';
import { TopicCard } from '../components/ui/TopicCard';
import { useTop } from '../hooks/useTop';

const periods = [
  { label: '6 часов', value: '6h' as const },
  { label: '24 часа', value: '24h' as const },
  { label: '72 часа', value: '72h' as const }
];

export const Topics = () => {
  const [period, setPeriod] = useState<'6h' | '24h' | '72h'>('24h');
  const region = typeof window !== 'undefined' ? window.localStorage.getItem('selectedRegion') ?? '' : '';
  const filters = useMemo(() => ({ period, limit: 10, region: region || undefined }), [period, region]);
  const { data, isLoading, error } = useTop(filters);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-2xl font-semibold">Топ тем</h2>
          <p className="text-sm text-slate-400">
            {region ? `Фильтр по региону: ${region}` : 'Без регионального фильтра'}
          </p>
        </div>
        <div className="flex gap-2">
          {periods.map((item) => (
            <button
              key={item.value}
              onClick={() => setPeriod(item.value)}
              className={`rounded px-3 py-1 text-sm ${period === item.value ? 'bg-blue-600' : 'bg-slate-700'}`}
            >
              {item.label}
            </button>
          ))}
        </div>
      </div>

      {isLoading && <LoadingState />}
      {error && <ErrorState message={(error as Error).message} />}
      <div className="grid gap-3">{data?.items.map((topic) => <TopicCard key={topic.cluster_id} topic={topic} />)}</div>
    </div>
  );
};
