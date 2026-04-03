import { useState } from 'react';
import { ErrorState } from '../components/ui/ErrorState';
import { LoadingState } from '../components/ui/LoadingState';
import { TopicCard } from '../components/ui/TopicCard';
import { useTopics } from '../hooks/useTopics';

const ranges = [
  { label: 'Сегодня', days: 1 },
  { label: '7 дней', days: 7 },
  { label: '30 дней', days: 30 }
];

export const Topics = () => {
  const [period, setPeriod] = useState(7);
  const now = new Date();
  const dateFrom = new Date(now.getTime() - period * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { data, isLoading, error } = useTopics({ limit: 10, date_from: dateFrom, date_to: now.toISOString().slice(0, 10) });

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Топ тем</h2>
      <div className="flex gap-2">
        {ranges.map((r) => (
          <button
            key={r.days}
            onClick={() => setPeriod(r.days)}
            className={`rounded px-3 py-1 ${period === r.days ? 'bg-blue-600' : 'bg-slate-700'}`}
          >
            {r.label}
          </button>
        ))}
      </div>

      {isLoading && <LoadingState />}
      {error && <ErrorState message={(error as Error).message} />}
      <div className="grid gap-3">{data?.items.map((topic) => <TopicCard key={topic.rank} topic={topic} />)}</div>
    </div>
  );
};
