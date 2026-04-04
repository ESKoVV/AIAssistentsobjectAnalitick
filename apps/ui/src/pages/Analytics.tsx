import { TimelineLineChart } from '../components/charts/TimelineLineChart';
import { ErrorState } from '../components/ui/ErrorState';
import { LoadingState } from '../components/ui/LoadingState';
import { useHealth, useHistory } from '../hooks/useTop';

export const Analytics = () => {
  const historyQuery = useHistory({
    from_dt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
    to_dt: new Date().toISOString(),
    granularity: 'daily'
  });
  const healthQuery = useHealth();

  if (historyQuery.isLoading || healthQuery.isLoading) return <LoadingState />;
  if (historyQuery.error || healthQuery.error) return <ErrorState message="Не удалось загрузить аналитику" />;

  const history = historyQuery.data;
  const health = healthQuery.data;

  if (!history || !health) return null;

  const leaderScoreData = history.buckets.map((bucket) => ({
    date: new Date(bucket.bucket_start).toLocaleDateString('ru-RU', { timeZone: 'UTC' }),
    count: Number((bucket.items[0]?.score ?? 0).toFixed(2))
  }));

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-semibold">Аналитика приоритетов</h2>
        <p className="text-sm text-slate-400">История изменения лидирующих тем по дневным срезам.</p>
      </div>

      <TimelineLineChart data={leaderScoreData} />

      <section className="grid gap-4 lg:grid-cols-2">
        {history.buckets.map((bucket) => (
          <div key={bucket.bucket_start} className="rounded-lg border border-slate-700 bg-panel p-4">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="font-semibold">
                {new Date(bucket.bucket_start).toLocaleDateString('ru-RU', { timeZone: 'UTC' })}
              </h3>
              <span className="text-xs text-slate-400">
                {new Date(bucket.computed_at).toLocaleTimeString('ru-RU', { timeZone: 'UTC', hour12: false })}
              </span>
            </div>
            <div className="space-y-2">
              {bucket.items.map((item) => (
                <div key={`${bucket.bucket_start}-${item.cluster_id}`} className="rounded border border-slate-700 p-3">
                  <div className="flex items-center justify-between gap-3">
                    <p className="font-mono text-sm">#{item.rank}</p>
                    <p className="text-xs text-slate-400">score {item.score.toFixed(2)}</p>
                  </div>
                  <p className="mt-2 text-sm text-slate-100">{item.summary}</p>
                </div>
              ))}
            </div>
          </div>
        ))}
      </section>

      <section className="rounded-lg border border-slate-700 bg-panel p-4">
        <h3 className="mb-3 font-semibold">Состояние контура</h3>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          {Object.entries(health.pipeline_status).map(([name, status]) => (
            <div key={name} className="rounded border border-slate-700 p-3">
              <p className="text-xs uppercase tracking-wide text-slate-400">{name}</p>
              <p className="mt-1 font-mono text-xl">{status}</p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
};
