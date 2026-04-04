import { TimelineLineChart } from '../components/charts/TimelineLineChart';
import { ErrorState } from '../components/ui/ErrorState';
import { KpiCard } from '../components/ui/KpiCard';
import { LoadingState } from '../components/ui/LoadingState';
import { TopicCard } from '../components/ui/TopicCard';
import { useHealth, useHistory, useTop } from '../hooks/useTop';
import { formatDateTime } from '../utils/formatDate';

export const Dashboard = () => {
  const topQuery = useTop({ period: '24h', limit: 5 });
  const healthQuery = useHealth();
  const historyQuery = useHistory({
    from_dt: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
    to_dt: new Date().toISOString(),
    granularity: '6h'
  });

  if (topQuery.isLoading || healthQuery.isLoading || historyQuery.isLoading) return <LoadingState />;
  if (topQuery.error || healthQuery.error || historyQuery.error) {
    return <ErrorState message="Не удалось загрузить обзорные данные" />;
  }

  const top = topQuery.data;
  const health = healthQuery.data;
  const history = historyQuery.data;

  if (!top || !health || !history) return null;

  const historyData = history.buckets.map((bucket) => ({
    date: new Date(bucket.bucket_start).toLocaleString('ru-RU', {
      timeZone: 'UTC',
      month: 'numeric',
      day: 'numeric',
      hour: '2-digit'
    }),
    count: bucket.items[0]?.mention_count ?? 0
  }));

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-semibold">Обзор приоритетов</h2>
        <p className="text-sm text-slate-400">
          Последний пересчёт рейтинга: {formatDateTime(health.last_ranking_at)} UTC
        </p>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <KpiCard label="Кластеров в топе" value={top.total_clusters} />
        <KpiCard label="Возраст рейтинга, мин" value={health.ranking_age_minutes} />
        <KpiCard label="Документов за час" value={health.documents_last_hour} />
        <KpiCard label="Статус пайплайна" value={health.status.toUpperCase()} />
      </div>

      <section>
        <h3 className="mb-2 text-lg font-semibold">Топ-5 тем за 24 часа</h3>
        <div className="grid gap-3 lg:grid-cols-2">
          {top.items.map((topic) => <TopicCard key={topic.cluster_id} topic={topic} />)}
        </div>
      </section>

      <section>
        <h3 className="mb-2 text-lg font-semibold">Изменение лидера по периодам</h3>
        <TimelineLineChart data={historyData} />
      </section>

      <section className="rounded-lg border border-slate-700 bg-panel p-4">
        <h3 className="mb-3 text-lg font-semibold">Статус upstream сервисов</h3>
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
