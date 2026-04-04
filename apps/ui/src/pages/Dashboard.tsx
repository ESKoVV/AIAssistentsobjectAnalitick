import { ActivityBarChart } from '../components/charts/ActivityBarChart';
import { DocumentCard } from '../components/ui/DocumentCard';
import { ErrorState } from '../components/ui/ErrorState';
import { KpiCard } from '../components/ui/KpiCard';
import { LoadingState } from '../components/ui/LoadingState';
import { TopicCard } from '../components/ui/TopicCard';
import { useDocuments } from '../hooks/useDocuments';
import { useStats } from '../hooks/useStats';
import { useTopics } from '../hooks/useTopics';

const hoursMock = Array.from({ length: 24 }).map((_, idx) => ({
  hour: `${String(idx).padStart(2, '0')}:00`,
  count: 20 + Math.floor(Math.random() * 70)
}));

export const Dashboard = () => {
  const statsQuery = useStats();
  const topicsQuery = useTopics({ limit: 5 });
  const docsQuery = useDocuments({ page: 1, limit: 10 });

  if (statsQuery.isLoading || topicsQuery.isLoading || docsQuery.isLoading) return <LoadingState />;
  if (statsQuery.error || topicsQuery.error || docsQuery.error) {
    return <ErrorState message="Не удалось загрузить обзорные данные" />;
  }

  const stats = statsQuery.data!;

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Обзор за текущие сутки</h2>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <KpiCard label="Всего постов за 24ч" value={stats.docs_last_24h} />
        <KpiCard label="Официальных источников" value={Math.round(stats.docs_last_24h * 0.22)} />
        <KpiCard label="Активных регионов" value={stats.by_region.length} />
        <KpiCard label="Новых тем" value={topicsQuery.data?.items.length ?? 0} />
      </div>

      <section>
        <h3 className="mb-2 text-lg font-semibold">Топ-5 тем</h3>
        <div className="grid gap-3 lg:grid-cols-2">
          {topicsQuery.data?.items.map((topic) => <TopicCard key={topic.rank} topic={topic} />)}
        </div>
      </section>

      <section>
        <h3 className="mb-2 text-lg font-semibold">Активность за 24 часа</h3>
        <ActivityBarChart data={hoursMock} />
      </section>

      <section>
        <h3 className="mb-2 text-lg font-semibold">Последние 10 постов</h3>
        <div className="grid gap-3">{docsQuery.data?.items.map((doc) => <DocumentCard key={doc.doc_id} doc={doc} />)}</div>
      </section>
    </div>
  );
};
