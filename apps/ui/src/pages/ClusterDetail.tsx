import { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { ActivityBarChart } from '../components/charts/ActivityBarChart';
import { DocumentCard } from '../components/ui/DocumentCard';
import { EmptyState } from '../components/ui/EmptyState';
import { ErrorState } from '../components/ui/ErrorState';
import { LoadingState } from '../components/ui/LoadingState';
import { Pagination } from '../components/ui/Pagination';
import { SourceBadge } from '../components/ui/SourceBadge';
import { UrgencyIndicator } from '../components/ui/UrgencyIndicator';
import { useClusterDetail, useClusterDocuments, useClusterTimeline } from '../hooks/useTop';
import { formatDateTime } from '../utils/formatDate';

export const ClusterDetail = () => {
  const { clusterId } = useParams();
  const [page, setPage] = useState(1);
  const detailQuery = useClusterDetail(clusterId);
  const timelineQuery = useClusterTimeline(clusterId);
  const documentsQuery = useClusterDocuments(clusterId, { page, page_size: 10 });

  if (detailQuery.isLoading || timelineQuery.isLoading || documentsQuery.isLoading) {
    return <LoadingState />;
  }

  if (detailQuery.error || timelineQuery.error || documentsQuery.error) {
    return <ErrorState message="Не удалось загрузить карточку кластера" />;
  }

  const detail = detailQuery.data;
  const timeline = timelineQuery.data;
  const documents = documentsQuery.data;

  if (!detail || !timeline || !documents) return null;

  return (
    <div className="space-y-6">
      <div className="text-sm text-slate-400">
        <Link to="/" className="underline">Обзор</Link> → <Link to="/topics" className="underline">Топ тем</Link> → Кластер
      </div>

      <section className="rounded-lg border border-slate-700 bg-panel p-5">
        <div className="mb-4 flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="font-mono text-sm text-slate-400">{detail.cluster_id}</p>
            <h2 className="mt-1 text-2xl font-semibold">{detail.summary}</h2>
          </div>
          <UrgencyIndicator urgency={detail.urgency} reason={detail.urgency_reason} />
        </div>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div className="rounded border border-slate-700 p-3">
            <p className="text-xs text-slate-400">Упоминания</p>
            <p className="mt-1 font-mono text-2xl">{detail.mention_count}</p>
          </div>
          <div className="rounded border border-slate-700 p-3">
            <p className="text-xs text-slate-400">Уникальные авторы</p>
            <p className="mt-1 font-mono text-2xl">{detail.unique_authors}</p>
          </div>
          <div className="rounded border border-slate-700 p-3">
            <p className="text-xs text-slate-400">Охват</p>
            <p className="mt-1 font-mono text-2xl">{detail.reach_total.toLocaleString('ru-RU')}</p>
          </div>
          <div className="rounded border border-slate-700 p-3">
            <p className="text-xs text-slate-400">Рост</p>
            <p className="mt-1 font-mono text-2xl">{detail.growth_rate.toFixed(1)}x</p>
          </div>
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          {detail.key_phrases.map((phrase) => (
            <span key={phrase} className="rounded-full bg-blue-500/15 px-3 py-1 text-xs text-blue-200">
              {phrase}
            </span>
          ))}
        </div>
        <div className="mt-4 flex flex-wrap gap-2 text-sm text-slate-300">
          {detail.sources.map((source) => (
            <SourceBadge key={`${source.source_type}-${source.count}`} sourceType={source.source_type} />
          ))}
        </div>
        <div className="mt-4 text-sm text-slate-400">
          Регионы: {detail.all_regions.join(', ')}
        </div>
      </section>

      <section>
        <h3 className="mb-2 text-lg font-semibold">Динамика за 72 часа</h3>
        <ActivityBarChart
          data={timeline.points.map((point) => ({
            hour: formatDateTime(point.hour).slice(11, 16),
            count: point.count
          }))}
        />
      </section>

      <section>
        <h3 className="mb-2 text-lg font-semibold">Примеры публикаций</h3>
        <div className="grid gap-3 md:grid-cols-2">
          {detail.sample_posts.map((sample) => (
            <a
              key={sample.doc_id}
              href={sample.source_url ?? '#'}
              target={sample.source_url ? '_blank' : undefined}
              rel={sample.source_url ? 'noreferrer' : undefined}
              className="rounded-lg border border-slate-700 bg-panel p-4"
            >
              <div className="mb-2 flex items-center justify-between text-xs text-slate-400">
                <span>{sample.doc_id}</span>
                <span>{formatDateTime(sample.created_at)}</span>
              </div>
              <p className="text-sm text-slate-100">{sample.text_preview}</p>
            </a>
          ))}
        </div>
      </section>

      <section>
        <h3 className="mb-2 text-lg font-semibold">Документы кластера</h3>
        {documents.items.length === 0 && <EmptyState title="Документы не найдены" />}
        <div className="grid gap-3">
          {documents.items.map((doc) => (
            <DocumentCard key={doc.doc_id} doc={doc} />
          ))}
        </div>
        <Pagination page={documents.page} total={documents.total} limit={documents.page_size} onChange={setPage} />
      </section>
    </div>
  );
};
