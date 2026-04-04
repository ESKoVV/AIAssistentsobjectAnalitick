import { Link, useParams } from 'react-router-dom';
import { ErrorState } from '../components/ui/ErrorState';
import { LoadingState } from '../components/ui/LoadingState';
import { SourceBadge } from '../components/ui/SourceBadge';
import { useDocument } from '../hooks/useDocuments';
import { formatDateTime } from '../utils/formatDate';

export const DocumentDetail = () => {
  const { id } = useParams();
  const { data, isLoading, error } = useDocument(id);

  if (isLoading) return <LoadingState />;
  if (error) return <ErrorState message={(error as Error).message} />;
  if (!data) return null;

  return (
    <div className="space-y-4">
      <div className="text-sm text-slate-400">
        <Link to="/" className="underline">Обзор</Link> → <Link to="/feed" className="underline">Лента</Link> → Пост
      </div>
      <div className="rounded-lg border border-slate-700 bg-panel p-4 text-sm">
        <div className="grid gap-2 md:grid-cols-2">
          <div><SourceBadge sourceType={data.source_type} /></div>
          <div>Создан: {formatDateTime(data.created_at)}</div>
          <div>Собран: {formatDateTime(data.collected_at)}</div>
          <div>Автор: <span className="font-mono">{data.author_id}</span></div>
          <div>Регион: {data.region_hint ?? '—'}</div>
        </div>
      </div>

      <article className="rounded-lg border border-slate-700 bg-panel p-4 leading-7">{data.text}</article>
      <div className="flex gap-4 text-sm text-slate-300">
        <span>👁 {data.reach}</span>
        <span>❤️ {data.likes}</span>
        <span>🔁 {data.reposts}</span>
        <span>💬 {data.comments_count}</span>
      </div>
      {data.parent_id && <Link className="text-blue-400 underline" to={`/post/${data.parent_id}`}>Родительский пост: {data.parent_id}</Link>}
    </div>
  );
};
