import { ExternalLink } from 'lucide-react';
import { ClusterDocument } from '../../types';
import { formatDateTime } from '../../utils/formatDate';
import { SourceBadge } from './SourceBadge';

export const DocumentCard = ({ doc }: { doc: ClusterDocument }) => {
  const content = (
    <div className="rounded-lg border border-slate-700 bg-panel p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <SourceBadge sourceType={doc.source_type} />
        <div className="text-xs text-slate-400">
          {formatDateTime(doc.created_at)} · {doc.region ?? 'Регион не указан'}
        </div>
      </div>
      <p className="mb-3 text-sm leading-6 text-slate-100">{doc.text_preview}</p>
      <div className="flex flex-wrap gap-4 text-xs text-slate-400">
        <span>Охват: {doc.reach.toLocaleString('ru-RU')}</span>
        <span>Лайки: {doc.likes}</span>
        <span>Репосты: {doc.reposts}</span>
        <span>Комментарии: {doc.comments_count}</span>
      </div>
    </div>
  );

  if (!doc.source_url) {
    return content;
  }

  return (
    <a href={doc.source_url} target="_blank" rel="noreferrer" className="block transition hover:border-blue-500">
      {content}
      <div className="-mt-10 mr-4 flex justify-end text-xs text-blue-300">
        <span className="inline-flex items-center gap-1 rounded bg-slate-900/70 px-2 py-1">
          Открыть оригинал <ExternalLink size={12} />
        </span>
      </div>
    </a>
  );
};
