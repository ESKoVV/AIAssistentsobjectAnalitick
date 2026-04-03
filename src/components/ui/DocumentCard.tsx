import { Link } from 'react-router-dom';
import { NormalizedDocument } from '../../types';
import { formatDateTime } from '../../utils/formatDate';
import { SourceBadge } from './SourceBadge';

export const DocumentCard = ({ doc }: { doc: NormalizedDocument }) => (
  <Link to={`/document/${doc.doc_id}`} className="block rounded-lg border border-slate-700 bg-panel p-4 hover:border-blue-500">
    <div className="mb-2 flex items-center justify-between gap-2">
      <SourceBadge sourceType={doc.source_type} />
      {doc.is_official && <span className="rounded bg-blue-700 px-2 py-0.5 text-xs">Official</span>}
    </div>
    <div className="text-xs text-slate-400">
      {formatDateTime(doc.created_at)} · {doc.region_hint ?? 'Не указан'}
    </div>
    <p className="my-2 text-sm text-slate-100">{doc.text.slice(0, 300)}</p>
    <div className="flex gap-4 text-xs text-slate-400">
      <span>👁 {doc.reach}</span>
      <span>❤️ {doc.likes}</span>
      <span>🔁 {doc.reposts}</span>
      <span>💬 {doc.comments_count}</span>
    </div>
  </Link>
);
