import { Link } from 'react-router-dom';
import { NormalizedDocument } from '../../types';
import { inferDocumentTags } from '../../utils/documentTags';
import { formatDateTime } from '../../utils/formatDate';

export const DocumentCard = ({ doc }: { doc: NormalizedDocument }) => {
  const tags = inferDocumentTags(doc);

  return (
    <Link to={`/post/${doc.doc_id}`} className="block rounded-lg border border-slate-700 bg-panel p-4 hover:border-blue-500">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap gap-1">
          {tags.map((tag) => (
            <span key={tag} className="rounded-full bg-blue-500/15 px-2 py-0.5 text-xs text-blue-200">
              {tag}
            </span>
          ))}
        </div>
        {doc.is_official && <span className="rounded bg-emerald-700/40 px-2 py-0.5 text-xs">Официально</span>}
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
};
