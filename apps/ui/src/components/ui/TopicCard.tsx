import { Link } from 'react-router-dom';
import { TopicItem } from '../../types';
import { UrgencyIndicator } from './UrgencyIndicator';

export const TopicCard = ({ topic }: { topic: TopicItem }) => (
  <div className="rounded-lg border border-slate-700 bg-panel p-4">
    <div className="mb-2 flex items-center justify-between">
      <p className="font-mono text-2xl">#{topic.rank}</p>
      <p className="text-sm text-slate-400">Постов: {topic.doc_count}</p>
    </div>
    <h3 className="mb-1 text-lg font-semibold">{topic.title}</h3>
    <p className="mb-3 text-sm text-slate-300">{topic.summary}</p>
    <UrgencyIndicator score={topic.urgency_score} />
    <div className="mt-3 flex flex-wrap gap-1">
      {topic.tags.map((tag) => (
        <span key={tag} className="rounded-full bg-blue-500/15 px-2 py-0.5 text-xs text-blue-200">
          {tag}
        </span>
      ))}
    </div>
    <div className="mt-3 text-sm">
      Примеры:{' '}
      {topic.sample_doc_ids.slice(0, 3).map((id) => (
        <Link key={id} to={`/post/${id}`} className="mr-2 text-blue-400 underline">
          {id}
        </Link>
      ))}
    </div>
  </div>
);
