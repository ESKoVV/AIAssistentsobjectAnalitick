import { Link } from 'react-router-dom';
import { TopItem } from '../../types';
import { UrgencyIndicator } from './UrgencyIndicator';

const fallbackReason = (topic: TopItem) =>
  topic.dashboard_reason
  ?? `Рост ${topic.growth_rate.toFixed(1)}x, ${topic.mention_count} публикаций, охват ${topic.reach_total.toLocaleString('ru-RU')}.`;

export const TopicCard = ({ topic }: { topic: TopItem }) => (
  <Link to={`/clusters/${topic.cluster_id}`} className="block rounded-lg border border-slate-700 bg-panel p-4 transition hover:border-blue-500">
    <div className="mb-3 flex items-start justify-between gap-4">
      <div>
        <p className="font-mono text-2xl">#{topic.rank}</p>
        <p className="text-xs text-slate-400">{topic.cluster_id}</p>
      </div>
      <div className="text-right text-sm text-slate-300">
        <p>Упоминаний: {topic.mention_count}</p>
        <p>Охват: {topic.reach_total.toLocaleString('ru-RU')}</p>
      </div>
    </div>
    <div className="mb-2 flex flex-wrap items-center gap-2 text-xs text-slate-400">
      <span className="rounded-full bg-slate-800 px-2 py-1">{topic.category_label}</span>
      <span>
        Период: {new Date(topic.period_start).toLocaleDateString('ru-RU')} -{' '}
        {new Date(topic.period_end).toLocaleDateString('ru-RU')}
      </span>
    </div>
    <h3 className="mb-2 text-lg font-semibold">{topic.summary}</h3>
    <p className="mb-2 text-sm text-slate-300">
      Почему в топе: {fallbackReason(topic)}
    </p>
    <UrgencyIndicator urgency={topic.urgency} reason={topic.urgency_reason} />
    <p className="mt-3 text-sm leading-6 text-slate-300">{topic.importance_reason}</p>
    <div className="mt-3 flex flex-wrap gap-1">
      {topic.key_phrases.slice(0, 5).map((phrase) => (
        <span key={phrase} className="rounded-full bg-blue-500/15 px-2 py-0.5 text-xs text-blue-200">
          {phrase}
        </span>
      ))}
    </div>
    <div className="mt-3 flex flex-wrap gap-3 text-xs text-slate-400">
      <span>Регионов: {topic.geo_regions.length}</span>
      <span>Авторов: {topic.unique_authors}</span>
      <span>Рост: {topic.growth_rate.toFixed(1)}x</span>
    </div>
  </Link>
);
