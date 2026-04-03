import { DocumentFilters, TopicTag } from '../../types';

interface FilterPanelProps {
  filters: DocumentFilters;
  regions: string[];
  onChange: (next: DocumentFilters) => void;
  onReset: () => void;
  allTags: TopicTag[];
}

export const FilterPanel = ({ filters, regions, onChange, onReset, allTags }: FilterPanelProps) => (
  <div className="rounded-xl border border-slate-700 bg-panel p-5 shadow-lg shadow-slate-950/20">
    <div className="mb-4 flex items-center justify-between">
      <h3 className="text-lg font-semibold">Фильтры ленты</h3>
      <button onClick={onReset} className="rounded-md border border-slate-600 px-3 py-1 text-sm hover:bg-slate-800">
        Сбросить
      </button>
    </div>

    <div className="grid gap-4 lg:grid-cols-3">
      <div className="lg:col-span-2">
        <p className="mb-2 text-xs uppercase tracking-wide text-slate-400">Тематики</p>
        <div className="flex flex-wrap gap-2">
          {allTags.map((tag) => {
            const isActive = filters.tags?.includes(tag) ?? false;
            return (
              <button
                key={tag}
                onClick={() => {
                  const set = new Set(filters.tags ?? []);
                  isActive ? set.delete(tag) : set.add(tag);
                  onChange({ ...filters, page: 1, tags: Array.from(set) as TopicTag[] });
                }}
                className={`rounded-full border px-3 py-1 text-sm transition ${
                  isActive
                    ? 'border-blue-400 bg-blue-500/20 text-blue-200'
                    : 'border-slate-600 bg-slate-800 text-slate-200 hover:border-slate-400'
                }`}
              >
                {tag}
              </button>
            );
          })}
        </div>
      </div>

      <div className="space-y-3">
        <label className="block text-xs uppercase tracking-wide text-slate-400">Регион</label>
        <select
          value={filters.region ?? ''}
          onChange={(e) => onChange({ ...filters, page: 1, region: e.target.value })}
          className="w-full rounded-md border border-slate-600 bg-slate-900 px-3 py-2"
        >
          <option value="">Все регионы</option>
          {regions.map((region) => (
            <option key={region} value={region}>
              {region}
            </option>
          ))}
        </select>

        <div className="grid grid-cols-2 gap-2">
          <input
            type="date"
            value={filters.date_from ?? ''}
            onChange={(e) => onChange({ ...filters, page: 1, date_from: e.target.value })}
            className="rounded-md border border-slate-600 bg-slate-900 px-3 py-2"
          />
          <input
            type="date"
            value={filters.date_to ?? ''}
            onChange={(e) => onChange({ ...filters, page: 1, date_to: e.target.value })}
            className="rounded-md border border-slate-600 bg-slate-900 px-3 py-2"
          />
        </div>
      </div>
    </div>
  </div>
);
