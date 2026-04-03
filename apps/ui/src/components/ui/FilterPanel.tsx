import { CalendarDays, Filter, RotateCcw, SlidersHorizontal } from 'lucide-react';
import { DocumentFilters, TopicTag } from '../../types';

interface FilterPanelProps {
  filters: DocumentFilters;
  regions: string[];
  onChange: (next: DocumentFilters) => void;
  onReset: () => void;
  allTags: TopicTag[];
}

export const FilterPanel = ({ filters, regions, onChange, onReset, allTags }: FilterPanelProps) => (
  <section className="rounded-2xl border border-slate-700/80 bg-gradient-to-b from-slate-900 to-slate-900/80 p-5 shadow-xl shadow-slate-950/20">
    <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
      <div className="flex items-center gap-2">
        <Filter className="h-4 w-4 text-blue-300" />
        <h3 className="text-lg font-semibold">Фильтры ленты</h3>
      </div>
      <button
        onClick={onReset}
        className="inline-flex items-center gap-2 rounded-md border border-slate-600 px-3 py-1.5 text-sm text-slate-200 hover:bg-slate-800"
      >
        <RotateCcw className="h-4 w-4" />
        Сбросить
      </button>
    </div>

    <div className="grid gap-4 xl:grid-cols-[2fr_1fr]">
      <div className="rounded-xl border border-slate-700/70 bg-slate-900/60 p-4">
        <p className="mb-2 flex items-center gap-2 text-xs uppercase tracking-wide text-slate-400">
          <SlidersHorizontal className="h-4 w-4" />
          Тематики
        </p>
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
                className={`rounded-full border px-3 py-1 text-sm font-medium transition ${
                  isActive
                    ? 'border-blue-400 bg-blue-500/20 text-blue-100 shadow-[0_0_0_1px_rgba(96,165,250,0.25)]'
                    : 'border-slate-600 bg-slate-800/80 text-slate-200 hover:border-slate-400'
                }`}
              >
                {tag}
              </button>
            );
          })}
        </div>
      </div>

      <div className="space-y-3 rounded-xl border border-slate-700/70 bg-slate-900/60 p-4">
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

        <p className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-400">
          <CalendarDays className="h-4 w-4" />
          Период
        </p>

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
  </section>
);
