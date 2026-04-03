import { useMemo, useState } from 'react';
import { DocumentFilters, TopicTag } from '../../types';

interface FilterPanelProps {
  filters: DocumentFilters;
  regions: string[];
  onChange: (next: DocumentFilters) => void;
  onReset: () => void;
  allTags: TopicTag[];
}

type PeriodMode = 'all' | 'today' | '7d' | '30d' | 'custom';

const toDate = (date: Date) => date.toISOString().slice(0, 10);

export const FilterPanel = ({ filters, regions, onChange, onReset, allTags }: FilterPanelProps) => {
  const [showPeriodOptions, setShowPeriodOptions] = useState(false);

  const periodMode = useMemo<PeriodMode>(() => {
    if (!filters.date_from && !filters.date_to) return 'all';
    return 'custom';
  }, [filters.date_from, filters.date_to]);

  const applyPeriod = (mode: Exclude<PeriodMode, 'custom'>) => {
    const now = new Date();
    const today = toDate(now);

    if (mode === 'all') {
      onChange({ ...filters, page: 1, date_from: undefined, date_to: undefined });
      setShowPeriodOptions(false);
      return;
    }

    if (mode === 'today') {
      onChange({ ...filters, page: 1, date_from: today, date_to: today });
      setShowPeriodOptions(false);
      return;
    }

    const days = mode === '7d' ? 6 : 29;
    const from = new Date(now);
    from.setDate(now.getDate() - days);

    onChange({ ...filters, page: 1, date_from: toDate(from), date_to: today });
    setShowPeriodOptions(false);
  };

  return (
    <div className="rounded-xl border border-slate-700 bg-panel p-5 shadow-lg shadow-slate-950/20">
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-lg font-semibold">Фильтры ленты</h3>
        <button
          onClick={onReset}
          className="rounded-md border border-slate-600 bg-slate-900 px-3 py-1.5 text-sm text-slate-100 transition hover:border-slate-400 hover:bg-slate-800"
        >
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
            className="w-full rounded-md border border-slate-600 bg-slate-900 px-3 py-2 text-slate-100"
          >
            <option value="">Все регионы</option>
            {regions.map((region) => (
              <option key={region} value={region}>
                {region}
              </option>
            ))}
          </select>

          <div className="relative">
            <button
              onClick={() => setShowPeriodOptions((prev) => !prev)}
              className="w-full rounded-md border border-slate-600 bg-slate-900 px-3 py-2 text-left text-sm text-slate-100 transition hover:border-slate-400 hover:bg-slate-800"
            >
              Период новостей
            </button>
            {showPeriodOptions && (
              <div className="absolute z-10 mt-2 grid w-full gap-2 rounded-md border border-slate-600 bg-slate-950 p-2 shadow-xl">
                <button onClick={() => applyPeriod('all')} className="rounded-md px-3 py-2 text-left text-sm hover:bg-slate-800">
                  За всё время
                </button>
                <button onClick={() => applyPeriod('today')} className="rounded-md px-3 py-2 text-left text-sm hover:bg-slate-800">
                  Сегодня
                </button>
                <button onClick={() => applyPeriod('7d')} className="rounded-md px-3 py-2 text-left text-sm hover:bg-slate-800">
                  Последние 7 дней
                </button>
                <button onClick={() => applyPeriod('30d')} className="rounded-md px-3 py-2 text-left text-sm hover:bg-slate-800">
                  Последние 30 дней
                </button>
                <button
                  onClick={() => {
                    setShowPeriodOptions(false);
                    onChange({ ...filters, page: 1 });
                  }}
                  className="rounded-md px-3 py-2 text-left text-sm hover:bg-slate-800"
                >
                  Задать вручную
                </button>
              </div>
            )}
          </div>

          {periodMode === 'custom' && (
            <div className="grid grid-cols-2 gap-2">
              <input
                type="date"
                value={filters.date_from ?? ''}
                onChange={(e) => onChange({ ...filters, page: 1, date_from: e.target.value })}
                className="rounded-md border border-slate-600 bg-slate-900 px-3 py-2 text-slate-100"
              />
              <input
                type="date"
                value={filters.date_to ?? ''}
                onChange={(e) => onChange({ ...filters, page: 1, date_to: e.target.value })}
                className="rounded-md border border-slate-600 bg-slate-900 px-3 py-2 text-slate-100"
              />
            </div>
          )}

          <p className="text-xs text-slate-400">
            Текущий период: {filters.date_from && filters.date_to ? `${filters.date_from} — ${filters.date_to}` : 'за всё время'}
          </p>
        </div>
      </div>
    </div>
  );
};
