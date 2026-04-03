import { DocumentFilters, SourceType } from '../../types';

const sourceTypes: SourceType[] = [
  'vk_post',
  'vk_comment',
  'telegram_post',
  'telegram_comment',
  'rss_article',
  'portal_appeal'
];

interface FilterPanelProps {
  filters: DocumentFilters;
  regions: string[];
  onChange: (next: DocumentFilters) => void;
  onReset: () => void;
}

export const FilterPanel = ({ filters, regions, onChange, onReset }: FilterPanelProps) => (
  <div className="rounded-lg border border-slate-700 bg-panel p-4">
    <p className="mb-3 font-semibold">Фильтры</p>
    <div className="grid gap-3 md:grid-cols-2">
      <div>
        <p className="mb-1 text-xs text-slate-400">Тип источника</p>
        <div className="grid grid-cols-2 gap-1 text-sm">
          {sourceTypes.map((source) => {
            const checked = filters.source_type?.includes(source) ?? false;
            return (
              <label key={source} className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => {
                    const selected = new Set(filters.source_type ?? []);
                    checked ? selected.delete(source) : selected.add(source);
                    onChange({ ...filters, page: 1, source_type: Array.from(selected) });
                  }}
                />
                {source}
              </label>
            );
          })}
        </div>
      </div>

      <div className="space-y-2">
        <label className="block text-xs text-slate-400">Регион</label>
        <select
          value={filters.region ?? ''}
          onChange={(e) => onChange({ ...filters, page: 1, region: e.target.value })}
          className="w-full rounded bg-slate-800 p-2"
        >
          <option value="">Все регионы</option>
          {regions.map((region) => (
            <option key={region} value={region}>
              {region}
            </option>
          ))}
        </select>

        <input
          type="date"
          value={filters.date_from ?? ''}
          onChange={(e) => onChange({ ...filters, page: 1, date_from: e.target.value })}
          className="w-full rounded bg-slate-800 p-2"
        />
        <input
          type="date"
          value={filters.date_to ?? ''}
          onChange={(e) => onChange({ ...filters, page: 1, date_to: e.target.value })}
          className="w-full rounded bg-slate-800 p-2"
        />

        <button onClick={onReset} className="rounded bg-slate-700 px-3 py-1">
          Сбросить
        </button>
      </div>
    </div>
  </div>
);
