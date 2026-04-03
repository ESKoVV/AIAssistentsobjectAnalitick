import { RefreshCw } from 'lucide-react';

interface HeaderProps {
  onRefresh?: () => void;
}

export const Header = ({ onRefresh }: HeaderProps) => (
  <header className="flex items-center justify-between border-b border-slate-700 bg-panel px-6 py-4">
    <div>
      <h1 className="text-4xl font-bold leading-tight">Региональный аналитический дашборд</h1>
      <p className="font-mono text-sm text-slate-400">
        UTC: {new Date().toLocaleString('ru-RU', { hour12: false, timeZone: 'UTC' })}
      </p>
    </div>
    {onRefresh && (
      <button
        onClick={onRefresh}
        className="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-2 text-sm hover:bg-blue-500"
      >
        <RefreshCw size={16} />
        Обновить
      </button>
    )}
  </header>
);
