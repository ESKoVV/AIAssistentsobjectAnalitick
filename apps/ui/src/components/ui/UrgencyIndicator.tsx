import { UrgencyLevel } from '../../types';

const labelByUrgency: Record<UrgencyLevel, string> = {
  low: 'Фоновый',
  medium: 'Требует внимания',
  high: 'Высокий приоритет',
  critical: 'Немедленная реакция'
};

const classByUrgency: Record<UrgencyLevel, string> = {
  low: 'bg-slate-700 text-slate-100',
  medium: 'bg-amber-700/60 text-amber-100',
  high: 'bg-orange-700/70 text-orange-100',
  critical: 'bg-rose-700/80 text-rose-100'
};

export const UrgencyIndicator = ({ urgency, reason }: { urgency: UrgencyLevel; reason: string }) => (
  <div className="space-y-2">
    <span className={`inline-flex rounded-full px-3 py-1 text-xs font-semibold ${classByUrgency[urgency]}`}>
      {labelByUrgency[urgency]}
    </span>
    <p className="text-xs text-slate-400">{reason}</p>
  </div>
);
