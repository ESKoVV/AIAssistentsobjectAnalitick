import { urgencyColor } from '../../utils/urgencyColor';

export const UrgencyIndicator = ({ score }: { score: number }) => (
  <div className="space-y-1">
    <div className="h-2 w-full overflow-hidden rounded bg-slate-700">
      <div className={`h-full ${urgencyColor(score)}`} style={{ width: `${Math.round(score * 100)}%` }} />
    </div>
    <p className="text-xs text-slate-400">Срочность: {score.toFixed(2)}</p>
  </div>
);
