interface KpiCardProps {
  label: string;
  value: string | number;
}

export const KpiCard = ({ label, value }: KpiCardProps) => (
  <div className="rounded-lg border border-slate-700 bg-panel p-4">
    <p className="text-sm text-slate-400">{label}</p>
    <p className="mt-1 font-mono text-2xl font-bold">{value}</p>
  </div>
);
