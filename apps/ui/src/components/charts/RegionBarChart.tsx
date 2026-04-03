import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

export const RegionBarChart = ({ data }: { data: { region: string; count: number }[] }) => (
  <div className="h-80 rounded-lg border border-slate-700 bg-panel p-3">
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={data} layout="vertical">
        <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
        <XAxis type="number" stroke="#94a3b8" />
        <YAxis type="category" dataKey="region" width={140} stroke="#cbd5e1" />
        <Tooltip
          formatter={(value: number) => [`${value}`, 'Документов']}
          contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: 8 }}
          labelStyle={{ color: '#e2e8f0' }}
          itemStyle={{ color: '#93c5fd' }}
        />
        <Bar dataKey="count" name="Документов" fill="#3b82f6" />
      </BarChart>
    </ResponsiveContainer>
  </div>
);
