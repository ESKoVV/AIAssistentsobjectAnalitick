import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from 'recharts';

const COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7', '#06b6d4'];

export const SourcePieChart = ({ data, nameKey }: { data: { count: number; [key: string]: string | number }[]; nameKey: string }) => (
  <div className="h-80 rounded-lg border border-slate-700 bg-panel p-3">
    <ResponsiveContainer width="100%" height="100%">
      <PieChart>
        <Pie data={data} dataKey="count" nameKey={nameKey} outerRadius={110}>
          {data.map((_, i) => (
            <Cell key={i} fill={COLORS[i % COLORS.length]} />
          ))}
        </Pie>
        <Tooltip
          formatter={(value: number) => [`${value}`, 'Посты']}
          contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: 8 }}
          labelStyle={{ color: '#e2e8f0' }}
          itemStyle={{ color: '#93c5fd' }}
        />
      </PieChart>
    </ResponsiveContainer>
  </div>
);
