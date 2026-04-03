import { RegionBarChart } from '../components/charts/RegionBarChart';
import { SourcePieChart } from '../components/charts/SourcePieChart';
import { TimelineLineChart } from '../components/charts/TimelineLineChart';
import { ErrorState } from '../components/ui/ErrorState';
import { LoadingState } from '../components/ui/LoadingState';
import { useStats } from '../hooks/useStats';

export const Analytics = () => {
  const { data, isLoading, error } = useStats();

  if (isLoading) return <LoadingState />;
  if (error) return <ErrorState message={(error as Error).message} />;
  if (!data) return null;

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Аналитика</h2>
      <TimelineLineChart data={data.timeline} />
      <div className="grid gap-4 lg:grid-cols-2">
        <SourcePieChart data={data.by_tag} nameKey="tag" />
        <RegionBarChart data={data.by_region.slice(0, 10)} />
      </div>
      <div className="rounded-lg border border-slate-700 bg-panel p-4">
        <h3 className="mb-3 font-semibold">Тег / количество / доля официальных / средний охват</h3>
        <table className="w-full text-sm">
          <thead className="text-slate-400">
            <tr>
              <th className="text-left">Тег</th>
              <th className="text-left">Количество</th>
              <th className="text-left">Официальные</th>
              <th className="text-left">Средний охват</th>
            </tr>
          </thead>
          <tbody>
            {data.by_tag.map((tagRow) => (
              <tr key={tagRow.tag} className="border-t border-slate-700">
                <td className="py-2">{tagRow.tag}</td>
                <td>{tagRow.count}</td>
                <td>{tagRow.official_share}%</td>
                <td>{tagRow.avg_reach}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
