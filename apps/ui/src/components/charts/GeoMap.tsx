import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { CircleMarker, MapContainer, Popup, TileLayer } from 'react-leaflet';
import { GeoCluster, UrgencyLevel } from '../../types';

const DEFAULT_CENTER: [number, number] = [47.4743, 40.6964];
const DEFAULT_ZOOM = 7;

const colorByUrgency: Record<UrgencyLevel, string> = {
  low: '#94a3b8',
  medium: '#facc15',
  high: '#f97316',
  critical: '#ef4444'
};

const tileLayerUrl =
  import.meta.env.VITE_MAP_TILE_URL || 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';

const tileLayerAttribution =
  import.meta.env.VITE_MAP_ATTRIBUTION || '&copy; OpenStreetMap contributors';

const markerRadius = (mentionCount: number) =>
  Math.max(8, Math.min(28, Math.round(8 + Math.sqrt(Math.max(mentionCount, 1)) * 0.9)));

export const GeoMap = ({ clusters }: { clusters: GeoCluster[] }) => {
  const navigate = useNavigate();

  const points = useMemo(
    () =>
      clusters.flatMap((cluster) =>
        cluster.geo_points.map((point) => ({
          ...point,
          cluster_id: cluster.cluster_id,
          summary: cluster.summary,
          category_label: cluster.category_label,
          rank: cluster.rank,
          mention_count: cluster.mention_count,
          urgency: cluster.urgency
        }))
      ),
    [clusters]
  );

  const center = useMemo<[number, number]>(() => {
    if (!points.length) return DEFAULT_CENTER;
    const lat = points.reduce((acc, point) => acc + point.lat, 0) / points.length;
    const lon = points.reduce((acc, point) => acc + point.lon, 0) / points.length;
    return [lat, lon];
  }, [points]);

  if (!clusters.length) {
    return (
      <div className="rounded-lg border border-slate-700 bg-panel p-6 text-sm text-slate-400">
        Нет кластеров для отображения на карте.
      </div>
    );
  }

  if (!points.length) {
    return (
      <div className="rounded-lg border border-slate-700 bg-panel p-6 text-sm text-slate-400">
        Для текущего набора кластеров не найдены координаты регионов.
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-xl border border-slate-700 bg-panel">
      <div className="border-b border-slate-700 px-4 py-3">
        <h3 className="font-semibold text-slate-100">Карта геопривязки</h3>
        <p className="mt-1 text-sm text-slate-400">
          Радиус круга зависит от числа упоминаний, цвет отражает срочность темы.
        </p>
      </div>
      <MapContainer center={center} zoom={DEFAULT_ZOOM} className="h-[420px] w-full">
        <TileLayer attribution={tileLayerAttribution} url={tileLayerUrl} />
        {points.map((point) => {
          const color = colorByUrgency[point.urgency];
          return (
            <CircleMarker
              key={`${point.cluster_id}-${point.region}`}
              center={[point.lat, point.lon]}
              radius={markerRadius(point.mention_count)}
              pathOptions={{
                color,
                fillColor: color,
                fillOpacity: 0.58,
                weight: 2
              }}
              eventHandlers={{
                click: () => navigate(`/clusters/${point.cluster_id}`)
              }}
            >
              <Popup>
                <div className="space-y-1">
                  <p className="text-xs font-mono text-slate-400">#{point.rank}</p>
                  <p className="text-sm font-semibold text-slate-100">{point.summary}</p>
                  <p className="text-xs text-slate-300">{point.category_label}</p>
                  <p className="text-xs text-slate-400">
                    {point.region} · {point.mention_count} упоминаний
                  </p>
                </div>
              </Popup>
            </CircleMarker>
          );
        })}
      </MapContainer>
    </div>
  );
};
