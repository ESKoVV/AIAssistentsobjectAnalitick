import regions from '../mocks/regions.json';

export interface RegionPoint {
  name: string;
  lat: number;
  lon: number;
}

export const REGION_POINTS = regions as RegionPoint[];
export const RF_SUBJECTS_EXPECTED_TOTAL = 89;

if (REGION_POINTS.length !== RF_SUBJECTS_EXPECTED_TOTAL) {
  // Защита от неполного списка субъектов в мок-данных.
  // Нам нужен полный перечень для корректного выбора ближайшего региона.
  throw new Error(`regions.json must contain ${RF_SUBJECTS_EXPECTED_TOTAL} subjects, got ${REGION_POINTS.length}`);
}

export const getClosestRegion = (lat: number, lon: number): string => {
  const nearest = REGION_POINTS.reduce(
    (acc, region) => {
      const dLat = region.lat - lat;
      const dLon = region.lon - lon;
      const distance = dLat * dLat + dLon * dLon;
      if (distance < acc.distance) return { name: region.name, distance };
      return acc;
    },
    { name: REGION_POINTS[0].name, distance: Number.POSITIVE_INFINITY }
  );

  return nearest.name;
};
