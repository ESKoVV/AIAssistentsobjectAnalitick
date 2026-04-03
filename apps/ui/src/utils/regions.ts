import regions from '../mocks/regions.json';

export interface RegionPoint {
  name: string;
  lat: number;
  lon: number;
}

export const REGION_POINTS = regions as RegionPoint[];

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
