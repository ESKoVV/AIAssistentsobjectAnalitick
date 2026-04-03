export interface RegionPoint {
  name: string;
  lat: number;
  lon: number;
}

export const REGION_POINTS: RegionPoint[] = [
  { name: 'Северный район', lat: 55.75, lon: 37.61 },
  { name: 'Центральный район', lat: 55.76, lon: 37.62 },
  { name: 'Промышленный округ', lat: 55.70, lon: 37.56 },
  { name: 'Восточный район', lat: 55.77, lon: 37.70 },
  { name: 'Южный район', lat: 55.68, lon: 37.61 }
];

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
