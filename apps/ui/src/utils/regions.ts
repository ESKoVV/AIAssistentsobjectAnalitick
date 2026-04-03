export interface RegionPoint {
  name: string;
  lat: number;
  lon: number;
}

export const REGION_POINTS: RegionPoint[] = [
  { name: 'Москва', lat: 55.7558, lon: 37.6176 },
  { name: 'Московская область', lat: 55.5043, lon: 38.0359 },
  { name: 'Ростовская область', lat: 47.2357, lon: 39.7015 },
  { name: 'Краснодарский край', lat: 45.0355, lon: 38.9753 },
  { name: 'Свердловская область', lat: 56.8389, lon: 60.6057 },
  { name: 'Республика Татарстан', lat: 55.8304, lon: 49.0661 },
  { name: 'Новосибирская область', lat: 55.0302, lon: 82.9204 },
  { name: 'Приморский край', lat: 43.1198, lon: 131.8869 }
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
