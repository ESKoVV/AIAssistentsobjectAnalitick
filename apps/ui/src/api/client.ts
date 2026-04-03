const BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

export const useMocks = !BASE_URL || BASE_URL.includes('localhost:8000');

export const buildUrl = (path: string, params?: Record<string, string | number | undefined>) => {
  const url = new URL(`${BASE_URL}${path}`, window.location.origin);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== '') {
        url.searchParams.set(key, String(value));
      }
    });
  }
  return url.toString();
};
