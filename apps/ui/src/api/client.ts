const BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

export const useMocks = import.meta.env.VITE_USE_MOCKS === 'true' || !BASE_URL;

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

export const buildHeaders = (): HeadersInit => {
  const token =
    import.meta.env.VITE_API_TOKEN ||
    (typeof window !== 'undefined' ? window.localStorage.getItem('apiToken') : null);

  return token
    ? {
        Authorization: `Bearer ${token}`
      }
    : {};
};

export const fetchJson = async <T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> => {
  const response = await fetch(buildUrl(path, params), {
    headers: {
      'Content-Type': 'application/json',
      ...buildHeaders()
    }
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(errorText || `Request failed with status ${response.status}`);
  }

  return response.json() as Promise<T>;
};
