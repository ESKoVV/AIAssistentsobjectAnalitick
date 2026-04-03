export const formatDateTime = (value: string) =>
  new Date(value).toLocaleString('ru-RU', { timeZone: 'UTC', hour12: false });

export const formatDate = (value: string) =>
  new Date(value).toLocaleDateString('ru-RU', { timeZone: 'UTC' });
