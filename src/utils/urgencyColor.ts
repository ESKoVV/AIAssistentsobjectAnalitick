export const urgencyColor = (score: number) => {
  if (score >= 0.7) return 'bg-red-500';
  if (score >= 0.4) return 'bg-yellow-500';
  return 'bg-green-500';
};
