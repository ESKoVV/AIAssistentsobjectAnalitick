export const ErrorState = ({ message }: { message?: string }) => (
  <p className="rounded-md border border-red-500/40 bg-red-950/40 p-3 text-red-300">
    Ошибка загрузки. {message}
  </p>
);
