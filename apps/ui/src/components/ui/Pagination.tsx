interface PaginationProps {
  page: number;
  total: number;
  limit: number;
  onChange: (page: number) => void;
}

export const Pagination = ({ page, total, limit, onChange }: PaginationProps) => {
  const pages = Math.max(1, Math.ceil(total / limit));
  return (
    <div className="flex items-center gap-2">
      <button
        disabled={page <= 1}
        onClick={() => onChange(page - 1)}
        className="rounded bg-slate-700 px-3 py-1 disabled:opacity-50"
      >
        Назад
      </button>
      <span className="text-sm text-slate-400">
        Страница {page} / {pages}
      </span>
      <button
        disabled={page >= pages}
        onClick={() => onChange(page + 1)}
        className="rounded bg-slate-700 px-3 py-1 disabled:opacity-50"
      >
        Вперёд
      </button>
    </div>
  );
};
