import { useMemo, useState } from 'react';
import { DocumentCard } from '../components/ui/DocumentCard';
import { EmptyState } from '../components/ui/EmptyState';
import { ErrorState } from '../components/ui/ErrorState';
import { FilterPanel } from '../components/ui/FilterPanel';
import { LoadingState } from '../components/ui/LoadingState';
import { Pagination } from '../components/ui/Pagination';
import { useDocuments } from '../hooks/useDocuments';
import documents from '../mocks/documents.json';
import { DocumentFilters, NormalizedDocument } from '../types';

export const Feed = () => {
  const [filters, setFilters] = useState<DocumentFilters>({ page: 1, limit: 20, source_type: [] });
  const { data, isLoading, error } = useDocuments(filters);
  const regions = useMemo(
    () => Array.from(new Set((documents as NormalizedDocument[]).map((d) => d.region_hint).filter(Boolean))) as string[],
    []
  );

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-semibold">Лента документов</h2>
      <FilterPanel
        filters={filters}
        regions={regions}
        onChange={setFilters}
        onReset={() => setFilters({ page: 1, limit: 20, source_type: [] })}
      />
      {isLoading && <LoadingState />}
      {error && <ErrorState message={(error as Error).message} />}
      {!isLoading && !error && data?.items.length === 0 && <EmptyState title="Документы не найдены" />}
      <div className="grid gap-3">
        {data?.items.map((doc) => (
          <DocumentCard key={doc.doc_id} doc={doc} />
        ))}
      </div>
      {data && <Pagination page={data.page} total={data.total} limit={data.limit} onChange={(page) => setFilters({ ...filters, page })} />}
    </div>
  );
};
