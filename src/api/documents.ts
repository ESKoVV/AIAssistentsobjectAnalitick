import documentsMock from '../mocks/documents.json';
import { DocumentFilters, DocumentsResponse, NormalizedDocument } from '../types';
import { buildUrl, useMocks } from './client';

const mockData = documentsMock as NormalizedDocument[];

export const getDocuments = async (filters: DocumentFilters): Promise<DocumentsResponse> => {
  if (useMocks) {
    let filtered = [...mockData];
    if (filters.source_type?.length) {
      filtered = filtered.filter((d) => filters.source_type?.includes(d.source_type));
    }
    if (filters.region) {
      filtered = filtered.filter((d) => d.region_hint === filters.region);
    }
    if (filters.date_from) {
      filtered = filtered.filter((d) => d.created_at >= filters.date_from!);
    }
    if (filters.date_to) {
      filtered = filtered.filter((d) => d.created_at <= filters.date_to!);
    }
    const page = filters.page ?? 1;
    const limit = filters.limit ?? 20;
    const start = (page - 1) * limit;
    return { items: filtered.slice(start, start + limit), total: filtered.length, page, limit };
  }

  const url = buildUrl('/api/documents', {
    page: filters.page,
    limit: filters.limit,
    source_type: filters.source_type?.join(','),
    region: filters.region,
    date_from: filters.date_from,
    date_to: filters.date_to
  });
  const response = await fetch(url);
  if (!response.ok) throw new Error('Failed to load documents');
  return response.json();
};

export const getDocumentById = async (id: string): Promise<NormalizedDocument> => {
  if (useMocks) {
    const found = mockData.find((d) => d.doc_id === id);
    if (!found) throw new Error('Document not found');
    return found;
  }
  const response = await fetch(buildUrl(`/api/documents/${id}`));
  if (!response.ok) throw new Error('Failed to load document');
  return response.json();
};
