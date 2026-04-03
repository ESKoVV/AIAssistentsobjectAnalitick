import { useQuery } from '@tanstack/react-query';
import { getDocumentById, getDocuments } from '../api/documents';
import { DocumentFilters } from '../types';

export const useDocuments = (filters: DocumentFilters) =>
  useQuery({
    queryKey: ['documents', filters],
    queryFn: () => getDocuments(filters)
  });

export const useDocument = (id?: string) =>
  useQuery({
    queryKey: ['document', id],
    queryFn: () => getDocumentById(id ?? ''),
    enabled: Boolean(id)
  });
