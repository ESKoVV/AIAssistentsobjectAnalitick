CREATE TABLE IF NOT EXISTS document_fingerprints (
    doc_id TEXT PRIMARY KEY REFERENCES normalized_documents (doc_id) ON DELETE CASCADE,
    text_fingerprint TEXT NOT NULL,
    normalized_title TEXT NULL,
    normalized_text TEXT NOT NULL,
    duplicate_of TEXT NULL REFERENCES normalized_documents (doc_id) ON DELETE SET NULL,
    cluster_id TEXT NOT NULL,
    dedup_method TEXT NOT NULL DEFAULT 'none',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_document_fingerprints_text_fingerprint
    ON document_fingerprints (text_fingerprint);

CREATE INDEX IF NOT EXISTS idx_document_fingerprints_duplicate_of
    ON document_fingerprints (duplicate_of);

CREATE INDEX IF NOT EXISTS idx_document_fingerprints_cluster_id
    ON document_fingerprints (cluster_id);
