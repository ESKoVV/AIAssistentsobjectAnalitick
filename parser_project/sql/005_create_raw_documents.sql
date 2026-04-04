CREATE TABLE IF NOT EXISTS raw_documents (
    doc_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    parent_source_id TEXT NULL,
    text_raw TEXT NOT NULL,
    title_raw TEXT NULL,
    author_raw TEXT NULL,
    created_at_raw TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    source_url TEXT NULL,
    source_domain TEXT NULL,
    region_hint_raw TEXT NULL,
    geo_raw JSONB NULL,
    engagement_raw JSONB NOT NULL,
    raw_payload JSONB NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_documents_source_type
    ON raw_documents (source_type);

CREATE INDEX IF NOT EXISTS idx_raw_documents_source_id
    ON raw_documents (source_id);

CREATE INDEX IF NOT EXISTS idx_raw_documents_created_at
    ON raw_documents (created_at);

CREATE INDEX IF NOT EXISTS idx_raw_documents_collected_at
    ON raw_documents (collected_at);

CREATE INDEX IF NOT EXISTS idx_raw_documents_source_domain
    ON raw_documents (source_domain);
