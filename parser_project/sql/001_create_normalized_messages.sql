CREATE TABLE IF NOT EXISTS normalized_messages (
    doc_id TEXT PRIMARY KEY,
    raw_message_id UUID NOT NULL UNIQUE REFERENCES raw_messages (id) ON DELETE CASCADE,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    parent_id TEXT NULL,
    text TEXT NOT NULL,
    media_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    author_id TEXT NOT NULL,
    is_official BOOLEAN NOT NULL DEFAULT FALSE,
    reach INTEGER NOT NULL DEFAULT 0,
    likes INTEGER NOT NULL DEFAULT 0,
    reposts INTEGER NOT NULL DEFAULT 0,
    comments_count INTEGER NOT NULL DEFAULT 0,
    region_hint TEXT NULL,
    geo_lat DOUBLE PRECISION NULL,
    geo_lon DOUBLE PRECISION NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    language TEXT NOT NULL,
    language_confidence DOUBLE PRECISION NOT NULL,
    is_supported_language BOOLEAN NOT NULL,
    filter_status TEXT NOT NULL,
    filter_reasons TEXT[] NOT NULL DEFAULT '{}'::text[],
    quality_weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    anomaly_flags TEXT[] NOT NULL DEFAULT '{}'::text[],
    anomaly_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    normalized_text TEXT NOT NULL,
    token_count INTEGER NOT NULL DEFAULT 0,
    cleanup_flags TEXT[] NOT NULL DEFAULT '{}'::text[],
    text_sha256 TEXT NOT NULL,
    duplicate_group_id TEXT NOT NULL,
    near_duplicate_flag BOOLEAN NOT NULL DEFAULT FALSE,
    duplicate_cluster_size INTEGER NOT NULL DEFAULT 1,
    canonical_doc_id TEXT NOT NULL,
    region_id TEXT NULL,
    municipality_id TEXT NULL,
    geo_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    geo_source TEXT NOT NULL DEFAULT 'unresolved',
    geo_evidence TEXT[] NOT NULL DEFAULT '{}'::text[],
    engagement INTEGER NOT NULL DEFAULT 0,
    metadata_version TEXT NOT NULL DEFAULT 'meta-v1',
    category TEXT NOT NULL DEFAULT 'other',
    category_label TEXT NOT NULL DEFAULT 'Прочее',
    category_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    secondary_category TEXT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT normalized_messages_source_type_source_id_key UNIQUE (source_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_normalized_messages_source_type
    ON normalized_messages (source_type);

CREATE INDEX IF NOT EXISTS idx_normalized_messages_source_id
    ON normalized_messages (source_id);

CREATE INDEX IF NOT EXISTS idx_normalized_messages_created_at
    ON normalized_messages (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_normalized_messages_author_id
    ON normalized_messages (author_id);

CREATE INDEX IF NOT EXISTS idx_normalized_messages_filter_status
    ON normalized_messages (filter_status);

CREATE INDEX IF NOT EXISTS idx_normalized_messages_duplicate_group_id
    ON normalized_messages (duplicate_group_id);
