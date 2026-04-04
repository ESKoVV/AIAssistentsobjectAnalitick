DROP VIEW IF EXISTS documents_with_ml;

DROP TABLE IF EXISTS ml_results;
DROP TABLE IF EXISTS document_fingerprints;
DROP TABLE IF EXISTS normalized_messages;

CREATE TABLE normalized_messages (
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
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT normalized_messages_source_type_source_id_key UNIQUE (source_type, source_id)
);

CREATE INDEX idx_normalized_messages_source_type
    ON normalized_messages (source_type);

CREATE INDEX idx_normalized_messages_source_id
    ON normalized_messages (source_id);

CREATE INDEX idx_normalized_messages_created_at
    ON normalized_messages (created_at DESC);

CREATE INDEX idx_normalized_messages_author_id
    ON normalized_messages (author_id);

CREATE INDEX idx_normalized_messages_filter_status
    ON normalized_messages (filter_status);

CREATE INDEX idx_normalized_messages_duplicate_group_id
    ON normalized_messages (duplicate_group_id);

CREATE TABLE document_fingerprints (
    doc_id TEXT PRIMARY KEY REFERENCES normalized_messages (doc_id) ON DELETE CASCADE,
    text_fingerprint TEXT NOT NULL,
    normalized_title TEXT NULL,
    normalized_text TEXT NOT NULL,
    duplicate_of TEXT NULL REFERENCES normalized_messages (doc_id) ON DELETE SET NULL,
    cluster_id TEXT NOT NULL,
    dedup_method TEXT NOT NULL DEFAULT 'none',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_document_fingerprints_text_fingerprint
    ON document_fingerprints (text_fingerprint);

CREATE INDEX idx_document_fingerprints_duplicate_of
    ON document_fingerprints (duplicate_of);

CREATE INDEX idx_document_fingerprints_cluster_id
    ON document_fingerprints (cluster_id);

CREATE TABLE ml_results (
    doc_id TEXT PRIMARY KEY REFERENCES normalized_messages (doc_id) ON DELETE CASCADE,
    summary TEXT NULL,
    score DOUBLE PRECISION NULL,
    category TEXT NULL,
    model_version TEXT NULL,
    prompt_version TEXT NULL,
    status TEXT NOT NULL DEFAULT 'processed',
    error_message TEXT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_result JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_ml_results_processed_at
    ON ml_results (processed_at);

CREATE INDEX idx_ml_results_status
    ON ml_results (status);

CREATE INDEX idx_ml_results_category
    ON ml_results (category);

CREATE VIEW documents_with_ml AS
SELECT
    nd.doc_id,
    nd.raw_message_id,
    nd.source_type,
    nd.source_id,
    nd.parent_id,
    nd.text,
    nd.media_type,
    nd.created_at,
    nd.collected_at,
    nd.author_id,
    nd.is_official,
    nd.reach,
    nd.likes,
    nd.reposts,
    nd.comments_count,
    nd.region_hint,
    nd.geo_lat,
    nd.geo_lon,
    nd.language,
    nd.language_confidence,
    nd.is_supported_language,
    nd.filter_status,
    nd.filter_reasons,
    nd.quality_weight,
    nd.anomaly_flags,
    nd.anomaly_confidence,
    nd.normalized_text,
    nd.cleanup_flags,
    nd.text_sha256,
    nd.duplicate_group_id,
    nd.near_duplicate_flag,
    nd.duplicate_cluster_size,
    nd.canonical_doc_id,
    nd.region_id,
    nd.municipality_id,
    nd.geo_confidence,
    nd.geo_source,
    nd.geo_evidence,
    nd.engagement,
    nd.metadata_version,
    df.duplicate_of,
    df.cluster_id,
    df.dedup_method,
    mr.summary,
    mr.score,
    mr.category,
    mr.model_version,
    mr.prompt_version,
    mr.status AS ml_status,
    mr.error_message AS ml_error_message,
    mr.processed_at
FROM normalized_messages AS nd
LEFT JOIN document_fingerprints AS df
    ON df.doc_id = nd.doc_id
LEFT JOIN ml_results AS mr
    ON mr.doc_id = nd.doc_id;
