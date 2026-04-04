CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS raw_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    author_id TEXT NULL,
    text TEXT NOT NULL,
    media_type TEXT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload JSONB NOT NULL,
    is_official BOOLEAN NULL DEFAULT FALSE,
    reach INTEGER NULL DEFAULT 0,
    likes INTEGER NULL DEFAULT 0,
    reposts INTEGER NULL DEFAULT 0,
    comments_count INTEGER NULL DEFAULT 0,
    parent_id TEXT NULL,
    CONSTRAINT raw_messages_source_type_source_id_key UNIQUE (source_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_messages_source_type
    ON raw_messages (source_type);

CREATE INDEX IF NOT EXISTS idx_raw_messages_created_at_utc
    ON raw_messages (created_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_raw_messages_author_id
    ON raw_messages (author_id);
