CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS document_sentiments (
    doc_id TEXT PRIMARY KEY REFERENCES normalized_messages (doc_id) ON DELETE CASCADE,
    sentiment_score DOUBLE PRECISION NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_result JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_document_sentiments_processed_at
    ON document_sentiments (processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_document_sentiments_score
    ON document_sentiments (sentiment_score);
