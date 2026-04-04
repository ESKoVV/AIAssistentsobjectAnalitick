CREATE TABLE IF NOT EXISTS ml_results (
    doc_id TEXT PRIMARY KEY REFERENCES normalized_documents (doc_id) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS idx_ml_results_processed_at
    ON ml_results (processed_at);

CREATE INDEX IF NOT EXISTS idx_ml_results_status
    ON ml_results (status);

CREATE INDEX IF NOT EXISTS idx_ml_results_category
    ON ml_results (category);
