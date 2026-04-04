import psycopg

from config import load_config, validate_db_config

CONFIG = load_config()
validate_db_config(CONFIG)

sql = """
CREATE TABLE IF NOT EXISTS normalized_documents (
    doc_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    parent_id TEXT NULL,

    text TEXT NOT NULL,
    media_type TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,

    author_id TEXT NOT NULL,
    is_official BOOLEAN NOT NULL,

    reach INTEGER NOT NULL,
    likes INTEGER NOT NULL,
    reposts INTEGER NOT NULL,
    comments_count INTEGER NOT NULL,

    region_hint TEXT NULL,
    geo_lat DOUBLE PRECISION NULL,
    geo_lon DOUBLE PRECISION NULL,

    raw_payload JSONB NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_normalized_documents_source_type
    ON normalized_documents(source_type);

CREATE INDEX IF NOT EXISTS idx_normalized_documents_source_id
    ON normalized_documents(source_id);

CREATE INDEX IF NOT EXISTS idx_normalized_documents_created_at
    ON normalized_documents(created_at);

CREATE INDEX IF NOT EXISTS idx_normalized_documents_author_id
    ON normalized_documents(author_id);

CREATE TABLE IF NOT EXISTS document_fingerprints (
    doc_id TEXT PRIMARY KEY REFERENCES normalized_documents(doc_id) ON DELETE CASCADE,
    text_fingerprint TEXT NOT NULL,
    normalized_title TEXT NULL,
    normalized_text TEXT NOT NULL,
    duplicate_of TEXT NULL REFERENCES normalized_documents(doc_id) ON DELETE SET NULL,
    cluster_id TEXT NOT NULL,
    dedup_method TEXT NOT NULL DEFAULT 'none',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_document_fingerprints_text_fingerprint
    ON document_fingerprints(text_fingerprint);

CREATE INDEX IF NOT EXISTS idx_document_fingerprints_duplicate_of
    ON document_fingerprints(duplicate_of);

CREATE INDEX IF NOT EXISTS idx_document_fingerprints_cluster_id
    ON document_fingerprints(cluster_id);
"""

with psycopg.connect(CONFIG.database_url) as conn:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

print("✅ Таблица normalized_documents создана или уже существует в удалённой БД.")
