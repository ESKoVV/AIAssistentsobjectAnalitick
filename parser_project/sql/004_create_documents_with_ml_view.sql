CREATE OR REPLACE VIEW documents_with_ml AS
SELECT
    nd.doc_id,
    nd.source_type,
    nd.source_id,
    nd.text,
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
FROM normalized_documents AS nd
LEFT JOIN document_fingerprints AS df
    ON df.doc_id = nd.doc_id
LEFT JOIN ml_results AS mr
    ON mr.doc_id = nd.doc_id;
