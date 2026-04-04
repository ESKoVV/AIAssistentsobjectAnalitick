-- Legacy compatibility migration.
--
-- The canonical table in the message pipeline is `normalized_messages`
-- (created in 001_create_normalized_messages.sql).
--
-- Some local scripts still check for this legacy filename,
-- so we keep this no-op migration to avoid FileNotFoundError.
SELECT 1;
