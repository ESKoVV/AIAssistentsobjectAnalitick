ALTER TABLE IF EXISTS public.raw_messages
    ADD COLUMN IF NOT EXISTS text_compressed BYTEA NULL,
    ADD COLUMN IF NOT EXISTS raw_payload_compressed BYTEA NULL,
    ADD COLUMN IF NOT EXISTS compression_codec TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_messages_source_type
    ON public.raw_messages (source_type);

CREATE INDEX IF NOT EXISTS idx_raw_messages_created_at_utc
    ON public.raw_messages (created_at_utc);

CREATE INDEX IF NOT EXISTS idx_raw_messages_collected_at
    ON public.raw_messages (collected_at);
