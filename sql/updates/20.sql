ALTER TABLE dataset_upload_session_part
  ADD COLUMN IF NOT EXISTS ws_id VARCHAR(128);

ALTER TABLE dataset_upload_session_part
  ADD COLUMN IF NOT EXISTS valid_until_ms BIGINT;
