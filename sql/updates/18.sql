-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db

SET search_path TO texera_db;

-- ============================================
-- 2. Update the table schema (ALTER only)
-- ============================================
BEGIN;

-- columns: safe
ALTER TABLE dataset_upload_session
    ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT NOT NULL DEFAULT 1;

ALTER TABLE dataset_upload_session
    ADD COLUMN IF NOT EXISTS part_size_bytes BIGINT NOT NULL DEFAULT 5242880;

-- constraints: drop then add (idempotent)
ALTER TABLE dataset_upload_session
    DROP CONSTRAINT IF EXISTS dataset_upload_session_num_parts_requested_positive;

ALTER TABLE dataset_upload_session
    DROP CONSTRAINT IF EXISTS chk_dataset_upload_session_file_size_bytes_positive;

ALTER TABLE dataset_upload_session
    DROP CONSTRAINT IF EXISTS chk_dataset_upload_session_part_size_bytes_positive;

ALTER TABLE dataset_upload_session
    DROP CONSTRAINT IF EXISTS dataset_upload_session_part_size_bytes_positive;

ALTER TABLE dataset_upload_session
    DROP CONSTRAINT IF EXISTS dataset_upload_session_part_size_bytes_s3_upper_bound;

ALTER TABLE dataset_upload_session
    ADD CONSTRAINT dataset_upload_session_num_parts_requested_positive
        CHECK (num_parts_requested >= 1);

ALTER TABLE dataset_upload_session
    ADD CONSTRAINT chk_dataset_upload_session_file_size_bytes_positive
        CHECK (file_size_bytes > 0);

ALTER TABLE dataset_upload_session
    ADD CONSTRAINT chk_dataset_upload_session_part_size_bytes_positive
        CHECK (part_size_bytes > 0);

ALTER TABLE dataset_upload_session
    ADD CONSTRAINT dataset_upload_session_part_size_bytes_positive
        CHECK (part_size_bytes >= 1);

ALTER TABLE dataset_upload_session
    ADD CONSTRAINT dataset_upload_session_part_size_bytes_s3_upper_bound
        CHECK (part_size_bytes <= 5368709120);

COMMIT;

