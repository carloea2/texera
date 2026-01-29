-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db
SET search_path TO texera_db, public;

BEGIN;

-- Step 1: Add the column (no default yet)
ALTER TABLE dataset_upload_session
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;

-- Step 2: Add the default for future inserts
ALTER TABLE dataset_upload_session
    ALTER COLUMN created_at SET DEFAULT now();

ALTER TABLE dataset_upload_session
    ALTER COLUMN created_at SET NOT NULL;

COMMIT;
