ALTER TABLE reader_memory_profiles
 ADD COLUMN IF NOT EXISTS suggestions_status VARCHAR(16) NOT NULL DEFAULT 'idle',
 ADD COLUMN IF NOT EXISTS suggestions_dispatch_after DATETIME(6) NULL,
 ADD COLUMN IF NOT EXISTS suggestions_started_at DATETIME(6) NULL,
 ADD INDEX IF NOT EXISTS reader_memory_suggestion_dispatch (suggestions_status,suggestions_dispatch_after),
 ADD INDEX IF NOT EXISTS reader_memory_suggestion_started (suggestions_started_at)
