-- One-time upgrade for an existing cdc.ddl_replay_journal created before
-- transformation provenance was persisted. Stop stream-binlog before running.
-- Preflight must confirm both columns are absent; this migration intentionally
-- fails rather than hiding a partially applied schema.
ALTER TABLE cdc.ddl_replay_journal
    ADD COLUMN transformation_version VARCHAR(64) NULL AFTER raw_sql,
    ADD COLUMN generated_sql LONGTEXT NULL AFTER transformation_version;

-- Existing rows represent the legacy raw-SQL execution path. Preserve that
-- provenance explicitly instead of presenting them as transformed statements.
UPDATE cdc.ddl_replay_journal
SET transformation_version = 'legacy-raw-v0',
    generated_sql = raw_sql
WHERE transformation_version IS NULL;

ALTER TABLE cdc.ddl_replay_journal
    MODIFY COLUMN transformation_version VARCHAR(64) NOT NULL;

-- Re-run ddl-replay-journal-bootstrap.sql after this migration to replace the
-- immutable-evidence trigger and trigger-inventory procedure with definitions
-- that include transformation_version and generated_sql.
