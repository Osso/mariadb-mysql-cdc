-- Run once with target admin credentials before a phased unified sync or
-- resume-lost-binlog invocation. This is additive: it does not alter
-- cdc.sync_runs or stream checkpoint/journal state.
--
-- Default --progress-table is cdc.sync_runs, so its phase table is
-- cdc.sync_runs_phases. For another progress table, create and grant the
-- corresponding <progress-table>_phases table instead.
CREATE TABLE IF NOT EXISTS cdc.sync_runs_phases (
    run_id VARBINARY(512) NOT NULL,
    table_name VARBINARY(1020) NOT NULL,
    phase VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    last_primary_key_json JSON NULL,
    complete BOOLEAN NOT NULL DEFAULT FALSE,
    chunks BIGINT UNSIGNED NOT NULL DEFAULT 0,
    rows_scanned BIGINT UNSIGNED NOT NULL DEFAULT 0,
    inserts BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updates BIGINT UNSIGNED NOT NULL DEFAULT 0,
    deletes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, table_name, phase),
    CHECK (phase IN ('insert_missing', 'update_divergent', 'delete_extras')),
    CHECK (complete IN (0, 1))
) ENGINE=InnoDB;

-- The deployed one-shot sync/recovery runtime uses cdc_stream. Keep this
-- grant table-specific; do not grant schema-wide cdc privileges.
GRANT SELECT, INSERT, UPDATE
    ON cdc.sync_runs_phases
    TO 'cdc_stream'@'%';
