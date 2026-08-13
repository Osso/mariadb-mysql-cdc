-- Target-only migration for the live stream_recovery_records schema observed on
-- 2026-08-13. It intentionally refuses a second run before making changes.
-- Run with target admin credentials while stream-binlog is stopped.
-- The existing prepared row for recovery_id
-- cdc-lost-binlog-2026-08-09-drop-trigger must remain present and exact.
-- This migration changes only prepared -> abandoned/committed and committed -> verified guards.
DELIMITER //

DROP PROCEDURE IF EXISTS cdc.assert_stream_recovery_abandoned_replacement_preflight//
CREATE PROCEDURE cdc.assert_stream_recovery_abandoned_replacement_preflight()
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'cdc'
          AND table_name = 'stream_recovery_records'
          AND column_name = 'abandoned_evidence_json'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'abandoned replacement migration already applied; rerun refused';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM cdc.stream_recovery_records
        WHERE status IN ('prepared', 'committed', 'verified')
        GROUP BY old_barrier_source_identity,
                 old_barrier_file,
                 old_barrier_start_position,
                 old_barrier_end_position,
                 old_barrier_raw_sql_sha256
        HAVING COUNT(*) > 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'duplicate active stream recovery barrier owners require manual review';
    END IF;
END//

CALL cdc.assert_stream_recovery_abandoned_replacement_preflight()//
DROP PROCEDURE cdc.assert_stream_recovery_abandoned_replacement_preflight//
DELIMITER ;

ALTER TABLE cdc.stream_recovery_records
    ADD COLUMN abandoned_evidence_json LONGTEXT NULL AFTER verified_at,
    ADD COLUMN abandoned_at TIMESTAMP(6) NULL AFTER abandoned_evidence_json,
    ADD COLUMN active_barrier_identity CHAR(64) CHARACTER SET ascii COLLATE ascii_bin
        GENERATED ALWAYS AS (
            CASE
                WHEN status IN ('prepared', 'committed', 'verified') THEN barrier_identity
                ELSE NULL
            END
        ) STORED AFTER abandoned_at;

ALTER TABLE cdc.stream_recovery_records
    DROP CHECK stream_recovery_records_chk_6,
    ADD CONSTRAINT stream_recovery_records_chk_6 CHECK (
        status IN ('prepared', 'committed', 'verified', 'abandoned')
        AND (
            (
                status = 'abandoned'
                AND abandoned_evidence_json IS NOT NULL
                AND abandoned_evidence_json <> ''
                AND JSON_VALID(abandoned_evidence_json)
                AND abandoned_at IS NOT NULL
            )
            OR (
                status <> 'abandoned'
                AND abandoned_evidence_json IS NULL
                AND abandoned_at IS NULL
            )
        )
    );

-- Add the nullable active identity before removing the old all-history unique key.
-- Multiple abandoned rows may then share the historical barrier while active rows
-- remain unique because MySQL permits multiple NULLs in a unique index.
ALTER TABLE cdc.stream_recovery_records
    ADD UNIQUE KEY stream_recovery_active_barrier (active_barrier_identity);

ALTER TABLE cdc.stream_recovery_records
    DROP INDEX stream_recovery_exact_barrier;

DELIMITER //
DROP TRIGGER IF EXISTS cdc.stream_recovery_records_insert_guard//
CREATE TRIGGER cdc.stream_recovery_records_insert_guard
BEFORE INSERT ON cdc.stream_recovery_records
FOR EACH ROW
BEGIN
    IF NEW.status <> 'prepared'
       OR NEW.committed_evidence_json IS NOT NULL
       OR NEW.verified_evidence_json IS NOT NULL
       OR NEW.committed_at IS NOT NULL
       OR NEW.verified_at IS NOT NULL
       OR NEW.abandoned_evidence_json IS NOT NULL
       OR NEW.abandoned_at IS NOT NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'stream recovery records must be inserted prepared';
    END IF;
END//

DROP TRIGGER IF EXISTS cdc.stream_recovery_records_update_guard//
CREATE TRIGGER cdc.stream_recovery_records_update_guard
BEFORE UPDATE ON cdc.stream_recovery_records
FOR EACH ROW
BEGIN
    IF NOT (OLD.recovery_id <=> NEW.recovery_id)
       OR NOT (OLD.checkpoint_name <=> NEW.checkpoint_name)
       OR NOT (OLD.source_identity <=> NEW.source_identity)
       OR NOT (OLD.scope_hash <=> NEW.scope_hash)
       OR NOT (OLD.old_checkpoint_json <=> NEW.old_checkpoint_json)
       OR NOT (OLD.new_checkpoint_json <=> NEW.new_checkpoint_json)
       OR NOT (OLD.old_barrier_source_identity <=> NEW.old_barrier_source_identity)
       OR NOT (OLD.old_barrier_file <=> NEW.old_barrier_file)
       OR NOT (OLD.old_barrier_start_position <=> NEW.old_barrier_start_position)
       OR NOT (OLD.old_barrier_end_position <=> NEW.old_barrier_end_position)
       OR NOT (OLD.old_barrier_raw_sql <=> NEW.old_barrier_raw_sql)
       OR NOT (OLD.operator_identity <=> NEW.operator_identity)
       OR NOT (OLD.reason <=> NEW.reason)
       OR NOT (OLD.prepared_evidence_json <=> NEW.prepared_evidence_json)
       OR NOT (OLD.prepared_at <=> NEW.prepared_at)
       OR NOT (
           (
               OLD.status = 'prepared'
               AND NEW.status = 'abandoned'
               AND OLD.abandoned_evidence_json IS NULL
               AND OLD.abandoned_at IS NULL
               AND NEW.abandoned_evidence_json IS NOT NULL
               AND NEW.abandoned_evidence_json <> ''
               AND JSON_VALID(NEW.abandoned_evidence_json)
               AND NEW.abandoned_at IS NOT NULL
               AND NEW.committed_evidence_json IS NULL
               AND NEW.committed_at IS NULL
               AND NEW.verified_evidence_json IS NULL
               AND NEW.verified_at IS NULL
           )
           OR (
               OLD.status = 'prepared'
               AND NEW.status = 'committed'
               AND OLD.abandoned_evidence_json IS NULL
               AND OLD.abandoned_at IS NULL
               AND NEW.abandoned_evidence_json IS NULL
               AND NEW.abandoned_at IS NULL
               AND NEW.committed_evidence_json IS NOT NULL
               AND NEW.committed_evidence_json <> ''
               AND NEW.committed_at IS NOT NULL
               AND NEW.verified_evidence_json IS NULL
               AND NEW.verified_at IS NULL
           )
           OR (
               OLD.status = 'committed'
               AND NEW.status = 'verified'
               AND OLD.committed_evidence_json <=> NEW.committed_evidence_json
               AND OLD.committed_at <=> NEW.committed_at
               AND OLD.abandoned_evidence_json IS NULL
               AND OLD.abandoned_at IS NULL
               AND NEW.abandoned_evidence_json IS NULL
               AND NEW.abandoned_at IS NULL
               AND NEW.verified_evidence_json IS NOT NULL
               AND NEW.verified_evidence_json <> ''
               AND NEW.verified_at IS NOT NULL
           )
       ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'stream recovery identity is immutable and status transition is not allowed';
    END IF;
END//
DELIMITER ;

DELIMITER //
DROP PROCEDURE IF EXISTS cdc.assert_stream_recovery_abandoned_replacement_postflight//
CREATE PROCEDURE cdc.assert_stream_recovery_abandoned_replacement_postflight()
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM cdc.stream_recovery_records
        WHERE recovery_id = 'cdc-lost-binlog-2026-08-09-drop-trigger'
          AND status = 'prepared'
          AND active_barrier_identity IS NOT NULL
          AND active_barrier_identity = barrier_identity
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'current prepared recovery is not active after abandoned replacement migration';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM cdc.stream_recovery_records
        WHERE status IN ('prepared', 'committed', 'verified')
        GROUP BY active_barrier_identity
        HAVING COUNT(*) > 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'active barrier ownership is not unique after migration';
    END IF;
END//

CALL cdc.assert_stream_recovery_abandoned_replacement_postflight()//
DROP PROCEDURE cdc.assert_stream_recovery_abandoned_replacement_postflight//
DELIMITER ;

SHOW CREATE TABLE cdc.stream_recovery_records;
SHOW TRIGGERS FROM cdc LIKE 'stream_recovery_records';
