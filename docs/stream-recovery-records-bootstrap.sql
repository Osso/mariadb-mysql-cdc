-- Run with target admin credentials while stream-binlog is stopped.
-- Records are immutable except for prepared -> committed -> verified status
-- transitions carrying durable evidence for each transition.
CREATE DATABASE IF NOT EXISTS cdc;

CREATE TABLE IF NOT EXISTS cdc.stream_recovery_records (
    recovery_id VARCHAR(191) NOT NULL,
    checkpoint_name VARCHAR(512) NOT NULL,
    source_identity VARCHAR(512) NOT NULL,
    scope_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    old_checkpoint_json LONGTEXT NOT NULL,
    new_checkpoint_json LONGTEXT NOT NULL,
    old_barrier_source_identity VARCHAR(512) NOT NULL,
    old_barrier_file VARCHAR(255) NOT NULL,
    old_barrier_start_position BIGINT UNSIGNED NOT NULL,
    old_barrier_end_position BIGINT UNSIGNED NOT NULL,
    old_barrier_raw_sql LONGTEXT NOT NULL,
    old_barrier_raw_sql_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin
        GENERATED ALWAYS AS (SHA2(old_barrier_raw_sql, 256)) STORED NOT NULL,
    operator_identity VARCHAR(255) NOT NULL,
    reason TEXT NOT NULL,
    prepared_evidence_json LONGTEXT NOT NULL,
    committed_evidence_json LONGTEXT NULL,
    verified_evidence_json LONGTEXT NULL,
    status VARCHAR(16) NOT NULL,
    prepared_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    committed_at TIMESTAMP(6) NULL,
    verified_at TIMESTAMP(6) NULL,
    CHECK (JSON_VALID(old_checkpoint_json)),
    CHECK (JSON_VALID(new_checkpoint_json)),
    CHECK (JSON_VALID(prepared_evidence_json)),
    CHECK (committed_evidence_json IS NULL OR JSON_VALID(committed_evidence_json)),
    CHECK (verified_evidence_json IS NULL OR JSON_VALID(verified_evidence_json)),
    CHECK (status IN ('prepared', 'committed', 'verified')),
    CHECK (old_barrier_end_position > old_barrier_start_position),
    PRIMARY KEY (recovery_id),
    UNIQUE KEY stream_recovery_exact_barrier (
        old_barrier_source_identity,
        old_barrier_file,
        old_barrier_start_position,
        old_barrier_end_position,
        old_barrier_raw_sql_sha256
    ),
    KEY stream_recovery_checkpoint_status (checkpoint_name, status)
);

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
       OR NEW.verified_at IS NOT NULL THEN
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
       OR OLD.status = 'prepared' AND NOT (
           NEW.status = 'committed'
           AND NEW.committed_evidence_json IS NOT NULL
           AND NEW.committed_evidence_json <> ''
           AND NEW.committed_at IS NOT NULL
           AND NEW.verified_evidence_json IS NULL
           AND NEW.verified_at IS NULL
       )
       OR OLD.status = 'committed' AND NOT (
           NEW.status = 'verified'
           AND OLD.committed_evidence_json <=> NEW.committed_evidence_json
           AND OLD.committed_at <=> NEW.committed_at
           AND NEW.verified_evidence_json IS NOT NULL
           AND NEW.verified_evidence_json <> ''
           AND NEW.verified_at IS NOT NULL
       )
       OR OLD.status = 'verified' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'stream recovery identity is immutable and status transition is not allowed';
    END IF;
END//

DROP TRIGGER IF EXISTS cdc.stream_recovery_records_delete_guard//
CREATE TRIGGER cdc.stream_recovery_records_delete_guard
BEFORE DELETE ON cdc.stream_recovery_records
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'stream recovery records are immutable';
END//

DROP PROCEDURE IF EXISTS cdc.stream_recovery_records_trigger_inventory//
CREATE DEFINER=CURRENT_USER PROCEDURE cdc.stream_recovery_records_trigger_inventory()
SQL SECURITY DEFINER
READS SQL DATA
BEGIN
    SELECT
        trigger_name,
        event_object_schema,
        event_object_table,
        event_manipulation,
        action_timing,
        action_statement,
        action_order
    FROM information_schema.triggers
    WHERE event_object_schema = 'cdc'
      AND event_object_table = 'stream_recovery_records'
    ORDER BY event_manipulation, action_order;
END//
DELIMITER ;

GRANT SELECT, INSERT, UPDATE
    ON cdc.stream_recovery_records
    TO 'cdc_stream'@'%';

GRANT EXECUTE
    ON PROCEDURE cdc.stream_recovery_records_trigger_inventory
    TO 'cdc_stream'@'%';

SHOW CREATE PROCEDURE cdc.stream_recovery_records_trigger_inventory;
SHOW GRANTS FOR 'cdc_stream'@'%';
