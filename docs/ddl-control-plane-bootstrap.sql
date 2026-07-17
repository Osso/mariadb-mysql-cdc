-- Run with admin/resolver credentials while stream-binlog is stopped.
-- This file bootstraps the checkpoint and durable row-conflict control-plane
-- objects. The automatic replay journal is a separate object;
-- run ddl-replay-journal-bootstrap.sql after its grant and trigger contract has
-- been reviewed.
CREATE DATABASE IF NOT EXISTS cdc;

CREATE TABLE IF NOT EXISTS cdc.row_conflicts (
    conflict_identity CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_identity VARCHAR(255) NOT NULL,
    source_server_id BIGINT UNSIGNED NOT NULL,
    source_file VARCHAR(255) NOT NULL,
    source_start_position BIGINT UNSIGNED NOT NULL,
    source_end_position BIGINT UNSIGNED NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(16) NOT NULL,
    source_primary_key_json TEXT NOT NULL,
    duplicate_index VARCHAR(255) NULL,
    duplicate_owner_primary_key_json TEXT NULL,
    error_code INT UNSIGNED NOT NULL,
    error_text TEXT NOT NULL,
    first_observed_at_ms BIGINT UNSIGNED NOT NULL,
    last_observed_at_ms BIGINT UNSIGNED NOT NULL,
    attempt_count BIGINT UNSIGNED NOT NULL DEFAULT 1,
    status VARCHAR(16) NOT NULL,
    repair_run_id VARCHAR(255) NULL,
    resolution_evidence TEXT NULL,
    CHECK (status IN ('unresolved', 'resolved')),
    PRIMARY KEY (conflict_identity)
);

CREATE TABLE IF NOT EXISTS cdc.stream_checkpoint (
    checkpoint_name VARCHAR(512) PRIMARY KEY,
    checkpoint_json LONGTEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

ALTER TABLE cdc.stream_checkpoint
    MODIFY checkpoint_name VARCHAR(512) NOT NULL;

DELIMITER //
DROP TRIGGER IF EXISTS cdc.row_conflicts_insert_guard//
CREATE TRIGGER cdc.row_conflicts_insert_guard
BEFORE INSERT ON cdc.row_conflicts
FOR EACH ROW
BEGIN
    IF NEW.status <> 'unresolved'
       OR NEW.attempt_count <> 1
       OR NEW.repair_run_id IS NOT NULL
       OR NEW.resolution_evidence IS NOT NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'row conflicts may only be inserted unresolved';
    END IF;
END//

DROP TRIGGER IF EXISTS cdc.row_conflicts_update_guard//
CREATE TRIGGER cdc.row_conflicts_update_guard
BEFORE UPDATE ON cdc.row_conflicts
FOR EACH ROW
BEGIN
    IF NOT (OLD.conflict_identity <=> NEW.conflict_identity)
       OR NOT (OLD.source_identity <=> NEW.source_identity)
       OR NOT (OLD.source_server_id <=> NEW.source_server_id)
       OR NOT (OLD.source_file <=> NEW.source_file)
       OR NOT (OLD.source_start_position <=> NEW.source_start_position)
       OR NOT (OLD.source_end_position <=> NEW.source_end_position)
       OR NOT (OLD.schema_name <=> NEW.schema_name)
       OR NOT (OLD.table_name <=> NEW.table_name)
       OR NOT (OLD.operation <=> NEW.operation)
       OR NOT (OLD.source_primary_key_json <=> NEW.source_primary_key_json)
       OR OLD.status = 'resolved' AND (
           NEW.status <> 'resolved'
           OR NOT (OLD.duplicate_index <=> NEW.duplicate_index)
           OR NOT (OLD.duplicate_owner_primary_key_json <=> NEW.duplicate_owner_primary_key_json)
           OR NOT (OLD.error_code <=> NEW.error_code)
           OR NOT (OLD.error_text <=> NEW.error_text)
           OR NOT (OLD.first_observed_at_ms <=> NEW.first_observed_at_ms)
           OR NOT (OLD.last_observed_at_ms <=> NEW.last_observed_at_ms)
           OR NOT (OLD.attempt_count <=> NEW.attempt_count)
           OR NOT (OLD.repair_run_id <=> NEW.repair_run_id)
           OR NOT (OLD.resolution_evidence <=> NEW.resolution_evidence)
       )
       OR OLD.status = 'unresolved' AND (
           (NEW.status = 'unresolved' AND (
               NEW.repair_run_id IS NOT NULL
               OR NEW.resolution_evidence IS NOT NULL
               OR NEW.attempt_count <> OLD.attempt_count + 1
           ))
           OR (NEW.status = 'resolved' AND (
               NEW.repair_run_id IS NULL
               OR NEW.repair_run_id = ''
               OR NEW.resolution_evidence IS NULL
               OR NEW.resolution_evidence = ''
               OR NOT (OLD.duplicate_index <=> NEW.duplicate_index)
               OR NOT (OLD.duplicate_owner_primary_key_json <=> NEW.duplicate_owner_primary_key_json)
               OR NOT (OLD.error_code <=> NEW.error_code)
               OR NOT (OLD.error_text <=> NEW.error_text)
               OR NOT (OLD.first_observed_at_ms <=> NEW.first_observed_at_ms)
               OR NOT (OLD.last_observed_at_ms <=> NEW.last_observed_at_ms)
               OR NOT (OLD.attempt_count <=> NEW.attempt_count)
           ))
       ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'row conflict identity is immutable and status transition is not allowed';
    END IF;
END//

DROP PROCEDURE IF EXISTS cdc.row_conflicts_trigger_inventory//
CREATE DEFINER=CURRENT_USER PROCEDURE cdc.row_conflicts_trigger_inventory()
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
      AND event_object_table = 'row_conflicts'
    ORDER BY event_manipulation, action_order;
END//

DELIMITER ;

GRANT SELECT, INSERT, UPDATE
    ON cdc.stream_checkpoint
    TO 'cdc_stream'@'%';

GRANT SELECT, INSERT, UPDATE
    ON cdc.row_conflicts
    TO 'cdc_stream'@'%';

GRANT EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory TO 'cdc_stream'@'%';

-- Inspect these as admin/resolver credentials. Runtime startup has only the
-- exact EXECUTE grant and validates CALL result rows; it never runs SHOW CREATE
-- PROCEDURE or requires routine metadata privileges.
SHOW CREATE PROCEDURE cdc.row_conflicts_trigger_inventory;
SHOW GRANTS FOR 'cdc_stream'@'%';
