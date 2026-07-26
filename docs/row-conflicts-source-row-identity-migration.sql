-- One-time transition from the exact legacy cdc.row_conflicts contract.
-- Run with admin credentials while stream-binlog and conflict repair are stopped.
-- The ALTER fails closed if the column or index already exists. The stored
-- generated column backfills every existing row from its immutable source-row
-- fields; runtime never writes the column directly.
ALTER TABLE cdc.row_conflicts
    ADD COLUMN source_row_identity CHAR(64)
        CHARACTER SET ascii COLLATE ascii_bin
        GENERATED ALWAYS AS (
            SHA2(CONCAT(
                UNHEX(LPAD(HEX(LENGTH(source_identity)), 16, '0')),
                CONVERT(source_identity USING binary),
                UNHEX(LPAD(HEX(LENGTH(schema_name)), 16, '0')),
                CONVERT(schema_name USING binary),
                UNHEX(LPAD(HEX(LENGTH(table_name)), 16, '0')),
                CONVERT(table_name USING binary),
                UNHEX(LPAD(HEX(LENGTH(source_primary_key_json)), 16, '0')),
                CONVERT(source_primary_key_json USING binary)
            ), 256)
        ) STORED NOT NULL
        AFTER source_primary_key_json,
    ADD INDEX row_conflicts_source_row_status
        (source_row_identity, status);

DELIMITER //
DROP TRIGGER IF EXISTS cdc.row_conflicts_update_guard//
CREATE TRIGGER cdc.row_conflicts_update_guard
BEFORE UPDATE ON cdc.row_conflicts
FOR EACH ROW
BEGIN
    IF NOT (OLD.conflict_identity <=> NEW.conflict_identity)
       OR NOT (OLD.source_row_identity <=> NEW.source_row_identity)
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
DELIMITER ;

SHOW CREATE TABLE cdc.row_conflicts;
SHOW CREATE TRIGGER cdc.row_conflicts_update_guard;
