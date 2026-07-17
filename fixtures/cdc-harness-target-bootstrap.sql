CREATE DATABASE IF NOT EXISTS globalcomix;
CREATE DATABASE IF NOT EXISTS cdc;

CREATE USER IF NOT EXISTS 'cdc_stream'@'%' IDENTIFIED BY 'cdc-stream-password' REQUIRE SSL;

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

CREATE TABLE IF NOT EXISTS cdc.ddl_replay_journal (
    source_identity VARCHAR(384) NOT NULL,
    source_server_id INT UNSIGNED NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    event_start_position BIGINT UNSIGNED NOT NULL,
    event_end_position BIGINT UNSIGNED NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    raw_sql LONGTEXT NOT NULL,
    transformation_version VARCHAR(64) NOT NULL,
    generated_sql LONGTEXT NULL,
    canonical_ast LONGTEXT NOT NULL,
    pre_state LONGTEXT NOT NULL,
    expected_post_state LONGTEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CHECK (status IN ('translation_pending', 'prepared', 'applied', 'checkpointed', 'blocked')),
    PRIMARY KEY (source_identity, binlog_file, event_start_position)
) ENGINE=InnoDB;

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

DROP TRIGGER IF EXISTS cdc.ddl_replay_journal_insert_guard//
CREATE TRIGGER cdc.ddl_replay_journal_insert_guard
BEFORE INSERT ON cdc.ddl_replay_journal
FOR EACH ROW
BEGIN
    IF NOT (
        (NEW.status = 'translation_pending'
         AND NEW.transformation_version = 'translator-unavailable'
         AND NEW.generated_sql IS NULL
         AND NEW.canonical_ast = ''
         AND NEW.pre_state = ''
         AND NEW.expected_post_state = '')
        OR
        (NEW.status = 'prepared'
         AND NEW.transformation_version <> ''
         AND NEW.canonical_ast <> ''
         AND NEW.pre_state <> ''
         AND NEW.expected_post_state <> '')
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'automatic DDL journal rows must begin translation_pending or prepared with valid evidence';
    END IF;
END//

DROP TRIGGER IF EXISTS cdc.ddl_replay_journal_update_guard//
CREATE TRIGGER cdc.ddl_replay_journal_update_guard
BEFORE UPDATE ON cdc.ddl_replay_journal
FOR EACH ROW
BEGIN
    IF NOT (OLD.source_identity <=> NEW.source_identity)
       OR NOT (OLD.source_server_id <=> NEW.source_server_id)
       OR NOT (OLD.binlog_file <=> NEW.binlog_file)
       OR NOT (OLD.event_start_position <=> NEW.event_start_position)
       OR NOT (OLD.event_end_position <=> NEW.event_end_position)
       OR NOT (OLD.schema_name <=> NEW.schema_name)
       OR NOT (OLD.raw_sql <=> NEW.raw_sql)
       OR NOT (
           (OLD.status = 'translation_pending'
            AND NEW.status = 'prepared'
            AND OLD.transformation_version = 'translator-unavailable'
            AND OLD.generated_sql IS NULL
            AND OLD.canonical_ast = ''
            AND OLD.pre_state = ''
            AND OLD.expected_post_state = ''
            AND NEW.transformation_version <> ''
            AND NEW.canonical_ast <> ''
            AND NEW.pre_state <> ''
            AND NEW.expected_post_state <> '')
           OR
           ((OLD.transformation_version <=> NEW.transformation_version)
            AND (OLD.generated_sql <=> NEW.generated_sql)
            AND (OLD.canonical_ast <=> NEW.canonical_ast)
            AND (OLD.pre_state <=> NEW.pre_state)
            AND (OLD.expected_post_state <=> NEW.expected_post_state)
            AND ((OLD.status = 'prepared' AND NEW.status IN ('applied', 'blocked'))
                 OR (OLD.status = 'applied' AND NEW.status = 'checkpointed')))
       ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'automatic DDL journal identity/evidence is immutable and status transition is not allowed';
    END IF;
END//

DROP PROCEDURE IF EXISTS cdc.row_conflicts_trigger_inventory//
CREATE DEFINER=CURRENT_USER PROCEDURE cdc.row_conflicts_trigger_inventory()
SQL SECURITY DEFINER
READS SQL DATA
BEGIN
    SELECT trigger_name, event_object_schema, event_object_table,
           event_manipulation, action_timing, action_statement, action_order
    FROM information_schema.triggers
    WHERE event_object_schema = 'cdc' AND event_object_table = 'row_conflicts'
    ORDER BY event_manipulation, action_order;
END//

DROP PROCEDURE IF EXISTS cdc.ddl_replay_journal_trigger_inventory//
CREATE DEFINER=CURRENT_USER PROCEDURE cdc.ddl_replay_journal_trigger_inventory()
SQL SECURITY DEFINER
READS SQL DATA
BEGIN
    SELECT trigger_name, event_object_schema, event_object_table,
           event_manipulation, action_timing, action_statement, action_order
    FROM information_schema.triggers
    WHERE event_object_schema = 'cdc' AND event_object_table = 'ddl_replay_journal'
    ORDER BY event_manipulation, action_order;
END//
DELIMITER ;

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, REFERENCES,
      CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, EXECUTE, EVENT, TRIGGER
      ON globalcomix.* TO 'cdc_stream'@'%';
GRANT SELECT, INSERT, UPDATE ON cdc.stream_checkpoint TO 'cdc_stream'@'%';
GRANT SELECT, INSERT, UPDATE ON cdc.row_conflicts TO 'cdc_stream'@'%';
GRANT SELECT, INSERT, UPDATE ON cdc.ddl_replay_journal TO 'cdc_stream'@'%';
GRANT EXECUTE ON PROCEDURE cdc.row_conflicts_trigger_inventory TO 'cdc_stream'@'%';
GRANT EXECUTE ON PROCEDURE cdc.ddl_replay_journal_trigger_inventory TO 'cdc_stream'@'%';
FLUSH PRIVILEGES;
