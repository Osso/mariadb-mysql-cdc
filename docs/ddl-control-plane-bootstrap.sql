-- Run with admin/resolver credentials while mariadb-mysql-cdc-stream is stopped.
CREATE DATABASE IF NOT EXISTS cdc;

CREATE TABLE IF NOT EXISTS cdc.stream_checkpoint (
    checkpoint_name VARCHAR(512) PRIMARY KEY,
    checkpoint_json LONGTEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

ALTER TABLE cdc.stream_checkpoint
    MODIFY checkpoint_name VARCHAR(512) NOT NULL;

CREATE TABLE IF NOT EXISTS cdc.ddl_events (
    source_identity VARCHAR(384) NOT NULL,
    source_server_id INT UNSIGNED NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    event_start_position BIGINT UNSIGNED NOT NULL,
    event_end_position BIGINT UNSIGNED NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    raw_sql LONGTEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    resolution_note TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CHECK (status IN ('pending', 'resolved')),
    PRIMARY KEY (source_identity, binlog_file, event_start_position)
);

DROP TRIGGER IF EXISTS cdc.ddl_events_pending_insert_guard;
DELIMITER //
CREATE TRIGGER cdc.ddl_events_pending_insert_guard
BEFORE INSERT ON cdc.ddl_events
FOR EACH ROW
BEGIN
    IF NEW.status <> 'pending' OR NEW.resolution_note IS NOT NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'DDL events may only be inserted pending';
    END IF;
END//

DROP TRIGGER IF EXISTS cdc.ddl_events_monotonic_resolution_guard//
CREATE TRIGGER cdc.ddl_events_monotonic_resolution_guard
BEFORE UPDATE ON cdc.ddl_events
FOR EACH ROW
BEGIN
    IF NOT (OLD.source_identity <=> NEW.source_identity)
       OR NOT (OLD.source_server_id <=> NEW.source_server_id)
       OR NOT (OLD.binlog_file <=> NEW.binlog_file)
       OR NOT (OLD.event_start_position <=> NEW.event_start_position)
       OR NOT (OLD.event_end_position <=> NEW.event_end_position)
       OR NOT (OLD.schema_name <=> NEW.schema_name)
       OR NOT (OLD.raw_sql <=> NEW.raw_sql)
       OR OLD.status <> 'pending'
       OR NEW.status <> 'resolved'
       OR NEW.resolution_note IS NULL
       OR NEW.resolution_note = '' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'DDL resolution must preserve coordinates and transition pending to resolved once';
    END IF;
END//
DELIMITER ;
