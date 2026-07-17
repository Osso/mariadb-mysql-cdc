-- Run with admin/resolver credentials while stream-binlog is stopped, after
-- ddl-control-plane-bootstrap.sql. Together the two files provision the exact
-- checkpoint, row-conflict, manual-ledger, journal, trigger, procedure, and
-- grant contract required by startup validation.
CREATE DATABASE IF NOT EXISTS cdc;

CREATE TABLE IF NOT EXISTS cdc.ddl_replay_journal (
    source_identity VARCHAR(384) NOT NULL,
    source_server_id INT UNSIGNED NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    event_start_position BIGINT UNSIGNED NOT NULL,
    event_end_position BIGINT UNSIGNED NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    raw_sql LONGTEXT NOT NULL,
    canonical_ast LONGTEXT NOT NULL,
    pre_state LONGTEXT NOT NULL,
    expected_post_state LONGTEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CHECK (status IN ('prepared', 'applied', 'checkpointed', 'blocked')),
    PRIMARY KEY (source_identity, binlog_file, event_start_position)
) ENGINE=InnoDB;

DELIMITER //
DROP TRIGGER IF EXISTS cdc.ddl_replay_journal_insert_guard//
CREATE TRIGGER cdc.ddl_replay_journal_insert_guard
BEFORE INSERT ON cdc.ddl_replay_journal
FOR EACH ROW
BEGIN
    IF NEW.status <> 'prepared' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'automatic DDL journal rows must begin prepared';
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
       OR NOT (OLD.canonical_ast <=> NEW.canonical_ast)
       OR NOT (OLD.pre_state <=> NEW.pre_state)
       OR NOT (OLD.expected_post_state <=> NEW.expected_post_state)
       OR NOT (
           (OLD.status = 'prepared' AND NEW.status IN ('applied', 'blocked'))
           OR (OLD.status = 'applied' AND NEW.status = 'checkpointed')
       ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'automatic DDL journal identity/evidence is immutable and status transition is not allowed';
    END IF;
END//

DROP PROCEDURE IF EXISTS cdc.ddl_replay_journal_trigger_inventory//
CREATE DEFINER=CURRENT_USER PROCEDURE cdc.ddl_replay_journal_trigger_inventory()
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
      AND event_object_table = 'ddl_replay_journal'
    ORDER BY event_manipulation, action_order;
END//
DELIMITER ;

GRANT SELECT, INSERT, UPDATE
    ON cdc.stream_checkpoint
    TO 'cdc_stream'@'%';

GRANT SELECT, INSERT
    ON cdc.ddl_events
    TO 'cdc_stream'@'%';

GRANT SELECT, INSERT, UPDATE
    ON cdc.ddl_replay_journal
    TO 'cdc_stream'@'%';

GRANT EXECUTE
    ON PROCEDURE cdc.ddl_replay_journal_trigger_inventory
    TO 'cdc_stream'@'%';

GRANT
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE,
    ALTER,
    DROP,
    INDEX,
    REFERENCES,
    CREATE VIEW,
    SHOW VIEW,
    CREATE ROUTINE,
    ALTER ROUTINE,
    EXECUTE,
    EVENT,
    TRIGGER
    ON globalcomix.*
    TO 'cdc_stream'@'%';

-- Admin/resolver evidence only. Bootstrap/startup validates the static
-- schema/trigger/procedure/grant contract once before source replication;
-- event handling uses known internal operations and does not rerun grant policy.
-- Runtime uses exact EXECUTE plus CALL output.
SHOW CREATE TABLE cdc.ddl_replay_journal;
SHOW CREATE PROCEDURE cdc.ddl_replay_journal_trigger_inventory;
SHOW TRIGGERS FROM cdc WHERE `Table` = 'ddl_replay_journal';
SHOW GRANTS FOR 'cdc_stream'@'%';
