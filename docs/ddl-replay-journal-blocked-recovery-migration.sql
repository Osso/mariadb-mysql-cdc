-- Apply with CDC admin credentials while stream-binlog is stopped or crash-looping.
-- Enables code-verified recovery when a blocked DDL's target state exactly matches
-- newly modeled post-state evidence. Identity and raw event fields remain immutable.
DELIMITER //
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
           (OLD.status = 'blocked'
            AND NEW.status = 'applied'
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
DELIMITER ;
