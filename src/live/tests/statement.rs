use super::*;

#[test]
fn extracts_statement_events_with_coordinates_and_database() {
    let events = extract_statement_events(
        "\
# at 100
#250601 12:00:00 server id 1  end_log_pos 180
use `test_cdc`/*!*/;
SET TIMESTAMP=1/*!*/;
SET @@session.time_zone='SYSTEM'/*!*/;
INSERT INTO accounts (id, name) VALUES (1, 'alpha')/*!*/;
# at 180
#250601 12:00:01 server id 1  end_log_pos 220
UPDATE accounts SET name = 'beta' WHERE id = 1/*!*/;
# at 220
# Rotate to mysqld-bin.000002  pos: 4
# at 4
#250601 12:00:02 server id 1  end_log_pos 99
DELETE FROM accounts WHERE id = 1/*!*/;
",
        &BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 4,
        },
    );

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].coordinate.position, 100);
    assert_eq!(events[0].resume_position, 180);
    assert_eq!(events[0].default_database, Some("test_cdc".to_string()));
    assert_eq!(
        events[0].sql,
        "INSERT INTO accounts (id, name) VALUES (1, 'alpha')"
    );
    assert_eq!(events[2].coordinate.file, "mysqld-bin.000002");
    assert_eq!(events[2].resume_position, 99);
}

#[test]
fn extractor_ignores_zero_positions_after_resume_coordinate() {
    let events = extract_statement_events(
        "\
# at 0
#250601 12:00:00 server id 1  end_log_pos 0
use `test_cdc`/*!*/;
SET TIMESTAMP=1/*!*/;
INSERT INTO accounts (id, name) VALUES (1, 'alpha')/*!*/;
",
        &BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 905_294_149,
        },
    );

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].coordinate.position, 905_294_149);
    assert_eq!(events[0].resume_position, 905_294_149);
}

#[test]
fn keeps_semicolon_lines_inside_multiline_string_literals() {
    let events = extract_statement_events(
        "\
# at 100
use `test_cdc`/*!*/;
SET TIMESTAMP=1/*!*/;
INSERT INTO email_history (body) VALUES (\"<style>
body {
    margin: 0 !important;
}
</style>\")
/*!*/;
",
        &BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 4,
        },
    );

    assert_eq!(events.len(), 1);
    assert!(events[0].sql.contains("margin: 0 !important;"));
    assert!(events[0].sql.contains("</style>"));
}

#[test]
fn ignores_multiline_annotate_query_text_that_starts_with_sql_keywords() {
    let events = extract_statement_events(
        "\
# at 100
#250630  6:26:16 server id 1  end_log_pos 180 Annotate_rows:
#Q> INSERT INTO `email_history` (`body`) VALUES (\"<html>
    <p>
        Create your own page and start publishing.
    </p>
</html>\")
# at 180
#250630  6:26:16 server id 1  end_log_pos 240 Table_map: `globalcomix`.`email_history` mapped to number 1
### INSERT INTO `globalcomix`.`email_history`
### SET
###   @1='body'
# at 240
use `test_cdc`/*!*/;
SET TIMESTAMP=1/*!*/;
UPDATE accounts SET name = 'beta' WHERE id = 1/*!*/;
",
        &BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 4,
        },
    );

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].coordinate.position, 240);
    assert_eq!(
        events[0].sql,
        "UPDATE accounts SET name = 'beta' WHERE id = 1"
    );
}

#[test]
fn ignores_body_text_line_starting_with_create() {
    let events = extract_statement_events(
        "\
# at 100
use `test_cdc`/*!*/;
Create your own page and start publishing/*!*/;
# at 180
#250630  6:26:16 server id 1  end_log_pos 240
UPDATE accounts SET name = 'beta' WHERE id = 1/*!*/;
",
        &BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 4,
        },
    );

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].coordinate.position, 180);
    assert_eq!(
        events[0].sql,
        "UPDATE accounts SET name = 'beta' WHERE id = 1"
    );
}

#[test]
fn extracts_real_create_table_statement() {
    let events = extract_statement_events(
        "\
# at 100
use `test_cdc`/*!*/;
CREATE TABLE accounts (id INT PRIMARY KEY)/*!*/;
",
        &BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 4,
        },
    );

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].coordinate.position, 100);
    assert_eq!(events[0].sql, "CREATE TABLE accounts (id INT PRIMARY KEY)");
}

#[test]
fn extracts_supported_schema_ddl_statements() {
    let events = extract_statement_events(
        "\
# at 100
use `test_cdc`/*!*/;
CREATE DATABASE IF NOT EXISTS archive/*!*/;
# at 200
CREATE VIEW active_accounts AS SELECT id FROM accounts/*!*/;
# at 300
CREATE PROCEDURE refresh_accounts() BEGIN SELECT 1; SELECT 2; END/*!*/;
",
        &BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 4,
        },
    );

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].sql, "CREATE DATABASE IF NOT EXISTS archive");
    assert_eq!(
        events[1].sql,
        "CREATE VIEW active_accounts AS SELECT id FROM accounts"
    );
    assert_eq!(
        events[2].sql,
        "CREATE PROCEDURE refresh_accounts() BEGIN SELECT 1; SELECT 2; END"
    );
}

#[test]
fn applies_extracted_compatible_statements() {
    let events = vec![StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 100,
        },
        resume_position: 180,
        default_database: Some("test_cdc".to_string()),
        sql: "INSERT INTO accounts (id, name) VALUES (1, 'alpha')".to_string(),
    }];
    let executor = RecordingExecutor::default();

    let report =
        apply_statement_events(events, executor, RecordingQuarantine::default()).expect("apply");

    assert_eq!(
        report,
        ApplyBinlogReport {
            applied_statements: 1,
            quarantined_statements: 0,
        }
    );
}

#[test]
fn skips_administrative_ddl_as_applied_without_target_statement() {
    let events = vec![StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 100,
        },
        resume_position: 180,
        default_database: Some("test_cdc".to_string()),
        sql: "GRANT SELECT ON app.* TO 'reader'@'%'".to_string(),
    }];
    let executor = RecordingExecutor::default();

    let report =
        apply_statement_events(events, executor, RecordingQuarantine::default()).expect("apply");

    assert_eq!(
        report,
        ApplyBinlogReport {
            applied_statements: 1,
            quarantined_statements: 0,
        }
    );
}

#[test]
fn refuses_quarantined_statements() {
    let events = vec![StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 100,
        },
        resume_position: 180,
        default_database: Some("test_cdc".to_string()),
        sql: "ANALYZE FORMAT=JSON SELECT * FROM accounts".to_string(),
    }];
    let executor = RecordingExecutor::default();

    let error = apply_statement_events(events, executor, RecordingQuarantine::default())
        .expect_err("unsupported statement should quarantine")
        .to_string();

    assert!(error.contains("quarantined"));
}

#[test]
fn extracts_sanitized_production_query_shapes() {
    let fixture = include_str!("../../../fixtures/prod-derived/sanitized-query-events.txt");
    let events = extract_statement_events(
        fixture,
        &BinlogCoordinate {
            file: "mysqld-bin.002523".to_string(),
            position: 955857729,
        },
    );

    assert_eq!(events.len(), 6);
    assert_eq!(events[0].coordinate.position, 955857729);
    assert_eq!(
        events[0].sql,
        "UPDATE `guests` `g`\nSET `supports_cookies` = 1\nWHERE `g`.`guest_id` = 1001"
    );
    assert_eq!(events[1].coordinate.position, 957812859);
    assert!(events[1].sql.contains("UPDATE phrases p set"));
    assert!(events[1].sql.contains("WHERE `p`.`id`"));
    assert_eq!(events[2].coordinate.position, 957812400);
    assert!(
        events[2]
            .sql
            .contains("INSERT INTO `users_search_queries_history`")
    );
    assert!(events[2].sql.contains("\\\"semantic\\\":true"));
    assert_eq!(events[3].coordinate.position, 960000100);
    assert!(events[3].sql.contains("ADD COLUMN `filter_prompt_version`"));
    assert_eq!(events[4].coordinate.position, 960001000);
    assert!(
        events[4]
            .sql
            .contains("ADD KEY `idx_hfb_variant_status_published`")
    );
    assert_eq!(events[5].coordinate.position, 960002000);
    assert!(events[5].sql.contains("RENAME COLUMN IF EXISTS"));
}

#[test]
fn repair_table_name_extracts_known_dml_tables() {
    assert_eq!(
        repair_table_name("INSERT INTO `accounts` (id) VALUES (1)"),
        Some("accounts".to_string())
    );
    assert_eq!(
        repair_table_name("UPDATE `globalcomix`.`releases` SET title = 'x' WHERE id = 1"),
        Some("releases".to_string())
    );
    assert_eq!(
        repair_table_name("DELETE FROM comics WHERE id = 1"),
        Some("comics".to_string())
    );
}

#[test]
fn delete_statement_failure_is_not_repairable_without_delete_support() {
    let checkpoint_store = MemoryCheckpointStore::default();
    let executor = RecordingExecutor::with_failure("target down");
    let applier = StatementApplier::new(executor, RecordingQuarantine::default());
    let event = StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 12345,
        },
        resume_position: 12399,
        default_database: Some("globalcomix".to_string()),
        sql: "DELETE FROM accounts WHERE id = 1".to_string(),
    };
    let mut progress = StreamProgress::new(BinlogCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 4,
    });
    let repairer = RecordingRepairer::default();

    let error = apply_stream_event(
        &applier,
        &repairer,
        &event,
        &mut progress,
        Some(&checkpoint_store),
    )
    .expect_err("delete not repairable");

    assert!(error.to_string().contains("target down"));
    assert!(repairer.requests.borrow().is_empty());
    assert!(checkpoint_store.saved.borrow().is_none());
    assert_eq!(repairable_table_name(&event.sql), None);
}
