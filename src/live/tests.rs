use super::*;
use std::cell::RefCell;

#[test]
fn extracts_statement_events_with_coordinates_and_database() {
    let events = extract_statement_events(
        "\
# at 100
use `test_cdc`/*!*/;
SET TIMESTAMP=1/*!*/;
SET @@session.time_zone='SYSTEM'/*!*/;
INSERT INTO accounts (id, name) VALUES (1, 'alpha')/*!*/;
# at 180
UPDATE accounts SET name = 'beta' WHERE id = 1/*!*/;
# at 220
# Rotate to mysqld-bin.000002  pos: 4
# at 4
DELETE FROM accounts WHERE id = 1/*!*/;
",
        &BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 4,
        },
    );

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].coordinate.position, 100);
    assert_eq!(events[0].default_database, Some("test_cdc".to_string()));
    assert_eq!(
        events[0].sql,
        "INSERT INTO accounts (id, name) VALUES (1, 'alpha')"
    );
    assert_eq!(events[2].coordinate.file, "mysqld-bin.000002");
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
fn applies_extracted_compatible_statements() {
    let events = vec![StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 100,
        },
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
fn refuses_quarantined_statements() {
    let events = vec![StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 100,
        },
        default_database: Some("test_cdc".to_string()),
        sql: "CREATE TABLE accounts (id INT PRIMARY KEY)".to_string(),
    }];
    let executor = RecordingExecutor::default();

    let error = apply_statement_events(events, executor, RecordingQuarantine::default())
        .expect_err("ddl should quarantine")
        .to_string();

    assert!(error.contains("quarantined"));
}

#[test]
fn stop_never_args_keep_binlog_file_last() {
    let source = SourceBinlogConfig {
        host: "10.0.0.1".to_string(),
        user: "cdc".to_string(),
        password: "secret".to_string(),
        database: Some("test".to_string()),
        binlog_file: "mysqld-bin.000001".to_string(),
        start_position: 4,
        ..SourceBinlogConfig::default()
    };

    let args = binlog_command::stop_never_args(&source);

    assert!(args.contains(&"--stop-never".to_string()));
    assert_eq!(args.last(), Some(&"mysqld-bin.000001".to_string()));
}

#[test]
fn extracts_sanitized_production_query_shapes() {
    let fixture = include_str!("../../fixtures/prod-derived/sanitized-query-events.txt");
    let events = extract_statement_events(
        fixture,
        &BinlogCoordinate {
            file: "mysqld-bin.002523".to_string(),
            position: 955857729,
        },
    );

    assert_eq!(events.len(), 3);
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
}

#[test]
fn target_session_init_removes_ansi_quotes() {
    assert_eq!(
        target_session_init_command(),
        "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"
    );
    assert!(!target_session_init_command().contains("ANSI_QUOTES"));
}

#[derive(Default)]
struct RecordingExecutor {
    statements: RefCell<Vec<String>>,
}

impl TargetExecutor for RecordingExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        self.statements.borrow_mut().push(statement.sql.clone());
        Ok(())
    }
}
