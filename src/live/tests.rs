use super::*;
use crate::checkpoint::{Checkpoint, LastEvent};
use crate::live::repair::{StatementRepairRequest, repair_table_name, repairable_table_name};
use std::cell::RefCell;
use std::time::{Duration, Instant};

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
fn refuses_quarantined_statements() {
    let events = vec![StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 100,
        },
        resume_position: 180,
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

#[test]
fn target_client_uses_utf8mb4_connection_charset() {
    assert_eq!(
        target_client_character_set_arg(),
        "--default-character-set=utf8mb4"
    );
}

#[test]
fn stream_resume_prefers_existing_checkpoint_over_static_coordinates() {
    let checkpoint_store = MemoryCheckpointStore::with_checkpoint(Checkpoint {
        source_file: "mysqld-bin.000999".to_string(),
        source_position: 98765,
        gtid: None,
        event_timestamp: 0,
        last_event: LastEvent {
            event_type: "StatementEvent".to_string(),
            description: "INSERT INTO accounts".to_string(),
        },
    });
    let mut config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
            ..SourceBinlogConfig::default()
        },
        ..ApplyBinlogConfig::default()
    };

    resume_from_checkpoint(&mut config, Some(&checkpoint_store)).expect("resume checkpoint");

    assert_eq!(config.source.binlog_file, "mysqld-bin.000999");
    assert_eq!(config.source.start_position, 98765);
}

#[test]
fn stream_checkpoint_is_saved_after_successful_apply() {
    let checkpoint_store = MemoryCheckpointStore::default();
    let executor = RecordingExecutor::default();
    let applier = StatementApplier::new(executor, RecordingQuarantine::default());
    let event = StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 12345,
        },
        resume_position: 12399,
        default_database: Some("globalcomix".to_string()),
        sql: "INSERT INTO accounts (id) VALUES (1)".to_string(),
    };
    let mut progress = StreamProgress::new(BinlogCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 4,
    });
    let repairer = RecordingRepairer::default();

    apply_stream_event(
        &applier,
        &repairer,
        &event,
        &mut progress,
        Some(&checkpoint_store),
    )
    .expect("apply event");

    let saved = checkpoint_store.saved.borrow();
    let checkpoint = saved.as_ref().expect("saved checkpoint");
    assert_eq!(checkpoint.source_file, "mysqld-bin.000777");
    assert_eq!(checkpoint.source_position, 12399);
    assert_eq!(checkpoint.last_event.event_type, "StatementEvent");
    assert!(repairer.requests.borrow().is_empty());
}

#[test]
fn stream_checkpoint_does_not_move_backwards_to_zero() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000777", 12_399));
    let event = StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 0,
        },
        resume_position: 0,
        default_database: Some("globalcomix".to_string()),
        sql: "INSERT INTO accounts (id) VALUES (1)".to_string(),
    };

    save_stream_checkpoint(Some(&checkpoint_store), &event).expect("skip checkpoint");

    let loaded = checkpoint_store.loaded.borrow();
    let checkpoint = loaded.as_ref().expect("existing checkpoint");
    assert_eq!(checkpoint.source_position, 12_399);
    assert!(checkpoint_store.saved.borrow().is_none());
}

#[test]
fn stream_checkpoint_does_not_move_backwards_in_same_file() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000777", 12_399));
    let event = StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 12_000,
        },
        resume_position: 12_100,
        default_database: Some("globalcomix".to_string()),
        sql: "INSERT INTO accounts (id) VALUES (1)".to_string(),
    };

    save_stream_checkpoint(Some(&checkpoint_store), &event).expect("skip checkpoint");

    let loaded = checkpoint_store.loaded.borrow();
    let checkpoint = loaded.as_ref().expect("existing checkpoint");
    assert_eq!(checkpoint.source_position, 12_399);
    assert!(checkpoint_store.saved.borrow().is_none());
}

#[test]
fn stream_checkpoint_is_saved_after_failed_apply_is_repaired() {
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
        sql: "INSERT INTO accounts (id) VALUES (1)".to_string(),
    };
    let mut progress = StreamProgress::new(BinlogCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 4,
    });
    let repairer = RecordingRepairer::default();

    apply_stream_event(
        &applier,
        &repairer,
        &event,
        &mut progress,
        Some(&checkpoint_store),
    )
    .expect("repaired target failure");

    let saved = checkpoint_store.saved.borrow();
    let checkpoint = saved.as_ref().expect("saved checkpoint");
    assert_eq!(checkpoint.source_position, 12399);
    assert_eq!(
        repairer.requests.borrow().as_slice(),
        &[StatementRepairRequest {
            coordinate: event.coordinate,
            default_database: Some("globalcomix".to_string()),
            table: "accounts".to_string(),
            sql: "INSERT INTO accounts (id) VALUES (1)".to_string(),
            error: "target down".to_string(),
        }]
    );
}

#[test]
fn stream_checkpoint_is_not_saved_when_failed_apply_repair_fails() {
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
        sql: "UPDATE accounts SET name = 'Ada' WHERE id = 1".to_string(),
    };
    let mut progress = StreamProgress::new(BinlogCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 4,
    });
    let repairer = RecordingRepairer::failing("repair failed");

    let error = apply_stream_event(
        &applier,
        &repairer,
        &event,
        &mut progress,
        Some(&checkpoint_store),
    )
    .expect_err("repair failure");

    assert!(error.to_string().contains("repair failed"));
    assert!(checkpoint_store.saved.borrow().is_none());
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

#[test]
fn reconnects_only_transient_source_errors_with_remaining_attempts() {
    let transient = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: TLS/SSL error: Connection reset by peer"
            .to_string(),
    );
    let auth = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: Access denied".to_string(),
    );

    assert!(should_reconnect(&transient, 0, 3));
    assert!(!should_reconnect(&transient, 3, 3));
    assert!(!should_reconnect(&auth, 0, 3));
}

#[test]
fn reconnect_loop_resumes_from_checkpoint_after_transient_loss() {
    let checkpoint_store = MemoryCheckpointStore::default();
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
            ..SourceBinlogConfig::default()
        },
        max_reconnects: 2,
        ..ApplyBinlogConfig::default()
    };
    let seen_starts = RefCell::new(Vec::new());
    let attempts = RefCell::new(0);

    run_stream_reconnect_loop(
        &config,
        Some(&checkpoint_store),
        |attempt_config| {
            seen_starts.borrow_mut().push((
                attempt_config.source.binlog_file.clone(),
                attempt_config.source.start_position,
            ));
            let mut attempts_ref = attempts.borrow_mut();
            *attempts_ref += 1;
            if *attempts_ref == 1 {
                checkpoint_store
                    .save_checkpoint(&checkpoint_at("mysqld-bin.000777", 12345))
                    .expect("save checkpoint");
                return Err(ApplyBinlogError::SourceCommand(
                    "TLS/SSL error: Connection reset by peer".to_string(),
                ));
            }
            Ok(())
        },
        |_delay: Duration| {},
    )
    .expect("reconnect loop");

    assert_eq!(
        seen_starts.into_inner(),
        vec![
            ("mysqld-bin.000001".to_string(), 4),
            ("mysqld-bin.000777".to_string(), 12345)
        ]
    );
}

#[test]
fn reconnect_loop_requires_checkpoint_when_static_coordinates_are_absent() {
    let checkpoint_store = MemoryCheckpointStore::default();
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: String::new(),
            start_position: 0,
            ..SourceBinlogConfig::default()
        },
        checkpoint_file: Some("/var/lib/cdc/checkpoint.json".into()),
        ..ApplyBinlogConfig::default()
    };

    let error = run_stream_reconnect_loop(
        &config,
        Some(&checkpoint_store),
        |_attempt_config| Ok(()),
        |_delay: Duration| {},
    )
    .expect_err("missing checkpoint coordinate");

    assert_eq!(error.to_string(), "binlog file is required");
}

#[test]
fn slow_target_query_log_includes_bounded_sql_preview() {
    let statement = SqlStatement {
        sql: "INSERT INTO events VALUES ('alpha')".repeat(200),
        params: Vec::new(),
    };
    let started_at = Instant::now() - Duration::from_secs(21);

    let log_line = format_slow_target_query_log(&statement, started_at);

    assert!(log_line.starts_with("cdc_target_slow_query elapsed_seconds="));
    assert!(log_line.contains(&format!("sql_bytes={}", statement.sql.len())));
    assert!(log_line.contains("sql_truncated=true"));
    assert!(log_line.contains("INSERT INTO events VALUES"));
    assert!(log_line.len() < statement.sql.len());
}

#[test]
fn truncate_sql_for_log_keeps_utf8_boundary() {
    let sql = "éééSELECT";

    assert_eq!(truncate_sql_for_log(sql, 3), "ééé");
}

#[derive(Default)]
struct RecordingExecutor {
    statements: RefCell<Vec<String>>,
    failure: Option<String>,
}

impl RecordingExecutor {
    fn with_failure(message: &str) -> Self {
        Self {
            statements: RefCell::new(Vec::new()),
            failure: Some(message.to_string()),
        }
    }
}

impl TargetExecutor for RecordingExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        self.statements.borrow_mut().push(statement.sql.clone());
        match &self.failure {
            Some(message) => Err(TargetExecuteError::new(message)),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
struct RecordingRepairer {
    requests: RefCell<Vec<StatementRepairRequest>>,
    failure: Option<String>,
}

impl RecordingRepairer {
    fn failing(message: &str) -> Self {
        Self {
            requests: RefCell::new(Vec::new()),
            failure: Some(message.to_string()),
        }
    }
}

impl FailedStatementRepairer for RecordingRepairer {
    fn repair(&self, request: &StatementRepairRequest) -> Result<(), ApplyBinlogError> {
        self.requests.borrow_mut().push(request.clone());
        if let Some(message) = &self.failure {
            return Err(ApplyBinlogError::Statement(message.clone()));
        }
        Ok(())
    }
}

#[derive(Default)]
struct MemoryCheckpointStore {
    loaded: RefCell<Option<Checkpoint>>,
    saved: RefCell<Option<Checkpoint>>,
}

impl MemoryCheckpointStore {
    fn with_checkpoint(checkpoint: Checkpoint) -> Self {
        Self {
            loaded: RefCell::new(Some(checkpoint)),
            saved: RefCell::new(None),
        }
    }
}

fn checkpoint_at(file: &str, position: u64) -> Checkpoint {
    Checkpoint {
        source_file: file.to_string(),
        source_position: position,
        gtid: None,
        event_timestamp: 0,
        last_event: LastEvent {
            event_type: "StatementEvent".to_string(),
            description: "fixture".to_string(),
        },
    }
}

impl StreamCheckpointStore for MemoryCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<Checkpoint>, ApplyBinlogError> {
        Ok(self.loaded.borrow().clone())
    }

    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), ApplyBinlogError> {
        self.saved.replace(Some(checkpoint.clone()));
        self.loaded.replace(Some(checkpoint.clone()));
        Ok(())
    }
}
