use super::*;
use crate::checkpoint::{Checkpoint, LastEvent};

mod checkpoint;
mod cli;
mod config;
mod reconnect;
mod statement;

use crate::live::repair::{StatementRepairRequest, repair_table_name, repairable_table_name};
use std::cell::RefCell;
use std::time::{Duration, Instant};

#[test]
fn reconnects_only_transient_source_errors_with_remaining_attempts() {
    let transient = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: TLS/SSL error: Connection reset by peer"
            .to_string(),
    );
    let auth = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: Access denied".to_string(),
    );

    assert!(should_reconnect(&transient, 0, 3, false));
    assert!(!should_reconnect(&transient, 3, 3, false));
    assert!(!should_reconnect(&auth, 0, 3, false));
}

#[test]
fn max_zero_keeps_reconnects_disabled() {
    let transient = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: TLS/SSL error: Connection reset by peer"
            .to_string(),
    );
    let auth = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: Access denied".to_string(),
    );

    assert!(!should_reconnect(&transient, 0, 0, false));
    assert!(!should_reconnect(&auth, 0, 0, false));
}

#[test]
fn reconnect_forever_allows_unlimited_transient_reconnects() {
    let transient = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: TLS/SSL error: Connection reset by peer"
            .to_string(),
    );
    let auth = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: Access denied".to_string(),
    );

    assert!(should_reconnect(&transient, 1_000, 3, true));
    assert!(!should_reconnect(&auth, 1_000, 3, true));
}

#[test]
fn reconnect_loop_resumes_from_checkpoint_after_transient_loss() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000001", 4));
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
        ..ApplyBinlogConfig::default()
    };

    let error = run_stream_reconnect_loop(
        &config,
        Some(&checkpoint_store),
        |_attempt_config| Ok(()),
        |_delay: Duration| {},
    )
    .expect_err("missing checkpoint coordinate");

    assert_eq!(
        error.to_string(),
        "checkpoint failed: required source-scoped stream checkpoint is missing"
    );
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
    load_count: RefCell<usize>,
}

impl MemoryCheckpointStore {
    fn with_checkpoint(checkpoint: Checkpoint) -> Self {
        Self {
            loaded: RefCell::new(Some(checkpoint)),
            saved: RefCell::new(None),
            load_count: RefCell::new(0),
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
        *self.load_count.borrow_mut() += 1;
        Ok(self.loaded.borrow().clone())
    }

    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), ApplyBinlogError> {
        self.saved.replace(Some(checkpoint.clone()));
        self.loaded.replace(Some(checkpoint.clone()));
        Ok(())
    }

    fn checkpoint_for_skip(&self) -> Result<Option<Checkpoint>, ApplyBinlogError> {
        if self.saved.borrow().is_some() {
            return Ok(self.loaded.borrow().clone());
        }
        self.load_checkpoint()
    }
}
