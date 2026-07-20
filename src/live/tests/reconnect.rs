use super::*;
use crate::live::reconnect::{reconnect_delay, run_stream_reconnect_loop_with_recovery};

fn exact_sessions_guest_recovery() -> SessionsGuestRecovery {
    SessionsGuestRecovery {
        source_file: "mysqld-bin.002709".to_string(),
        source_start_position: 224_141_039,
        source_end_position: 224_142_261,
        child_event_timestamp: 1_752_710_400,
        schema: "globalcomix".to_string(),
        table: "sessions".to_string(),
        constraint: "fk_sessions_guest".to_string(),
        session_id: "109018328".to_string(),
        guest_id: "78011674".to_string(),
        guest_hash: "fb42c5a9-b717-4022-9f27-6b467e0ca28d515m".to_string(),
    }
}

#[test]
fn reconnect_delay_caps_at_five_seconds() {
    assert_eq!(reconnect_delay(1), Duration::from_secs(1));
    assert_eq!(reconnect_delay(4), Duration::from_secs(5));
    assert_eq!(reconnect_delay(36), Duration::from_secs(5));
}

#[test]
fn classifies_purged_or_missing_binlog_source_errors() {
    let missing_first_log = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: ERROR: Could not find first log file name in binary log index file"
            .to_string(),
    );
    let not_in_index = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: ERROR 1236: Could not find log file 'mysqld-bin.000001' in binary log index file"
            .to_string(),
    );
    let missing_explicitly_in_index = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: ERROR 1236: File 'mysqld-bin.000001' not found in binary log index"
            .to_string(),
    );
    let truncated = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: Event truncated; the event could not be read completely"
            .to_string(),
    );
    let transient = ApplyBinlogError::SourceCommand("reading packet: timed out".to_string());
    let target = ApplyBinlogError::Target(
        "Could not find first log file name in binary log index file".to_string(),
    );

    assert!(is_stale_or_missing_binlog_error(&missing_first_log));
    assert!(is_stale_or_missing_binlog_error(&not_in_index));
    assert!(is_stale_or_missing_binlog_error(
        &missing_explicitly_in_index
    ));
    assert!(!is_stale_or_missing_binlog_error(&truncated));
    assert!(!is_stale_or_missing_binlog_error(&transient));
    assert!(!is_stale_or_missing_binlog_error(&target));
}

#[test]
fn reconnects_when_source_restart_temporarily_refuses_connections() {
    let error = ApplyBinlogError::SourceCommand(
        "source binlog command failed: Connection refused".to_string(),
    );

    assert!(should_reconnect(&error, 0, 3, false));
}

#[test]
fn reconnect_forever_fails_without_changing_a_stale_checkpoint() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000001", 4));
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
            ..SourceBinlogConfig::default()
        },
        reconnect_forever: true,
        ..ApplyBinlogConfig::default()
    };
    let attempts = RefCell::new(0);

    let error = run_stream_reconnect_loop(
        &config,
        Some(&checkpoint_store),
        |_attempt_config| {
            *attempts.borrow_mut() += 1;
            Err(ApplyBinlogError::SourceCommand(
                "ERROR: Could not find first log file name in binary log index file".to_string(),
            ))
        },
        |_delay: Duration| {},
    )
    .expect_err("stale checkpoint must require operator repair");

    assert_eq!(*attempts.borrow(), 1);
    assert!(checkpoint_store.saved.borrow().is_none());
    assert!(is_stale_or_missing_binlog_error(&error));
}

#[test]
fn reconnect_forever_reloads_checkpoint_after_generic_transient_error() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000001", 4));
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
            ..SourceBinlogConfig::default()
        },
        reconnect_forever: true,
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
                    .save_checkpoint(&checkpoint_at("mysqld-bin.000333", 12345))
                    .expect("save checkpoint");
                return Err(ApplyBinlogError::SourceCommand(
                    "reading packet: timed out".to_string(),
                ));
            }
            Ok(())
        },
        |_delay: Duration| {},
    )
    .expect("transient reconnect");

    assert_eq!(
        seen_starts.into_inner(),
        vec![
            ("mysqld-bin.000001".to_string(), 4),
            ("mysqld-bin.000333".to_string(), 12345),
        ]
    );
}

#[test]
fn retries_durably_persisted_row_conflict_from_unchanged_checkpoint() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000001", 120));
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: "mysqld-bin.000001".to_string(),
            start_position: 120,
            ..SourceBinlogConfig::default()
        },
        max_reconnects: 2,
        ..ApplyBinlogConfig::default()
    };
    let starts = RefCell::new(Vec::new());
    let delays = RefCell::new(Vec::new());

    run_stream_reconnect_loop(
        &config,
        Some(&checkpoint_store),
        |attempt_config| {
            starts
                .borrow_mut()
                .push(attempt_config.source.start_position);
            if starts.borrow().len() == 1 {
                return Err(ApplyBinlogError::RowConflictPersisted {
                    message:
                        "Cannot add or update a child row: a foreign key constraint fails (1452)"
                            .to_string(),
                    sessions_guest_recovery: None,
                });
            }
            Ok(())
        },
        |delay| delays.borrow_mut().push(delay),
    )
    .expect("persisted row conflict should retry in-process");

    assert_eq!(starts.into_inner(), vec![120, 120]);
    assert_eq!(delays.into_inner(), vec![Duration::from_secs(1)]);
    assert!(checkpoint_store.saved.borrow().is_none());
}

#[test]
fn recovers_exact_sessions_guest_after_persisted_conflict_before_unchanged_checkpoint_retry() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.002709", 224_140_888));
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: "mysqld-bin.002709".to_string(),
            start_position: 224_140_888,
            ..SourceBinlogConfig::default()
        },
        max_reconnects: 1,
        ..ApplyBinlogConfig::default()
    };
    let request = exact_sessions_guest_recovery();
    let events = RefCell::new(Vec::new());
    let attempts = RefCell::new(0);

    run_stream_reconnect_loop_with_recovery(
        &config,
        Some(&checkpoint_store),
        |attempt_config| {
            events
                .borrow_mut()
                .push(format!("attempt:{}", attempt_config.source.start_position));
            let mut count = attempts.borrow_mut();
            *count += 1;
            if *count == 1 {
                events
                    .borrow_mut()
                    .push("rolled-back-and-persisted".to_string());
                return Err(ApplyBinlogError::RowConflictPersisted {
                    message: "fk_sessions_guest".to_string(),
                    sessions_guest_recovery: Some(Box::new(request.clone())),
                });
            }
            checkpoint_store
                .save_checkpoint(&checkpoint_at("mysqld-bin.002709", 224_142_261))
                .expect("child replay checkpoint");
            events.borrow_mut().push("child-committed".to_string());
            Ok(())
        },
        |actual| {
            assert_eq!(actual, &request);
            assert_eq!(
                checkpoint_store
                    .load_checkpoint()
                    .expect("load unchanged checkpoint")
                    .unwrap()
                    .source_position,
                224_140_888
            );
            events.borrow_mut().push("parent-recovered".to_string());
            Ok(())
        },
        |_delay| {},
    )
    .expect("parent recovery retries unchanged checkpoint");

    assert_eq!(
        events.into_inner(),
        vec![
            "attempt:224140888",
            "rolled-back-and-persisted",
            "parent-recovered",
            "attempt:224140888",
            "child-committed",
        ]
    );
    assert_eq!(
        checkpoint_store
            .load_checkpoint()
            .unwrap()
            .unwrap()
            .source_position,
        224_142_261
    );
}

#[test]
fn exhausted_reconnect_budget_does_not_recover_parent() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.002709", 224_140_888));
    let config = ApplyBinlogConfig {
        max_reconnects: 0,
        ..ApplyBinlogConfig::default()
    };
    let recoveries = RefCell::new(0);
    let request = exact_sessions_guest_recovery();

    let error = run_stream_reconnect_loop_with_recovery(
        &config,
        Some(&checkpoint_store),
        |_attempt_config| {
            Err(ApplyBinlogError::RowConflictPersisted {
                message: "fk_sessions_guest".to_string(),
                sessions_guest_recovery: Some(Box::new(request.clone())),
            })
        },
        |_request| {
            *recoveries.borrow_mut() += 1;
            Ok(())
        },
        |_delay| {},
    )
    .expect_err("exhausted retry budget returns the persisted conflict");

    assert!(matches!(
        error,
        ApplyBinlogError::RowConflictPersisted { .. }
    ));
    assert_eq!(*recoveries.borrow(), 0);
}

#[test]
fn recovers_each_exact_persisted_conflict_identity_once_per_loop() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.002709", 224_140_888));
    let config = ApplyBinlogConfig {
        max_reconnects: 2,
        ..ApplyBinlogConfig::default()
    };
    let attempts = RefCell::new(0);
    let recoveries = RefCell::new(0);
    let request = exact_sessions_guest_recovery();

    run_stream_reconnect_loop_with_recovery(
        &config,
        Some(&checkpoint_store),
        |_attempt_config| {
            let mut count = attempts.borrow_mut();
            *count += 1;
            if *count <= 2 {
                return Err(ApplyBinlogError::RowConflictPersisted {
                    message: "fk_sessions_guest".to_string(),
                    sessions_guest_recovery: Some(Box::new(request.clone())),
                });
            }
            Ok(())
        },
        |_request| {
            *recoveries.borrow_mut() += 1;
            Ok(())
        },
        |_delay| {},
    )
    .expect("reconnect remains available after bounded recovery");

    assert_eq!(*attempts.borrow(), 3);
    assert_eq!(*recoveries.borrow(), 1);
}

#[test]
fn does_not_retry_unrecoverable_target_failure() {
    let error = ApplyBinlogError::Target("permission denied".to_string());

    assert!(!should_reconnect(&error, 0, 3, false));
}
