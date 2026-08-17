use super::*;
use crate::live::reconnect::reconnect_delay;

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
    let transient = ApplyBinlogError::SourceCommand("reading packet: timed out".to_string());
    let target = ApplyBinlogError::Target(
        "Could not find first log file name in binary log index file".to_string(),
    );

    assert!(is_stale_or_missing_binlog_error(&missing_first_log));
    assert!(is_stale_or_missing_binlog_error(&not_in_index));
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
fn does_not_retry_target_failure() {
    let error = ApplyBinlogError::Target("permission denied".to_string());

    assert!(!should_reconnect(&error, 0, 3, false));
}
