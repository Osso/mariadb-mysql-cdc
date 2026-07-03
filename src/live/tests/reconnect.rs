use super::*;
use crate::live::reconnect::{
    BINLOG_START_POSITION, format_stale_binlog_auto_skip, reconnect_delay,
};

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
    let truncated_middle = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: truncated in the middle of event; consider out-of-order binlog"
            .to_string(),
    );
    let generic_index_phrase = ApplyBinlogError::SourceCommand(
        "mariadb-binlog exited with exit status: 1: warning: scanned in binary log index file"
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
    assert!(!is_stale_or_missing_binlog_error(&truncated_middle));
    assert!(!is_stale_or_missing_binlog_error(&generic_index_phrase));
    assert!(!is_stale_or_missing_binlog_error(&transient));
    assert!(!is_stale_or_missing_binlog_error(&target));
}

#[test]
fn reconnect_forever_auto_skips_stale_checkpoint_to_current_binlog_start() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000001", 4));
    let coordinate_reader = RecordingCoordinateReader::new(BinlogCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 98765,
    });
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
            ..SourceBinlogConfig::default()
        },
        reconnect_forever: true,
        max_reconnects: 1,
        ..ApplyBinlogConfig::default()
    };
    let seen_starts = RefCell::new(Vec::new());
    let attempts = RefCell::new(0);

    run_stream_reconnect_loop_with_coordinate_reader(
        &config,
        Some(&checkpoint_store),
        &coordinate_reader,
        |attempt_config| {
            seen_starts.borrow_mut().push((
                attempt_config.source.binlog_file.clone(),
                attempt_config.source.start_position,
            ));
            let mut attempts_ref = attempts.borrow_mut();
            *attempts_ref += 1;
            if *attempts_ref == 1 {
                return Err(ApplyBinlogError::SourceCommand(
                    "ERROR: Could not find first log file name in binary log index file"
                        .to_string(),
                ));
            }
            Ok(())
        },
        |_delay: Duration| {},
    )
    .expect("auto skip stale checkpoint");

    assert_eq!(
        seen_starts.into_inner(),
        vec![
            ("mysqld-bin.000001".to_string(), 4),
            ("mysqld-bin.000777".to_string(), 4),
        ]
    );
    assert_eq!(
        coordinate_reader.calls.borrow().as_slice(),
        &["mysqld-bin.000001:4"]
    );
    let saved = checkpoint_store.saved.borrow();
    let checkpoint = saved.as_ref().expect("saved auto-skip checkpoint");
    assert_eq!(checkpoint.source_file, "mysqld-bin.000777");
    assert_eq!(checkpoint.source_position, 4);
    assert_eq!(checkpoint.last_event.event_type, "AutoSkipStaleBinlog");
    assert!(
        checkpoint
            .last_event
            .description
            .contains("mysqld-bin.000001:4")
    );
    assert!(
        checkpoint
            .last_event
            .description
            .contains("mysqld-bin.000777:4")
    );
}

#[test]
fn stale_binlog_auto_skip_log_includes_master_eof_and_chosen_start() {
    let message = format_stale_binlog_auto_skip(
        &BinlogCoordinate {
            file: "mysqld-bin.000001".to_string(),
            position: 4,
        },
        &BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 98765,
        },
        &BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: BINLOG_START_POSITION,
        },
        &ApplyBinlogError::SourceCommand(
            "ERROR: Could not find first log file name in binary log index file".to_string(),
        ),
    );

    assert!(message.contains("master_file=mysqld-bin.000777"));
    assert!(message.contains("master_position=98765"));
    assert!(message.contains("new_file=mysqld-bin.000777"));
    assert!(message.contains("new_position=4"));
}

#[test]
fn reconnect_forever_refuses_auto_skip_when_chosen_start_coordinate_matches_attempt() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000001", 4));
    let coordinate_reader = RecordingCoordinateReader::new(BinlogCoordinate {
        file: "mysqld-bin.000001".to_string(),
        position: 98765,
    });
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

    let error = run_stream_reconnect_loop_with_coordinate_reader(
        &config,
        Some(&checkpoint_store),
        &coordinate_reader,
        |_attempt_config| {
            *attempts.borrow_mut() += 1;
            Err(ApplyBinlogError::SourceCommand(
                "ERROR: Could not find first log file name in binary log index file".to_string(),
            ))
        },
        |_delay: Duration| {},
    )
    .expect_err("same-coordinate auto-skip should fail clearly");

    assert_eq!(*attempts.borrow(), 1);
    assert_eq!(
        coordinate_reader.calls.borrow().as_slice(),
        &["mysqld-bin.000001:4"]
    );
    assert!(checkpoint_store.saved.borrow().is_none());
    let ApplyBinlogError::SourceCommand(message) = error else {
        panic!("expected SourceCommand error");
    };
    assert!(message.contains("stale binlog auto-skip refused"));
    assert!(message.contains("same coordinate mysqld-bin.000001:4"));
}

#[test]
fn reconnect_forever_refuses_second_immediate_stale_auto_skip() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000001", 4));
    let coordinate_reader = RecordingCoordinateReader::new(BinlogCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 98765,
    });
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

    let error = run_stream_reconnect_loop_with_coordinate_reader(
        &config,
        Some(&checkpoint_store),
        &coordinate_reader,
        |attempt_config| {
            seen_starts.borrow_mut().push((
                attempt_config.source.binlog_file.clone(),
                attempt_config.source.start_position,
            ));
            Err(ApplyBinlogError::SourceCommand(
                "ERROR: Could not find first log file name in binary log index file".to_string(),
            ))
        },
        |_delay: Duration| {},
    )
    .expect_err("repeated stale auto-skip should fail clearly");

    assert_eq!(
        seen_starts.into_inner(),
        vec![
            ("mysqld-bin.000001".to_string(), 4),
            ("mysqld-bin.000777".to_string(), 4),
        ]
    );
    assert_eq!(coordinate_reader.calls.borrow().len(), 1);
    let ApplyBinlogError::SourceCommand(message) = error else {
        panic!("expected SourceCommand error");
    };
    assert!(message.contains("immediate retry from mysqld-bin.000777:4"));
}

#[test]
fn reconnect_forever_does_not_auto_skip_generic_transient_errors() {
    let checkpoint_store =
        MemoryCheckpointStore::with_checkpoint(checkpoint_at("mysqld-bin.000001", 4));
    let coordinate_reader = RecordingCoordinateReader::new(BinlogCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 98765,
    });
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

    run_stream_reconnect_loop_with_coordinate_reader(
        &config,
        Some(&checkpoint_store),
        &coordinate_reader,
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

    assert_eq!(coordinate_reader.calls.borrow().len(), 0);
    assert_eq!(
        seen_starts.into_inner(),
        vec![
            ("mysqld-bin.000001".to_string(), 4),
            ("mysqld-bin.000333".to_string(), 12345),
        ]
    );
}

struct RecordingCoordinateReader {
    coordinate: BinlogCoordinate,
    calls: RefCell<Vec<String>>,
}

impl RecordingCoordinateReader {
    fn new(coordinate: BinlogCoordinate) -> Self {
        Self {
            coordinate,
            calls: RefCell::new(Vec::new()),
        }
    }
}

impl SourceCoordinateReader for RecordingCoordinateReader {
    fn current_coordinate(
        &self,
        config: &ApplyBinlogConfig,
    ) -> Result<BinlogCoordinate, ApplyBinlogError> {
        self.calls.borrow_mut().push(format!(
            "{}:{}",
            config.source.binlog_file, config.source.start_position
        ));
        Ok(self.coordinate.clone())
    }
}
