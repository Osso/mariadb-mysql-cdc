use super::*;

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
fn stream_checkpoint_uses_cached_position_after_first_save() {
    let checkpoint_store = MemoryCheckpointStore::default();
    let first = StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 12_300,
        },
        resume_position: 12_399,
        default_database: Some("globalcomix".to_string()),
        sql: "INSERT INTO accounts (id) VALUES (1)".to_string(),
    };
    let second = StatementEvent {
        coordinate: BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 12_400,
        },
        resume_position: 12_499,
        default_database: Some("globalcomix".to_string()),
        sql: "INSERT INTO accounts (id) VALUES (2)".to_string(),
    };

    save_stream_checkpoint(Some(&checkpoint_store), &first).expect("save first checkpoint");
    save_stream_checkpoint(Some(&checkpoint_store), &second).expect("save second checkpoint");

    assert_eq!(*checkpoint_store.load_count.borrow(), 1);
    assert_eq!(
        checkpoint_store
            .saved
            .borrow()
            .as_ref()
            .expect("saved checkpoint")
            .source_position,
        12_499
    );
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
