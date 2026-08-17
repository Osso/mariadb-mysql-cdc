use super::*;

#[test]
fn table_map_and_row_events_do_not_checkpoint_without_transaction_boundary() {
    let table_map = BinlogEvent::TableMapEvent(accounts_table_map_event(5));
    let write = write_rows_event(18, 1, "alpha");

    assert_eq!(
        classify_event("mysqld-bin.000777", &event_header(19, 200), &table_map).resume_coordinate,
        None
    );
    assert_eq!(
        classify_event("mysqld-bin.000777", &event_header(30, 220), &write).resume_coordinate,
        None
    );
}

#[test]
fn xid_event_checkpoints_after_transaction_rows_are_applied() {
    let event = BinlogEvent::XidEvent(XidEvent { xid: 42 });
    let outcome = classify_event("mysqld-bin.000777", &event_header(16, 260), &event);

    assert_eq!(outcome.policy, EventPolicy::CommitTransaction);
    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 260,
        })
    );
}

#[test]
fn wraps_target_writes_and_checkpoint_in_source_xid_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();

    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(19, 200),
        &BinlogEvent::TableMapEvent(accounts_table_map_event(5)),
    )
    .expect("table map");
    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(30, 220),
        &write_rows_event(18, 1, "alpha"),
    )
    .expect("write rows");
    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(16, 260),
        &BinlogEvent::XidEvent(XidEvent { xid: 42 }),
    )
    .expect("xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "LOCK_CHECKPOINT", "CHECKPOINT", "COMMIT"]
    );
}

#[test]
fn update_duplicate_error_rolls_back_without_checkpoint_or_commit() {
    let executor = TransactionRecordingExecutor::with_update_duplicate();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();

    let error = process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(30, 240),
        &account_update_event(),
    )
    .expect_err("UPDATE 1062 must fail the complete source transaction");

    assert!(error.to_string().contains("1062"));
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert!(!transaction.is_open());
}

#[test]
fn rolls_back_open_target_transaction_when_row_apply_fails() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::failing());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();

    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(19, 200),
        &BinlogEvent::TableMapEvent(accounts_table_map_event(5)),
    )
    .expect("table map");
    let result = process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(30, 220),
        &write_rows_event(18, 1, "alpha"),
    );

    assert!(result.is_err());
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert!(!transaction.is_open());
}

#[test]
fn file_checkpoint_waits_until_after_pending_target_commits() {
    let mut applier =
        crate::row::RowApplier::new(TransactionRecordingExecutor::with_pending_flush());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();

    let mut process = |header: EventHeader, event: BinlogEvent| {
        let mut context = StreamEventContext {
            schema_resolver: &resolver,
            state: &mut state,
            target_transaction: &mut transaction,
            checkpoint_store: Some(&checkpoint_store),
            transaction_checkpoint_table: None,
            transaction_checkpoint_name: None,
            current_file: &mut current_file,
            group_config: TargetTransactionGroupConfig::default(),
        };
        apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
    };

    process(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5)),
    )
    .expect("table map");
    process(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 }),
    )
    .expect("xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "COMMIT", "FLUSH", "CHECKPOINT"]
    );
}

#[test]
fn source_xid_boundary_keeps_parent_committed_when_stream_fails_after_child_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();

    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(19, 215_329_700),
        &BinlogEvent::TableMapEvent(accounts_table_map_event(5)),
    )
    .expect("accounts table map");
    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(30, 215_329_760),
        &write_rows_event(18, 1, "parent"),
    )
    .expect("first transaction row");
    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(16, 215_329_780),
        &BinlogEvent::XidEvent(XidEvent { xid: 101 }),
    )
    .expect("first transaction XID");
    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(30, 215_329_892),
        &write_rows_event(18, 2, "child"),
    )
    .expect("second transaction row");
    process_transactional_event(
        &mut applier,
        &resolver,
        &mut state,
        &mut current_file,
        &mut transaction,
        &event_header(16, 215_329_912),
        &BinlogEvent::XidEvent(XidEvent { xid: 102 }),
    )
    .expect("second transaction XID");

    transaction
        .rollback_if_open(applier.executor())
        .expect("inject stream failure after second XID");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT",
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT",
        ]
    );
}

#[test]
fn query_dml_does_not_open_or_checkpoint_target_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "INSERT INTO accounts (id, name) VALUES (999, 'query-event')".to_string(),
    });
    let header = event_header(99, 180);
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };

    let error = apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        .expect_err("statement DML must fail before target transaction");

    assert!(error.to_string().contains("ROW/FULL contract violation"));
    assert!(applier.executor().operations().is_empty());
    assert_eq!(current_file, "mysqld-bin.000777");
}

#[test]
fn direct_checkpoint_waits_for_pending_target_commits() {
    let executor = TransactionRecordingExecutor::with_pending_flush();
    let checkpoint_store = RecordingCheckpointStore::new(executor.shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&checkpoint_store),
        transaction_checkpoint_table: None,
        transaction_checkpoint_name: None,
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };
    let event = BinlogEvent::XidEvent(XidEvent { xid: 42 });
    let outcome = StructuredEventOutcome {
        policy: EventPolicy::CommitTransaction,
        resume_coordinate: Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 260,
        }),
    };

    save_outcome_checkpoint(&executor, &mut context, &event, &outcome)
        .expect("save direct checkpoint");

    assert_eq!(executor.operations(), ["FLUSH", "CHECKPOINT"]);
}

#[test]
fn groups_multiple_xids_in_one_mysql_target_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 2,
        timeout: Duration::ZERO,
    };
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config,
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("first row");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("first XID");
    process_event!(event_header(30, 280), write_rows_event(18, 2, "beta")).expect("second row");
    process_event!(
        event_header(16, 320),
        BinlogEvent::XidEvent(XidEvent { xid: 43 })
    )
    .expect("second XID");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT",
            "BEGIN",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT",
        ]
    );
}

#[test]
fn grouped_file_checkpoint_saves_last_xid_after_group_commit() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 2,
        timeout: Duration::ZERO,
    };
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&checkpoint_store),
                transaction_checkpoint_table: None,
                transaction_checkpoint_name: None,
                current_file: &mut current_file,
                group_config,
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("first row");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("first XID");
    process_event!(event_header(30, 280), write_rows_event(18, 2, "beta")).expect("second row");
    process_event!(
        event_header(16, 320),
        BinlogEvent::XidEvent(XidEvent { xid: 43 })
    )
    .expect("second XID");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "COMMIT",
            "CHECKPOINT",
            "BEGIN",
            "EXEC",
            "COMMIT",
            "CHECKPOINT",
        ]
    );
}

#[test]
fn rotate_flushes_open_group_before_rotate_checkpoint() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 10,
        timeout: Duration::ZERO,
    };
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&checkpoint_store),
                transaction_checkpoint_table: None,
                transaction_checkpoint_name: None,
                current_file: &mut current_file,
                group_config,
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write row");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("XID");
    process_event!(
        event_header(20, 4),
        BinlogEvent::RotateEvent(RotateEvent {
            binlog_position: 4,
            binlog_filename: "mysqld-bin.000778".to_string(),
        })
    )
    .expect("rotate");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "COMMIT", "CHECKPOINT", "CHECKPOINT"]
    );
}

#[test]
fn applies_primary_key_change_without_checkpoint_before_source_commit() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let update = BinlogEvent::UpdateRowsEvent(MysqlCdcUpdateRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_before_update: vec![true; 5],
        columns_after_update: vec![true; 5],
        rows: vec![UpdateRowData::new(
            account_row(1, "alpha"),
            account_row(2, "beta"),
        )],
    });

    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&checkpoint_store),
                transaction_checkpoint_table: None,
                transaction_checkpoint_name: None,
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally(&mut applier, &mut context, &header, &event)
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");
    process_event!(event_header(30, 220), update).expect("apply primary-key change");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC"]
    );
    assert!(transaction.is_open());

    process_event!(
        event_header(31, 240),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("commit source transaction");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "COMMIT", "CHECKPOINT"]
    );
    assert!(!transaction.is_open());
}

fn process_transactional_event(
    applier: &mut crate::row::RowApplier<TransactionRecordingExecutor>,
    resolver: &FixtureSchemaResolver,
    state: &mut StructuredEventState,
    current_file: &mut String,
    transaction: &mut TargetTransaction,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Result<StructuredEventOutcome, ApplyBinlogError> {
    let mut context = StreamEventContext {
        schema_resolver: resolver,
        state,
        target_transaction: transaction,
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };
    apply_stream_event_transactionally(applier, &mut context, header, event)
}

fn account_update_event() -> BinlogEvent {
    BinlogEvent::UpdateRowsEvent(MysqlCdcUpdateRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_before_update: vec![true; 5],
        columns_after_update: vec![true; 5],
        rows: vec![UpdateRowData::new(
            account_row(1, "alpha"),
            account_row(1, "beta"),
        )],
    })
}

fn account_row(id: u32, name: &str) -> RowData {
    RowData::new(vec![
        Some(MySqlValue::Int(id)),
        Some(MySqlValue::String(name.to_string())),
        Some(MySqlValue::Int(100)),
        Some(MySqlValue::String("safe".to_string())),
        Some(MySqlValue::DateTime(DateTime {
            year: 2026,
            month: 6,
            day: 22,
            hour: 12,
            minute: 3,
            second: 4,
            millis: 0,
        })),
    ])
}
