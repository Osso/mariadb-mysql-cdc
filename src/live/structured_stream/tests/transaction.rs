use super::*;

#[test]
fn table_map_and_row_events_do_not_checkpoint_without_transaction_boundary() {
    let table_map = BinlogEvent::TableMapEvent(accounts_table_map_event(5));
    let write = BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_present: vec![true; 5],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(1)),
            Some(MySqlValue::String("alpha".to_string())),
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
        ])],
    });

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
    let header = event_header(16, 260);

    let outcome = classify_event("mysqld-bin.000777", &header, &event);

    assert_eq!(outcome.policy, EventPolicy::CommitTransaction);
    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 260,
        })
    );
}

fn guests_table_map_event(table_id: u64) -> MysqlCdcTableMapEvent {
    MysqlCdcTableMapEvent {
        table_id,
        database_name: "fixture_cdc".to_string(),
        table_name: "guests".to_string(),
        column_types: vec![8, 254],
        column_metadata: vec![0, 0],
        null_bitmap: vec![false, false],
        table_metadata: None,
    }
}

fn sessions_table_map_event(table_id: u64) -> MysqlCdcTableMapEvent {
    MysqlCdcTableMapEvent {
        table_id,
        database_name: "fixture_cdc".to_string(),
        table_name: "sessions".to_string(),
        column_types: vec![8, 8, 254],
        column_metadata: vec![0, 0, 0],
        null_bitmap: vec![false, false, false],
        table_metadata: None,
    }
}

fn guest_write_rows_event(table_id: u64) -> BinlogEvent {
    BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id,
        flags: 0,
        columns_number: 2,
        columns_present: vec![true, true],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(78_806_710)),
            Some(MySqlValue::String(
                "02f12400-1020-4c7b-907b-0613c292bcd6MD3X".to_string(),
            )),
        ])],
    })
}

fn sessions_write_rows_event(table_id: u64) -> BinlogEvent {
    BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id,
        flags: 0,
        columns_number: 3,
        columns_present: vec![true, true, true],
        rows: vec![session_row(109_017_694)],
    })
}

fn sessions_write_rows_event_with_conflict_followup(table_id: u64) -> BinlogEvent {
    BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id,
        flags: 0,
        columns_number: 3,
        columns_present: vec![true, true, true],
        rows: vec![
            session_row(109_017_693),
            session_row(109_017_694),
            session_row(109_017_695),
        ],
    })
}

fn session_row(session_id: u32) -> RowData {
    RowData::new(vec![
        Some(MySqlValue::Int(session_id)),
        Some(MySqlValue::Int(78_806_710)),
        Some(MySqlValue::String(
            "02f12400-1020-4c7b-907b-0613c292bcd6MD3X".to_string(),
        )),
    ])
}

#[test]
fn source_xid_boundary_keeps_parent_committed_when_stream_fails_after_child_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let group_config = TargetTransactionGroupConfig {
        size: 25,
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
        event_header(19, 215329700),
        BinlogEvent::TableMapEvent(guests_table_map_event(19))
    )
    .expect("guests table map");
    process_event!(
        event_header(19, 215329720),
        BinlogEvent::TableMapEvent(sessions_table_map_event(20))
    )
    .expect("sessions table map");
    process_event!(event_header(30, 215329760), guest_write_rows_event(19))
        .expect("parent write in XID A");
    process_event!(
        event_header(16, 215329780),
        BinlogEvent::XidEvent(XidEvent { xid: 101 })
    )
    .expect("XID A");
    process_event!(event_header(30, 215329892), sessions_write_rows_event(20))
        .expect("child write in XID B");
    process_event!(
        event_header(16, 215329912),
        BinlogEvent::XidEvent(XidEvent { xid: 102 })
    )
    .expect("XID B");

    transaction
        .rollback_if_open(applier.executor())
        .expect("inject stream failure after XID B");

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
fn staged_success_resolution_is_discarded_on_target_rollback() {
    let executor = TransactionRecordingExecutor::default();
    let mut transaction = TargetTransaction::default();
    transaction
        .begin_if_needed(&executor)
        .expect("begin target transaction");
    transaction.pending_conflict_resolutions_mut().push(
        crate::conflict_repair::ConflictResolution {
            source_identity: "source".to_string(),
            schema: "fixture_cdc".to_string(),
            table: "accounts".to_string(),
            source_primary_key: vec!["1".to_string()],
            repair_run_id: "run".to_string(),
            evidence: "successful replay".to_string(),
        },
    );

    transaction
        .rollback_if_open(&executor)
        .expect("rollback target transaction");

    assert!(!transaction.has_pending_conflict_resolutions());
    assert_eq!(executor.operations(), ["BEGIN", "ROLLBACK"]);
}

#[test]
fn wraps_target_writes_and_checkpoint_in_source_xid_transaction() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
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
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "LOCK_CHECKPOINT", "CHECKPOINT", "COMMIT"]
    );
}

#[test]
fn equal_duplicate_commits_multi_row_transaction_and_checkpoints() {
    let executor = TransactionRecordingExecutor::with_equal_duplicate_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
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
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut applier,
                &mut context,
                &header,
                &event,
                "test-source",
                &mut conflicts,
            )
        }};
    }

    process_event!(
        event_header(19, 200),
        BinlogEvent::TableMapEvent(accounts_table_map_event(5))
    )
    .expect("table map");

    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("first row");
    process_event!(event_header(31, 240), write_rows_event(18, 2, "beta"))
        .expect("ignored duplicate should not abort source transaction");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        [
            "BEGIN",
            "EXEC",
            "EXEC",
            "LOCK_CHECKPOINT",
            "CHECKPOINT",
            "COMMIT"
        ]
    );
    assert!(conflicts.records().is_empty());
}

#[test]
fn records_sessions_conflict_and_equal_resolution_with_real_row_boundary() {
    let divergent_executor = TransactionRecordingExecutor {
        duplicate_row_change_number: Some(2),
        duplicate_mode: DuplicateMode::Divergent,
        ..TransactionRecordingExecutor::default()
    };
    let mut divergent_applier = crate::row::RowApplier::new(divergent_executor);
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let mut row_header = event_header(30, 0);
    row_header.event_length = 435;

    macro_rules! process_divergent_event {
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
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut divergent_applier,
                &mut context,
                &header,
                &event,
                "test-source",
                &mut conflicts,
            )
        }};
    }

    process_divergent_event!(
        event_header(19, 215_329_700),
        BinlogEvent::TableMapEvent(guests_table_map_event(19))
    )
    .expect("guests table map");
    process_divergent_event!(
        event_header(19, 215_329_720),
        BinlogEvent::TableMapEvent(sessions_table_map_event(20))
    )
    .expect("sessions table map");
    process_divergent_event!(event_header(30, 215_329_760), guest_write_rows_event(19))
        .expect("guest row");
    state.record_event_position(215_330_725);
    process_divergent_event!(row_header, sessions_write_rows_event(20))
        .expect("divergent sessions conflict is deferred until XID");
    process_divergent_event!(
        event_header(16, 215_331_160),
        BinlogEvent::XidEvent(XidEvent { xid: 101 })
    )
    .expect_err("XID persists the divergent sessions conflict");

    let record = &conflicts.records()[0];
    assert_eq!(record.key.table, "sessions");
    assert_eq!(record.key.source_primary_key, ["109017694"]);
    assert_eq!(record.key.coordinate.start_position, 215_330_725);

    let equal_executor = TransactionRecordingExecutor::with_equal_duplicate_second_row_change();
    let mut equal_applier = crate::row::RowApplier::new(equal_executor);
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut row_header = event_header(30, 0);
    row_header.event_length = 435;

    macro_rules! process_equal_event {
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
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut equal_applier,
                &mut context,
                &header,
                &event,
                "test-source",
                &mut conflicts,
            )
        }};
    }

    process_equal_event!(
        event_header(19, 215_329_700),
        BinlogEvent::TableMapEvent(guests_table_map_event(19))
    )
    .expect("guests table map replay");
    process_equal_event!(
        event_header(19, 215_329_720),
        BinlogEvent::TableMapEvent(sessions_table_map_event(20))
    )
    .expect("sessions table map replay");
    process_equal_event!(event_header(30, 215_329_760), guest_write_rows_event(19))
        .expect("guest row replay");
    state.record_event_position(215_330_725);
    process_equal_event!(row_header, sessions_write_rows_event(20))
        .expect("equal sessions row replay");
    process_equal_event!(
        event_header(16, 215_331_160),
        BinlogEvent::XidEvent(XidEvent { xid: 102 })
    )
    .expect("XID replay");

    let record = &conflicts.records()[0];
    let evidence = record
        .resolution_evidence
        .as_deref()
        .expect("equal-row resolution evidence");
    assert!(evidence.contains(
        "equal target row already existed; source coordinate mysqld-bin.002709:215330725"
    ));
    assert!(evidence.contains("source transaction end position 215331160"));
}

#[test]
fn process_stream_core_defers_and_finalizes_real_row_boundary_at_xid() {
    let config = ApplyBinlogConfig::default();
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut progress = crate::live::progress::StreamProgress::new(BinlogCoordinate {
        file: "mysqld-bin.002709".to_string(),
        position: 215_329_700,
    });
    let mut source_row_transaction_open = false;
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let executor = TransactionRecordingExecutor {
        duplicate_row_change_number: Some(3),
        duplicate_mode: DuplicateMode::Divergent,
        ..TransactionRecordingExecutor::default()
    };
    let mut applier = crate::row::RowApplier::new(executor);

    {
        let mut dispatch = |state: &mut StructuredEventState,
                            input: SourceStreamEvent<'_>|
         -> Result<StructuredEventOutcome, ApplyBinlogError> {
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut applier,
                &mut context,
                input.header,
                input.event,
                "test-source",
                &mut conflicts,
            )
        };

        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &event_header(19, 215_329_700),
                event: &BinlogEvent::TableMapEvent(guests_table_map_event(19)),
                source_position: 215_329_700,
            },
            &mut dispatch,
        )
        .expect("guest table map");
        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &event_header(19, 215_329_720),
                event: &BinlogEvent::TableMapEvent(sessions_table_map_event(20)),
                source_position: 215_329_720,
            },
            &mut dispatch,
        )
        .expect("sessions table map");
        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &event_header(30, 215_329_760),
                event: &guest_write_rows_event(19),
                source_position: 215_329_760,
            },
            &mut dispatch,
        )
        .expect("guest row");
        let mut row_header = event_header(30, 0);
        row_header.event_length = 435;
        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &row_header,
                event: &sessions_write_rows_event_with_conflict_followup(20),
                source_position: 215_330_725,
            },
            &mut dispatch,
        )
        .expect("divergent row observation is deferred until XID");
    }
    assert!(conflicts.records().is_empty());
    assert!(transaction.has_pending_conflict_observations());
    let operations_after_conflict = applier.executor().operations();
    assert_eq!(operations_after_conflict, ["BEGIN", "EXEC", "EXEC", "EXEC"]);
    {
        let mut dispatch_doomed_row = |state: &mut StructuredEventState,
                                       input: SourceStreamEvent<'_>|
         -> Result<StructuredEventOutcome, ApplyBinlogError> {
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state,
                target_transaction: &mut transaction,
                checkpoint_store: Some(&NoopCheckpointStore),
                transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
                transaction_checkpoint_name: Some("stream-binlog:test-source"),
                current_file: &mut current_file,
                group_config: TargetTransactionGroupConfig::default(),
            };
            apply_stream_event_transactionally_with_conflicts(
                &mut applier,
                &mut context,
                input.header,
                input.event,
                "test-source",
                &mut conflicts,
            )
        };
        let mut doomed_row_header = event_header(30, 0);
        doomed_row_header.event_length = 435;
        process_stream_event_core(
            &config,
            &mut state,
            &mut progress,
            &mut source_row_transaction_open,
            SourceStreamEvent {
                header: &doomed_row_header,
                event: &sessions_write_rows_event(20),
                source_position: 215_330_900,
            },
            &mut dispatch_doomed_row,
        )
        .expect("doomed transaction drains later row without target write");
    }
    assert_eq!(applier.executor().operations(), operations_after_conflict);

    let mut dispatch_xid = |state: &mut StructuredEventState,
                            input: SourceStreamEvent<'_>|
     -> Result<StructuredEventOutcome, ApplyBinlogError> {
        let mut context = StreamEventContext {
            schema_resolver: &resolver,
            state,
            target_transaction: &mut transaction,
            checkpoint_store: Some(&NoopCheckpointStore),
            transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
            transaction_checkpoint_name: Some("stream-binlog:test-source"),
            current_file: &mut current_file,
            group_config: TargetTransactionGroupConfig::default(),
        };
        apply_stream_event_transactionally_with_conflicts(
            &mut applier,
            &mut context,
            input.header,
            input.event,
            "test-source",
            &mut conflicts,
        )
    };
    let xid_header = event_header(16, 215_331_160);
    process_stream_event_core(
        &config,
        &mut state,
        &mut progress,
        &mut source_row_transaction_open,
        SourceStreamEvent {
            header: &xid_header,
            event: &BinlogEvent::XidEvent(XidEvent { xid: 102 }),
            source_position: 215_331_160,
        },
        &mut dispatch_xid,
    )
    .expect_err("XID persists the finalized conflict and stops replay");
    assert_eq!(
        conflicts.records()[0].key.coordinate.start_position,
        215_330_725
    );
    assert_eq!(
        conflicts.records()[0].key.coordinate.end_position,
        215_331_160
    );
}

#[test]
fn replaced_divergent_primary_commits_and_checkpoints_with_durable_evidence() {
    let executor = TransactionRecordingExecutor::with_replaced_duplicate_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    crate::conflict_repair::ConflictStore::observe(
        &mut conflicts,
        crate::conflict_repair::ConflictObservation {
            source_identity: "test-source".to_string(),
            source_server_id: 1,
            coordinate: crate::conflict_repair::ConflictCoordinate {
                file: "prior-binlog".to_string(),
                start_position: 1,
                end_position: 2,
            },
            schema: "fixture_cdc".to_string(),
            table: "accounts".to_string(),
            operation: crate::conflict_repair::ConflictOperation::Insert,
            source_primary_key: vec!["2".to_string()],
            duplicate_index: Some("PRIMARY".to_string()),
            duplicate_owner_primary_key: None,
            error_code: 1062,
            error_text: "prior replacement conflict".to_string(),
            observed_at_ms: 1,
            sessions_guest_recovery: None,
        },
    )
    .expect("prior conflict");
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
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

    apply_stream_event_transactionally_with_conflicts(
        &mut applier,
        &mut context,
        &header,
        &event,
        "test-source",
        &mut conflicts,
    )
    .expect("replacement should continue");

    let xid_header = event_header(16, 260);
    let xid_event = BinlogEvent::XidEvent(XidEvent { xid: 42 });
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
    apply_stream_event_transactionally_with_conflicts(
        &mut applier,
        &mut context,
        &xid_header,
        &xid_event,
        "test-source",
        &mut conflicts,
    )
    .expect("replacement transaction should commit");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "LOCK_CHECKPOINT", "CHECKPOINT", "COMMIT"]
    );
    let record = &conflicts.records()[0];
    assert_eq!(
        record.status,
        crate::conflict_repair::ConflictStatus::Resolved
    );
    assert!(record.repair_run_id.is_some());
    assert!(
        record
            .resolution_evidence
            .as_deref()
            .is_some_and(|evidence| evidence.contains("target row replaced with source image"))
    );
    assert_eq!(record.error_text, "prior replacement conflict");
}

struct DeferredConflictFixture<'a> {
    resolver: &'a FixtureSchemaResolver,
    state: &'a mut StructuredEventState,
    current_file: &'a mut String,
    transaction: &'a mut TargetTransaction,
    conflicts: &'a mut crate::conflict_repair::InMemoryConflictStore,
}

fn apply_deferred_conflict_at_xid(
    applier: &mut crate::row::RowApplier<TransactionRecordingExecutor>,
    fixture: DeferredConflictFixture<'_>,
    header: &EventHeader,
    event: &BinlogEvent,
) -> ApplyBinlogError {
    apply_deferred_conflict_at_xid_position(applier, fixture, header, event, 260)
}

fn apply_deferred_conflict_at_xid_position(
    applier: &mut crate::row::RowApplier<TransactionRecordingExecutor>,
    fixture: DeferredConflictFixture<'_>,
    header: &EventHeader,
    event: &BinlogEvent,
    xid_end_position: u32,
) -> ApplyBinlogError {
    let DeferredConflictFixture {
        resolver,
        state,
        current_file,
        transaction,
        conflicts,
    } = fixture;
    {
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
        apply_stream_event_transactionally_with_conflicts(
            applier,
            &mut context,
            header,
            event,
            "test-source",
            conflicts,
        )
        .expect("row conflict is deferred until XID");
    }
    assert!(conflicts.records().is_empty());
    assert!(transaction.has_pending_conflict_observations());

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
    apply_stream_event_transactionally_with_conflicts(
        applier,
        &mut context,
        &event_header(16, xid_end_position),
        &BinlogEvent::XidEvent(XidEvent { xid: 42 }),
        "test-source",
        conflicts,
    )
    .expect_err("XID persists the deferred conflict and aborts replay")
}

#[test]
fn divergent_duplicate_rolls_back_and_persists_conflict_evidence() {
    let executor = TransactionRecordingExecutor::with_divergent_duplicate_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
    let error = apply_deferred_conflict_at_xid(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
    );

    assert!(
        error
            .to_string()
            .contains("row conflict persisted for repair")
    );
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(conflicts.records()[0].error_code, 1062);
    assert_eq!(
        conflicts.records()[0].duplicate_index.as_deref(),
        Some("PRIMARY")
    );
}

#[test]
fn update_unique_conflict_under_ignore_duplicate_rolls_back_and_records_ledger() {
    let executor = TransactionRecordingExecutor::with_update_unique_conflict();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let event = BinlogEvent::UpdateRowsEvent(MysqlCdcUpdateRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_before_update: vec![true; 5],
        columns_after_update: vec![true; 5],
        rows: vec![UpdateRowData::new(
            RowData::new(vec![
                Some(MySqlValue::Int(1)),
                Some(MySqlValue::String("alpha".to_string())),
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
            ]),
            RowData::new(vec![
                Some(MySqlValue::Int(1)),
                Some(MySqlValue::String("beta".to_string())),
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
            ]),
        )],
    });
    let header = event_header(30, 240);
    let error = apply_deferred_conflict_at_xid(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
    );

    assert!(
        error
            .to_string()
            .contains("row conflict persisted for repair")
    );
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(
        conflicts.records()[0].key.operation,
        crate::conflict_repair::ConflictOperation::Update
    );
    assert_eq!(conflicts.records()[0].error_code, 1062);
    assert_eq!(
        conflicts.records()[0].duplicate_index.as_deref(),
        Some("uq_accounts_name")
    );
}

#[test]
fn duplicate_insert_under_default_error_policy_rolls_back_without_ledger_entry() {
    let executor = TransactionRecordingExecutor::with_default_duplicate_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
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

    let error = apply_stream_event_transactionally_with_conflicts(
        &mut applier,
        &mut context,
        &header,
        &event,
        "test-source",
        &mut conflicts,
    )
    .expect_err("default duplicate policy must abort the source transaction");

    assert!(error.to_string().contains("duplicate"));
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert!(conflicts.records().is_empty());
}

#[test]
fn sessions_109018328_fk_conflict_carries_exact_guest_recovery_after_rollback_and_persistence() {
    let executor = TransactionRecordingExecutor::with_foreign_key_conflict_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(sessions_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut current_file = "mysqld-bin.002709".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let event = BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id: 20,
        flags: 0,
        columns_number: 3,
        columns_present: vec![true, true, true],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(109_018_328)),
            Some(MySqlValue::Int(78_011_674)),
            Some(MySqlValue::String(
                "fb42c5a9-b717-4022-9f27-6b467e0ca28d515m".to_string(),
            )),
        ])],
    });

    let error = apply_deferred_conflict_at_xid_position(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &event_header(30, 224_141_058),
        &event,
        224_142_261,
    );

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(
        conflicts.records()[0].key.coordinate.start_position,
        224_141_039
    );
    assert_eq!(
        conflicts.records()[0].key.coordinate.end_position,
        224_142_261
    );
    assert_eq!(
        error.sessions_guest_recovery(),
        Some(&crate::live::SessionsGuestRecovery {
            schema: "globalcomix".to_string(),
            table: "sessions".to_string(),
            constraint: "fk_sessions_guest".to_string(),
            session_id: "109018328".to_string(),
            guest_id: "78011674".to_string(),
            guest_hash: "fb42c5a9-b717-4022-9f27-6b467e0ca28d515m".to_string(),
        })
    );
}

#[test]
fn foreign_key_conflict_rolls_back_and_preserves_constraint_evidence() {
    let executor = TransactionRecordingExecutor::with_foreign_key_conflict_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
    let error = apply_deferred_conflict_at_xid(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
    );

    assert!(
        error
            .to_string()
            .contains("row conflict persisted for repair")
    );
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(conflicts.records()[0].error_code, 1452);
    assert_eq!(conflicts.records()[0].duplicate_index, None);
}

#[test]
fn check_conflict_rolls_back_and_preserves_constraint_evidence() {
    let executor = TransactionRecordingExecutor::with_check_conflict_second_row_change();
    let mut applier = crate::row::RowApplier::new(executor);
    applier.apply_table_map(accounts_row_table_map());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let mut conflicts = crate::conflict_repair::InMemoryConflictStore::default();
    let header = event_header(30, 240);
    let event = write_rows_event(18, 2, "beta");
    let error = apply_deferred_conflict_at_xid(
        &mut applier,
        DeferredConflictFixture {
            resolver: &resolver,
            state: &mut state,
            current_file: &mut current_file,
            transaction: &mut transaction,
            conflicts: &mut conflicts,
        },
        &header,
        &event,
    );

    assert!(
        error
            .to_string()
            .contains("row conflict persisted for repair")
    );
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert_eq!(conflicts.records().len(), 1);
    assert_eq!(conflicts.records()[0].error_code, 3819);
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
fn file_checkpoint_waits_until_after_target_commit() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
    let checkpoint_store = RecordingCheckpointStore::new(applier.executor().shared_operations());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
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
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("xid");

    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "COMMIT", "CHECKPOINT"]
    );
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
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("first xid");
    process_event!(event_header(30, 280), write_rows_event(18, 2, "beta")).expect("write rows");
    process_event!(
        event_header(16, 320),
        BinlogEvent::XidEvent(XidEvent { xid: 43 })
    )
    .expect("second xid");

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
            "COMMIT"
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
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("first xid");
    process_event!(event_header(30, 280), write_rows_event(18, 2, "beta")).expect("write rows");
    process_event!(
        event_header(16, 320),
        BinlogEvent::XidEvent(XidEvent { xid: 43 })
    )
    .expect("second xid");

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
            "CHECKPOINT"
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
    process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha")).expect("write rows");
    process_event!(
        event_header(16, 260),
        BinlogEvent::XidEvent(XidEvent { xid: 42 })
    )
    .expect("xid");
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
    let update = BinlogEvent::UpdateRowsEvent(MysqlCdcUpdateRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_before_update: vec![true; 5],
        columns_after_update: vec![true; 5],
        rows: vec![UpdateRowData::new(
            RowData::new(vec![
                Some(MySqlValue::Int(1)),
                Some(MySqlValue::String("alpha".to_string())),
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
            ]),
            RowData::new(vec![
                Some(MySqlValue::Int(2)),
                Some(MySqlValue::String("beta".to_string())),
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
            ]),
        )],
    });

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

#[test]
fn rolls_back_open_target_transaction_when_row_apply_fails() {
    let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::failing());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    macro_rules! process_event {
        ($header:expr, $event:expr) => {{
            let header = $header;
            let event = $event;
            let mut context = StreamEventContext {
                schema_resolver: &resolver,
                state: &mut state,
                target_transaction: &mut transaction,
                checkpoint_store: None::<&NoopCheckpointStore>,
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
    let result = process_event!(event_header(30, 220), write_rows_event(18, 1, "alpha"));

    assert!(result.is_err());
    assert_eq!(
        applier.executor().operations().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert!(!transaction.is_open());
}
