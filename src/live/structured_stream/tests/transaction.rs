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
fn replaced_divergent_primary_commits_and_checkpoints_with_durable_evidence() {
    let executor = TransactionRecordingExecutor::with_replaced_duplicate_second_row_change();
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
    assert!(
        record
            .error_text
            .starts_with("replace-divergent-pk: target row replaced with source image;")
    );
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
    .expect_err("divergent duplicate must abort the source transaction");

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
    .expect_err("update duplicate must abort the source transaction");

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
    .expect_err("foreign-key conflict must abort the source transaction");

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
    .expect_err("CHECK conflict must abort the source transaction");

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
        ["BEGIN", "EXEC", "EXEC", "COMMIT", "CHECKPOINT"]
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
