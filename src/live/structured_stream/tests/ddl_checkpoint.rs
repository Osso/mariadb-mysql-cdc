use super::*;

#[test]
fn startup_grants_validate_once_before_resolved_manual_ddl_routing() {
    let executor = TransactionRecordingExecutor::default();
    let ledger = RecordingDdlLedger::startup_validated(
        "CREATE TABLE now_manual (id int) WITH SYSTEM VERSIONING",
    );
    ledger.ensure().expect("startup grant validation");
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
        sql_statement: "CREATE TABLE now_manual (id int) WITH SYSTEM VERSIONING".to_string(),
    });
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

    let outcome = handle_manual_ddl_event(
        &executor,
        &ledger,
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect("resolved DDL")
    .expect("DDL outcome");

    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 180,
        })
    );
    assert_eq!(
        executor.operations(),
        vec!["BEGIN", "LOCK_CHECKPOINT", "CHECKPOINT", "COMMIT"]
    );
    assert_eq!(ledger.ensure_calls.get(), 1);
}

#[test]
fn resolved_ddl_with_different_raw_sql_does_not_advance_checkpoint() {
    let executor = TransactionRecordingExecutor::default();
    let ledger = RecordingDdlLedger::resolved("ALTER TABLE now_manual ADD COLUMN changed int");
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
        sql_statement: "CREATE TABLE now_manual (id int) WITH SYSTEM VERSIONING".to_string(),
    });
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

    let error = handle_manual_ddl_event(
        &executor,
        &ledger,
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect_err("different ledger SQL must block checkpoint advancement");

    assert!(error.to_string().contains("DDL ledger SQL mismatch"));
    assert!(executor.operations().is_empty());
}

#[test]
fn resolved_ddl_refuses_to_move_checkpoint_backward() {
    let executor = TransactionRecordingExecutor::default();
    let ledger =
        RecordingDdlLedger::resolved("CREATE TABLE now_manual (id int) WITH SYSTEM VERSIONING");
    let checkpoint_store = FixedCheckpointStore {
        checkpoint: crate::checkpoint::Checkpoint {
            source_file: "mysqld-bin.000777".to_string(),
            source_position: 200,
            gtid: None,
            event_timestamp: 0,
            last_event: crate::checkpoint::LastEvent {
                event_type: "XidEvent".to_string(),
                description: "later checkpoint".to_string(),
            },
        },
    };
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
        sql_statement: "CREATE TABLE now_manual (id int) WITH SYSTEM VERSIONING".to_string(),
    });
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&checkpoint_store),
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };

    let error = handle_manual_ddl_event(
        &executor,
        &ledger,
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect_err("checkpoint regression must block");

    assert!(error.to_string().contains("refusing checkpoint regression"));
    assert!(executor.operations().is_empty());
}

#[test]
fn resolved_ddl_locks_and_rejects_a_concurrently_advanced_checkpoint() {
    let executor =
        TransactionRecordingExecutor::with_locked_checkpoint(crate::checkpoint::Checkpoint {
            source_file: "mysqld-bin.000777".to_string(),
            source_position: 200,
            gtid: None,
            event_timestamp: 0,
            last_event: crate::checkpoint::LastEvent {
                event_type: "XidEvent".to_string(),
                description: "concurrent later checkpoint".to_string(),
            },
        });
    let ledger =
        RecordingDdlLedger::resolved("CREATE TABLE now_manual (id int) WITH SYSTEM VERSIONING");
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
        sql_statement: "CREATE TABLE now_manual (id int) WITH SYSTEM VERSIONING".to_string(),
    });
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: None::<&NoopCheckpointStore>,
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };

    let error = handle_manual_ddl_event(
        &executor,
        &ledger,
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect_err("concurrent checkpoint advance must block regression");

    assert!(error.to_string().contains("refusing checkpoint regression"));
    assert_eq!(
        executor.operations(),
        vec!["BEGIN", "LOCK_CHECKPOINT", "ROLLBACK"]
    );
}

#[test]
fn grouped_dml_checkpoint_rejects_a_concurrently_advanced_checkpoint() {
    let executor =
        TransactionRecordingExecutor::with_locked_checkpoint(crate::checkpoint::Checkpoint {
            source_file: "mysqld-bin.000777".to_string(),
            source_position: 200,
            gtid: None,
            event_timestamp: 0,
            last_event: crate::checkpoint::LastEvent {
                event_type: "XidEvent".to_string(),
                description: "concurrent later checkpoint".to_string(),
            },
        });
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    transaction
        .begin_if_needed(&executor)
        .expect("begin target transaction");
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: None::<&NoopCheckpointStore>,
        transaction_checkpoint_table: Some("cdc.stream_checkpoint"),
        transaction_checkpoint_name: Some("stream-binlog:test-source"),
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig::default(),
    };
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "INSERT INTO accounts VALUES (1)".to_string(),
    });
    let outcome = StructuredEventOutcome {
        policy: EventPolicy::CommitTransaction,
        resume_coordinate: Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 180,
        }),
    };

    let error = save_outcome_checkpoint(&executor, &mut context, &event, &outcome)
        .expect_err("concurrent checkpoint advance must block DML regression");

    assert!(error.to_string().contains("refusing checkpoint regression"));
    assert_eq!(executor.operations(), vec!["BEGIN", "LOCK_CHECKPOINT"]);
}

#[test]
fn source_query_mariadb_only_ddl_is_quarantined_without_checkpointing_past_it() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "ALTER TABLE accounts DROP COLUMN IF EXISTS handle".to_string(),
    });

    let error = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &event,
    )
    .expect_err("mariadb-only DDL should quarantine");

    assert!(error.to_string().contains("quarantined"));
    assert!(applier.executor().statements.borrow().is_empty());
}

#[test]
fn source_query_dml_is_rejected_as_row_full_contract_violation() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "INSERT INTO accounts (id, name) VALUES (999, 'query-event')".to_string(),
    });

    let error = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &event,
    )
    .expect_err("statement DML must not replay under ROW/FULL");

    assert!(error.to_string().contains("ROW/FULL contract violation"));
    assert!(applier.executor().statements.borrow().is_empty());
}
