use super::*;

#[test]
fn source_query_ddl_is_replayed_as_checkpointed_statement() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "CREATE TABLE now_applied (id int)".to_string(),
    });

    let outcome = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &event,
    )
    .expect("source DDL should replay");

    assert_eq!(outcome.policy, EventPolicy::CommitTransaction);
    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 180,
        })
    );
    assert_eq!(
        applier
            .executor()
            .statements
            .borrow()
            .iter()
            .map(|statement| statement.sql.clone())
            .collect::<Vec<_>>(),
        vec!["CREATE TABLE now_applied (id int)".to_string()]
    );
}

#[test]
fn supported_ddl_replays_without_manual_resolution() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
    let ledger = RecordingDdlLedger::default();
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
        sql_statement: "CREATE INDEX idx_home_feed_panel_candidates_filter ON home_feed_panel_candidates (filter_prompt_version)".to_string(),
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

    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    let outcome = handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &RecordingSemanticInventory::default(),
            ledger: &ledger,
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect("compatible DDL replay")
    .expect("automatic DDL outcome");

    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 180,
        })
    );
    assert_eq!(
        operations.borrow().as_slice(),
        &[
            "PREPARE",
            "EXEC",
            "APPLIED",
            "BEGIN",
            "LOCK_CHECKPOINT",
            "EXEC",
            "CHECKPOINT",
            "COMMIT",
        ]
    );
    assert!(ledger.recorded.borrow().is_empty());
}

#[test]
fn unsupported_index_ddl_is_manual_before_journal_prepare_or_execution() {
    for sql in [
        "CREATE UNIQUE INDEX idx_handle ON accounts (handle)",
        "CREATE INDEX idx_handle ON accounts (handle) USING HASH",
        "CREATE INDEX idx_handle ON accounts (handle) INVISIBLE",
        "DROP INDEX IF EXISTS idx_handle ON accounts",
        "CREATE INDEX idx_handle ON other_db.accounts (handle)",
        "CREATE INDEX idx_handle ON other_db . accounts (handle)",
        "CREATE INDEX idx_handle ON other_db /* comment */ . accounts (handle)",
        "CREATE INDEX idx_handle ON other_db. /* comment */ accounts (handle)",
        "CREATE INDEX `idx_handle` ON `other_db`/**/.`accounts` (`handle`)",
        "CREATE INDEX \"idx_handle\" ON \"accounts\" (\"handle\")",
        "CREATE INDEX other_db.idx_handle ON accounts (handle)",
        "CREATE INDEX idx_handle ON accounts (handle), idx_other ON accounts (id)",
        "CREATE INDEX idx_handle ON accounts ((lower(email)))",
        "CREATE INDEX idx_handle ON accounts (handle",
    ] {
        let event = BinlogEvent::QueryEvent(QueryEvent {
            thread_id: 1,
            duration: 0,
            error_code: 0,
            status_variables: Vec::new(),
            database_name: "fixture_cdc".to_string(),
            sql_statement: sql.to_string(),
        });
        let state = StructuredEventState::new(Some("fixture_cdc".to_string()));
        assert!(
            automatically_handled_ddl_event(
                "production-source",
                "mysqld-bin.000777",
                &event_header(2, 180),
                &event,
                &state,
            )
            .is_none(),
            "unsupported DDL entered automatic admission: {sql}"
        );
        assert!(
            manual_ddl_event(
                "production-source",
                "mysqld-bin.000777",
                &event_header(2, 180),
                &event,
                &state,
            )
            .is_some(),
            "unsupported DDL did not route to manual ledger: {sql}"
        );
    }
}

#[test]
fn applied_only_restart_finalizes_journal_and_checkpoint_atomically_without_replay() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
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
        sql_statement: "DROP INDEX idx_old_accounts ON accounts".to_string(),
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
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    *journal.status.borrow_mut() = Some(DdlReplayStatus::Applied);

    handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &RecordingSemanticInventory::default(),
            ledger: &RecordingDdlLedger::default(),
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect("applied-only restart should finalize")
    .expect("automatic DDL outcome");

    assert_eq!(
        operations.borrow().as_slice(),
        &["BEGIN", "LOCK_CHECKPOINT", "EXEC", "CHECKPOINT", "COMMIT",]
    );
}

#[test]
fn prepared_restart_with_proven_post_state_finalizes_without_replay() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
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
        sql_statement: "CREATE INDEX idx_accounts_handle ON accounts (handle)".to_string(),
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
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    *journal.status.borrow_mut() = Some(DdlReplayStatus::Prepared);
    *journal.evidence.borrow_mut() = Some(RecordingSemanticInventory::default().evidence);

    handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &RecordingSemanticInventory::default(),
            ledger: &RecordingDdlLedger::default(),
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect("prepared semantic recovery")
    .expect("automatic DDL outcome");

    assert_eq!(
        operations.borrow().as_slice(),
        &[
            "APPLIED",
            "BEGIN",
            "LOCK_CHECKPOINT",
            "EXEC",
            "CHECKPOINT",
            "COMMIT",
        ]
    );
}

#[test]
fn prepared_restart_with_pre_state_blocks_without_replay_or_checkpoint() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
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
        sql_statement: "CREATE INDEX idx_accounts_handle ON accounts (handle)".to_string(),
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
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    *journal.status.borrow_mut() = Some(DdlReplayStatus::Prepared);
    *journal.evidence.borrow_mut() = Some(RecordingSemanticInventory::default().evidence);
    let semantic_inventory = RecordingSemanticInventory {
        observed_state: "external-drift".to_string(),
        ..RecordingSemanticInventory::default()
    };

    let error = handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &semantic_inventory,
            ledger: &RecordingDdlLedger::default(),
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect_err("ambiguous prepared state must block");

    assert!(
        error
            .to_string()
            .contains("semantic reconciliation blocked")
    );
    assert!(
        error
            .to_string()
            .contains("neither immutable pre-state nor expected post-state")
    );
    assert_eq!(operations.borrow().as_slice(), &["BLOCKED"]);
}

#[test]
fn automatic_ddl_checkpoint_predecessor_mismatch_rolls_back_before_journal_transition() {
    let executor =
        TransactionRecordingExecutor::with_locked_checkpoint(crate::checkpoint::Checkpoint {
            source_file: "mysqld-bin.000777".to_string(),
            source_position: 200,
            gtid: None,
            event_timestamp: 0,
            last_event: crate::checkpoint::LastEvent {
                event_type: "Rows".to_string(),
                description: "wrong predecessor".to_string(),
            },
        });
    let mut applier = crate::row::RowApplier::new(executor);
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
        sql_statement: "CREATE INDEX idx_accounts_handle ON accounts (handle)".to_string(),
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
    let journal = RecordingDdlReplayJournal::default();
    *journal.status.borrow_mut() = Some(DdlReplayStatus::Applied);

    let error = handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &RecordingSemanticInventory::default(),
            ledger: &RecordingDdlLedger::default(),
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect_err("mismatched predecessor must block finalization");

    assert!(error.to_string().contains("predecessor mismatch"));
    assert_eq!(
        applier.executor().operations(),
        vec!["BEGIN", "LOCK_CHECKPOINT", "ROLLBACK"]
    );
}

#[test]
fn event_position_evidence_failure_routes_ddl_to_manual_ledger() {
    let executor = TransactionRecordingExecutor::default();
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::default();
    let ledger = RecordingDdlLedger::default();
    let semantic_inventory = RecordingSemanticInventory {
        capture_error: Some(
            "source semantic inventory is not event-position consistent".to_string(),
        ),
        ..RecordingSemanticInventory::default()
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
        sql_statement: "CREATE INDEX idx_accounts_handle ON accounts (handle)".to_string(),
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

    let error = handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &semantic_inventory,
            ledger: &ledger,
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect_err("unfenced source inventory must route manual");

    assert!(error.to_string().contains("manual DDL resolution required"));
    assert_eq!(ledger.recorded.borrow().len(), 1);
    assert!(journal.operations.borrow().is_empty());
    assert!(applier.executor().operations().is_empty());
}

#[test]
fn failed_supported_ddl_replay_does_not_checkpoint() {
    let executor = TransactionRecordingExecutor::failing();
    let mut applier = crate::row::RowApplier::new(executor);
    let ledger = RecordingDdlLedger::default();
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
        sql_statement: "CREATE INDEX idx_accounts_handle ON accounts (handle)".to_string(),
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

    let journal = RecordingDdlReplayJournal::default();
    let error = handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &RecordingSemanticInventory::default(),
            ledger: &ledger,
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect_err("target DDL failure must stop replay");

    assert!(error.to_string().contains("failed to replay statement"));
    assert_eq!(applier.executor().operations(), vec!["EXEC"]);
    assert_eq!(journal.operations.borrow().as_slice(), &["PREPARE"]);
    assert_eq!(*journal.status.borrow(), Some(DdlReplayStatus::Prepared));
    assert!(ledger.recorded.borrow().is_empty());
}

#[test]
fn qualified_ddl_with_different_default_database_still_requires_manual_resolution() {
    let state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "other_db".to_string(),
        sql_statement: "ALTER TABLE fixture_cdc . accounts ADD COLUMN handle varchar(64)"
            .to_string(),
    });

    let manual = manual_ddl_event(
        "production-source",
        "mysqld-bin.000777",
        &event_header(2, 180),
        &event,
        &state,
    );

    assert!(manual.is_some());
}

#[test]
fn mariadb_only_ddl_requires_manual_resolution() {
    let state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "CREATE SEQUENCE invoice_numbers".to_string(),
    });

    let manual = manual_ddl_event(
        "production-source",
        "mysqld-bin.000777",
        &event_header(2, 180),
        &event,
        &state,
    );

    assert!(manual.is_some());
}

#[test]
fn transactional_stream_records_ddl_pending_without_executing_or_checkpointing() {
    let executor = TransactionRecordingExecutor::default();
    let ledger = RecordingDdlLedger::default();
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
    .expect_err("unresolved DDL must stop");

    assert!(error.to_string().contains("manual DDL resolution required"));
    assert!(executor.operations().is_empty());
    let recorded = ledger.recorded.borrow();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].source_identity,
        "production-source#server-id=1"
    );
    assert_eq!(recorded[0].event_start_position, 161);
    assert_eq!(recorded[0].event_end_position, 180);
}
