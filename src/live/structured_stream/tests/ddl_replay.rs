use super::*;

#[test]
fn admitted_source_only_release_move_procedures_bypass_qualification_rejection() {
    for source_sql in [
        include_str!("../../../../fixtures/ddl/create-apply-release-move-purchase-repair.sql"),
        include_str!("../../../../fixtures/ddl/create-apply-release-move-purchase-repair-95.sql"),
    ] {
        let event = BinlogEvent::QueryEvent(QueryEvent {
            thread_id: 1,
            duration: 0,
            error_code: 0,
            status_variables: Vec::new(),
            database_name: "fixture_cdc".to_string(),
            sql_statement: source_sql.trim_end().to_string(),
        });
        let state = StructuredEventState::new(Some("fixture_cdc".to_string()));

        assert!(
            automatically_handled_ddl_event(
                "production-source",
                "mysqld-bin.000777",
                &event_header(2, 777),
                &event,
                &state,
            )
            .is_some(),
            "source-only procedure variant must enter automatic replay"
        );
    }
}

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
fn supported_ddl_replays_without_translation_barrier() {
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
}

#[test]
fn production_add_column_ddl_is_admitted_by_live_stream() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
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
        sql_statement: "ALTER TABLE `home_feed_panel_candidates` ADD COLUMN `filter_prompt_version` VARCHAR(64) DEFAULT NULL COMMENT 'sanitized description' AFTER `filter_reason`".to_string(),
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

    let outcome = handle_ddl_event(
        &mut applier,
        &journal,
        &RecordingSemanticInventory::default(),
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect("production ADD COLUMN must enter automatic replay")
    .expect("DDL outcome");

    assert_eq!(
        outcome.resume_coordinate.map(|value| value.position),
        Some(180)
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
}

#[test]
fn production_float_unsigned_add_column_is_admitted_by_live_stream() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut current_file = "mysqld-bin.002769".to_string();
    let mut transaction = TargetTransaction::default();
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "globalcomix".to_string(),
        sql_statement: "ALTER TABLE `comics_top_stats`\n    ADD COLUMN `value_1_day` FLOAT UNSIGNED NOT NULL DEFAULT 0 AFTER `statistic`"
            .to_string(),
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

    let outcome = handle_ddl_event(
        &mut applier,
        &journal,
        &RecordingSemanticInventory::default(),
        "globalcomix-prod-mariadb-2026-01",
        &mut context,
        &event_header(3, 329601175),
        &event,
    )
    .expect("FLOAT UNSIGNED ADD COLUMN must enter automatic replay")
    .expect("DDL outcome");

    assert_eq!(
        outcome.resume_coordinate.map(|value| value.position),
        Some(329601175)
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
}

#[test]
fn exact_drop_trigger_replays_and_checkpoints_after_normal_journal_proof() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "globalcomix".to_string(),
        sql_statement: "DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives".to_string(),
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

    let outcome = handle_ddl_event(
        &mut applier,
        &journal,
        &RecordingSemanticInventory::default(),
        "source-1",
        &mut context,
        &event_header(4, 200),
        &event,
    )
    .expect("exact DROP TRIGGER must not create a durable translation barrier")
    .expect("automatic DROP TRIGGER outcome");

    assert_eq!(
        outcome.resume_coordinate.map(|value| value.position),
        Some(200)
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
}

#[test]
fn mariadb_rename_column_if_exists_executes_generated_mysql8_sql() {
    let executor = TransactionRecordingExecutor::failing();
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
        sql_statement: "ALTER TABLE home_feed_captions RENAME COLUMN IF EXISTS arc_start_order TO deprecated_arc_start_order".to_string(),
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
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect_err("forced target failure must expose generated SQL");

    let message = error.to_string();
    assert!(message.contains("ALTER TABLE `home_feed_captions` RENAME COLUMN `arc_start_order` TO `deprecated_arc_start_order`"), "{message}");
    assert!(
        !message.contains("IF EXISTS"),
        "source MariaDB SQL reached target: {message}"
    );
    assert_eq!(*journal.status.borrow(), Some(DdlReplayStatus::Prepared));
}

#[test]
fn unsupported_create_table_stays_translation_pending_without_target_or_checkpoint_execution() {
    let executor = TransactionRecordingExecutor::default();
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::default();
    let semantic_inventory = RecordingSemanticInventory {
        use_live_transform: true,
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
        sql_statement: "CREATE TABLE accounts (\
            id BIGINT NOT NULL PRIMARY KEY, \
            email VARCHAR(255) NOT NULL, \
            payload VARCHAR(64) NOT NULL, \
            created_at DATETIME NOT NULL, \
            KEY idx_accounts_payload (payload)\
        ) ENGINE=InnoDB"
            .to_string(),
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

    let error = handle_ddl_event(
        &mut applier,
        &journal,
        &semantic_inventory,
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect_err("unsupported CREATE TABLE must remain translation-pending");

    assert!(
        error
            .to_string()
            .to_ascii_lowercase()
            .contains("translator unavailable")
    );
    assert_eq!(
        *journal.status.borrow(),
        Some(DdlReplayStatus::TranslationPending)
    );
    assert_eq!(
        journal.operations.borrow().as_slice(),
        &["TRANSLATION_PENDING"]
    );
    assert!(applier.executor().operations().is_empty());
}

struct DurableBlockedCheckpointStore {
    checkpoint: crate::checkpoint::Checkpoint,
    saved: RefCell<Option<crate::checkpoint::Checkpoint>>,
}

impl StreamCheckpointStore for DurableBlockedCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<crate::checkpoint::Checkpoint>, ApplyBinlogError> {
        Ok(Some(self.checkpoint.clone()))
    }

    fn save_checkpoint(
        &self,
        checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), ApplyBinlogError> {
        self.saved.replace(Some(checkpoint.clone()));
        Ok(())
    }
}

#[test]
fn unsupported_ddl_keeps_replicator_alive_at_unchanged_checkpoint() {
    let checkpoint_store = DurableBlockedCheckpointStore {
        checkpoint: crate::checkpoint::Checkpoint {
            source_file: "mysqld-bin.000777".to_string(),
            source_position: 100,
            gtid: None,
            event_timestamp: 0,
            last_event: crate::checkpoint::LastEvent {
                event_type: "QueryEvent".to_string(),
                description: "before unsupported DDL".to_string(),
            },
        },
        saved: RefCell::new(None),
    };
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            binlog_file: "mysqld-bin.000777".to_string(),
            start_position: 100,
            ..SourceBinlogConfig::default()
        },
        max_reconnects: 0,
        reconnect_forever: false,
        ..ApplyBinlogConfig::default()
    };
    let journal = RecordingDdlReplayJournal::default();
    let semantic_inventory = RecordingSemanticInventory {
        use_live_transform: true,
        ..RecordingSemanticInventory::default()
    };
    let starts = RefCell::new(Vec::new());
    let attempts = std::cell::Cell::new(0);

    crate::live::reconnect::run_stream_reconnect_loop(
        &config,
        Some(&checkpoint_store),
        |attempt_config| {
            starts
                .borrow_mut()
                .push(attempt_config.source.start_position);
            attempts.set(attempts.get() + 1);
            if attempts.get() > 1 {
                return Ok(());
            }

            let mut applier = crate::row::RowApplier::new(TransactionRecordingExecutor::default());
            let resolver = FixtureSchemaResolver;
            let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
            let mut current_file = attempt_config.source.binlog_file.clone();
            let mut transaction = TargetTransaction::default();
            let event = BinlogEvent::QueryEvent(QueryEvent {
                thread_id: 1,
                duration: 0,
                error_code: 0,
                status_variables: Vec::new(),
                database_name: "fixture_cdc".to_string(),
                sql_statement: "CREATE TABLE accounts (\
                    id BIGINT NOT NULL PRIMARY KEY, \
                    email VARCHAR(255) NOT NULL, \
                    payload VARCHAR(64) NOT NULL, \
                    created_at DATETIME NOT NULL, \
                    KEY idx_accounts_payload (payload)\
                ) ENGINE=InnoDB"
                    .to_string(),
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

            let error = handle_ddl_event(
                &mut applier,
                &journal,
                &semantic_inventory,
                "production-source",
                &mut context,
                &event_header(2, 180),
                &event,
            )
            .expect_err("unsupported CREATE TABLE must block durably");

            assert!(applier.executor().operations().is_empty());
            Err(error)
        },
        |_| {},
    )
    .expect("durably blocked DDL must keep the replicator process alive");

    assert_eq!(starts.into_inner(), vec![100, 100]);
    assert_eq!(attempts.get(), 2);
    assert_eq!(
        *journal.status.borrow(),
        Some(DdlReplayStatus::TranslationPending)
    );
    assert!(checkpoint_store.saved.borrow().is_none());
}

#[test]
fn production_create_table_replays_existing_translation_pending_barrier() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    *journal.status.borrow_mut() = Some(DdlReplayStatus::TranslationPending);
    let semantic_inventory = RecordingSemanticInventory {
        use_live_transform: true,
        ..RecordingSemanticInventory::default()
    };
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("globalcomix".to_string()));
    let mut current_file = "mysqld-bin.002768".to_string();
    let mut transaction = TargetTransaction::default();
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "globalcomix".to_string(),
        sql_statement: "CREATE TABLE IF NOT EXISTS `assistant_reply_reports` (\n    `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n    `contact_form_id` int(11) UNSIGNED NOT NULL,\n    `reason` varchar(32) NOT NULL COMMENT 'inaccurate | offensive | sexual_content | other',\n    `reported_reply_index` smallint(5) UNSIGNED NOT NULL\n        COMMENT 'Zero-based index into conversation.messages of the reported assistant turn',\n    `conversation` mediumtext NOT NULL\n        COMMENT 'Slim message JSON as sent to /v1/assistant: ordered role + blocks, card blocks collapsed to entity ids',\n    `is_active` tinyint(1) UNSIGNED NOT NULL DEFAULT 1,\n    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n    `creator_id` int(11) UNSIGNED NOT NULL,\n    UNIQUE KEY `uk_contact_form` (`contact_form_id`),\n    KEY `idx_reason_create_time` (`reason`, `create_time`),\n    CONSTRAINT `fk_assistant_reply_reports_contact_form_id`\n        FOREIGN KEY (`contact_form_id`) REFERENCES `contact_forms` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
            .to_string(),
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

    let outcome = handle_ddl_event(
        &mut applier,
        &journal,
        &semantic_inventory,
        "globalcomix-prod-mariadb-2026-01",
        &mut context,
        &event_header(3, 1019084595),
        &event,
    )
    .expect("production CREATE TABLE replay")
    .expect("production CREATE TABLE outcome");

    assert_eq!(outcome.policy, EventPolicy::CommitTransaction);
    assert_eq!(
        operations.borrow().as_slice(),
        &[
            "PROMOTE",
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
fn altered_assistant_reply_reports_create_remains_translation_pending() {
    let state = StructuredEventState::new(Some("globalcomix".to_string()));
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "globalcomix".to_string(),
        sql_statement: "CREATE TABLE IF NOT EXISTS `assistant_reply_reports` (`id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci".to_string(),
    });

    assert!(
        automatically_handled_ddl_event(
            "globalcomix-prod-mariadb-2026-01",
            "mysqld-bin.002768",
            &event_header(3, 1019084595),
            &event,
            &state,
        )
        .is_none()
    );
    assert!(
        manual_ddl_event(
            "globalcomix-prod-mariadb-2026-01",
            "mysqld-bin.002768",
            &event_header(3, 1019084595),
            &event,
            &state,
        )
        .is_some()
    );
}

#[test]
fn fixture_create_table_executes_evidence_sql_and_checkpoints_once() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    let semantic_inventory = RecordingSemanticInventory {
        absent_target_create_evidence: true,
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
        sql_statement: "CREATE TABLE accounts (\
            id BIGINT NOT NULL PRIMARY KEY, \
            email VARCHAR(255) NOT NULL, \
            payload VARCHAR(64) NOT NULL, \
            KEY idx_accounts_payload (payload)\
        ) ENGINE=InnoDB"
            .to_string(),
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

    let outcome = handle_ddl_event(
        &mut applier,
        &journal,
        &semantic_inventory,
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect("fixture CREATE TABLE replay")
    .expect("fixture CREATE TABLE outcome");

    assert_eq!(outcome.policy, EventPolicy::CommitTransaction);
    assert_eq!(
        journal
            .evidence
            .borrow()
            .as_ref()
            .and_then(|evidence| evidence.generated_sql.as_deref()),
        Some(
            "CREATE TABLE `accounts` (`id` BIGINT NOT NULL, `email` VARCHAR(255) NOT NULL, `payload` VARCHAR(64) NOT NULL, PRIMARY KEY (`id`), KEY `idx_accounts_payload` (`payload`)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
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
            "COMMIT"
        ]
    );
}

#[test]
fn fixture_create_table_rejects_present_target_without_execution_or_checkpoint() {
    let executor = TransactionRecordingExecutor::default();
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::default();
    let semantic_inventory = RecordingSemanticInventory {
        present_target_create_evidence: true,
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
        sql_statement: "CREATE TABLE accounts (\
            id BIGINT NOT NULL PRIMARY KEY, \
            email VARCHAR(255) NOT NULL, \
            payload VARCHAR(64) NOT NULL, \
            KEY idx_accounts_payload (payload)\
        ) ENGINE=InnoDB"
            .to_string(),
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

    let header = event_header(2, 180);
    let BinlogEvent::QueryEvent(query) = &event else {
        unreachable!("fixture CREATE TABLE query event");
    };
    let ddl_event = ddl_event("production-source", "mysqld-bin.000777", &header, query);
    let error = prepare_and_execute_automatic_ddl(
        &mut applier,
        &journal,
        &semantic_inventory,
        AutomaticDdlInput {
            context: &mut context,
            header: &header,
            event: &event,
        },
        &ddl_event,
    )
    .expect_err("present target must reject CREATE TABLE evidence");

    assert!(error.to_string().contains("already exists"), "{error}");
    assert_eq!(
        *journal.status.borrow(),
        Some(DdlReplayStatus::TranslationPending)
    );
    assert_eq!(
        journal.operations.borrow().as_slice(),
        &["TRANSLATION_PENDING"]
    );
    assert!(applier.executor().operations().is_empty());
}

#[test]
fn unsupported_ddl_persists_barrier_then_replays_after_translator_upgrade() {
    let operations = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let executor = TransactionRecordingExecutor::with_operations(operations.clone());
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::with_operations(operations.clone());
    let semantic_inventory = RecordingSemanticInventory::default();
    semantic_inventory.translator_available.set(false);
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
        sql_statement: "ALTER TABLE home_feed_captions RENAME COLUMN IF EXISTS arc_start_order TO deprecated_arc_start_order".to_string(),
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

    let first_error = handle_ddl_event(
        &mut applier,
        &journal,
        &semantic_inventory,
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect_err("missing translator must block the checkpoint");

    assert!(
        first_error
            .to_string()
            .contains("translator implementation unavailable")
    );
    assert_eq!(
        *journal.status.borrow(),
        Some(DdlReplayStatus::TranslationPending)
    );
    assert_eq!(operations.borrow().as_slice(), &["TRANSLATION_PENDING"]);

    semantic_inventory.translator_available.set(true);
    let outcome = handle_ddl_event(
        &mut applier,
        &journal,
        &semantic_inventory,
        "production-source",
        &mut context,
        &event_header(2, 180),
        &event,
    )
    .expect("translator upgrade must replay automatically")
    .expect("DDL event outcome");

    assert_eq!(
        outcome
            .resume_coordinate
            .as_ref()
            .map(|value| value.position),
        Some(180)
    );
    assert_eq!(
        operations.borrow().as_slice(),
        &[
            "TRANSLATION_PENDING",
            "PROMOTE",
            "EXEC",
            "APPLIED",
            "BEGIN",
            "LOCK_CHECKPOINT",
            "EXEC",
            "CHECKPOINT",
            "COMMIT",
        ]
    );
    assert_eq!(*journal.status.borrow(), Some(DdlReplayStatus::Applied));
}

#[test]
fn unsupported_index_ddl_is_manual_before_journal_prepare_or_execution() {
    for sql in [
        "CREATE UNIQUE INDEX idx_handle ON accounts (handle)",
        "CREATE INDEX idx_handle ON accounts (handle) USING HASH",
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
fn blocked_create_with_matching_current_post_state_recovers_without_replay() {
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
        sql_statement: "CREATE TABLE accounts (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB"
            .to_string(),
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
    *journal.status.borrow_mut() = Some(DdlReplayStatus::Blocked);
    *journal.evidence.borrow_mut() = Some(RecordingSemanticInventory::default().evidence);

    handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &RecordingSemanticInventory::default(),
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect("matching blocked CREATE recovery")
    .expect("automatic DDL outcome");

    assert_eq!(
        operations.borrow().as_slice(),
        &[
            "RECOVER_BLOCKED",
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
fn event_position_evidence_failure_persists_translation_pending_barrier() {
    let executor = TransactionRecordingExecutor::default();
    let mut applier = crate::row::RowApplier::new(executor);
    let journal = RecordingDdlReplayJournal::default();
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
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect_err("unfenced source inventory must block checkpoint advancement");

    assert!(
        error
            .to_string()
            .contains("DDL transformation evidence unavailable")
    );
    assert_eq!(
        *journal.status.borrow(),
        Some(DdlReplayStatus::TranslationPending)
    );
    assert_eq!(
        journal.operations.borrow().as_slice(),
        &["TRANSLATION_PENDING"]
    );
    assert!(applier.executor().operations().is_empty());
}

#[test]
fn failed_supported_ddl_replay_does_not_checkpoint() {
    let executor = TransactionRecordingExecutor::failing();
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
    let error = handle_automatic_ddl_event(
        &mut applier,
        AutomaticDdlDependencies {
            journal: &journal,
            semantic_inventory: &RecordingSemanticInventory::default(),
            source_identity: "production-source",
        },
        AutomaticDdlInput {
            context: &mut context,
            header: &event_header(2, 180),
            event: &event,
        },
    )
    .expect_err("target DDL failure must stop replay");

    assert!(error.to_string().contains("failed transformed DDL"));
    assert_eq!(applier.executor().operations(), vec!["EXEC"]);
    assert_eq!(journal.operations.borrow().as_slice(), &["PREPARE"]);
    assert_eq!(*journal.status.borrow(), Some(DdlReplayStatus::Prepared));
}

#[test]
fn qualified_ddl_with_different_default_database_routes_to_translation_pending() {
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
fn mariadb_only_ddl_routes_to_translation_pending() {
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
