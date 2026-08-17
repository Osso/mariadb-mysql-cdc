use super::*;
use crate::live::TargetMySqlConfig;

#[test]
fn source_binlog_contract_requires_row_and_full() {
    assert!(
        validate_source_binlog_settings(&crate::inventory::SourceBinlogSettings {
            format: "ROW".to_string(),
            row_image: "FULL".to_string(),
        })
        .is_ok()
    );
    assert!(
        validate_source_binlog_settings(&crate::inventory::SourceBinlogSettings {
            format: "MIXED".to_string(),
            row_image: "FULL".to_string(),
        })
        .is_err()
    );
    assert!(
        validate_source_binlog_settings(&crate::inventory::SourceBinlogSettings {
            format: "ROW".to_string(),
            row_image: "MINIMAL".to_string(),
        })
        .is_err()
    );
}

#[test]
fn bounded_stop_dispatches_event_that_ends_at_requested_position() {
    let decision = stop_position_decision(Some(180), &event_header(30, 180), false)
        .expect("event at stop boundary should be dispatchable");
    assert_eq!(decision, StopPositionDecision::DispatchAndStop);
}

#[test]
fn bounded_stop_rejects_event_that_would_exceed_requested_position() {
    let error = stop_position_decision(Some(179), &event_header(30, 180), false)
        .expect_err("event beyond stop boundary must not be dispatched");
    assert!(error.to_string().contains("falls inside event"));
}

#[test]
fn bounded_stop_rejects_position_between_events_as_unreachable() {
    let error = stop_position_decision(Some(150), &event_header(30, 180), false)
        .expect_err("stop position between events must fail explicitly");
    assert!(error.to_string().contains("cannot be reached"));
}

#[test]
fn bounded_stop_rejects_boundary_inside_open_row_transaction() {
    let error = stop_position_decision(Some(190), &event_header(30, 200), true)
        .expect_err("stop boundary inside open row transaction must fail");
    assert!(error.to_string().contains("transaction"));
}

#[test]
fn bounded_stop_requires_transaction_boundary_after_equal_row_event_end() {
    let decision = stop_position_decision(Some(180), &event_header(30, 180), false)
        .expect("equal row event end should be dispatchable");
    assert_eq!(decision, StopPositionDecision::DispatchAndStop);
    let error =
        bounded_stop_completion_error(true).expect_err("open row transaction must block success");
    assert!(error.to_string().contains("transaction"));
}

#[test]
fn unbounded_stream_dispatches_without_stop_completion() {
    let decision = stop_position_decision(None, &event_header(30, 180), true)
        .expect("unbounded stream must retain normal dispatch behavior");
    assert_eq!(decision, StopPositionDecision::Dispatch);
}

#[test]
fn bounded_completion_flushes_completed_grouped_target_work() {
    let executor = TransactionRecordingExecutor::default();
    let applier = crate::row::RowApplier::new(executor);
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let mut current_file = "mysqld-bin.000777".to_string();
    let mut transaction = TargetTransaction::default();
    transaction
        .begin_if_needed(applier.executor())
        .expect("begin grouped target transaction");
    transaction.record_source_transaction();
    let mut context = StreamEventContext {
        schema_resolver: &resolver,
        state: &mut state,
        target_transaction: &mut transaction,
        checkpoint_store: Some(&NoopCheckpointStore),
        transaction_checkpoint_table: None,
        transaction_checkpoint_name: None,
        current_file: &mut current_file,
        group_config: TargetTransactionGroupConfig {
            size: 8,
            timeout: Duration::from_secs(60),
        },
    };

    flush_grouped_transaction(applier.executor(), &mut context)
        .expect("bounded completion should flush grouped target work");

    assert_eq!(applier.executor().operations(), vec!["BEGIN", "COMMIT"]);
    assert!(!transaction.is_open());
}

#[test]
fn parallel_progress_advances_only_from_committed_checkpoints() {
    let mut progress = StreamProgress::new(BinlogCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 4,
    });
    let checkpoints = [180, 260]
        .into_iter()
        .map(|position| crate::checkpoint::Checkpoint {
            source_file: "mysqld-bin.000777".to_string(),
            source_position: position,
            gtid: None,
            event_timestamp: 0,
            last_event: crate::checkpoint::LastEvent {
                event_type: "XidEvent".to_string(),
                description: "committed parallel target transaction".to_string(),
            },
        })
        .collect();

    record_committed_target_progress(&mut progress, checkpoints);

    assert_eq!(progress.applied_statements, 2);
    assert_eq!(progress.last_coordinate.position, 260);
}

#[test]
fn source_inventory_uses_explicit_plaintext_without_ca() {
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            host: "source-db".to_string(),
            port: 3307,
            user: "cdc_reader".to_string(),
            password: "secret".to_string(),
            tls_ca_file: String::new(),
            ..SourceBinlogConfig::default()
        },
        ..ApplyBinlogConfig::default()
    };

    let inventory = source_inventory_config(&config);

    assert_eq!(inventory.endpoint_role, InventoryEndpointRole::Source);
    assert!(!inventory.use_tls);
    assert_eq!(inventory.tls_ca_file, None);
}

#[test]
fn target_inventory_keeps_tls_ca_for_verified_connection() {
    let config = ApplyBinlogConfig {
        target: TargetMySqlConfig {
            host: "target-mysql.internal.example".to_string(),
            port: 25060,
            user: "target_user".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            tls_ca_file: "/etc/mariadb-mysql-cdc/do-ca.pem".to_string(),
            ..TargetMySqlConfig::default()
        },
        ..ApplyBinlogConfig::default()
    };

    let inventory = target_inventory_config(&config);

    assert_eq!(inventory.endpoint_role, InventoryEndpointRole::Target);
    assert!(inventory.use_tls);
    assert_eq!(
        inventory.tls_ca_file.as_deref(),
        Some("/etc/mariadb-mysql-cdc/do-ca.pem")
    );
}

#[test]
fn builds_mysql_cdc_replica_options_from_source_position() {
    let source = SourceBinlogConfig {
        host: "10.0.0.2".to_string(),
        port: 3307,
        user: "cdc".to_string(),
        password: "secret".to_string(),
        database: Some("app".to_string()),
        binlog_file: "mysqld-bin.000777".to_string(),
        start_position: 12345,
        stop_never_slave_server_id: Some(4242),
        ..SourceBinlogConfig::default()
    };

    let options = replica_options_from_source(&source).expect("options");

    assert_eq!(options.hostname, "10.0.0.2");
    assert_eq!(options.port, 3307);
    assert_eq!(options.username, "cdc");
    assert_eq!(options.password, "secret");
    assert_eq!(options.ssl_mode, SslMode::Disabled);
    assert_eq!(options.ssl_ca_file, None);
    assert_eq!(options.database, Some("app".to_string()));
    assert_eq!(options.server_id, 4242);
    assert!(options.blocking);
    assert_eq!(options.binlog.filename, "mysqld-bin.000777");
    assert_eq!(options.binlog.position, 12345);
    assert_eq!(
        options.binlog.starting_strategy,
        StartingStrategy::FromPosition
    );
}

#[test]
fn mysql_cdc_dns_source_uses_explicit_plaintext_without_ca() {
    let source = SourceBinlogConfig {
        host: "db.internal.example".to_string(),
        tls_ca_file: String::new(),
        ..SourceBinlogConfig::default()
    };

    let options = replica_options_from_source(&source).expect("options");

    assert_eq!(options.ssl_mode, SslMode::Disabled);
    assert_eq!(options.ssl_ca_file, None);
}

#[test]
fn rejects_mysql_cdc_start_positions_that_do_not_fit_crate_api() {
    let source = SourceBinlogConfig {
        binlog_file: "mysqld-bin.000777".to_string(),
        start_position: u64::from(u32::MAX) + 1,
        ..SourceBinlogConfig::default()
    };

    let error = match replica_options_from_source(&source) {
        Ok(_) => panic!("expected overflow error"),
        Err(error) => error,
    };

    assert_eq!(
        error.to_string(),
        "start position 4294967296 exceeds mysql_cdc u32 position limit"
    );
}

#[test]
fn rotate_event_checkpoint_uses_structured_rotate_payload() {
    let event = BinlogEvent::RotateEvent(RotateEvent {
        binlog_filename: "mysqld-bin.000778".to_string(),
        binlog_position: 4,
    });
    let header = event_header(4, 0);

    let outcome = classify_event("mysqld-bin.000777", &header, &event);

    assert_eq!(outcome.policy, EventPolicy::Ignore);
    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000778".to_string(),
            position: 4,
        })
    );
}

#[test]
fn binlog_options_use_from_position_for_live_stream_start() {
    let options = binlog_options_from_source_position("mysqld-bin.000777".to_string(), 12345)
        .expect("binlog options");

    assert_eq!(options.filename, "mysqld-bin.000777");
    assert_eq!(options.position, 12345);
    assert_eq!(options.starting_strategy, StartingStrategy::FromPosition);
}
