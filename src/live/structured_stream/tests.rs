use super::*;
use mysql::Value;
use mysql_cdc::binlog_reader::BinlogReader;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::events::query_event::QueryEvent;
use mysql_cdc::events::rotate_event::RotateEvent;
use mysql_cdc::events::row_events::row_data::{RowData, UpdateRowData};
use mysql_cdc::events::row_events::update_rows_event::UpdateRowsEvent as MysqlCdcUpdateRowsEvent;
use mysql_cdc::events::row_events::write_rows_event::WriteRowsEvent as MysqlCdcWriteRowsEvent;
use mysql_cdc::events::rows_query_event::RowsQueryEvent;
use mysql_cdc::events::xid_event::XidEvent;
use mysql_cdc::starting_strategy::StartingStrategy;
use std::fs::File;

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
fn source_query_ddl_is_quarantined_without_checkpointing_past_it() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "CREATE TABLE should_not_be_applied (id int)".to_string(),
    });

    let error = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &event,
    )
    .expect_err("source DDL should quarantine");

    assert!(error.to_string().contains("quarantined"));
    assert!(applier.executor().statements.borrow().is_empty());
}

#[test]
fn source_query_dml_is_applied_before_transaction_checkpoint() {
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

    let outcome = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &event,
    )
    .expect("source DML should apply");

    assert_eq!(outcome.policy, EventPolicy::ApplyRows);
    assert_eq!(outcome.resume_coordinate, None);
    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(
        statements[0].sql,
        "INSERT INTO accounts (id, name) VALUES (999, 'query-event')"
    );
}

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
fn wraps_target_writes_in_source_xid_transaction() {
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
                checkpoint_store: None::<&NoopCheckpointStore>,
                current_file: &mut current_file,
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
        applier.executor().operations.borrow().as_slice(),
        ["BEGIN", "EXEC", "COMMIT"]
    );
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
                current_file: &mut current_file,
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
        applier.executor().operations.borrow().as_slice(),
        ["BEGIN", "EXEC", "ROLLBACK"]
    );
    assert!(!transaction.is_open());
}

#[test]
fn annotation_rows_query_events_are_ignored_even_when_text_starts_with_sql() {
    let event = BinlogEvent::RowsQueryEvent(RowsQueryEvent {
        query: "INSERT INTO email_history VALUES ('annotation only')".to_string(),
    });
    let header = event_header(160, 240);

    let outcome = classify_event("mysqld-bin.000777", &header, &event);

    assert_eq!(outcome.policy, EventPolicy::IgnoreAnnotation);
    assert_eq!(outcome.resume_coordinate, None);
}

#[test]
fn signed_integer_detection_uses_inventory_type_and_unsigned_marker() {
    assert!(is_signed_integer_column("smallint", "smallint(6)"));
    assert!(is_signed_integer_column("bigint", "bigint(20)"));
    assert!(!is_signed_integer_column(
        "smallint",
        "smallint(6) unsigned"
    ));
    assert!(!is_signed_integer_column("int", "INT(11) UNSIGNED"));
    assert!(!is_signed_integer_column("varchar", "varchar(255)"));
}

#[test]
fn metadata_table_map_supplies_column_names_and_primary_keys() {
    let resolver = EmptySchemaResolver;
    let table_map = MysqlCdcTableMapEvent {
        table_id: 77,
        database_name: "app".to_string(),
        table_name: "accounts".to_string(),
        column_types: vec![3, 253],
        column_metadata: vec![0, 64],
        null_bitmap: vec![false, true],
        table_metadata: Some(TableMetadata {
            signedness: None,
            default_charset: None,
            column_charsets: None,
            column_names: Some(vec!["id".to_string(), "name".to_string()]),
            set_string_values: None,
            enum_string_values: None,
            geometry_types: None,
            simple_primary_keys: Some(vec![0]),
            primary_keys_with_prefix: None,
            enum_and_set_default_charset: None,
            enum_and_set_column_charsets: None,
            column_visibility: None,
        }),
    };

    let mapped = map_table_map_event(&stream_coordinate(100), &table_map, &resolver)
        .expect("map table metadata");

    assert_eq!(mapped.table.table_id, 77);
    assert_eq!(mapped.table.columns, vec!["id", "name"]);
    assert_eq!(mapped.table.primary_key, vec!["id"]);
}

#[test]
fn metadata_table_map_uses_inventory_enum_values_when_metadata_omits_them() {
    let resolver = ReleasesSchemaResolver;
    let table_map = MysqlCdcTableMapEvent {
        table_id: 78,
        database_name: "app".to_string(),
        table_name: "releases".to_string(),
        column_types: vec![3, MYSQL_COLUMN_TYPE_ENUM],
        column_metadata: vec![0, 1],
        null_bitmap: vec![false, true],
        table_metadata: Some(TableMetadata {
            signedness: None,
            default_charset: None,
            column_charsets: None,
            column_names: Some(vec!["id".to_string(), "public_time_delta".to_string()]),
            set_string_values: None,
            enum_string_values: None,
            geometry_types: None,
            simple_primary_keys: Some(vec![0]),
            primary_keys_with_prefix: None,
            enum_and_set_default_charset: None,
            enum_and_set_column_charsets: None,
            column_visibility: None,
        }),
    };

    let mapped = map_table_map_event(&stream_coordinate(100), &table_map, &resolver)
        .expect("map table metadata");

    assert_eq!(
        mapped.table.enum_columns.get("public_time_delta"),
        Some(&vec!["1".to_string(), "2".to_string(), "14".to_string()])
    );
}

#[test]
fn parses_enum_values_from_inventory_column_type() {
    assert_eq!(
        parse_enum_column_type("enum('1','2','14')"),
        Some(vec!["1".to_string(), "2".to_string(), "14".to_string()])
    );
    assert_eq!(
        parse_enum_column_type("enum('can''t','back\\\\slash')"),
        Some(vec!["can't".to_string(), "back\\slash".to_string()])
    );
}

#[test]
fn fixture_row_events_apply_through_row_applier_with_schema_resolver() {
    let events = fixture_events("fixtures/mixed-binlog/mysql-bin.000001");
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));

    for (header, event) in &events {
        if matches!(event, BinlogEvent::QueryEvent(_)) {
            continue;
        }
        handle_structured_event(
            &mut applier,
            &resolver,
            &mut state,
            "mysql-bin.000001",
            header,
            event,
        )
        .expect("handle fixture event");
    }

    let statements = applier.executor().statements.borrow();
    assert!(statements.iter().any(|statement| {
        statement.sql == "UPDATE `accounts` SET `balance` = ?, `note` = ? WHERE `id` = ?"
            && statement.params == vec![Value::UInt(125), bytes("row update"), Value::UInt(1)]
    }));
    assert!(statements.iter().any(|statement| {
        statement.sql == "DELETE FROM `accounts` WHERE `id` = ?"
            && statement.params == vec![Value::UInt(2)]
    }));
    assert!(statements.iter().any(|statement| {
        statement
            .sql
            .starts_with("INSERT INTO `accounts` (`id`, `name`, `balance`, `note`, `created_at`)")
            && statement.params
                == vec![
                    Value::UInt(3),
                    bytes("gamma"),
                    Value::UInt(300),
                    bytes("row insert"),
                    bytes("2026-06-21 20:58:55"),
                ]
    }));
}

#[test]
fn non_source_schema_table_maps_and_rows_are_ignored_without_target_apply() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let table_map = MysqlCdcTableMapEvent {
        table_id: 99,
        database_name: "mysql".to_string(),
        table_name: "accounts".to_string(),
        column_types: vec![3; 5],
        column_metadata: vec![0; 5],
        null_bitmap: vec![false; 5],
        table_metadata: None,
    };
    let write = MysqlCdcWriteRowsEvent {
        table_id: 99,
        flags: 0,
        columns_number: 5,
        columns_present: vec![true; 5],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(999)),
            Some(MySqlValue::String("system".to_string())),
            Some(MySqlValue::Int(1)),
            Some(MySqlValue::String("ignored".to_string())),
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
    };

    let table_outcome = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysql-bin.000001",
        &event_header(19, 100),
        &BinlogEvent::TableMapEvent(table_map),
    )
    .expect("ignore non-source table map");
    let rows_outcome = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysql-bin.000001",
        &event_header(30, 120),
        &BinlogEvent::WriteRowsEvent(write),
    )
    .expect("ignore non-source rows");

    assert_eq!(table_outcome.policy, EventPolicy::Ignore);
    assert_eq!(rows_outcome.policy, EventPolicy::Ignore);
    assert!(applier.executor().statements.borrow().is_empty());
}

#[test]
fn structured_rows_preserve_null_and_blob_values_as_mysql_params() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let table_map = BinlogEvent::TableMapEvent(accounts_table_map_event(6));
    let write = BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 6,
        columns_present: vec![true; 6],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(7)),
            None,
            Some(MySqlValue::Blob(vec![0, 159, 146, 150, 255])),
            Some(MySqlValue::Blob(b"uuid-bytes".to_vec())),
            Some(MySqlValue::DateTime(DateTime {
                year: 2026,
                month: 6,
                day: 22,
                hour: 12,
                minute: 3,
                second: 4,
                millis: 0,
            })),
            Some(MySqlValue::String("active".to_string())),
        ])],
    });

    for event in [&table_map, &write] {
        handle_structured_event(
            &mut applier,
            &resolver,
            &mut state,
            "mysql-bin.000001",
            &event_header(99, 120),
            event,
        )
        .expect("apply typed row event");
    }

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(statements[0].params[0], Value::UInt(7));
    assert_eq!(statements[0].params[1], Value::NULL);
    assert_eq!(
        statements[0].params[2],
        Value::Bytes(vec![0, 159, 146, 150, 255])
    );
    assert_eq!(
        statements[0].params[3],
        Value::Bytes(b"uuid-bytes".to_vec())
    );
}

#[test]
fn query_dml_applies_insert_id_intvar_before_statement() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let intvar = BinlogEvent::IntVarEvent(IntVarEvent {
        intvar_type: 2,
        value: 42,
    });
    let query = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "INSERT INTO accounts (name) VALUES ('query-event')".to_string(),
    });

    handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(5, 100),
        &intvar,
    )
    .expect("record intvar");
    handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &query,
    )
    .expect("apply query with intvar");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 2);
    assert_eq!(statements[0].sql, "SET INSERT_ID = ?");
    assert_eq!(statements[0].params, vec![Value::UInt(42)]);
    assert_eq!(
        statements[1].sql,
        "INSERT INTO accounts (name) VALUES ('query-event')"
    );
}

#[test]
fn query_with_user_variables_is_rejected_before_checkpoint() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let uservar = BinlogEvent::UserVarEvent(UserVarEvent {
        name: "account_id".to_string(),
        value: None,
    });
    let query = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "INSERT INTO accounts (id) VALUES (@account_id)".to_string(),
    });

    handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(14, 100),
        &uservar,
    )
    .expect("record uservar");
    let error = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &query,
    )
    .expect_err("uservar query should not replay");

    assert!(error.to_string().contains("user variables"));
    assert!(applier.executor().statements.borrow().is_empty());
}

#[test]
fn qualified_query_dml_is_rejected_as_ambiguous() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let query = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "fixture_cdc".to_string(),
        sql_statement: "INSERT INTO `other_db`.`accounts` (id) VALUES (1)".to_string(),
    });

    let error = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &query,
    )
    .expect_err("qualified query should not replay");

    assert!(error.to_string().contains("qualified identifier"));
    assert!(applier.executor().statements.borrow().is_empty());
}

#[test]
fn non_source_query_and_rows_query_events_do_not_execute_sql_text() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let events = vec![
        BinlogEvent::QueryEvent(QueryEvent {
            thread_id: 1,
            duration: 0,
            error_code: 0,
            status_variables: Vec::new(),
            database_name: "globalcomix".to_string(),
            sql_statement: "INSERT INTO accounts VALUES (999, 'must-not-run')".to_string(),
        }),
        BinlogEvent::RowsQueryEvent(RowsQueryEvent {
            query: "DELETE FROM accounts WHERE id = 1".to_string(),
        }),
    ];

    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));

    for event in &events {
        handle_structured_event(
            &mut applier,
            &resolver,
            &mut state,
            "mysql-bin.000001",
            &event_header(99, 120),
            event,
        )
        .expect("skip text event");
    }

    assert!(applier.executor().statements.borrow().is_empty());
}

#[test]
fn maps_constructed_write_update_and_delete_rows_to_recording_executor() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let table_map = BinlogEvent::TableMapEvent(accounts_table_map_event(5));
    let write = BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_present: vec![true; 5],
        rows: vec![RowData::new(vec![
            Some(MySqlValue::Int(9)),
            Some(MySqlValue::String("nine".to_string())),
            Some(MySqlValue::Int(900)),
            Some(MySqlValue::String("manual".to_string())),
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
    let update = BinlogEvent::UpdateRowsEvent(MysqlCdcUpdateRowsEvent {
        table_id: 18,
        flags: 0,
        columns_number: 5,
        columns_before_update: vec![true; 5],
        columns_after_update: vec![true; 5],
        rows: vec![UpdateRowData::new(
            RowData::new(vec![
                Some(MySqlValue::Int(9)),
                Some(MySqlValue::String("nine".to_string())),
                Some(MySqlValue::Int(900)),
                Some(MySqlValue::String("manual".to_string())),
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
                Some(MySqlValue::Int(9)),
                Some(MySqlValue::String("niner".to_string())),
                Some(MySqlValue::Int(901)),
                Some(MySqlValue::String("manual update".to_string())),
                Some(MySqlValue::DateTime(DateTime {
                    year: 2026,
                    month: 6,
                    day: 22,
                    hour: 12,
                    minute: 3,
                    second: 5,
                    millis: 0,
                })),
            ]),
        )],
    });

    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));

    for event in [&table_map, &write, &update] {
        handle_structured_event(
            &mut applier,
            &resolver,
            &mut state,
            "mysql-bin.000001",
            &event_header(99, 120),
            event,
        )
        .expect("apply event");
    }

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 2);
    assert_eq!(
        statements[0].params,
        vec![
            Value::UInt(9),
            bytes("nine"),
            Value::UInt(900),
            bytes("manual"),
            bytes("2026-06-22 12:03:04")
        ]
    );
    assert_eq!(
        statements[1].params,
        vec![
            bytes("niner"),
            Value::UInt(901),
            bytes("manual update"),
            bytes("2026-06-22 12:03:05"),
            Value::UInt(9)
        ]
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

#[test]
fn formats_mysql_cdc_values_like_snapshot_text_rows() {
    assert_eq!(format_timestamp(1_782_075_535_000), "2026-06-21 20:58:55");
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::Blob(b"hello".to_vec())), false),
        Value::Bytes(b"hello".to_vec())
    );
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::Bit(vec![true])), false),
        Value::Bytes(vec![1])
    );
    assert_eq!(
        convert_mysql_value(
            &Some(MySqlValue::Bit(vec![
                true, false, true, false, true, false, true, false, true
            ])),
            false,
        ),
        Value::Bytes(vec![1, 85])
    );
    assert_eq!(
        convert_mysql_value(
            &Some(MySqlValue::Time(Time {
                hour: 26,
                minute: 3,
                second: 4,
                millis: 0,
            })),
            false,
        ),
        Value::Bytes(b"26:03:04".to_vec())
    );
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::SmallInt(0xfd68)), true),
        Value::Int(-664)
    );
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::SmallInt(840)), true),
        Value::Int(840)
    );
    assert_eq!(
        convert_mysql_value(&Some(MySqlValue::SmallInt(0xfd68)), false),
        Value::UInt(64872)
    );
}

#[test]
fn converts_enum_ordinals_to_metadata_strings() {
    let enum_values = vec!["1".to_string(), "2".to_string(), "14".to_string()];

    assert_eq!(
        mysql_value_to_target_value(&Some(MySqlValue::Enum(3)), false, Some(&enum_values))
            .expect("enum value"),
        Value::Bytes(b"14".to_vec())
    );
}

#[test]
fn converts_enum_zero_ordinal_to_mysql_empty_value() {
    let enum_values = vec!["1".to_string()];

    assert_eq!(
        mysql_value_to_target_value(&Some(MySqlValue::Enum(0)), false, Some(&enum_values))
            .expect("enum zero value"),
        Value::Bytes(Vec::new())
    );
}

#[test]
fn rejects_enum_ordinals_outside_metadata() {
    let enum_values = vec!["1".to_string()];
    let error = mysql_value_to_target_value(&Some(MySqlValue::Enum(2)), false, Some(&enum_values))
        .expect_err("enum ordinal error")
        .to_string();

    assert!(error.contains("enum ordinal 2 exceeds 1 metadata values"));
}

fn convert_mysql_value(value: &Option<MySqlValue>, signed: bool) -> Value {
    mysql_value_to_target_value(value, signed, None).expect("convert mysql value")
}

fn fixture_events(path: &str) -> Vec<(EventHeader, BinlogEvent)> {
    let file = File::open(path).expect("open fixture");
    let reader = BinlogReader::new(file).expect("create binlog reader");
    reader
        .read_events()
        .map(|event| event.expect("fixture event"))
        .collect()
}

fn write_rows_event(table_id: u64, id: u32, name: &str) -> BinlogEvent {
    BinlogEvent::WriteRowsEvent(MysqlCdcWriteRowsEvent {
        table_id,
        flags: 0,
        columns_number: 5,
        columns_present: vec![true; 5],
        rows: vec![RowData::new(vec![
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
        ])],
    })
}

fn accounts_table_map_event(column_count: usize) -> MysqlCdcTableMapEvent {
    MysqlCdcTableMapEvent {
        table_id: 18,
        database_name: "fixture_cdc".to_string(),
        table_name: "accounts".to_string(),
        column_types: vec![3; column_count],
        column_metadata: vec![0; column_count],
        null_bitmap: vec![false; column_count],
        table_metadata: None,
    }
}

fn stream_coordinate(position: u64) -> BinlogCoordinate {
    BinlogCoordinate {
        file: "mysql-bin.000001".to_string(),
        position,
    }
}

fn bytes(item: &str) -> Value {
    Value::Bytes(item.as_bytes().to_vec())
}

fn event_header(event_type: u8, next_event_position: u32) -> EventHeader {
    EventHeader {
        timestamp: 0,
        event_type,
        server_id: 1,
        event_length: 19,
        next_event_position,
        event_flags: 0,
    }
}

struct FixtureSchemaResolver;

impl TableSchemaResolver for FixtureSchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError> {
        assert_eq!(schema, "fixture_cdc");
        fixture_table_schema(table, column_count)
    }
}

fn fixture_table_schema(
    table: &str,
    column_count: usize,
) -> Result<ResolvedTableSchema, ApplyBinlogError> {
    match (table, column_count) {
        ("audit_log", 3) => Ok(schema(vec!["id", "account_id", "message"])),
        ("accounts", 5) => Ok(schema(vec!["id", "name", "balance", "note", "created_at"])),
        ("accounts", 6) => Ok(schema(vec![
            "id",
            "name",
            "balance",
            "uuid",
            "created_at",
            "status",
        ])),
        _ => Err(mapping_error(format!(
            "unexpected fixture table {table}/{column_count}"
        ))),
    }
}

fn schema(columns: Vec<&str>) -> ResolvedTableSchema {
    ResolvedTableSchema {
        columns: columns.into_iter().map(str::to_string).collect(),
        primary_key: vec!["id".to_string()],
        generated_columns: Vec::new(),
        signed_columns: Vec::new(),
        enum_columns: BTreeMap::new(),
    }
}

struct ReleasesSchemaResolver;

impl TableSchemaResolver for ReleasesSchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError> {
        assert_eq!(schema, "app");
        assert_eq!(table, "releases");
        assert_eq!(column_count, 2);
        Ok(ResolvedTableSchema {
            columns: vec!["id".to_string(), "public_time_delta".to_string()],
            primary_key: vec!["id".to_string()],
            generated_columns: Vec::new(),
            signed_columns: Vec::new(),
            enum_columns: BTreeMap::from([(
                "public_time_delta".to_string(),
                vec!["1".to_string(), "2".to_string(), "14".to_string()],
            )]),
        })
    }
}

struct NoopCheckpointStore;

impl StreamCheckpointStore for NoopCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<crate::checkpoint::Checkpoint>, ApplyBinlogError> {
        Ok(None)
    }

    fn save_checkpoint(
        &self,
        _checkpoint: &crate::checkpoint::Checkpoint,
    ) -> Result<(), ApplyBinlogError> {
        Ok(())
    }
}

struct EmptySchemaResolver;

impl TableSchemaResolver for EmptySchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        _column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError> {
        Err(mapping_error(format!(
            "unexpected fallback for {schema}.{table}"
        )))
    }
}

#[derive(Default)]
struct TransactionRecordingExecutor {
    operations: RefCell<Vec<&'static str>>,
    fail_execute: bool,
}

impl TransactionRecordingExecutor {
    fn failing() -> Self {
        Self {
            operations: RefCell::new(Vec::new()),
            fail_execute: true,
        }
    }
}

impl TargetExecutor for TransactionRecordingExecutor {
    fn execute(
        &self,
        _statement: &crate::target::SqlStatement,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("EXEC");
        if self.fail_execute {
            return Err(crate::target::TargetExecuteError::new("forced failure"));
        }
        Ok(())
    }
}

impl crate::target::TransactionalTargetExecutor for TransactionRecordingExecutor {
    fn begin_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("BEGIN");
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("COMMIT");
        Ok(())
    }

    fn rollback_transaction(&self) -> Result<(), crate::target::TargetExecuteError> {
        self.operations.borrow_mut().push("ROLLBACK");
        Ok(())
    }
}

#[derive(Default)]
struct RecordingExecutor {
    statements: RefCell<Vec<crate::target::SqlStatement>>,
}

impl TargetExecutor for RecordingExecutor {
    fn execute(
        &self,
        statement: &crate::target::SqlStatement,
    ) -> Result<(), crate::target::TargetExecuteError> {
        self.statements.borrow_mut().push(statement.clone());
        Ok(())
    }
}
