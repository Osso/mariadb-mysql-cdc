use super::*;
use mysql_cdc::binlog_reader::BinlogReader;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::events::query_event::QueryEvent;
use mysql_cdc::events::rotate_event::RotateEvent;
use mysql_cdc::events::row_events::row_data::{RowData, UpdateRowData};
use mysql_cdc::events::row_events::update_rows_event::UpdateRowsEvent as MysqlCdcUpdateRowsEvent;
use mysql_cdc::events::row_events::write_rows_event::WriteRowsEvent as MysqlCdcWriteRowsEvent;
use mysql_cdc::events::rows_query_event::RowsQueryEvent;
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
fn structured_query_events_are_skipped_without_parsing_sql_keywords() {
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "globalcomix".to_string(),
        sql_statement: "CREATE TABLE should_not_be_applied (id int)".to_string(),
    });
    let header = event_header(99, 180);

    let outcome = classify_event("mysqld-bin.000777", &header, &event);

    assert_eq!(outcome.policy, EventPolicy::SkipQuery);
    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 180,
        })
    );
}

#[test]
fn annotation_rows_query_events_are_ignored_even_when_text_starts_with_sql() {
    let event = BinlogEvent::RowsQueryEvent(RowsQueryEvent {
        query: "INSERT INTO email_history VALUES ('annotation only')".to_string(),
    });
    let header = event_header(160, 240);

    let outcome = classify_event("mysqld-bin.000777", &header, &event);

    assert_eq!(outcome.policy, EventPolicy::IgnoreAnnotation);
    assert_eq!(
        outcome.resume_coordinate,
        Some(BinlogCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 240,
        })
    );
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
fn fixture_row_events_apply_through_row_applier_with_schema_resolver() {
    let events = fixture_events("fixtures/mixed-binlog/mysql-bin.000001");
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;

    for (header, event) in &events {
        handle_structured_event(&mut applier, &resolver, "mysql-bin.000001", header, event)
            .expect("handle fixture event");
    }

    let statements = applier.executor().statements.borrow();
    assert!(statements.iter().any(|statement| {
            statement.sql == "UPDATE `accounts` SET `name` = ?, `balance` = ?, `note` = ?, `created_at` = ? WHERE `id` = ?"
                && statement.params == vec!["alpha", "125", "row update", "2026-06-21 20:58:55", "1"]
        }));
    assert!(statements.iter().any(|statement| {
        statement.sql == "DELETE FROM `accounts` WHERE `id` = ?" && statement.params == vec!["2"]
    }));
    assert!(statements.iter().any(|statement| {
        statement
            .sql
            .starts_with("INSERT INTO `accounts` (`id`, `name`, `balance`, `note`, `created_at`)")
            && statement.params == vec!["3", "gamma", "300", "row insert", "2026-06-21 20:58:55"]
    }));
}

#[test]
fn structured_query_and_rows_query_events_do_not_execute_sql_text() {
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

    for event in &events {
        handle_structured_event(
            &mut applier,
            &resolver,
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

    for event in [&table_map, &write, &update] {
        handle_structured_event(
            &mut applier,
            &resolver,
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
        vec!["9", "nine", "900", "manual", "2026-06-22 12:03:04"]
    );
    assert_eq!(
        statements[1].params,
        vec!["niner", "901", "manual update", "2026-06-22 12:03:05", "9"]
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
        mysql_value_to_target_string(&Some(MySqlValue::Blob(b"hello".to_vec()))),
        "hello"
    );
    assert_eq!(
        mysql_value_to_target_string(&Some(MySqlValue::Time(Time {
            hour: 26,
            minute: 3,
            second: 4,
            millis: 0,
        }))),
        "26:03:04"
    );
}

fn fixture_events(path: &str) -> Vec<(EventHeader, BinlogEvent)> {
    let file = File::open(path).expect("open fixture");
    let reader = BinlogReader::new(file).expect("create binlog reader");
    reader
        .read_events()
        .map(|event| event.expect("fixture event"))
        .collect()
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
