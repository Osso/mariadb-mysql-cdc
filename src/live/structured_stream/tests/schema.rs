use super::*;

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
fn metadata_table_map_supplies_set_member_names_for_duplicate_comparison() {
    let resolver = EmptySchemaResolver;
    let table_map = MysqlCdcTableMapEvent {
        table_id: 79,
        database_name: "app".to_string(),
        table_name: "labels".to_string(),
        column_types: vec![3, MYSQL_COLUMN_TYPE_SET],
        column_metadata: vec![0, 3],
        null_bitmap: vec![false, true],
        table_metadata: Some(TableMetadata {
            signedness: None,
            default_charset: None,
            column_charsets: None,
            column_names: Some(vec!["id".to_string(), "labels".to_string()]),
            set_string_values: Some(vec![vec![
                "red".to_string(),
                "green".to_string(),
                "blue".to_string(),
            ]]),
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
        .expect("map SET metadata");

    assert_eq!(
        mapped.table.set_columns.get("labels"),
        Some(&vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ])
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
