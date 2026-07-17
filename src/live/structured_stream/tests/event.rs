use super::*;

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
fn query_dml_contract_violation_does_not_apply_insert_id_intvar() {
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
    let error = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &query,
    )
    .expect_err("statement DML must fail under ROW/FULL");

    assert!(error.to_string().contains("ROW/FULL contract violation"));
    assert!(applier.executor().statements.borrow().is_empty());
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

    assert!(error.to_string().contains("ROW/FULL contract violation"));
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
