use super::*;

#[test]
fn detects_qualified_identifiers_across_mysql_quoting_forms() {
    for sql in [
        "INSERT INTO other_db.accounts VALUES (1)",
        "INSERT INTO `other_db`.accounts VALUES (1)",
        "INSERT INTO other_db.`accounts` VALUES (1)",
        "INSERT INTO `other_db`.`accounts` VALUES (1)",
        "INSERT INTO \"other_db\".\"accounts\" VALUES (1)",
        "INSERT INTO other_db . accounts VALUES (1)",
        "INSERT INTO other_db /* comment */ . accounts VALUES (1)",
        "INSERT INTO other_db. /* comment */ accounts VALUES (1)",
        "INSERT INTO other_db -- comment\n . accounts VALUES (1)",
        "INSERT INTO `other``db`.`accounts` VALUES (1)",
        "--not-a-comment.other",
    ] {
        assert!(query_contains_qualified_identifier(sql), "missed {sql}");
    }
    assert!(!query_contains_qualified_identifier(
        "INSERT INTO accounts (amount, note) VALUES (1.5, 'other_db.accounts')"
    ));
    assert!(!query_contains_qualified_identifier(
        "-- Sentence ending here. NULL remains valid.\r\nALTER TABLE `accounts` ADD COLUMN `variant_id` SMALLINT"
    ));
    assert!(!query_contains_qualified_identifier(
        "/* prose mentions other_db.accounts but is not SQL */ ALTER TABLE `accounts` ADD COLUMN `variant_id` SMALLINT"
    ));
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
        database_name: "other_db".to_string(),
        sql_statement: "INSERT INTO `fixture_cdc`.`accounts` (id) VALUES (1)".to_string(),
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

    assert!(error.to_string().contains("ROW/FULL contract violation"));
    assert!(applier.executor().statements.borrow().is_empty());
}

#[test]
fn unrelated_qualified_statement_dml_is_ignored_with_source_filter() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(Some("fixture_cdc".to_string()));
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: "other_db".to_string(),
        sql_statement: "UPDATE other_db.accounts SET name='safe' WHERE id=1".to_string(),
    });

    let outcome = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &event,
    )
    .expect("unrelated database DML should be ignored");

    assert_eq!(outcome.policy, EventPolicy::Ignore);
    assert!(applier.executor().statements.borrow().is_empty());
}

#[test]
fn transaction_control_query_is_ignored_without_source_database_filter() {
    let mut applier = crate::row::RowApplier::new(RecordingExecutor::default());
    let resolver = FixtureSchemaResolver;
    let mut state = StructuredEventState::new(None);
    let event = BinlogEvent::QueryEvent(QueryEvent {
        thread_id: 1,
        duration: 0,
        error_code: 0,
        status_variables: Vec::new(),
        database_name: String::new(),
        sql_statement: "BEGIN".to_string(),
    });

    let outcome = handle_structured_event(
        &mut applier,
        &resolver,
        &mut state,
        "mysqld-bin.000777",
        &event_header(99, 180),
        &event,
    )
    .expect("BEGIN should be ignored under ROW/FULL");

    assert_eq!(outcome.policy, EventPolicy::Ignore);
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
