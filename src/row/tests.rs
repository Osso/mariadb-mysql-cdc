use super::conflict::format_row_conflict_skipped;
use super::*;
use crate::probe::BinlogCoordinate;
use crate::target::{SqlStatement, TargetExecuteError, TargetExecutionOutcome, TargetExecutor};
use mysql::Value;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

#[test]
fn applies_write_rows_as_independent_plain_inserts() {
    let applier = applier_with_accounts_table();
    let event = WriteRowsEvent {
        coordinate: coordinate(120),
        table_id: 7,
        rows: vec![row("1", "alpha"), row("2", "beta")],
    };

    applier.apply_write_rows(&event).expect("apply write rows");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 2);
    assert_eq!(
        statements[0],
        SqlStatement {
            sql: "INSERT INTO `accounts` (`id`, `name`) VALUES (?, ?)".to_string(),
            params: values(["1", "alpha"]),
        }
    );
    assert_eq!(
        statements[1],
        SqlStatement {
            sql: "INSERT INTO `accounts` (`id`, `name`) VALUES (?, ?)".to_string(),
            params: values(["2", "beta"]),
        }
    );
}

#[test]
fn continues_after_one_duplicate_insert_is_ignored() {
    let executor = RecordingExecutor {
        row_outcomes: RefCell::new(VecDeque::from([
            TargetExecutionOutcome::DuplicateIgnored(crate::target::DuplicateConflict {
                error_code: 1062,
                error_text: "Duplicate entry '1' for key 'PRIMARY'".to_string(),
                duplicate_index: Some("PRIMARY".to_string()),
            }),
            TargetExecutionOutcome::Applied,
        ])),
        ..RecordingExecutor::default()
    };
    let mut applier = RowApplier::new(executor);
    applier.apply_table_map(accounts_table_map());
    let event = WriteRowsEvent {
        coordinate: coordinate(130),
        table_id: 7,
        rows: vec![row("1", "conflict"), row("2", "applied")],
    };

    applier.apply_write_rows(&event).expect("apply write rows");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 2);
    assert_eq!(statements[0].params, values(["1", "conflict"]));
    assert_eq!(statements[1].params, values(["2", "applied"]));
}

#[test]
fn ignored_duplicate_row_is_persisted_before_apply_returns() {
    let executor = RecordingExecutor {
        row_outcomes: RefCell::new(VecDeque::from([TargetExecutionOutcome::DuplicateIgnored(
            crate::target::DuplicateConflict {
                error_code: 1062,
                error_text: "Duplicate entry 'x' for key 'uq_accounts_email'".to_string(),
                duplicate_index: Some("uq_accounts_email".to_string()),
            },
        )])),
        ..RecordingExecutor::default()
    };
    let mut applier = RowApplier::new(executor);
    applier.apply_table_map(accounts_table_map());
    let event = WriteRowsEvent {
        coordinate: coordinate(160),
        table_id: 7,
        rows: vec![row("A", "conflict")],
    };
    let mut ledger = crate::conflict_repair::InMemoryConflictStore::default();
    let mut context = RowConflictContext {
        store: &mut ledger,
        source_identity: "source-a",
        source_server_id: 7,
        end_position: 200,
        observed_at_ms: 100,
    };

    applier
        .apply_write_rows_with_conflicts(&event, &mut context)
        .expect("ignored duplicate should be recorded");

    let record = &ledger.records()[0];
    assert_eq!(record.key.coordinate.end_position, 200);
    assert_eq!(record.duplicate_index.as_deref(), Some("uq_accounts_email"));
    assert_eq!(record.error_code, 1062);
    assert_eq!(
        record.error_text,
        "Duplicate entry 'x' for key 'uq_accounts_email'"
    );
}

#[test]
fn records_skipped_duplicate_with_coordinate_primary_key_index_and_error() {
    let applier = applier_with_accounts_table();
    let mut ledger = crate::conflict_repair::InMemoryConflictStore::default();
    applier
        .record_duplicate_conflict(
            &mut ledger,
            DuplicateConflictInput {
                source_identity: "source-a",
                source_server_id: 7,
                coordinate: &coordinate(160),
                end_position: 200,
                schema: "fixture_cdc",
                table: "accounts",
                operation: RowOperation::Insert,
                primary_key: &[value("A")],
                duplicate_index: Some("uq_accounts_email".to_string()),
                duplicate_owner_primary_key: Some(vec!["B".to_string()]),
                error_code: 1062,
                error_text: "ERROR 1062 duplicate email",
                observed_at_ms: 100,
            },
        )
        .expect("record conflict");
    let record = &ledger.records()[0];
    assert_eq!(record.key.coordinate.start_position, 160);
    assert_eq!(record.key.coordinate.end_position, 200);
    assert_eq!(record.key.source_primary_key, vec!["A"]);
    assert_eq!(record.duplicate_index.as_deref(), Some("uq_accounts_email"));
    assert_eq!(
        record.duplicate_owner_primary_key,
        Some(vec!["B".to_string()])
    );
    assert_eq!(record.error_code, 1062);
    assert_eq!(
        record.status,
        crate::conflict_repair::ConflictStatus::Unresolved
    );
}

#[test]
fn formats_ignored_conflict_with_table_coordinate_and_primary_key() {
    let message = format_row_conflict_skipped(
        RowOperation::Insert,
        &accounts_table_map().table,
        &coordinate(130),
        &[value("1")],
    );

    assert_eq!(
        message,
        "cdc_row_conflict_skipped operation=insert schema=app table=accounts source_file=mysql-bin.000001 source_position=130 primary_key=[\"1\"]"
    );
}

#[test]
fn applies_update_rows_using_after_image_and_primary_key() {
    let applier = applier_with_accounts_table();
    let event = UpdateRowsEvent {
        coordinate: coordinate(140),
        table_id: 7,
        rows: vec![RowUpdate {
            before: row("1", "alpha"),
            after: row("1", "updated"),
        }],
    };

    applier
        .apply_update_rows(&event)
        .expect("apply update rows");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(
        statements[0].sql,
        "UPDATE `accounts` SET `name` = ? WHERE `id` = ?"
    );
    assert_eq!(statements[0].params, values(["updated", "1"]));
}

#[test]
fn applies_primary_key_change_using_before_key_predicate() {
    let applier = applier_with_accounts_table();
    let event = UpdateRowsEvent {
        coordinate: coordinate(145),
        table_id: 7,
        rows: vec![RowUpdate {
            before: row("A", "before"),
            after: row("B", "after"),
        }],
    };

    applier
        .apply_update_rows(&event)
        .expect("apply primary-key change");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(
        statements[0].sql,
        "UPDATE `accounts` SET `id` = ?, `name` = ? WHERE `id` = ?"
    );
    assert_eq!(statements[0].params, values(["B", "after", "A"]));
}

#[test]
fn primary_key_change_assigns_every_writable_after_image_column() {
    let mut applier = RowApplier::new(RecordingExecutor::default());
    applier.apply_table_map(TableMapEvent {
        coordinate: coordinate(100),
        table: RowTableMap {
            table_id: 9,
            schema: "app".to_string(),
            table: "accounts_with_status".to_string(),
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "status".to_string(),
                "generated_label".to_string(),
            ],
            primary_key: vec!["id".to_string()],
            generated_columns: vec!["generated_label".to_string()],
            signed_columns: Vec::new(),
            enum_columns: BTreeMap::new(),
        },
    });
    let event = UpdateRowsEvent {
        coordinate: coordinate(147),
        table_id: 9,
        rows: vec![RowUpdate {
            before: BTreeMap::from([
                ("id".to_string(), value("A")),
                ("name".to_string(), value("before")),
                ("status".to_string(), value("active")),
                ("generated_label".to_string(), value("old-generated")),
            ]),
            after: BTreeMap::from([
                ("id".to_string(), value("B")),
                ("name".to_string(), value("after")),
                ("status".to_string(), value("active")),
                ("generated_label".to_string(), value("new-generated")),
            ]),
        }],
    };

    applier
        .apply_update_rows(&event)
        .expect("apply complete primary-key transition");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(
        statements[0].sql,
        "UPDATE `accounts_with_status` SET `id` = ?, `name` = ?, `status` = ? WHERE `id` = ?"
    );
    assert_eq!(statements[0].params, values(["B", "after", "active", "A"]));
}

#[test]
fn applies_composite_primary_key_change_using_complete_before_key() {
    let mut applier = RowApplier::new(RecordingExecutor::default());
    applier.apply_table_map(composite_accounts_table_map());
    let event = UpdateRowsEvent {
        coordinate: coordinate(150),
        table_id: 8,
        rows: vec![RowUpdate {
            before: composite_row("tenant-a", "1", "before"),
            after: composite_row("tenant-b", "1", "after"),
        }],
    };

    applier
        .apply_update_rows(&event)
        .expect("apply composite primary-key change");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(
        statements[0].sql,
        "UPDATE `composite_accounts` SET `tenant_id` = ?, `id` = ?, `name` = ? WHERE `tenant_id` = ? AND `id` = ?"
    );
    assert_eq!(
        statements[0].params,
        values(["tenant-b", "1", "after", "tenant-a", "1"])
    );
}

#[test]
fn excludes_generated_columns_from_write_and_update_statements() {
    let mut applier = RowApplier::new(RecordingExecutor::default());
    applier.apply_table_map(TableMapEvent {
        coordinate: coordinate(100),
        table: RowTableMap {
            table_id: 9,
            schema: "app".to_string(),
            table: "releases".to_string(),
            columns: vec![
                "id".to_string(),
                "slug".to_string(),
                "public_time".to_string(),
            ],
            primary_key: vec!["id".to_string()],
            generated_columns: vec!["public_time".to_string()],
            signed_columns: Vec::new(),
            enum_columns: BTreeMap::new(),
        },
    });
    let row = BTreeMap::from([
        ("id".to_string(), value("1")),
        ("slug".to_string(), value("alpha")),
        ("public_time".to_string(), value("2026-07-01 00:00:00")),
    ]);

    applier
        .apply_write_rows(&WriteRowsEvent {
            coordinate: coordinate(120),
            table_id: 9,
            rows: vec![row.clone()],
        })
        .expect("write row");
    applier
        .apply_update_rows(&UpdateRowsEvent {
            coordinate: coordinate(140),
            table_id: 9,
            rows: vec![RowUpdate {
                before: row.clone(),
                after: row,
            }],
        })
        .expect("update row");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(
        statements[0].sql,
        "INSERT INTO `releases` (`id`, `slug`) VALUES (?, ?)"
    );
    assert_eq!(statements[0].params, values(["1", "alpha"]));
}

#[test]
fn update_rows_only_write_changed_columns() {
    let applier = applier_with_accounts_table();
    let event = UpdateRowsEvent {
        coordinate: coordinate(140),
        table_id: 7,
        rows: vec![RowUpdate {
            before: row("1", "alpha"),
            after: row("1", "updated"),
        }],
    };

    applier
        .apply_update_rows(&event)
        .expect("apply update rows");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(
        statements[0].sql,
        "UPDATE `accounts` SET `name` = ? WHERE `id` = ?"
    );
    assert_eq!(statements[0].params, values(["updated", "1"]));
}

#[test]
fn applies_delete_rows_using_before_image_primary_key() {
    let applier = applier_with_accounts_table();
    let event = DeleteRowsEvent {
        coordinate: coordinate(160),
        table_id: 7,
        rows: vec![row("2", "beta")],
    };

    applier
        .apply_delete_rows(&event)
        .expect("apply delete rows");

    let statements = applier.executor().statements.borrow();
    assert_eq!(statements.len(), 1);
    assert_eq!(statements[0].sql, "DELETE FROM `accounts` WHERE `id` = ?");
    assert_eq!(statements[0].params, values(["2"]));
}

#[test]
fn rejects_row_event_without_table_map() {
    let applier = RowApplier::new(RecordingExecutor::default());
    let event = DeleteRowsEvent {
        coordinate: coordinate(160),
        table_id: 99,
        rows: vec![row("2", "beta")],
    };

    let error = applier
        .apply_delete_rows(&event)
        .expect_err("missing table map")
        .to_string();

    assert!(error.contains("missing table map"));
    assert!(error.contains("99"));
    assert!(error.contains("mysql-bin.000001:160"));
}

#[test]
fn rejects_row_event_without_primary_key_value() {
    let applier = applier_with_accounts_table();
    let mut row = RowImage::new();
    row.insert("name".to_string(), value("orphan"));
    let event = WriteRowsEvent {
        coordinate: coordinate(180),
        table_id: 7,
        rows: vec![row],
    };

    let error = applier
        .apply_write_rows(&event)
        .expect_err("missing primary key")
        .to_string();

    assert!(error.contains("missing primary key column id"));
    assert!(error.contains("app.accounts"));
    assert!(error.contains("mysql-bin.000001:180"));
}

#[test]
fn target_errors_include_operation_table_and_coordinate() {
    let executor = RecordingExecutor {
        error: Some(TargetExecuteError::new("deadlock")),
        ..RecordingExecutor::default()
    };
    let mut applier = RowApplier::new(executor);
    applier.apply_table_map(accounts_table_map());
    let event = DeleteRowsEvent {
        coordinate: coordinate(200),
        table_id: 7,
        rows: vec![row("2", "beta")],
    };

    let error = applier
        .apply_delete_rows(&event)
        .expect_err("target error")
        .to_string();

    assert!(error.contains("delete"));
    assert!(error.contains("app.accounts"));
    assert!(error.contains("mysql-bin.000001:200"));
    assert!(error.contains("deadlock"));
}

fn applier_with_accounts_table() -> RowApplier<RecordingExecutor> {
    let mut applier = RowApplier::new(RecordingExecutor::default());
    applier.apply_table_map(accounts_table_map());
    applier
}

fn accounts_table_map() -> TableMapEvent {
    TableMapEvent {
        coordinate: coordinate(100),
        table: RowTableMap {
            table_id: 7,
            schema: "app".to_string(),
            table: "accounts".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            primary_key: vec!["id".to_string()],
            generated_columns: Vec::new(),
            signed_columns: Vec::new(),
            enum_columns: BTreeMap::new(),
        },
    }
}

fn row(id: &str, name: &str) -> RowImage {
    BTreeMap::from([
        ("id".to_string(), value(id)),
        ("name".to_string(), value(name)),
    ])
}

fn composite_accounts_table_map() -> TableMapEvent {
    TableMapEvent {
        coordinate: coordinate(100),
        table: RowTableMap {
            table_id: 8,
            schema: "app".to_string(),
            table: "composite_accounts".to_string(),
            columns: vec![
                "tenant_id".to_string(),
                "id".to_string(),
                "name".to_string(),
            ],
            primary_key: vec!["tenant_id".to_string(), "id".to_string()],
            generated_columns: Vec::new(),
            signed_columns: Vec::new(),
            enum_columns: BTreeMap::new(),
        },
    }
}

fn composite_row(tenant_id: &str, id: &str, name: &str) -> RowImage {
    BTreeMap::from([
        ("tenant_id".to_string(), value(tenant_id)),
        ("id".to_string(), value(id)),
        ("name".to_string(), value(name)),
    ])
}

fn values<const N: usize>(items: [&str; N]) -> Vec<Value> {
    items.into_iter().map(value).collect()
}

fn value(item: &str) -> Value {
    Value::Bytes(item.as_bytes().to_vec())
}

fn coordinate(position: u64) -> BinlogCoordinate {
    BinlogCoordinate {
        file: "mysql-bin.000001".to_string(),
        position,
    }
}

#[derive(Default)]
struct RecordingExecutor {
    statements: RefCell<Vec<SqlStatement>>,
    row_outcomes: RefCell<VecDeque<TargetExecutionOutcome>>,
    error: Option<TargetExecuteError>,
}

impl TargetExecutor for RecordingExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        self.statements.borrow_mut().push(statement.clone());

        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn execute_row_change(
        &self,
        statement: &SqlStatement,
    ) -> Result<TargetExecutionOutcome, TargetExecuteError> {
        self.statements.borrow_mut().push(statement.clone());
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok(self
            .row_outcomes
            .borrow_mut()
            .pop_front()
            .unwrap_or(TargetExecutionOutcome::Applied))
    }
}
