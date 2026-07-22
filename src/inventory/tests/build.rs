use super::fixtures::*;
use crate::inventory::build::{build_column, build_inventory, group_primary_keys};
use crate::inventory::{ColumnRow, GeneratedColumn, PrimaryKeyRow, TableRow};

#[test]
fn builds_inventory_with_primary_keys_and_generated_columns() {
    let reader = fake_reader();

    let inventory = build_inventory("fixture_cdc", &reader).expect("inventory");

    assert_eq!(inventory.schema, "fixture_cdc");
    assert_eq!(inventory.tables.len(), 1);

    let table = &inventory.tables[0];
    assert_eq!(table.name, "accounts");
    assert_eq!(table.primary_key, vec!["id"]);
    assert_eq!(table.columns[0].name, "id");
    assert_eq!(table.columns[0].data_type, "int");
    assert_eq!(table.columns[1].name, "name");
    assert!(!table.columns[1].is_nullable);
    assert_eq!(table.columns[2].generated, Some(generated_balance_column()));
    assert_eq!(inventory.indexes.len(), 1);
    assert_eq!(inventory.indexes[0].table, "accounts");
    assert_eq!(inventory.indexes[0].name, "idx_accounts_name");
    assert!(inventory.indexes[0].unique);
    assert_eq!(inventory.indexes[0].columns[0].name, "name");
    assert_eq!(inventory.indexes[0].columns[0].sequence, 1);
}

#[test]
fn includes_views_triggers_routines_and_events() {
    let reader = fake_reader();

    let inventory = build_inventory("fixture_cdc", &reader).expect("inventory");

    assert_eq!(inventory.views[0].name, "account_balances");
    assert_eq!(
        inventory.views[0].definition,
        "select id, balance from accounts"
    );
    assert_eq!(inventory.triggers[0].name, "accounts_ai");
    assert_eq!(inventory.triggers[0].timing, "AFTER");
    assert_eq!(inventory.triggers[0].event, "INSERT");
    assert_eq!(inventory.routines[0].name, "recalculate_accounts");
    assert_eq!(inventory.routines[0].routine_type, "PROCEDURE");
    assert_eq!(inventory.events[0].name, "nightly_recalc");
    assert_eq!(inventory.events[0].status, "ENABLED");
}

#[test]
fn excludes_views_from_table_inventory() {
    let reader = FakeInventoryReader {
        tables: vec![accounts_table(), account_balances_view_table()],
        columns: account_columns(),
        primary_keys: vec![account_primary_key()],
        indexes: vec![account_name_index()],
        views: vec![account_balances_view()],
        triggers: Vec::new(),
        routines: Vec::new(),
        events: Vec::new(),
    };

    let inventory = build_inventory("fixture_cdc", &reader).expect("inventory");
    let table_names = inventory
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(table_names, vec!["accounts"]);
}

#[test]
fn orders_composite_primary_keys_by_ordinal_position() {
    let keys = group_primary_keys(vec![
        PrimaryKeyRow {
            table_name: "edges".to_string(),
            column_name: "right_id".to_string(),
            ordinal_position: 2,
        },
        PrimaryKeyRow {
            table_name: "edges".to_string(),
            column_name: "left_id".to_string(),
            ordinal_position: 1,
        },
    ]);

    assert_eq!(keys["edges"], vec!["left_id", "right_id"]);
}

#[test]
fn classifies_stored_generated_columns() {
    let column = build_column(ColumnRow {
        table_name: "accounts".to_string(),
        column_name: "balance_copy".to_string(),
        ordinal_position: 1,
        column_type: "int".to_string(),
        data_type: "int".to_string(),
        is_nullable: false,
        character_set: None,
        collation: None,
        column_default: None,
        extra: "STORED GENERATED".to_string(),
        column_comment: "copied balance".to_string(),
        generation_expression: Some("`balance`".to_string()),
    });

    assert_eq!(column.comment, "copied balance");
    assert_eq!(
        column.generated,
        Some(GeneratedColumn {
            expression: "`balance`".to_string(),
            generation_kind: "STORED".to_string(),
        })
    );
}

#[test]
fn builds_empty_metadata_for_tables_without_related_rows() {
    let reader = FakeInventoryReader {
        tables: vec![TableRow {
            table_name: "no_columns_yet".to_string(),
            table_type: "BASE TABLE".to_string(),
            engine: Some("InnoDB".to_string()),
            table_collation: Some("utf8mb4_unicode_ci".to_string()),
        }],
        columns: Vec::new(),
        primary_keys: Vec::new(),
        indexes: Vec::new(),
        views: Vec::new(),
        triggers: Vec::new(),
        routines: Vec::new(),
        events: Vec::new(),
    };

    let inventory = build_inventory("fixture_cdc", &reader).expect("inventory");

    assert_eq!(inventory.tables[0].name, "no_columns_yet");
    assert!(inventory.tables[0].columns.is_empty());
    assert!(inventory.tables[0].primary_key.is_empty());
}
