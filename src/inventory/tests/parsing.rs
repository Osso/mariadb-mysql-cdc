use super::fixtures::{accounts_insert_trigger, nightly_recalc_event};
use crate::conflict_repair::canonicalize_foreign_keys;
use crate::inventory::parse::*;
use crate::inventory::query::*;
use crate::inventory::values::inventory_value_to_string;
use crate::inventory::*;

#[test]
fn parses_native_mysql_values_and_quotes_schema_names() {
    let row = vec![
        "accounts".to_string(),
        "BASE TABLE".to_string(),
        "InnoDB".to_string(),
        "utf8mb4_unicode_ci".to_string(),
    ];
    let table = parse_table_row(&row).expect("table row");

    assert_eq!(table.table_name, "accounts");
    assert_eq!(table.engine, Some("InnoDB".to_string()));
    assert_eq!(quote_sql_string("app's\\schema"), "'app''s\\\\schema'");
}

#[test]
fn builds_information_schema_queries_with_quoted_schema() {
    let schema = "app's\\schema";
    let quoted = "'app''s\\\\schema'";

    assert!(tables_query(schema).contains(&format!("TABLE_SCHEMA = {quoted}")));
    assert!(columns_query(schema).contains(&format!("TABLE_SCHEMA = {quoted}")));
    assert!(primary_keys_query(schema).contains(&format!("TABLE_SCHEMA = {quoted}")));
    let source_indexes = indexes_query(schema, InventoryEndpointRole::Source);
    let target_indexes = indexes_query(schema, InventoryEndpointRole::Target);
    assert!(source_indexes.contains(&format!("TABLE_SCHEMA = {quoted}")));
    assert!(source_indexes.contains("IGNORED = 'YES'"));
    assert!(target_indexes.contains("IS_VISIBLE"));
    assert!(views_query(schema).contains(&format!("TABLE_SCHEMA = {quoted}")));
    assert!(triggers_query(schema).contains(&format!("TRIGGER_SCHEMA = {quoted}")));
    assert!(routines_query(schema).contains(&format!("ROUTINE_SCHEMA = {quoted}")));
    assert!(events_query(schema).contains(&format!("EVENT_SCHEMA = {quoted}")));
    assert!(foreign_keys_query(schema).contains("REFERENCED_TABLE_SCHEMA"));
    let foreign_keys = canonical_foreign_keys_query(schema);
    assert!(foreign_keys.contains("REFERENTIAL_CONSTRAINTS"));
    assert!(foreign_keys.contains("UPDATE_RULE"));
    assert!(foreign_keys.contains("DELETE_RULE"));
    assert!(foreign_keys.contains("ORDINAL_POSITION"));
}

#[test]
fn parses_foreign_key_parent_schema() {
    let parsed = parse_foreign_key_row(&[
        "children".to_string(),
        "child_parent_fk".to_string(),
        "parent_id".to_string(),
        "1".to_string(),
        "shared".to_string(),
        "parents".to_string(),
        "id".to_string(),
    ])
    .expect("foreign-key row");

    assert_eq!(parsed.referenced_schema, "shared");
    assert_eq!(parsed.referenced_table, "parents");
}

#[test]
fn parses_and_builds_complete_canonical_foreign_key_inventory() {
    let row = vec![
        "fixture_cdc".to_string(),
        "child_parent_fk".to_string(),
        "fixture_cdc".to_string(),
        "children".to_string(),
        "parent_id".to_string(),
        "1".to_string(),
        "fixture_cdc".to_string(),
        "parents".to_string(),
        "id".to_string(),
        "RESTRICT".to_string(),
        "CASCADE".to_string(),
        "NONE".to_string(),
        "YES".to_string(),
    ];
    let parsed = parse_canonical_foreign_key_row(&row).expect("canonical foreign-key row");
    let inventory =
        canonicalize_foreign_keys(vec![parsed]).expect("canonical foreign-key inventory");
    assert_eq!(inventory[0].child_table, "children");
    assert_eq!(inventory[0].parent_table, "parents");
    assert_eq!(inventory[0].child_columns, vec!["parent_id"]);
    assert_eq!(inventory[0].parent_columns, vec!["id"]);
    assert_eq!(inventory[0].update_rule, "RESTRICT");
    assert_eq!(inventory[0].delete_rule, "CASCADE");
}

#[test]
fn parses_source_master_coordinate_for_snapshot_fencing() {
    assert_eq!(source_master_coordinate_query(), "SHOW MASTER STATUS");
    assert_eq!(
        parse_source_master_coordinate(&[
            "mysqld-bin.000777".to_string(),
            "180".to_string(),
            String::new(),
        ])
        .expect("master coordinate"),
        SourceMasterCoordinate {
            file: "mysqld-bin.000777".to_string(),
            position: 180,
        }
    );
    assert!(parse_source_master_coordinate(&["mysqld-bin.000777".to_string()]).is_err());
}

#[test]
fn builds_exact_table_runtime_query_with_quoted_identifiers() {
    assert_eq!(
        table_runtime_query("fixture_cdc", "account`history"),
        "SELECT (SELECT COUNT(*) FROM `fixture_cdc`.`account``history`), AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'fixture_cdc' AND TABLE_NAME = 'account`history' AND TABLE_TYPE = 'BASE TABLE'"
    );
    assert_eq!(
        parse_table_runtime_row(&["7".to_string(), "8".to_string()]).expect("runtime metadata"),
        TableRuntimeMetadata {
            row_count: 7,
            auto_increment: Some(8),
        }
    );
    assert_eq!(
        parse_table_runtime_row(&["0".to_string(), String::new()])
            .expect("runtime metadata without auto increment"),
        TableRuntimeMetadata {
            row_count: 0,
            auto_increment: None,
        }
    );
}

#[test]
fn parses_all_inventory_row_types() {
    assert_eq!(
        parse_view_row(&["account_balances".to_string(), "select 1".to_string()])
            .expect("view row"),
        ViewRow {
            table_name: "account_balances".to_string(),
            view_definition: "select 1".to_string(),
        }
    );
    assert_eq!(
        parse_trigger_row(&[
            "accounts_ai".to_string(),
            "INSERT".to_string(),
            "AFTER".to_string(),
            "accounts".to_string(),
            "insert into audit_log values (...)".to_string(),
        ])
        .expect("trigger row"),
        accounts_insert_trigger()
    );
    assert_eq!(
        parse_routine_row(&[
            "recalculate_accounts".to_string(),
            "PROCEDURE".to_string(),
            String::new(),
        ])
        .expect("routine row"),
        RoutineRow {
            routine_name: "recalculate_accounts".to_string(),
            routine_type: "PROCEDURE".to_string(),
            routine_definition: None,
        }
    );
    assert_eq!(
        parse_event_row(&[
            "nightly_recalc".to_string(),
            "ENABLED".to_string(),
            "call recalculate_accounts()".to_string(),
        ])
        .expect("event row"),
        nightly_recalc_event()
    );
}

#[test]
fn decodes_null_inventory_values_as_empty_optional_fields() {
    let row = vec![
        "accounts".to_string(),
        "BASE TABLE".to_string(),
        inventory_value_to_string(mysql::Value::NULL),
        inventory_value_to_string(mysql::Value::NULL),
    ];
    let table = parse_table_row(&row).expect("table row");

    assert_eq!(table.engine, None);
    assert_eq!(table.table_collation, None);
}

#[test]
fn reports_malformed_inventory_rows_with_row_type() {
    let error = parse_column_row(&["accounts".to_string()]).expect_err("short column row");

    assert_eq!(error.to_string(), "column row has 1 fields, expected 12");
}

#[test]
fn reports_invalid_numeric_inventory_fields_with_context() {
    let error = parse_primary_key_row(&[
        "accounts".to_string(),
        "id".to_string(),
        "not-a-number".to_string(),
    ])
    .expect_err("invalid ordinal");

    assert_eq!(
        error.to_string(),
        "primary key ordinal is not numeric: not-a-number"
    );
}
