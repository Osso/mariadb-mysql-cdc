use super::*;
use crate::target::{SqlStatement, TargetExecuteError, TargetRowChange, TargetRowChangeKind};
use mysql::Value;
use std::collections::BTreeMap;

#[test]
fn resolves_prefixed_duplicate_index_and_locks_exact_owner() {
    let change = parent_change(
        "comics",
        [
            ("id", Value::UInt(49125)),
            ("label", bytes("source-parent")),
            ("slug", bytes("night-shift")),
        ],
    );
    let error =
        TargetExecuteError::from_mysql(1062, "Duplicate entry 'night-shift' for key 'comics.slug'");
    let metadata = build_duplicate_parent_metadata(
        &change,
        &error,
        vec![
            index_column("PRIMARY", "id", 1, None),
            index_column("slug", "slug", 1, None),
        ],
    )
    .expect("resolve duplicate metadata");

    let statement =
        build_duplicate_owner_select_statement(&change, &metadata).expect("build owner query");

    assert_eq!(metadata.primary_key, ["id"]);
    assert_eq!(metadata.duplicate_index.name, "slug");
    assert_eq!(metadata.duplicate_index.columns, ["slug"]);
    assert_eq!(
        statement.sql,
        "SELECT `id`, (`id` <=> ?) FROM `globalcomix`.`comics` WHERE `slug` <=> ? LIMIT 2 FOR UPDATE"
    );
    assert_eq!(statement.params, [Value::UInt(49125), bytes("night-shift")]);
}

#[test]
fn rejects_prefix_or_ambiguous_duplicate_indexes() {
    let change = parent_change(
        "comics",
        [("id", Value::UInt(49125)), ("slug", bytes("night-shift"))],
    );
    let prefix_error =
        TargetExecuteError::from_mysql(1062, "Duplicate entry 'night' for key 'comics.slug'");
    let prefix = build_duplicate_parent_metadata(
        &change,
        &prefix_error,
        vec![
            index_column("PRIMARY", "id", 1, None),
            index_column("slug", "slug", 1, Some(5)),
        ],
    )
    .expect_err("prefix unique index must fail closed");
    assert!(prefix.to_string().contains("prefix column"));

    let ambiguous_error = TargetExecuteError::from_mysql(
        1062,
        "Duplicate entry 'night-shift' for key 'schema.comics.slug'",
    );
    let ambiguous = build_duplicate_parent_metadata(
        &change,
        &ambiguous_error,
        vec![
            index_column("PRIMARY", "id", 1, None),
            index_column("slug", "slug", 1, None),
            index_column("comics.slug", "slug", 1, None),
        ],
    )
    .expect_err("ambiguous index suffix must fail closed");
    assert!(ambiguous.to_string().contains("ambiguous"));
}

#[test]
fn plans_same_primary_key_owner_update_without_reinsert() {
    let change = parent_change(
        "users",
        [
            ("id", Value::UInt(2108466)),
            ("label", bytes("source-user")),
            ("name", bytes("OvalTeen")),
        ],
    );
    let metadata = metadata("PRIMARY", ["id"], ["id"]);
    let owner = DuplicateParentOwner {
        primary_key: vec![bytes("2108466")],
        owns_intended_primary_key: true,
    };

    let reconciliation = plan_duplicate_parent_reconciliation(&change, &metadata, owner, None)
        .expect("plan same-PK reconciliation");

    assert_eq!(
        reconciliation.owner_change.kind,
        TargetRowChangeKind::Update
    );
    assert!(!reconciliation.retry_parent_insert);
    assert_eq!(
        reconciliation.owner_change.statement.sql,
        "UPDATE `globalcomix`.`users` SET `id` = ?, `label` = ?, `name` = ? WHERE `id` <=> ?"
    );
    assert_eq!(
        reconciliation.owner_change.statement.params,
        [
            Value::UInt(2108466),
            bytes("source-user"),
            bytes("OvalTeen"),
            bytes("2108466"),
        ]
    );
    assert!(
        reconciliation
            .verification
            .sql
            .ends_with("LIMIT 2 FOR UPDATE")
    );
}

#[test]
fn plans_different_primary_key_owner_update_before_reinsert() {
    let change = parent_change(
        "comics",
        [
            ("id", Value::UInt(49125)),
            ("label", bytes("source-parent")),
            ("slug", bytes("night-shift")),
        ],
    );
    let metadata = metadata("slug", ["id"], ["slug"]);
    let owner = DuplicateParentOwner {
        primary_key: vec![bytes("44083")],
        owns_intended_primary_key: false,
    };
    let source_owner = BTreeMap::from([
        ("id".to_string(), Value::UInt(44083)),
        ("label".to_string(), bytes("source-owner")),
        ("slug".to_string(), bytes("old-night-shift")),
    ]);

    let reconciliation =
        plan_duplicate_parent_reconciliation(&change, &metadata, owner, Some(source_owner))
            .expect("plan different-PK reconciliation");

    assert_eq!(
        reconciliation.owner_change.kind,
        TargetRowChangeKind::Update
    );
    assert!(reconciliation.retry_parent_insert);
    assert_eq!(
        reconciliation.owner_change.statement.params.last(),
        Some(&bytes("44083"))
    );
    assert_eq!(
        reconciliation.owner_change.values.get("slug"),
        Some(&bytes("old-night-shift"))
    );
}

#[test]
fn plans_source_absent_owner_delete_before_reinsert() {
    let change = parent_change(
        "comics",
        [
            ("id", Value::UInt(49126)),
            ("label", bytes("source-parent")),
            ("slug", bytes("deleted-owner")),
        ],
    );
    let metadata = metadata("slug", ["id"], ["slug"]);
    let owner = DuplicateParentOwner {
        primary_key: vec![bytes("44084")],
        owns_intended_primary_key: false,
    };

    let reconciliation = plan_duplicate_parent_reconciliation(&change, &metadata, owner, None)
        .expect("plan source-absent reconciliation");

    assert_eq!(
        reconciliation.owner_change.kind,
        TargetRowChangeKind::Delete
    );
    assert!(reconciliation.retry_parent_insert);
    assert_eq!(
        reconciliation.owner_change.statement,
        SqlStatement {
            sql: "DELETE FROM `globalcomix`.`comics` WHERE `id` <=> ?".to_string(),
            params: vec![bytes("44084")],
        }
    );
}

#[test]
fn rejects_missing_or_multiple_duplicate_owners() {
    let change = parent_change(
        "comics",
        [("id", Value::UInt(49125)), ("slug", bytes("night-shift"))],
    );
    let metadata = metadata("slug", ["id"], ["slug"]);

    for rows in [
        Vec::new(),
        vec![
            vec![bytes("44083"), bytes("0")],
            vec![bytes("44084"), bytes("0")],
        ],
    ] {
        let error = duplicate_parent_owner_from_rows(&change, &metadata, rows)
            .expect_err("ambiguous owner must fail closed");
        assert!(error.to_string().contains("owner query returned"));
    }
}

#[test]
fn verifies_byte_values_with_binary_exact_predicates() {
    let change = parent_change(
        "users",
        [
            ("gc_service_fee_percentage", bytes("65.00")),
            ("id", Value::UInt(2108466)),
            ("name", bytes("OvalTeen")),
        ],
    );

    let statement = build_parent_verification_statement(&change);

    assert!(
        statement
            .sql
            .contains("CAST(`gc_service_fee_percentage` AS BINARY) <=> CAST(? AS BINARY)")
    );
    assert!(
        statement
            .sql
            .contains("CAST(`name` AS BINARY) <=> CAST(? AS BINARY)")
    );
    assert!(statement.sql.contains("`id` <=> ?"));
    assert_eq!(statement.params[0], bytes("65.00"));
}

fn index_column(
    index: &str,
    column: &str,
    sequence: u64,
    prefix_length: Option<u64>,
) -> UniqueIndexColumn {
    UniqueIndexColumn {
        index: index.to_string(),
        column: Some(column.to_string()),
        sequence,
        prefix_length,
    }
}

fn metadata<const P: usize, const U: usize>(
    index: &str,
    primary_key: [&str; P],
    duplicate_columns: [&str; U],
) -> DuplicateParentMetadata {
    DuplicateParentMetadata {
        primary_key: primary_key.into_iter().map(str::to_string).collect(),
        duplicate_index: UniqueIndex {
            name: index.to_string(),
            columns: duplicate_columns.into_iter().map(str::to_string).collect(),
        },
    }
}

fn parent_change<const N: usize>(table: &str, values: [(&str, Value); N]) -> TargetRowChange {
    let values = values
        .into_iter()
        .map(|(column, value)| (column.to_string(), value))
        .collect::<BTreeMap<_, _>>();
    TargetRowChange {
        statement: SqlStatement {
            sql: format!("INSERT INTO `{table}` VALUES (...)"),
            params: values.values().cloned().collect(),
        },
        kind: TargetRowChangeKind::Insert,
        schema: "globalcomix".to_string(),
        table: table.to_string(),
        values,
    }
}

fn bytes(value: &str) -> Value {
    Value::Bytes(value.as_bytes().to_vec())
}
