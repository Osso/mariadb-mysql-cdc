use super::{ForeignKeyReference, MissingForeignKeyRepairKey, SupersededSourceInsert};
use crate::mysql_client::PersistentMySqlSource;
use crate::mysql_support::quote_ident;
use crate::target::{SqlStatement, TargetExecuteError, TargetRowChange, TargetRowChangeKind};
use mysql::prelude::Queryable;
use mysql::{Conn, Params, Row, Value};
use std::collections::BTreeMap;

pub(super) fn load_superseded_source_insert(
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
    reference: &ForeignKeyReference,
    repair_key: MissingForeignKeyRepairKey,
) -> Result<SupersededSourceInsert, TargetExecuteError> {
    let current_change = fetch_current_source_insert(source, change)?
        .map(|current| require_changed_foreign_key(change, reference, current))
        .transpose()?;
    Ok(SupersededSourceInsert {
        current_change,
        constraint: reference.constraint.clone(),
        repair_key,
    })
}

fn fetch_current_source_insert(
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
) -> Result<Option<TargetRowChange>, TargetExecuteError> {
    let mut conn = source.conn.borrow_mut();
    let primary_key = query_source_primary_key(&mut conn, change)?;
    let primary_key_values = change_values(change, &primary_key, "primary key")?;
    let columns = query_source_writable_columns(&mut conn, change)?;
    let sql = build_source_current_row_select(change, &columns, &primary_key);
    let rows = query_source_current_rows(&mut conn, change, sql, primary_key_values)?;
    decode_source_current_row(change, columns, rows)
        .map(|values| values.map(|values| build_current_source_insert(change, values)))
}

fn query_source_primary_key(
    conn: &mut Conn,
    change: &TargetRowChange,
) -> Result<Vec<String>, TargetExecuteError> {
    let columns = conn
        .exec::<String, _, _>(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
             WHERE CONSTRAINT_SCHEMA = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ? \
               AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
            (&change.schema, &change.schema, &change.table),
        )
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "superseded INSERT source primary-key query failed for {}.{}: {error}",
                change.schema, change.table
            ))
        })?;
    if columns.is_empty() {
        return Err(TargetExecuteError::new(format!(
            "superseded INSERT source table has no primary key: {}.{}",
            change.schema, change.table
        )));
    }
    Ok(columns)
}

fn query_source_writable_columns(
    conn: &mut Conn,
    change: &TargetRowChange,
) -> Result<Vec<String>, TargetExecuteError> {
    let columns = conn
        .exec::<String, _, _>(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA NOT LIKE '%GENERATED%' \
             ORDER BY ORDINAL_POSITION",
            (&change.schema, &change.table),
        )
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "superseded INSERT source column query failed for {}.{}: {error}",
                change.schema, change.table
            ))
        })?;
    if columns.is_empty() {
        return Err(TargetExecuteError::new(format!(
            "superseded INSERT source table has no writable columns: {}.{}",
            change.schema, change.table
        )));
    }
    Ok(columns)
}

fn build_source_current_row_select(
    change: &TargetRowChange,
    columns: &[String],
    primary_key: &[String],
) -> String {
    let selected_columns = quoted_columns(columns);
    let predicates = primary_key
        .iter()
        .map(|column| format!("{} <=> ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "SELECT {selected_columns} FROM {}.{} WHERE {predicates} LIMIT 2",
        quote_ident(&change.schema),
        quote_ident(&change.table)
    )
}

fn query_source_current_rows(
    conn: &mut Conn,
    change: &TargetRowChange,
    sql: String,
    primary_key_values: Vec<Value>,
) -> Result<Vec<Row>, TargetExecuteError> {
    conn.exec(sql, Params::Positional(primary_key_values))
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "superseded INSERT source row query failed for {}.{}: {error}",
                change.schema, change.table
            ))
        })
}

fn decode_source_current_row(
    change: &TargetRowChange,
    columns: Vec<String>,
    rows: Vec<Row>,
) -> Result<Option<BTreeMap<String, Value>>, TargetExecuteError> {
    match rows.len() {
        0 => Ok(None),
        1 => {
            let values = rows.into_iter().next().expect("one source row").unwrap();
            Ok(Some(columns.into_iter().zip(values).collect()))
        }
        count => Err(TargetExecuteError::new(format!(
            "superseded INSERT source row query returned {count} rows for {}.{}",
            change.schema, change.table
        ))),
    }
}

fn require_changed_foreign_key(
    historical: &TargetRowChange,
    reference: &ForeignKeyReference,
    current: TargetRowChange,
) -> Result<TargetRowChange, TargetExecuteError> {
    let changed = reference.columns.iter().any(|(child_column, _)| {
        historical.values.get(child_column) != current.values.get(child_column)
    });
    if changed {
        return Ok(current);
    }
    Err(TargetExecuteError::new(format!(
        "source current INSERT still references the missing parent for {}.{} constraint {}",
        historical.schema, historical.table, reference.constraint
    )))
}

fn build_current_source_insert(
    historical: &TargetRowChange,
    values: BTreeMap<String, Value>,
) -> TargetRowChange {
    let columns = values.keys().cloned().collect::<Vec<_>>();
    let placeholders = vec!["?"; columns.len()].join(", ");
    TargetRowChange {
        statement: SqlStatement {
            sql: format!(
                "INSERT INTO {}.{} ({}) VALUES ({placeholders})",
                quote_ident(&historical.schema),
                quote_ident(&historical.table),
                quoted_columns(&columns)
            ),
            params: values.values().cloned().collect(),
        },
        kind: TargetRowChangeKind::Insert,
        schema: historical.schema.clone(),
        table: historical.table.clone(),
        values,
    }
}

fn change_values(
    change: &TargetRowChange,
    columns: &[String],
    label: &str,
) -> Result<Vec<Value>, TargetExecuteError> {
    columns
        .iter()
        .map(|column| {
            change.values.get(column).cloned().ok_or_else(|| {
                TargetExecuteError::new(format!(
                    "superseded INSERT {label} value is absent for {}.{} column {column}",
                    change.schema, change.table
                ))
            })
        })
        .collect()
}

fn quoted_columns(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_current_source_insert_with_changed_foreign_key() {
        let historical = release_insert(2);
        let current = release_insert(1);

        let resolved = require_changed_foreign_key(&historical, &release_reference(), current)
            .expect("changed FK should be source-authoritative");

        assert_eq!(resolved.values["comic_format_id"], Value::UInt(1));
    }

    #[test]
    fn rejects_current_source_insert_that_still_references_missing_parent() {
        let historical = release_insert(2);
        let current = release_insert(2);

        let error = require_changed_foreign_key(&historical, &release_reference(), current)
            .expect_err("unchanged missing FK must fail closed");

        assert!(
            error
                .to_string()
                .contains("still references the missing parent")
        );
    }

    fn release_insert(comic_format_id: u64) -> TargetRowChange {
        let values = BTreeMap::from([
            ("id".to_string(), Value::UInt(391468)),
            ("comic_id".to_string(), Value::UInt(49868)),
            ("comic_format_id".to_string(), Value::UInt(comic_format_id)),
        ]);
        build_current_source_insert(
            &TargetRowChange {
                statement: SqlStatement {
                    sql: String::new(),
                    params: Vec::new(),
                },
                kind: TargetRowChangeKind::Insert,
                schema: "globalcomix".to_string(),
                table: "releases".to_string(),
                values: values.clone(),
            },
            values,
        )
    }

    fn release_reference() -> ForeignKeyReference {
        ForeignKeyReference {
            constraint: "releases_ibfk_format".to_string(),
            parent_schema: "globalcomix".to_string(),
            parent_table: "comics".to_string(),
            columns: vec![
                ("comic_id".to_string(), "id".to_string()),
                ("comic_format_id".to_string(), "comic_format_id".to_string()),
            ],
        }
    }
}
