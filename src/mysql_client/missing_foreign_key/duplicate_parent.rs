use super::{DuplicateParentReconciliation, DuplicateParentRepairKey, render_repair_key_value};
use crate::mysql_client::PersistentMySqlSource;
use crate::mysql_support::quote_ident;
use crate::target::{
    SqlStatement, TargetExecuteError, TargetRowChange, TargetRowChangeKind,
    duplicate_index_from_error,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Params, Row, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
struct UniqueIndexColumn {
    index: String,
    column: Option<String>,
    sequence: u64,
    prefix_length: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UniqueIndex {
    name: String,
    columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DuplicateParentMetadata {
    primary_key: Vec<String>,
    duplicate_index: UniqueIndex,
}

#[derive(Clone, Debug, PartialEq)]
struct DuplicateParentOwner {
    primary_key: Vec<Value>,
    owns_intended_primary_key: bool,
}

pub(crate) struct DuplicateParentProbe {
    metadata: DuplicateParentMetadata,
    pub(crate) owner_statement: SqlStatement,
}

pub(crate) fn load_duplicate_parent_reconciliation(
    target: &mut Conn,
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
    error: &TargetExecuteError,
) -> Result<DuplicateParentReconciliation, TargetExecuteError> {
    let probe = prepare_duplicate_parent_probe(target, change, error)?;
    let owner_rows = execute_mysql_row_query(target, &probe.owner_statement)?;
    finish_duplicate_parent_probe(source, change, probe, owner_rows)
}

pub(crate) fn prepare_duplicate_parent_probe(
    target: &mut Conn,
    change: &TargetRowChange,
    error: &TargetExecuteError,
) -> Result<DuplicateParentProbe, TargetExecuteError> {
    let rows = query_unique_index_columns(target, change)?;
    let metadata = build_duplicate_parent_metadata(change, error, rows)?;
    let owner_statement = build_duplicate_owner_select_statement(change, &metadata)?;
    Ok(DuplicateParentProbe {
        metadata,
        owner_statement,
    })
}

pub(crate) fn finish_duplicate_parent_probe(
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
    probe: DuplicateParentProbe,
    owner_rows: Vec<Vec<Value>>,
) -> Result<DuplicateParentReconciliation, TargetExecuteError> {
    let owner = duplicate_parent_owner_from_rows(change, &probe.metadata, owner_rows)?;
    let source_owner = if owner.owns_intended_primary_key {
        None
    } else {
        fetch_source_owner_values(source, change, &probe.metadata, &owner.primary_key)?
    };
    plan_duplicate_parent_reconciliation(change, &probe.metadata, owner, source_owner)
}

pub(crate) fn verify_duplicate_parent_reconciliation(
    target: &mut Conn,
    change: &TargetRowChange,
    reconciliation: &DuplicateParentReconciliation,
) -> Result<(), TargetExecuteError> {
    let rows = execute_mysql_row_query(target, &reconciliation.verification)?;
    verify_parent_query_rows(change, rows)
}

pub(crate) fn verify_parent_query_rows(
    change: &TargetRowChange,
    rows: Vec<Vec<Value>>,
) -> Result<(), TargetExecuteError> {
    let verified = rows.as_slice() == [vec![Value::Int(1)]].as_slice()
        || rows.as_slice() == [vec![Value::UInt(1)]].as_slice()
        || rows.as_slice() == [vec![Value::Bytes(b"1".to_vec())]].as_slice();
    if verified {
        return Ok(());
    }
    Err(TargetExecuteError::new(format!(
        "duplicate-parent verification returned {} non-matching rows for {}.{}",
        rows.len(),
        change.schema,
        change.table
    )))
}

fn query_unique_index_columns(
    target: &mut Conn,
    change: &TargetRowChange,
) -> Result<Vec<UniqueIndexColumn>, TargetExecuteError> {
    let rows = target
        .exec::<(String, Option<String>, u64, Option<u64>), _, _>(
            "SELECT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,SUB_PART \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 \
             ORDER BY INDEX_NAME,SEQ_IN_INDEX",
            (&change.schema, &change.table),
        )
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "duplicate-parent unique-index query failed for {}.{}: {error}",
                change.schema, change.table
            ))
        })?;
    Ok(rows
        .into_iter()
        .map(
            |(index, column, sequence, prefix_length)| UniqueIndexColumn {
                index,
                column,
                sequence,
                prefix_length,
            },
        )
        .collect())
}

fn build_duplicate_parent_metadata(
    change: &TargetRowChange,
    error: &TargetExecuteError,
    rows: Vec<UniqueIndexColumn>,
) -> Result<DuplicateParentMetadata, TargetExecuteError> {
    let error_index = duplicate_index_from_error(&error.to_string()).ok_or_else(|| {
        TargetExecuteError::new(format!(
            "duplicate-parent error does not identify an index for {}.{}: {error}",
            change.schema, change.table
        ))
    })?;
    let mut indexes = BTreeMap::<String, Vec<UniqueIndexColumn>>::new();
    for row in rows {
        indexes.entry(row.index.clone()).or_default().push(row);
    }
    let duplicate_name = resolve_duplicate_index_name(change, &error_index, &indexes)?;
    let primary_key = build_unique_index(change, "PRIMARY", indexes.get("PRIMARY"))?.columns;
    let duplicate_index =
        build_unique_index(change, &duplicate_name, indexes.get(&duplicate_name))?;
    Ok(DuplicateParentMetadata {
        primary_key,
        duplicate_index,
    })
}

fn resolve_duplicate_index_name(
    change: &TargetRowChange,
    error_index: &str,
    indexes: &BTreeMap<String, Vec<UniqueIndexColumn>>,
) -> Result<String, TargetExecuteError> {
    if indexes.contains_key(error_index) {
        return Ok(error_index.to_string());
    }
    let matches = indexes
        .keys()
        .filter(|index| error_index.ends_with(&format!(".{index}")))
        .cloned()
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Ok(index.clone()),
        [] => Err(TargetExecuteError::new(format!(
            "duplicate-parent index {error_index} is absent for {}.{}",
            change.schema, change.table
        ))),
        _ => Err(TargetExecuteError::new(format!(
            "duplicate-parent index {error_index} is ambiguous for {}.{}: {}",
            change.schema,
            change.table,
            matches.join(", ")
        ))),
    }
}

fn build_unique_index(
    change: &TargetRowChange,
    index: &str,
    rows: Option<&Vec<UniqueIndexColumn>>,
) -> Result<UniqueIndex, TargetExecuteError> {
    let rows = rows.ok_or_else(|| {
        TargetExecuteError::new(format!(
            "duplicate-parent index metadata is absent for {}.{} index {index}",
            change.schema, change.table
        ))
    })?;
    let columns = ordered_unique_index_columns(change, index, rows)?;
    if columns.is_empty() {
        return Err(TargetExecuteError::new(format!(
            "duplicate-parent index {index} has no columns for {}.{}",
            change.schema, change.table
        )));
    }
    Ok(UniqueIndex {
        name: index.to_string(),
        columns,
    })
}

fn ordered_unique_index_columns(
    change: &TargetRowChange,
    index: &str,
    rows: &[UniqueIndexColumn],
) -> Result<Vec<String>, TargetExecuteError> {
    let mut rows = rows.to_vec();
    rows.sort_by_key(|row| row.sequence);
    rows.into_iter()
        .enumerate()
        .map(|(position, row)| unique_index_column_name(change, index, position, row))
        .collect()
}

fn unique_index_column_name(
    change: &TargetRowChange,
    index: &str,
    position: usize,
    row: UniqueIndexColumn,
) -> Result<String, TargetExecuteError> {
    let expected_sequence = u64::try_from(position + 1).expect("index position fits u64");
    if row.sequence != expected_sequence {
        return Err(TargetExecuteError::new(format!(
            "duplicate-parent index metadata is non-contiguous for {}.{} index {index}",
            change.schema, change.table
        )));
    }
    if row.prefix_length.is_some() {
        return Err(TargetExecuteError::new(format!(
            "duplicate-parent index {index} has a prefix column for {}.{}",
            change.schema, change.table
        )));
    }
    row.column.ok_or_else(|| {
        TargetExecuteError::new(format!(
            "duplicate-parent index {index} has an expression column for {}.{}",
            change.schema, change.table
        ))
    })
}

fn build_duplicate_owner_select_statement(
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
) -> Result<SqlStatement, TargetExecuteError> {
    let primary_key_values = change_values(change, &metadata.primary_key, "primary key")?;
    let duplicate_values =
        change_values(change, &metadata.duplicate_index.columns, "duplicate index")?;
    let selected_primary_key = quoted_columns(&metadata.primary_key);
    let same_primary_key = metadata
        .primary_key
        .iter()
        .map(|column| format!("{} <=> ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let owner_predicates = metadata
        .duplicate_index
        .columns
        .iter()
        .map(|column| format!("{} <=> ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut params = primary_key_values;
    params.extend(duplicate_values);
    Ok(SqlStatement {
        sql: format!(
            "SELECT {selected_primary_key}, ({same_primary_key}) FROM {}.{} WHERE {owner_predicates} LIMIT 2 FOR UPDATE",
            quote_ident(&change.schema),
            quote_ident(&change.table)
        ),
        params,
    })
}

fn duplicate_parent_owner_from_rows(
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
    rows: Vec<Vec<Value>>,
) -> Result<DuplicateParentOwner, TargetExecuteError> {
    if rows.len() != 1 {
        return Err(TargetExecuteError::new(format!(
            "duplicate-parent owner query returned {} rows for {}.{} index {}",
            rows.len(),
            change.schema,
            change.table,
            metadata.duplicate_index.name
        )));
    }
    let mut row = rows.into_iter().next().expect("one duplicate owner row");
    let expected_fields = metadata.primary_key.len() + 1;
    if row.len() != expected_fields {
        return Err(TargetExecuteError::new(format!(
            "duplicate-parent owner query returned {} fields for {}.{}; expected {expected_fields}",
            row.len(),
            change.schema,
            change.table
        )));
    }
    let same_primary_key = row.pop().expect("same-primary-key field");
    let owns_intended_primary_key = mysql_boolean(&same_primary_key).ok_or_else(|| {
        TargetExecuteError::new(format!(
            "duplicate-parent owner query returned an invalid identity flag for {}.{}",
            change.schema, change.table
        ))
    })?;
    if row.iter().any(|value| matches!(value, Value::NULL)) {
        return Err(TargetExecuteError::new(format!(
            "duplicate-parent owner has a NULL primary key for {}.{}",
            change.schema, change.table
        )));
    }
    Ok(DuplicateParentOwner {
        primary_key: row,
        owns_intended_primary_key,
    })
}

fn mysql_boolean(value: &Value) -> Option<bool> {
    match value {
        Value::Int(0) | Value::UInt(0) => Some(false),
        Value::Int(1) | Value::UInt(1) => Some(true),
        Value::Bytes(value) if value == b"0" => Some(false),
        Value::Bytes(value) if value == b"1" => Some(true),
        _ => None,
    }
}

fn fetch_source_owner_values(
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
    primary_key: &[Value],
) -> Result<Option<BTreeMap<String, Value>>, TargetExecuteError> {
    validate_owner_primary_key_width(change, metadata, primary_key)?;
    let columns = change.values.keys().cloned().collect::<Vec<_>>();
    let sql = build_source_owner_select(change, metadata, &columns);
    let rows = query_source_owner_rows(source, change, sql, primary_key)?;
    decode_source_owner_rows(change, columns, rows)
}

fn validate_owner_primary_key_width(
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
    primary_key: &[Value],
) -> Result<(), TargetExecuteError> {
    if primary_key.len() == metadata.primary_key.len() {
        return Ok(());
    }
    Err(TargetExecuteError::new(format!(
        "duplicate-parent owner primary key width mismatch for {}.{}",
        change.schema, change.table
    )))
}

fn build_source_owner_select(
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
    columns: &[String],
) -> String {
    format!(
        "SELECT {} FROM {}.{} WHERE {} LIMIT 2",
        quoted_columns(columns),
        quote_ident(&change.schema),
        quote_ident(&change.table),
        primary_key_predicates(&metadata.primary_key)
    )
}

fn query_source_owner_rows(
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
    sql: String,
    primary_key: &[Value],
) -> Result<Vec<Row>, TargetExecuteError> {
    source
        .conn
        .borrow_mut()
        .exec(sql, Params::Positional(primary_key.to_vec()))
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "duplicate-parent source owner query failed for {}.{}: {error}",
                change.schema, change.table
            ))
        })
}

fn decode_source_owner_rows(
    change: &TargetRowChange,
    columns: Vec<String>,
    rows: Vec<Row>,
) -> Result<Option<BTreeMap<String, Value>>, TargetExecuteError> {
    match rows.len() {
        0 => Ok(None),
        1 => {
            let values = rows
                .into_iter()
                .next()
                .expect("one source owner row")
                .unwrap();
            Ok(Some(columns.into_iter().zip(values).collect()))
        }
        count => Err(TargetExecuteError::new(format!(
            "duplicate-parent source owner query returned {count} rows for {}.{}",
            change.schema, change.table
        ))),
    }
}

fn plan_duplicate_parent_reconciliation(
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
    owner: DuplicateParentOwner,
    source_owner: Option<BTreeMap<String, Value>>,
) -> Result<DuplicateParentReconciliation, TargetExecuteError> {
    let retry_parent_insert = !owner.owns_intended_primary_key;
    let owner_change = if owner.owns_intended_primary_key {
        build_owner_update(change, metadata, &owner.primary_key, change.values.clone())?
    } else if let Some(source_owner) = source_owner {
        build_owner_update(change, metadata, &owner.primary_key, source_owner)?
    } else {
        build_owner_delete(change, metadata, owner.primary_key)?
    };
    let repair_key = build_duplicate_parent_repair_key(change, metadata)?;
    Ok(DuplicateParentReconciliation {
        owner_change,
        retry_parent_insert,
        verification: build_parent_verification_statement(change),
        repair_key,
    })
}

fn build_owner_update(
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
    owner_primary_key: &[Value],
    values: BTreeMap<String, Value>,
) -> Result<TargetRowChange, TargetExecuteError> {
    let columns = values.keys().cloned().collect::<Vec<_>>();
    let assignments = columns
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let predicates = primary_key_predicates(&metadata.primary_key);
    let mut params = map_values(&values, &columns)?;
    params.extend(owner_primary_key.iter().cloned());
    Ok(TargetRowChange {
        statement: SqlStatement {
            sql: format!(
                "UPDATE {}.{} SET {assignments} WHERE {predicates}",
                quote_ident(&change.schema),
                quote_ident(&change.table)
            ),
            params,
        },
        kind: TargetRowChangeKind::Update,
        schema: change.schema.clone(),
        table: change.table.clone(),
        values,
    })
}

fn build_owner_delete(
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
    owner_primary_key: Vec<Value>,
) -> Result<TargetRowChange, TargetExecuteError> {
    if owner_primary_key.len() != metadata.primary_key.len() {
        return Err(TargetExecuteError::new(format!(
            "duplicate-parent owner primary key width mismatch for {}.{}",
            change.schema, change.table
        )));
    }
    let values = metadata
        .primary_key
        .iter()
        .cloned()
        .zip(owner_primary_key.iter().cloned())
        .collect();
    Ok(TargetRowChange {
        statement: SqlStatement {
            sql: format!(
                "DELETE FROM {}.{} WHERE {}",
                quote_ident(&change.schema),
                quote_ident(&change.table),
                primary_key_predicates(&metadata.primary_key)
            ),
            params: owner_primary_key,
        },
        kind: TargetRowChangeKind::Delete,
        schema: change.schema.clone(),
        table: change.table.clone(),
        values,
    })
}

fn build_duplicate_parent_repair_key(
    change: &TargetRowChange,
    metadata: &DuplicateParentMetadata,
) -> Result<DuplicateParentRepairKey, TargetExecuteError> {
    let values = change_values(change, &metadata.duplicate_index.columns, "duplicate index")?
        .iter()
        .map(render_repair_key_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DuplicateParentRepairKey {
        schema: change.schema.clone(),
        table: change.table.clone(),
        index: metadata.duplicate_index.name.clone(),
        values,
    })
}

fn build_parent_verification_statement(change: &TargetRowChange) -> SqlStatement {
    let predicates = change
        .values
        .iter()
        .map(|(column, value)| match value {
            Value::Bytes(_) => format!(
                "CAST({} AS BINARY) <=> CAST(? AS BINARY)",
                quote_ident(column)
            ),
            _ => format!("{} <=> ?", quote_ident(column)),
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    SqlStatement {
        sql: format!(
            "SELECT 1 FROM {}.{} WHERE {predicates} LIMIT 2 FOR UPDATE",
            quote_ident(&change.schema),
            quote_ident(&change.table)
        ),
        params: change.values.values().cloned().collect(),
    }
}

fn execute_mysql_row_query(
    target: &mut Conn,
    statement: &SqlStatement,
) -> Result<Vec<Vec<Value>>, TargetExecuteError> {
    target
        .exec::<Row, _, _>(&statement.sql, Params::Positional(statement.params.clone()))
        .map(|rows| rows.into_iter().map(Row::unwrap).collect())
        .map_err(|error| {
            TargetExecuteError::new(format!("duplicate-parent target query failed: {error}"))
        })
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
                    "duplicate-parent {label} value is absent for {}.{} column {column}",
                    change.schema, change.table
                ))
            })
        })
        .collect()
}

fn map_values(
    values: &BTreeMap<String, Value>,
    columns: &[String],
) -> Result<Vec<Value>, TargetExecuteError> {
    columns
        .iter()
        .map(|column| {
            values.get(column).cloned().ok_or_else(|| {
                TargetExecuteError::new(format!(
                    "duplicate-parent source owner value is absent for column {column}"
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

fn primary_key_predicates(primary_key: &[String]) -> String {
    primary_key
        .iter()
        .map(|column| format!("{} <=> ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[cfg(test)]
#[path = "duplicate_parent_tests.rs"]
mod tests;
