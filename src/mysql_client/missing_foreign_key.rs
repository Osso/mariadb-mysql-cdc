use super::query::target_query_error;
use super::{PersistentMySqlSource, PersistentTargetExecutor};
use crate::mysql_support::quote_ident;
use crate::target::{SqlStatement, TargetExecuteError, TargetRowChange};
use mysql::prelude::Queryable;
use mysql::{Params, Row, Value};

struct ForeignKeyReference {
    constraint: String,
    parent_schema: String,
    parent_table: String,
    columns: Vec<(String, String)>,
}

impl PersistentTargetExecutor {
    pub(super) fn repair_missing_foreign_key_parent(
        &self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<(), TargetExecuteError> {
        let source = self.source.as_ref().ok_or_else(|| {
            TargetExecuteError::new("missing-FK repair source connection is unavailable")
        })?;
        let reference = self.foreign_key_reference(change, error)?;
        let key_values = foreign_key_values(change, &reference)?;
        let (columns, values) = source_parent_row(source, &reference, key_values)?;
        let insert = parent_insert_statement(&reference, columns, values);

        match self.execute_statement(&insert) {
            Ok(()) => {}
            Err(error) if error.mysql_code() == Some(1062) => {}
            Err(error) => {
                return Err(TargetExecuteError::new(format!(
                    "missing-FK parent insert failed for {}.{} constraint {}: {error}",
                    change.schema, change.table, reference.constraint
                )));
            }
        }

        eprintln!(
            "cdc_missing_fk_parent_inserted child={}.{} constraint={} parent={}.{}",
            change.schema,
            change.table,
            reference.constraint,
            reference.parent_schema,
            reference.parent_table
        );
        Ok(())
    }

    fn foreign_key_reference(
        &self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<ForeignKeyReference, TargetExecuteError> {
        let constraint = foreign_key_constraint(&error.to_string()).ok_or_else(|| {
            TargetExecuteError::new(format!(
                "missing-FK target error did not identify a constraint: {error}"
            ))
        })?;
        let rows = self.with_connection(|conn| {
            conn.exec::<(String, String, String, String), _, _>(
                "SELECT COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
                 FROM information_schema.KEY_COLUMN_USAGE \
                 WHERE CONSTRAINT_SCHEMA = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                   AND CONSTRAINT_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL \
                 ORDER BY ORDINAL_POSITION",
                (&change.schema, &change.schema, &change.table, &constraint),
            )
            .map_err(target_query_error)
        })?;
        build_foreign_key_reference(change, constraint, rows)
    }
}

fn foreign_key_constraint(error: &str) -> Option<String> {
    let marker = "CONSTRAINT `";
    let start = error.find(marker)? + marker.len();
    let remainder = &error[start..];
    let end = remainder.find('`')?;
    let constraint = &remainder[..end];
    (!constraint.is_empty()).then(|| constraint.to_string())
}

fn build_foreign_key_reference(
    change: &TargetRowChange,
    constraint: String,
    rows: Vec<(String, String, String, String)>,
) -> Result<ForeignKeyReference, TargetExecuteError> {
    let Some((_, parent_schema, parent_table, _)) = rows.first() else {
        return Err(TargetExecuteError::new(format!(
            "missing-FK constraint metadata is absent for {}.{} constraint {constraint}",
            change.schema, change.table
        )));
    };
    if parent_schema != &change.schema {
        return Err(TargetExecuteError::new(format!(
            "missing-FK repair does not support cross-schema constraint {constraint}: {} -> {parent_schema}.{parent_table}",
            change.schema
        )));
    }
    if rows
        .iter()
        .any(|(_, schema, table, _)| schema != parent_schema || table != parent_table)
    {
        return Err(TargetExecuteError::new(format!(
            "missing-FK constraint metadata is ambiguous for {}.{} constraint {constraint}",
            change.schema, change.table
        )));
    }
    Ok(ForeignKeyReference {
        constraint,
        parent_schema: parent_schema.clone(),
        parent_table: parent_table.clone(),
        columns: rows
            .into_iter()
            .map(|(child, _, _, parent)| (child, parent))
            .collect(),
    })
}

fn foreign_key_values(
    change: &TargetRowChange,
    reference: &ForeignKeyReference,
) -> Result<Vec<Value>, TargetExecuteError> {
    reference
        .columns
        .iter()
        .map(|(child_column, _)| {
            change.values.get(child_column).cloned().ok_or_else(|| {
                TargetExecuteError::new(format!(
                    "missing-FK child value is absent for {}.{} column {child_column}",
                    change.schema, change.table
                ))
            })
        })
        .collect()
}

fn source_parent_row(
    source: &PersistentMySqlSource,
    reference: &ForeignKeyReference,
    key_values: Vec<Value>,
) -> Result<(Vec<String>, Vec<Value>), TargetExecuteError> {
    let mut conn = source.conn.borrow_mut();
    let columns = conn
        .exec::<String, _, _>(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA NOT LIKE '%GENERATED%' \
             ORDER BY ORDINAL_POSITION",
            (&reference.parent_schema, &reference.parent_table),
        )
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "missing-FK source parent column query failed: {error}"
            ))
        })?;
    if columns.is_empty() {
        return Err(TargetExecuteError::new(format!(
            "missing-FK source parent table has no writable columns: {}.{}",
            reference.parent_schema, reference.parent_table
        )));
    }

    let select_columns = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let predicates = reference
        .columns
        .iter()
        .map(|(_, parent_column)| format!("{} <=> ?", quote_ident(parent_column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT {select_columns} FROM {}.{} WHERE {predicates} LIMIT 2",
        quote_ident(&reference.parent_schema),
        quote_ident(&reference.parent_table)
    );
    let rows = conn
        .exec::<Row, _, _>(sql, Params::Positional(key_values))
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "missing-FK source parent query failed for {}.{}: {error}",
                reference.parent_schema, reference.parent_table
            ))
        })?;
    if rows.len() != 1 {
        return Err(TargetExecuteError::new(format!(
            "missing-FK source parent query returned {} rows for {}.{} constraint {}",
            rows.len(),
            reference.parent_schema,
            reference.parent_table,
            reference.constraint
        )));
    }
    let values = rows
        .into_iter()
        .next()
        .expect("one source parent row")
        .unwrap();
    Ok((columns, values))
}

fn parent_insert_statement(
    reference: &ForeignKeyReference,
    columns: Vec<String>,
    values: Vec<Value>,
) -> SqlStatement {
    let column_sql = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; columns.len()].join(", ");
    SqlStatement {
        sql: format!(
            "INSERT INTO {}.{} ({column_sql}) VALUES ({placeholders})",
            quote_ident(&reference.parent_schema),
            quote_ident(&reference.parent_table)
        ),
        params: values,
    }
}
