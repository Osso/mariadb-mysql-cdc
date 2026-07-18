use super::model::{RowApplyError, RowImage, RowResult, RowTableMap, RowUpdate, row_error};
use crate::probe::BinlogCoordinate;
use crate::target::SqlStatement;
use mysql::Value;

pub(crate) fn validate_rows_have_primary_keys(
    table: &RowTableMap,
    rows: &[RowImage],
    coordinate: &BinlogCoordinate,
) -> RowResult<()> {
    for row in rows {
        validate_row_has_primary_key(table, row, coordinate)?;
    }
    Ok(())
}

pub(crate) fn validate_row_has_primary_key(
    table: &RowTableMap,
    values: &RowImage,
    coordinate: &BinlogCoordinate,
) -> RowResult<()> {
    primary_key_values(table, values, coordinate).map(|_| ())
}

pub(crate) fn primary_key_values(
    table: &RowTableMap,
    values: &RowImage,
    coordinate: &BinlogCoordinate,
) -> RowResult<Vec<Value>> {
    if table.primary_key.is_empty() {
        return Err(row_error(RowApplyError::MissingPrimaryKey {
            coordinate: coordinate.clone(),
            schema: table.schema.clone(),
            table: table.table.clone(),
        }));
    }

    table
        .primary_key
        .iter()
        .map(|column| primary_key_value(table, values, column, coordinate))
        .collect()
}

fn primary_key_value(
    table: &RowTableMap,
    values: &RowImage,
    column: &str,
    coordinate: &BinlogCoordinate,
) -> RowResult<Value> {
    values.get(column).cloned().ok_or_else(|| {
        row_error(RowApplyError::MissingPrimaryKeyValue {
            coordinate: coordinate.clone(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.to_string(),
        })
    })
}

pub(crate) fn build_insert_statement(table: &RowTableMap, row: &RowImage) -> SqlStatement {
    let writable_columns = writable_columns(table);
    let column_list = writable_columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>();
    let placeholders = vec!["?"; writable_columns.len()].join(", ");

    SqlStatement {
        sql: format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&table.table),
            column_list.join(", "),
            placeholders,
        ),
        params: ordered_values(row, &writable_columns),
    }
}

pub(crate) fn build_update_statement(
    table: &RowTableMap,
    update: &RowUpdate,
    coordinate: &BinlogCoordinate,
) -> RowResult<Option<SqlStatement>> {
    let writable_columns = writable_columns(table);
    let before_primary_key = primary_key_values(table, &update.before, coordinate)?;
    let after_primary_key = primary_key_values(table, &update.after, coordinate)?;
    let assignment_columns = if before_primary_key == after_primary_key {
        changed_columns(&writable_columns, update)
    } else {
        writable_columns
    };
    if assignment_columns.is_empty() {
        return Ok(None);
    }

    let assignments = assignment_columns
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect::<Vec<_>>();
    let predicates = primary_key_predicates(&table.primary_key);
    let mut params = ordered_values(&update.after, &assignment_columns);
    params.extend(before_primary_key);

    Ok(Some(SqlStatement {
        sql: format!(
            "UPDATE {} SET {} WHERE {}",
            quote_ident(&table.table),
            assignments.join(", "),
            predicates.join(" AND ")
        ),
        params,
    }))
}

fn changed_columns(writable_columns: &[String], update: &RowUpdate) -> Vec<String> {
    writable_columns
        .iter()
        .filter(|column| update.before.get(*column) != update.after.get(*column))
        .cloned()
        .collect()
}

pub(crate) fn build_delete_statement(
    table: &RowTableMap,
    row: &RowImage,
    coordinate: &BinlogCoordinate,
) -> RowResult<SqlStatement> {
    Ok(SqlStatement {
        sql: format!(
            "DELETE FROM {} WHERE {}",
            quote_ident(&table.table),
            primary_key_predicates(&table.primary_key).join(" AND ")
        ),
        params: primary_key_values(table, row, coordinate)?,
    })
}

pub(crate) fn writable_columns(table: &RowTableMap) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| !table.generated_columns.contains(column))
        .cloned()
        .collect()
}

pub(crate) fn ordered_values(row: &RowImage, columns: &[String]) -> Vec<Value> {
    columns
        .iter()
        .map(|column| row.get(column).cloned().unwrap_or(Value::NULL))
        .collect()
}

fn primary_key_predicates(primary_key: &[String]) -> Vec<String> {
    primary_key
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect()
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}
