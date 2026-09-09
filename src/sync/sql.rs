use super::model::{SyncChunkReadRequest, SyncPrimaryKeyOrdering, SyncTable, SyncUniqueIndex};
use crate::database_row::DatabaseRow;
use crate::mysql_support::{quote_ident, quote_sql_literal};
use crate::target::SqlStatement;
use mysql::Value;

#[cfg(test)]
#[path = "relation_sql_tests.rs"]
mod relation_sql_tests;

pub(crate) fn build_related_rows_select_statement(
    table: &SyncTable,
    columns: &[String],
    values: &[Option<String>],
    request: &SyncChunkReadRequest,
) -> Result<SqlStatement, String> {
    validate_related_rows_request(table, columns, values, request)?;
    let params = columns
        .iter()
        .zip(values)
        .map(|(column, value)| match value {
            Some(value) => parameter_value(table, column, value.clone()),
            None => Ok(Value::NULL),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut predicates = columns
        .iter()
        .map(|column| format!("{} <=> ?", quote_ident(column)))
        .collect::<Vec<_>>();
    predicates.extend(sync_bound_predicates(table, request));
    Ok(SqlStatement {
        sql: format!(
            "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {}",
            sync_select_columns(table),
            quote_ident(&table.name),
            predicates.join(" AND "),
            primary_key_order_by(&table.primary_key, &table.primary_key_ordering),
            request.limit
        ),
        params,
    })
}

fn validate_related_rows_request(
    table: &SyncTable,
    columns: &[String],
    values: &[Option<String>],
    request: &SyncChunkReadRequest,
) -> Result<(), String> {
    if columns.is_empty() || columns.len() != values.len() {
        return Err(format!(
            "relation for `{}` requires nonempty columns and matching values",
            table.name
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for column in columns {
        if !table.columns.contains(column) {
            return Err(format!(
                "relation column `{column}` is absent from `{}`",
                table.name
            ));
        }
        if !seen.insert(column) {
            return Err(format!(
                "duplicate relation column `{column}` for `{}`",
                table.name
            ));
        }
    }
    if request.limit == 0 {
        return Err(format!(
            "relation read limit for `{}` must be nonzero",
            table.name
        ));
    }
    for (label, cursor) in [
        ("start_after", &request.start_after),
        ("end_at", &request.end_at),
    ] {
        if let Some(cursor) = cursor
            && cursor.len() != table.primary_key.len()
        {
            return Err(format!(
                "relation {label} cursor width mismatch for `{}`: expected {}, found {}",
                table.name,
                table.primary_key.len(),
                cursor.len()
            ));
        }
    }
    Ok(())
}

pub(crate) fn build_sync_select_sql(table: &SyncTable, request: &SyncChunkReadRequest) -> String {
    let columns = sync_select_columns(table);
    let order_by = primary_key_order_by(&table.primary_key, &table.primary_key_ordering);
    let predicates = sync_bound_predicates(table, request);
    let bounds = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    format!(
        "SELECT {columns} FROM {}{bounds} ORDER BY {order_by} LIMIT {}",
        quote_ident(&table.name),
        request.limit
    )
}

pub(crate) fn build_exact_primary_key_select_statement(
    table: &SyncTable,
    primary_key: &[String],
) -> Result<SqlStatement, String> {
    if primary_key.len() != table.primary_key.len() {
        return Err(format!(
            "exact primary-key width mismatch for `{}`: expected {}, found {}",
            table.name,
            table.primary_key.len(),
            primary_key.len()
        ));
    }
    Ok(SqlStatement {
        sql: format!(
            "SELECT {} FROM {} WHERE {} LIMIT 2",
            sync_select_columns(table),
            quote_ident(&table.name),
            primary_key_predicates(&table.primary_key).join(" AND ")
        ),
        params: primary_key_params(table, primary_key, "exact primary key")?,
    })
}

pub(crate) fn build_unique_index_columns_statement(database: &str, table: &str) -> SqlStatement {
    SqlStatement {
        sql: "SELECT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 ORDER BY INDEX_NAME,SEQ_IN_INDEX".to_string(),
        params: vec![string_param(database.to_string()), string_param(table.to_string())],
    }
}

pub(crate) fn build_unique_owner_select_statement(
    table: &SyncTable,
    index: &SyncUniqueIndex,
    intended: &DatabaseRow,
) -> Result<SqlStatement, String> {
    if index.columns.is_empty() {
        return Err(format!(
            "secondary unique index `{}` has no columns for `{}`",
            index.name, table.name
        ));
    }
    let params = required_non_null_values(table, intended, &index.columns, "unique index")?;
    let predicates = index
        .columns
        .iter()
        .map(|column| format!("{} <=> ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(SqlStatement {
        sql: format!(
            "SELECT {} FROM {} WHERE {predicates} LIMIT 2",
            sync_select_columns(table),
            quote_ident(&table.name)
        ),
        params,
    })
}

pub(crate) fn build_lock_table_write_sql(database: &str, table: &str) -> String {
    format!(
        "LOCK TABLES {}.{} WRITE",
        quote_ident(database),
        quote_ident(table)
    )
}

pub(crate) fn build_strict_insert_statement(
    table: &SyncTable,
    rows: &[DatabaseRow],
) -> Result<SqlStatement, String> {
    let columns = quote_ident_list(&table.columns);
    let placeholders = row_placeholders(table.columns.len(), rows.len());
    let params = rows
        .iter()
        .map(|row| ordered_values(table, row, &table.columns))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(SqlStatement {
        sql: format!(
            "INSERT INTO {} ({columns}) VALUES {placeholders}",
            quote_ident(&table.name)
        ),
        params,
    })
}

pub(crate) fn build_strict_update_rows_statement(
    table: &SyncTable,
    rows: &[DatabaseRow],
) -> Result<SqlStatement, String> {
    let changed_columns = non_primary_columns(table);
    let assignments = changed_columns
        .iter()
        .map(|column| strict_case_assignment(table, column, rows.len()))
        .collect::<Vec<_>>()
        .join(", ");
    let row_filter = primary_key_row_filter(&table.primary_key, rows.len());
    let order_by = quote_ident_list(&table.primary_key);
    let params = ordered_update_params(table, &changed_columns, rows)?;
    Ok(SqlStatement {
        sql: format!(
            "UPDATE {} SET {assignments} WHERE {row_filter} ORDER BY {order_by}",
            quote_ident(&table.name)
        ),
        params,
    })
}

pub(crate) fn build_strict_delete_rows_statement(
    table: &SyncTable,
    primary_keys: &[Vec<String>],
) -> Result<SqlStatement, String> {
    let params = primary_keys
        .iter()
        .map(|primary_key| primary_key_params(table, primary_key, "delete primary key"))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(SqlStatement {
        sql: format!(
            "DELETE FROM {} WHERE {}",
            quote_ident(&table.name),
            primary_key_row_filter(&table.primary_key, primary_keys.len())
        ),
        params,
    })
}

fn strict_case_assignment(table: &SyncTable, column: &str, row_count: usize) -> String {
    let predicate = primary_key_predicates(&table.primary_key).join(" AND ");
    let cases = std::iter::repeat_n(format!("WHEN {predicate} THEN ?"), row_count)
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "{} = CASE {cases} ELSE {} END",
        quote_ident(column),
        quote_ident(column)
    )
}

fn primary_key_row_filter(primary_key: &[String], row_count: usize) -> String {
    if primary_key.len() == 1 {
        let values = std::iter::repeat_n("?", row_count)
            .collect::<Vec<_>>()
            .join(", ");
        return format!("{} IN ({values})", quote_ident(&primary_key[0]));
    }
    let columns = quote_ident_list(primary_key);
    format!(
        "({columns}) IN ({})",
        row_placeholders(primary_key.len(), row_count)
    )
}

fn ordered_update_params(
    table: &SyncTable,
    changed_columns: &[String],
    rows: &[DatabaseRow],
) -> Result<Vec<Value>, String> {
    let mut params = Vec::new();
    for column in changed_columns {
        for row in rows {
            params.extend(primary_key_params(
                table,
                &row.primary_key,
                "update primary key",
            )?);
            params.extend(ordered_values(table, row, std::slice::from_ref(column))?);
        }
    }
    for row in rows {
        params.extend(primary_key_params(
            table,
            &row.primary_key,
            "update filter primary key",
        )?);
    }
    Ok(params)
}

fn sync_bound_predicates(table: &SyncTable, request: &SyncChunkReadRequest) -> Vec<String> {
    let mut predicates = Vec::new();
    if let Some(start_after) = &request.start_after {
        predicates.push(primary_key_bound_predicate(
            &table.primary_key,
            &table.primary_key_ordering,
            start_after,
            ">",
        ));
    }
    if let Some(end_at) = &request.end_at {
        predicates.push(format!(
            "NOT {}",
            primary_key_bound_predicate(
                &table.primary_key,
                &table.primary_key_ordering,
                end_at,
                ">",
            )
        ));
    }
    predicates
}

fn primary_key_bound_predicate(
    columns: &[String],
    ordering: &[SyncPrimaryKeyOrdering],
    values: &[String],
    operator: &str,
) -> String {
    let branches = columns
        .iter()
        .enumerate()
        .map(|(index, _)| primary_key_bound_branch(columns, ordering, values, index, operator))
        .collect::<Vec<_>>();
    if branches.len() < 2 {
        branches.join(" OR ")
    } else {
        format!("({})", branches.join(" OR "))
    }
}

fn primary_key_bound_branch(
    columns: &[String],
    ordering: &[SyncPrimaryKeyOrdering],
    values: &[String],
    index: usize,
    operator: &str,
) -> String {
    let mut parts = Vec::new();
    for equal_index in 0..index {
        parts.push(format!(
            "{} = {}",
            quote_ident(&columns[equal_index]),
            quote_sql_literal(&values[equal_index])
        ));
    }
    let column = primary_key_order_expression(&columns[index], &ordering[index]);
    let value = primary_key_bound_expression(&values[index], &ordering[index]);
    parts.push(format!("{column} {operator} {value}"));
    format!("({})", parts.join(" AND "))
}

fn primary_key_order_by(columns: &[String], ordering: &[SyncPrimaryKeyOrdering]) -> String {
    columns
        .iter()
        .zip(ordering)
        .map(|(column, ordering)| primary_key_order_expression(column, ordering))
        .collect::<Vec<_>>()
        .join(", ")
}

fn primary_key_order_expression(column: &str, ordering: &SyncPrimaryKeyOrdering) -> String {
    match ordering {
        SyncPrimaryKeyOrdering::Native => quote_ident(column),
        SyncPrimaryKeyOrdering::Enum(labels) => enum_field_expression(&quote_ident(column), labels),
    }
}

fn primary_key_bound_expression(value: &str, ordering: &SyncPrimaryKeyOrdering) -> String {
    match ordering {
        SyncPrimaryKeyOrdering::Native => quote_sql_literal(value),
        SyncPrimaryKeyOrdering::Enum(labels) => {
            enum_field_expression(&quote_sql_literal(value), labels)
        }
    }
}

fn enum_field_expression(value: &str, labels: &[String]) -> String {
    let labels = labels
        .iter()
        .map(|label| quote_sql_literal(label))
        .collect::<Vec<_>>()
        .join(", ");
    format!("FIELD({value}, {labels})")
}

fn quote_ident_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn non_primary_columns(table: &SyncTable) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| !table.primary_key.contains(column))
        .cloned()
        .collect()
}

fn primary_key_predicates(primary_key: &[String]) -> Vec<String> {
    primary_key
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect()
}

fn required_non_null_values(
    table: &SyncTable,
    row: &DatabaseRow,
    columns: &[String],
    label: &str,
) -> Result<Vec<Value>, String> {
    columns
        .iter()
        .map(|column| {
            let value = row
                .values
                .get(column)
                .ok_or_else(|| format!("{label} column `{column}` is absent"))?
                .clone()
                .ok_or_else(|| format!("{label} column `{column}` is NULL"))?;
            parameter_value(table, column, value)
        })
        .collect()
}

fn ordered_values(
    table: &SyncTable,
    row: &DatabaseRow,
    columns: &[String],
) -> Result<Vec<Value>, String> {
    columns
        .iter()
        .map(|column| match row.values.get(column).cloned().flatten() {
            Some(value) => parameter_value(table, column, value),
            None => Ok(Value::NULL),
        })
        .collect()
}

fn primary_key_params(
    table: &SyncTable,
    primary_key: &[String],
    label: &str,
) -> Result<Vec<Value>, String> {
    if primary_key.len() != table.primary_key.len() {
        return Err(format!(
            "{label} width mismatch for `{}`: expected {}, found {}",
            table.name,
            table.primary_key.len(),
            primary_key.len()
        ));
    }
    table
        .primary_key
        .iter()
        .zip(primary_key)
        .map(|(column, value)| primary_key_parameter_value(table, column, value))
        .collect()
}

fn primary_key_parameter_value(
    table: &SyncTable,
    column: &str,
    value: &str,
) -> Result<Value, String> {
    let Some(labels) = table.enum_columns.get(column) else {
        return parameter_value(table, column, value.to_string());
    };
    let ordinal = labels
        .iter()
        .position(|label| label == value)
        .map(|index| u64::try_from(index + 1).expect("ENUM label index fits u64"))
        .ok_or_else(|| {
            format!(
                "ENUM primary-key column `{column}` in `{}` has undeclared label `{value}`",
                table.name
            )
        })?;
    Ok(Value::UInt(ordinal))
}

fn parameter_value(table: &SyncTable, column: &str, value: String) -> Result<Value, String> {
    if table.enum_columns.contains_key(column) {
        return enum_ordinal_parameter_value(table, column, &value);
    }
    if table
        .bit_columns
        .iter()
        .any(|bit_column| bit_column == column)
    {
        return value.parse::<u64>().map(Value::UInt).map_err(|error| {
            format!(
                "BIT column `{column}` in `{}` has invalid unsigned value `{value}`: {error}",
                table.name
            )
        });
    }
    if table
        .mediumblob_columns
        .iter()
        .any(|blob_column| blob_column == column)
    {
        return hex_bytes_parameter_value(table, column, &value);
    }
    Ok(string_param(value))
}

fn enum_ordinal_parameter_value(
    table: &SyncTable,
    column: &str,
    value: &str,
) -> Result<Value, String> {
    let labels = table
        .enum_columns
        .get(column)
        .expect("ENUM metadata exists for an ENUM column");
    let ordinal = value.parse::<u64>().map_err(|error| {
        format!(
            "ENUM column `{column}` in `{}` has invalid internal index `{value}`: {error}",
            table.name
        )
    })?;
    if ordinal > labels.len() as u64 {
        return Err(format!(
            "ENUM column `{column}` in `{}` has internal index `{ordinal}` outside its declaration",
            table.name
        ));
    }
    Ok(Value::UInt(ordinal))
}

fn hex_bytes_parameter_value(
    table: &SyncTable,
    column: &str,
    value: &str,
) -> Result<Value, String> {
    if !value.len().is_multiple_of(2) {
        return Err(format!(
            "MEDIUMBLOB column `{column}` in `{}` has invalid hexadecimal value `{value}`",
            table.name
        ));
    }
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(decode_hex_byte)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|()| {
            format!(
                "MEDIUMBLOB column `{column}` in `{}` has invalid hexadecimal value `{value}`",
                table.name
            )
        })?;
    Ok(Value::Bytes(bytes))
}

fn decode_hex_byte(pair: &[u8]) -> Result<u8, ()> {
    let high = decode_hex_digit(pair[0])?;
    let low = decode_hex_digit(pair[1])?;
    Ok(high << 4 | low)
}

fn decode_hex_digit(value: u8) -> Result<u8, ()> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(()),
    }
}

fn row_placeholders(column_count: usize, row_count: usize) -> String {
    let row = format!("({})", vec!["?"; column_count].join(", "));
    vec![row; row_count].join(", ")
}

fn sync_select_columns(table: &SyncTable) -> String {
    table
        .columns
        .iter()
        .map(|column| {
            if table.enum_columns.contains_key(column) || table.bit_columns.contains(column) {
                format!(
                    "CAST({} AS UNSIGNED) AS {}",
                    quote_ident(column),
                    quote_ident(column)
                )
            } else if table.mediumblob_columns.contains(column) {
                format!("HEX({}) AS {}", quote_ident(column), quote_ident(column))
            } else {
                quote_ident(column)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn string_param(value: String) -> Value {
    Value::Bytes(value.into_bytes())
}
