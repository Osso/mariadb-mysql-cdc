use super::model::{SyncChunkReadRequest, SyncPrimaryKeyOrdering, SyncTable};
use crate::mysql_support::{quote_ident, quote_sql_literal};
use crate::snapshot::SnapshotRow;
use crate::target::SqlStatement;
use mysql::Value;

pub(crate) fn build_sync_select_sql(table: &SyncTable, request: &SyncChunkReadRequest) -> String {
    let columns = quote_ident_list(&table.columns);
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

pub(crate) fn build_lock_table_write_sql(database: &str, table: &str) -> String {
    format!(
        "LOCK TABLES {}.{} WRITE",
        quote_ident(database),
        quote_ident(table)
    )
}

pub(crate) fn build_strict_insert_statement(
    table: &SyncTable,
    rows: &[SnapshotRow],
) -> SqlStatement {
    let columns = quote_ident_list(&table.columns);
    let placeholders = row_placeholders(table.columns.len(), rows.len());
    let params = rows
        .iter()
        .flat_map(|row| ordered_values(row, &table.columns))
        .collect();
    SqlStatement {
        sql: format!(
            "INSERT INTO {} ({columns}) VALUES {placeholders}",
            quote_ident(&table.name)
        ),
        params,
    }
}

pub(crate) fn build_strict_update_statement(table: &SyncTable, row: &SnapshotRow) -> SqlStatement {
    let changed_columns = non_primary_columns(table);
    let assignments = changed_columns
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let predicates = primary_key_predicates(&table.primary_key).join(" AND ");
    let mut params = ordered_values(row, &changed_columns);
    params.extend(row.primary_key.iter().cloned().map(string_param));
    SqlStatement {
        sql: format!(
            "UPDATE {} SET {assignments} WHERE {predicates}",
            quote_ident(&table.name)
        ),
        params,
    }
}

pub(crate) fn build_strict_update_rows_statement(
    table: &SyncTable,
    rows: &[SnapshotRow],
) -> SqlStatement {
    let changed_columns = non_primary_columns(table);
    let assignments = changed_columns
        .iter()
        .map(|column| strict_case_assignment(column, &table.primary_key, rows.len()))
        .collect::<Vec<_>>()
        .join(", ");
    let row_filter = primary_key_row_filter(&table.primary_key, rows.len());
    let order_by = quote_ident_list(&table.primary_key);
    let params = ordered_update_params(&changed_columns, rows);
    SqlStatement {
        sql: format!(
            "UPDATE {} SET {assignments} WHERE {row_filter} ORDER BY {order_by}",
            quote_ident(&table.name)
        ),
        params,
    }
}

pub(crate) fn build_strict_delete_statement(
    table: &SyncTable,
    primary_key: &[String],
) -> SqlStatement {
    SqlStatement {
        sql: format!(
            "DELETE FROM {} WHERE {}",
            quote_ident(&table.name),
            primary_key_predicates(&table.primary_key).join(" AND ")
        ),
        params: primary_key.iter().cloned().map(string_param).collect(),
    }
}

pub(crate) fn build_strict_delete_rows_statement(
    table: &SyncTable,
    primary_keys: &[Vec<String>],
) -> SqlStatement {
    SqlStatement {
        sql: format!(
            "DELETE FROM {} WHERE {}",
            quote_ident(&table.name),
            primary_key_row_filter(&table.primary_key, primary_keys.len())
        ),
        params: primary_keys
            .iter()
            .flatten()
            .cloned()
            .map(string_param)
            .collect(),
    }
}

fn strict_case_assignment(column: &str, primary_key: &[String], row_count: usize) -> String {
    let predicate = primary_key_predicates(primary_key).join(" AND ");
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

fn ordered_update_params(changed_columns: &[String], rows: &[SnapshotRow]) -> Vec<Value> {
    let changed_values = changed_columns.iter().flat_map(|column| {
        rows.iter().flat_map(|row| {
            let mut params = row
                .primary_key
                .iter()
                .cloned()
                .map(string_param)
                .collect::<Vec<_>>();
            params.extend(ordered_values(row, std::slice::from_ref(column)));
            params
        })
    });
    let filter_values = rows
        .iter()
        .flat_map(|row| row.primary_key.iter().cloned().map(string_param));
    changed_values.chain(filter_values).collect()
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

fn ordered_values(row: &SnapshotRow, columns: &[String]) -> Vec<Value> {
    columns
        .iter()
        .map(|column| match row.values.get(column).cloned().flatten() {
            Some(value) => string_param(value),
            None => Value::NULL,
        })
        .collect()
}

fn row_placeholders(column_count: usize, row_count: usize) -> String {
    let row = format!("({})", vec!["?"; column_count].join(", "));
    vec![row; row_count].join(", ")
}

fn string_param(value: String) -> Value {
    Value::Bytes(value.into_bytes())
}
