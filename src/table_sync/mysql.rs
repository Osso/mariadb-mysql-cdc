use super::{SyncChunkRequest, SyncTableReader, TableSyncError};
use crate::mysql_client::PersistentMySqlSource;
use crate::snapshot::{SnapshotError, SnapshotRow};
use std::cell::RefCell;
use std::collections::BTreeMap;

pub(super) struct MySqlSyncReader {
    config: crate::mysql_snapshot::MySqlConnectionConfig,
    source: RefCell<Option<PersistentMySqlSource>>,
}

impl MySqlSyncReader {
    pub fn new(config: crate::mysql_snapshot::MySqlConnectionConfig) -> Self {
        Self {
            config,
            source: RefCell::new(None),
        }
    }

    fn query_rows(&self, sql: &str) -> Result<Vec<Vec<String>>, TableSyncError> {
        self.ensure_source()?
            .query_rows_as_strings(sql)
            .map_err(snapshot_error_to_table_sync)
    }

    fn ensure_source(
        &self,
    ) -> Result<std::cell::RefMut<'_, PersistentMySqlSource>, TableSyncError> {
        if self.source.borrow().is_none() {
            let source =
                PersistentMySqlSource::new(&self.config).map_err(snapshot_error_to_table_sync)?;
            self.source.replace(Some(source));
        }
        Ok(std::cell::RefMut::map(self.source.borrow_mut(), |source| {
            source.as_mut().expect("sync source initialized")
        }))
    }
}

impl SyncTableReader for MySqlSyncReader {
    fn read_rows(&self, request: &SyncChunkRequest) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let sql = build_sync_select_sql(request);
        let rows = self.query_rows(&sql)?;
        parse_sync_rows(&request.columns, &request.primary_key, rows)
    }
}

fn snapshot_error_to_table_sync(error: SnapshotError) -> TableSyncError {
    TableSyncError::Read(error.to_string())
}

pub(crate) fn build_sync_select_sql(request: &SyncChunkRequest) -> String {
    let columns = quote_ident_list(&request.columns);
    let order_by = quote_ident_list(&request.primary_key);
    let bounds = sync_bounds(request);
    format!(
        "SELECT {columns} FROM {}{bounds} ORDER BY {order_by} LIMIT {}",
        quote_ident(&request.table),
        request.limit
    )
}

fn sync_bounds(request: &SyncChunkRequest) -> String {
    let predicates = sync_bound_predicates(request);
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
}

fn sync_bound_predicates(request: &SyncChunkRequest) -> Vec<String> {
    let mut predicates = Vec::new();
    if let Some(start_after) = &request.start_after {
        predicates.push(primary_key_after_predicate(
            &request.primary_key,
            start_after,
        ));
    }
    if let Some(end_at) = &request.end_at {
        predicates.push(primary_key_at_or_before_predicate(
            &request.primary_key,
            end_at,
        ));
    }
    predicates
}

fn parse_sync_rows(
    columns: &[String],
    primary_key: &[String],
    rows: Vec<Vec<String>>,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    rows.into_iter()
        .map(|fields| parse_sync_row(columns, primary_key, fields))
        .collect()
}

fn parse_sync_row(
    columns: &[String],
    primary_key: &[String],
    fields: Vec<String>,
) -> Result<SnapshotRow, TableSyncError> {
    if fields.len() != columns.len() {
        return Err(TableSyncError::Read(format!(
            "sync row has {} fields for {} columns",
            fields.len(),
            columns.len()
        )));
    }

    let values = columns
        .iter()
        .cloned()
        .zip(fields)
        .collect::<BTreeMap<_, _>>();
    let primary_key = primary_key_values(primary_key, &values)?;
    Ok(SnapshotRow {
        primary_key,
        values,
    })
}

fn primary_key_values(
    primary_key: &[String],
    values: &BTreeMap<String, String>,
) -> Result<Vec<String>, TableSyncError> {
    primary_key
        .iter()
        .map(|column| {
            values.get(column).cloned().ok_or_else(|| {
                TableSyncError::Read(format!("primary key column `{column}` missing from row"))
            })
        })
        .collect()
}

fn primary_key_after_predicate(columns: &[String], values: &[String]) -> String {
    primary_key_bound_predicate(columns, values, ">")
}

fn primary_key_at_or_before_predicate(columns: &[String], values: &[String]) -> String {
    format!(
        "NOT ({})",
        primary_key_bound_predicate(columns, values, ">")
    )
}

fn primary_key_bound_predicate(columns: &[String], values: &[String], operator: &str) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, _column)| primary_key_bound_branch(columns, values, index, operator))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn primary_key_bound_branch(
    columns: &[String],
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
    parts.push(format!(
        "{} {operator} {}",
        quote_ident(&columns[index]),
        quote_sql_literal(&values[index])
    ));
    format!("({})", parts.join(" AND "))
}

fn quote_ident_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
