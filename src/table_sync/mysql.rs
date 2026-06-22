use super::{SyncChunkRequest, SyncTableReader, TableSyncError};
use crate::snapshot::SnapshotRow;
use std::collections::BTreeMap;
use std::process::Command;

pub(super) struct MySqlSyncReader {
    config: crate::mysql_snapshot::MySqlConnectionConfig,
}

impl MySqlSyncReader {
    pub fn new(config: crate::mysql_snapshot::MySqlConnectionConfig) -> Self {
        Self { config }
    }
}

impl SyncTableReader for MySqlSyncReader {
    fn read_rows(&self, request: &SyncChunkRequest) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let sql = build_sync_select_sql(request);
        let output = run_mysql_query(&self.config, &sql)?;
        parse_sync_rows(&request.columns, &request.primary_key, &output)
    }
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

fn run_mysql_query(
    config: &crate::mysql_snapshot::MySqlConnectionConfig,
    sql: &str,
) -> Result<String, TableSyncError> {
    let output = Command::new(&config.mariadb)
        .args([
            "--batch",
            "--raw",
            "--skip-column-names",
            "--default-character-set=utf8mb4",
            "--host",
            &config.host,
            "--port",
            &config.port.to_string(),
            "--user",
            &config.user,
            &config.database,
            "-e",
            sql,
        ])
        .env("MYSQL_PWD", &config.password)
        .output()
        .map_err(|error| TableSyncError::Read(format!("failed to run mariadb: {error}")))?;

    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(TableSyncError::Read(format!(
        "mariadb exited with {}: {}",
        output.status,
        stderr.trim()
    )))
}

fn parse_sync_rows(
    columns: &[String],
    primary_key: &[String],
    output: &str,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    output
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| parse_sync_row(columns, primary_key, line))
        .collect()
}

fn parse_sync_row(
    columns: &[String],
    primary_key: &[String],
    line: &str,
) -> Result<SnapshotRow, TableSyncError> {
    let fields = line.split('\t').map(str::to_string).collect::<Vec<_>>();
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
