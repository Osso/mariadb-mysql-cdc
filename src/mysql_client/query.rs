use crate::mysql_support::{quote_ident, quote_identifier_path, quote_sql_literal};
use crate::snapshot::{
    ChunkRequest, SnapshotError, SnapshotProgress, SnapshotRow, SnapshotTable,
    TableSnapshotProgress,
};
use crate::table_sync::TableSyncError;
use crate::target::{SqlStatement, TargetExecuteError, render_sql_statement};
use mysql::Value;
use std::collections::BTreeMap;

pub(crate) fn snapshot_query_error(error: mysql::Error) -> SnapshotError {
    SnapshotError::InvalidTable(format!("source mysql query failed: {error}"))
}

pub(crate) fn target_query_error(error: mysql::Error) -> TargetExecuteError {
    let message = format!("target mysql query failed: {error}");
    match error {
        mysql::Error::MySqlError(server_error) => {
            TargetExecuteError::from_mysql(server_error.code, message)
        }
        _ => TargetExecuteError::new(message),
    }
}

pub(crate) fn progress_query_error(error: mysql::Error) -> TableSyncError {
    TableSyncError::Progress(format!("target progress query failed: {error}"))
}

pub(crate) type SnapshotProgressRow = (String, String, u64, String);

pub(crate) fn rows_to_tsv(rows: Vec<mysql::Row>) -> String {
    rows.into_iter()
        .map(row_to_tsv)
        .collect::<Vec<_>>()
        .join("\n")
}

fn row_to_tsv(row: mysql::Row) -> String {
    row.unwrap()
        .into_iter()
        .map(value_to_string)
        .map(|value| value.unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\t")
}

pub(crate) fn build_snapshot_progress_select_sql(progress_table: &str) -> String {
    format!(
        "SELECT table_name, COALESCE(last_primary_key_json, ''), rows_scanned, status FROM {}",
        quote_identifier_path(progress_table)
    )
}

pub(crate) fn build_progress_error_message_sql(
    progress_table: &str,
    table: &str,
    error: &str,
) -> String {
    format!(
        "INSERT INTO {} (table_name,mode,status,last_error) VALUES ({},'apply','error',{}) ON DUPLICATE KEY UPDATE status='error',last_error=VALUES(last_error)",
        quote_identifier_path(progress_table),
        quote_sql_literal(table),
        quote_sql_literal(error)
    )
}

pub(crate) fn snapshot_progress_from_rows(
    rows: Vec<SnapshotProgressRow>,
) -> Result<SnapshotProgress, TableSyncError> {
    let tables = rows
        .into_iter()
        .map(snapshot_table_progress_from_row)
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(SnapshotProgress { tables })
}

fn snapshot_table_progress_from_row(
    row: SnapshotProgressRow,
) -> Result<(String, TableSnapshotProgress), TableSyncError> {
    let (table, primary_key_json, rows_copied, status) = row;
    let progress = TableSnapshotProgress {
        last_primary_key: parse_progress_primary_key(&primary_key_json)?,
        rows_copied,
        complete: status == "complete",
    };
    Ok((table, progress))
}

fn parse_progress_primary_key(value: &str) -> Result<Option<Vec<String>>, TableSyncError> {
    if value.is_empty() {
        return Ok(None);
    }

    serde_json::from_str(value)
        .map(Some)
        .map_err(|error| TableSyncError::Progress(format!("invalid primary key json: {error}")))
}

pub(crate) fn snapshot_row_from_mysql_row(
    request: &ChunkRequest,
    row: mysql::Row,
) -> Result<SnapshotRow, SnapshotError> {
    let values = row
        .unwrap()
        .into_iter()
        .map(value_to_string)
        .collect::<Vec<_>>();
    let values_by_column = request
        .selected_columns
        .iter()
        .cloned()
        .zip(values)
        .collect::<BTreeMap<_, _>>();
    let primary_key = request
        .primary_key
        .iter()
        .map(|column| {
            let value = values_by_column.get(column).cloned().ok_or_else(|| {
                SnapshotError::InvalidTable(format!(
                    "primary-key column `{column}` was not selected"
                ))
            })?;
            value.ok_or_else(|| {
                SnapshotError::InvalidTable(format!("primary-key column `{column}` was NULL"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SnapshotRow {
        primary_key,
        values: values_by_column,
    })
}

pub(crate) fn row_to_strings(row: mysql::Row) -> Vec<Option<String>> {
    row.unwrap().into_iter().map(value_to_string).collect()
}

pub(crate) fn value_to_string(value: Value) -> Option<String> {
    match value {
        Value::NULL => None,
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).to_string()),
        Value::Int(value) => Some(value.to_string()),
        Value::UInt(value) => Some(value.to_string()),
        Value::Float(value) => Some(value.to_string()),
        Value::Double(value) => Some(value.to_string()),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            Some(format_date(year, month, day, hour, minute, second, micros))
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            Some(format_time(negative, days, hours, minutes, seconds, micros))
        }
    }
}

fn format_date(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> String {
    let base = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
    if micros == 0 {
        base
    } else {
        format!("{base}.{micros:06}")
    }
}

fn format_time(
    negative: bool,
    days: u32,
    hours: u8,
    minutes: u8,
    seconds: u8,
    micros: u32,
) -> String {
    let sign = if negative { "-" } else { "" };
    let total_hours = days * 24 + u32::from(hours);
    let base = format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}");
    if micros == 0 {
        base
    } else {
        format!("{base}.{micros:06}")
    }
}

pub(crate) fn generated_column_retry_sql(statement: &SqlStatement, error: &str) -> Option<String> {
    let generated_column = generated_column_from_error(error)?;
    let rendered = render_sql_statement(statement).ok()?;
    strip_insert_column(&rendered, &generated_column)
}

fn generated_column_from_error(error: &str) -> Option<String> {
    let marker = "generated column '";
    let start = error.find(marker)? + marker.len();
    let rest = &error[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

fn strip_insert_column(sql: &str, generated_column: &str) -> Option<String> {
    crate::live::strip_insert_column_for_retry(sql, generated_column)
}

pub(crate) fn build_stream_lease_sql(lease_name: &str) -> String {
    format!(
        "SELECT GET_LOCK(SHA2({},256),0)",
        quote_sql_literal(lease_name)
    )
}

pub(crate) fn ensure_stream_lease_acquired(
    lease_name: &str,
    acquired: Option<u8>,
) -> Result<(), TargetExecuteError> {
    match acquired {
        Some(1) => Ok(()),
        _ => Err(TargetExecuteError::new(format!(
            "stream lease `{lease_name}` is already held"
        ))),
    }
}

pub(crate) fn snapshot_boundary_offsets(total_rows: u64, workers: usize) -> Vec<u64> {
    if total_rows == 0 || workers <= 1 {
        return Vec::new();
    }

    let mut offsets = (1..workers)
        .map(|worker| snapshot_boundary_offset(total_rows, workers, worker))
        .filter(|offset| *offset < total_rows)
        .collect::<Vec<_>>();
    offsets.dedup();
    offsets
}

fn snapshot_boundary_offset(total_rows: u64, workers: usize, worker: usize) -> u64 {
    let numerator = total_rows * worker as u64;
    numerator.div_ceil(workers as u64).saturating_sub(1)
}

pub(crate) fn build_snapshot_boundary_select_sql(table: &SnapshotTable, offset: u64) -> String {
    let primary_key = quote_column_list(&table.primary_key);
    format!(
        "SELECT {primary_key} FROM {} ORDER BY {primary_key} LIMIT 1 OFFSET {offset}",
        quote_ident(&table.name)
    )
}

pub(crate) fn build_target_column_select_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
        quote_sql_literal(table)
    )
}

fn quote_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}
