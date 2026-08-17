use crate::mysql_support::quote_sql_literal;
use crate::snapshot::SnapshotError;
use crate::target::{SqlStatement, TargetExecuteError, render_sql_statement};
use mysql::Value;

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

pub(crate) fn build_target_column_select_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
        quote_sql_literal(table)
    )
}
