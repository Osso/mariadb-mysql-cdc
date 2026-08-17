use super::model::{SyncProgressRow, SyncProgressStatus, SyncStage};
use crate::mysql_support::{quote_ident, quote_identifier_path, quote_sql_literal};
use crate::target::SqlStatement;
use mysql::Value;

pub(crate) fn build_create_sync_progress_schema_sql(table: &str) -> Option<String> {
    let schema = table.split_once('.')?.0;
    Some(format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        quote_ident(schema)
    ))
}

pub(crate) fn build_create_sync_progress_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (run_id VARCHAR(128) NOT NULL, stage VARCHAR(32) NOT NULL, table_name VARCHAR(255) NOT NULL, run_spec_json LONGTEXT NOT NULL, last_primary_key_json TEXT NULL, chunks BIGINT UNSIGNED NOT NULL DEFAULT 0, rows_scanned BIGINT UNSIGNED NOT NULL DEFAULT 0, inserts_applied BIGINT UNSIGNED NOT NULL DEFAULT 0, updates_applied BIGINT UNSIGNED NOT NULL DEFAULT 0, deletes_applied BIGINT UNSIGNED NOT NULL DEFAULT 0, status VARCHAR(16) NOT NULL, last_error TEXT NULL, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), completed_at TIMESTAMP(6) NULL, CHECK (stage IN ('prerequisite_schema', 'rows', 'final_constraints')), CHECK (status IN ('running', 'complete', 'error')), CHECK (JSON_VALID(run_spec_json)), CHECK (last_primary_key_json IS NULL OR JSON_VALID(last_primary_key_json)), PRIMARY KEY (run_id, stage, table_name)) ENGINE=InnoDB",
        quote_identifier_path(table)
    )
}

pub(crate) fn build_sync_progress_select_sql(
    table: &str,
    run_id: &str,
    stage: SyncStage,
    table_name: &str,
) -> String {
    format!(
        "SELECT run_id, stage, table_name, run_spec_json, COALESCE(last_primary_key_json, ''), chunks, rows_scanned, inserts_applied, updates_applied, deletes_applied, status, COALESCE(last_error, ''), DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'), DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f'), COALESCE(DATE_FORMAT(completed_at, '%Y-%m-%d %H:%i:%s.%f'), '') FROM {} WHERE run_id = {} AND stage = {} AND table_name = {} LIMIT 1",
        quote_identifier_path(table),
        quote_sql_literal(run_id),
        quote_sql_literal(stage.as_str()),
        quote_sql_literal(table_name)
    )
}

pub(crate) fn build_sync_progress_upsert_sql(
    table: &str,
    progress: &SyncProgressRow,
) -> SqlStatement {
    SqlStatement {
        sql: format!(
            "INSERT INTO {} (run_id, stage, table_name, run_spec_json, last_primary_key_json, chunks, rows_scanned, inserts_applied, updates_applied, deletes_applied, status, last_error, completed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new ON DUPLICATE KEY UPDATE last_primary_key_json = new.last_primary_key_json, chunks = new.chunks, rows_scanned = new.rows_scanned, inserts_applied = new.inserts_applied, updates_applied = new.updates_applied, deletes_applied = new.deletes_applied, status = new.status, last_error = new.last_error, completed_at = new.completed_at",
            quote_identifier_path(table)
        ),
        params: vec![
            string_param(&progress.run_id),
            string_param(progress.stage.as_str()),
            string_param(&progress.table_name),
            string_param(&progress.run_spec_json),
            optional_json_param(progress.last_primary_key.as_ref()),
            Value::UInt(progress.chunks),
            Value::UInt(progress.rows_scanned),
            Value::UInt(progress.inserts),
            Value::UInt(progress.updates),
            Value::UInt(progress.deletes),
            string_param(progress.status.as_str()),
            optional_string_param(progress.last_error.as_deref()),
            optional_string_param(progress.completed_at.as_deref()),
        ],
    }
}

pub(crate) fn parse_sync_progress_row(output: &str) -> Result<SyncProgressRow, String> {
    let line = output.trim_end_matches(['\r', '\n']);
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() != 15 {
        return Err(format!(
            "sync progress row has {} fields, expected 15",
            fields.len()
        ));
    }

    validate_json("run specification", fields[3])?;
    Ok(SyncProgressRow {
        run_id: fields[0].to_string(),
        stage: SyncStage::parse(fields[1])?,
        table_name: fields[2].to_string(),
        run_spec_json: fields[3].to_string(),
        last_primary_key: parse_optional_cursor(fields[4])?,
        chunks: parse_count("chunks", fields[5])?,
        rows_scanned: parse_count("rows_scanned", fields[6])?,
        inserts: parse_count("inserts_applied", fields[7])?,
        updates: parse_count("updates_applied", fields[8])?,
        deletes: parse_count("deletes_applied", fields[9])?,
        status: SyncProgressStatus::parse(fields[10])?,
        last_error: optional_string(fields[11]),
        created_at: fields[12].to_string(),
        updated_at: fields[13].to_string(),
        completed_at: optional_string(fields[14]),
    })
}

fn parse_optional_cursor(value: &str) -> Result<Option<Vec<String>>, String> {
    if value.is_empty() {
        return Ok(None);
    }
    serde_json::from_str::<Vec<String>>(value)
        .map(Some)
        .map_err(|error| format!("invalid sync progress cursor JSON: {error}"))
}

fn validate_json(field: &str, value: &str) -> Result<(), String> {
    serde_json::from_str::<serde_json::Value>(value)
        .map(|_| ())
        .map_err(|error| format!("invalid sync progress {field} JSON: {error}"))
}

fn parse_count(field: &str, value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid sync progress {field} `{value}`: {error}"))
}

fn optional_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

fn string_param(value: &str) -> Value {
    Value::Bytes(value.as_bytes().to_vec())
}

fn optional_string_param(value: Option<&str>) -> Value {
    value.map(string_param).unwrap_or(Value::NULL)
}

fn optional_json_param(value: Option<&Vec<String>>) -> Value {
    value
        .map(|value| serde_json::to_string(value).expect("string vector serializes as JSON"))
        .map(|value| Value::Bytes(value.into_bytes()))
        .unwrap_or(Value::NULL)
}
