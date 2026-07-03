use super::{SyncMode, SyncTableReport, TableSyncError};
use crate::live::TargetMySqlConfig;
use crate::mysql_client::PersistentProgressWriter;
use crate::mysql_support::{
    qualified_table_parts, quote_ident, quote_identifier_path, quote_sql_literal,
};
use std::cell::RefCell;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncTableProgress {
    pub table: String,
    pub last_primary_key: Option<Vec<String>>,
    pub chunks: u64,
    pub rows_scanned: u64,
    pub total_rows: Option<u64>,
    pub inserts: u64,
    pub updates: u64,
    pub extra_target_rows: u64,
    pub mode: SyncMode,
    pub status: SyncProgressStatus,
    pub last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncProgressStatus {
    Running,
    Complete,
    Error,
}

pub trait SyncProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError>;
    fn load(&self, table: &str) -> Result<Option<SyncTableProgress>, TableSyncError>;
    fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError>;
    fn save_error(&mut self, table: &str, error: &TableSyncError) -> Result<(), TableSyncError>;
}

pub struct NoopSyncProgressStore;

impl SyncProgressStore for NoopSyncProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn load(&self, _table: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        Ok(None)
    }

    fn save(&mut self, _progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn save_error(&mut self, _table: &str, _error: &TableSyncError) -> Result<(), TableSyncError> {
        Ok(())
    }
}

pub struct MySqlSyncProgressStore {
    target: TargetMySqlConfig,
    table: String,
    writer: RefCell<Option<PersistentProgressWriter>>,
}

impl MySqlSyncProgressStore {
    pub fn new(_mariadb: String, target: TargetMySqlConfig, table: String) -> Self {
        Self {
            target,
            table,
            writer: RefCell::new(None),
        }
    }
}

impl SyncProgressStore for MySqlSyncProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        if let Some(schema_sql) = build_create_progress_schema_sql(&self.table) {
            self.execute(schema_sql)?;
        }
        self.execute(build_create_progress_table_sql(&self.table))?;
        self.execute(build_add_total_rows_column_sql(
            &self.target.database,
            &self.table,
        ))
    }

    fn load(&self, table: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        let output = self.query(build_progress_select_sql(&self.table, table))?;
        parse_progress_row(table, &output)
    }

    fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.execute(build_progress_upsert_sql(&self.table, progress))
    }

    fn save_error(&mut self, table: &str, error: &TableSyncError) -> Result<(), TableSyncError> {
        self.execute(build_progress_error_sql(&self.table, table, error))
    }
}

impl MySqlSyncProgressStore {
    fn execute(&self, statement: String) -> Result<(), TableSyncError> {
        self.writer()?.execute_table_sync_progress_sql(statement)
    }

    fn query(&self, sql: String) -> Result<String, TableSyncError> {
        self.writer()?.query_table_sync_progress_tsv(sql)
    }

    fn writer(&self) -> Result<std::cell::RefMut<'_, PersistentProgressWriter>, TableSyncError> {
        if self.writer.borrow().is_none() {
            let writer = PersistentProgressWriter::new(&self.target, self.table.clone())?;
            self.writer.replace(Some(writer));
        }
        Ok(std::cell::RefMut::map(self.writer.borrow_mut(), |writer| {
            writer.as_mut().expect("sync progress writer initialized")
        }))
    }
}

impl SyncTableProgress {
    pub fn started(table: String, mode: SyncMode) -> Self {
        Self {
            table,
            last_primary_key: None,
            chunks: 0,
            rows_scanned: 0,
            total_rows: None,
            inserts: 0,
            updates: 0,
            extra_target_rows: 0,
            mode,
            status: SyncProgressStatus::Running,
            last_error: None,
        }
    }

    pub fn mark_running(&mut self, mode: SyncMode) {
        self.mode = mode;
        self.status = SyncProgressStatus::Running;
        self.last_error = None;
    }

    pub fn mark_complete(&mut self) {
        self.status = SyncProgressStatus::Complete;
        self.last_error = None;
    }

    pub fn record_chunk(&mut self, report: &SyncTableReport, last_primary_key: Vec<String>) {
        self.last_primary_key = Some(last_primary_key);
        self.chunks = report.chunks;
        self.rows_scanned = report.rows_scanned;
        self.inserts = report.inserts;
        self.updates = report.updates;
        self.extra_target_rows = report.extra_target_rows;
        self.status = SyncProgressStatus::Running;
    }

    pub fn report(&self) -> SyncTableReport {
        SyncTableReport {
            table: self.table.clone(),
            chunks: self.chunks,
            rows_scanned: self.rows_scanned,
            inserts: self.inserts,
            updates: self.updates,
            extra_target_rows: self.extra_target_rows,
        }
    }
}

pub(crate) fn build_create_progress_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\
table_name VARCHAR(255) NOT NULL PRIMARY KEY,\
last_primary_key_json TEXT NULL,\
chunks BIGINT UNSIGNED NOT NULL DEFAULT 0,\
rows_scanned BIGINT UNSIGNED NOT NULL DEFAULT 0,\
total_rows BIGINT UNSIGNED NULL,\
inserts_applied BIGINT UNSIGNED NOT NULL DEFAULT 0,\
updates_applied BIGINT UNSIGNED NOT NULL DEFAULT 0,\
extra_target_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,\
mode VARCHAR(16) NOT NULL,\
status VARCHAR(16) NOT NULL,\
last_error TEXT NULL,\
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\
)",
        quote_identifier_path(table)
    )
}

pub(crate) fn build_add_total_rows_column_sql(
    default_schema: &str,
    progress_table: &str,
) -> String {
    let (schema, table) = qualified_table_parts(default_schema, progress_table);
    let alter_table = format!(
        "ALTER TABLE {} ADD COLUMN total_rows BIGINT UNSIGNED NULL AFTER rows_scanned",
        quote_identifier_path(progress_table)
    );
    format!(
        "SET @cdc_total_rows_ddl = (SELECT IF(COUNT(*) = 0, {}, 'SELECT 1') FROM information_schema.columns WHERE table_schema = {} AND table_name = {} AND column_name = 'total_rows'); PREPARE cdc_total_rows_stmt FROM @cdc_total_rows_ddl; EXECUTE cdc_total_rows_stmt; DEALLOCATE PREPARE cdc_total_rows_stmt",
        quote_sql_literal(&alter_table),
        quote_sql_literal(&schema),
        quote_sql_literal(&table)
    )
}

pub(crate) fn build_create_progress_schema_sql(table: &str) -> Option<String> {
    let schema = table.split_once('.')?.0;
    Some(format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        quote_ident(schema)
    ))
}

fn build_progress_select_sql(progress_table: &str, table: &str) -> String {
    format!(
        "SELECT COALESCE(last_primary_key_json, ''), chunks, rows_scanned, COALESCE(total_rows, ''), inserts_applied, updates_applied, extra_target_rows, mode, status, COALESCE(last_error, '') FROM {} WHERE table_name = {} LIMIT 1",
        quote_identifier_path(progress_table),
        quote_sql_literal(table)
    )
}

pub(crate) fn build_progress_upsert_sql(
    progress_table: &str,
    progress: &SyncTableProgress,
) -> String {
    let last_primary_key = progress
        .last_primary_key
        .as_ref()
        .map(|values| json_string(values))
        .unwrap_or_default();
    format!(
        "INSERT INTO {} (table_name,last_primary_key_json,chunks,rows_scanned,total_rows,inserts_applied,updates_applied,extra_target_rows,mode,status,last_error) VALUES ({},{},{},{},{},{},{},{},{},{},NULL) ON DUPLICATE KEY UPDATE last_primary_key_json=VALUES(last_primary_key_json),chunks=VALUES(chunks),rows_scanned=VALUES(rows_scanned),total_rows=VALUES(total_rows),inserts_applied=VALUES(inserts_applied),updates_applied=VALUES(updates_applied),extra_target_rows=VALUES(extra_target_rows),mode=VALUES(mode),status=VALUES(status),last_error=NULL",
        quote_identifier_path(progress_table),
        quote_sql_literal(&progress.table),
        nullable_sql_literal(&last_primary_key),
        progress.chunks,
        progress.rows_scanned,
        nullable_u64(progress.total_rows),
        progress.inserts,
        progress.updates,
        progress.extra_target_rows,
        quote_sql_literal(progress.mode.as_str()),
        quote_sql_literal(progress.status.as_str())
    )
}

fn build_progress_error_sql(progress_table: &str, table: &str, error: &TableSyncError) -> String {
    format!(
        "INSERT INTO {} (table_name,mode,status,last_error) VALUES ({},'unknown','error',{}) ON DUPLICATE KEY UPDATE status='error',last_error=VALUES(last_error)",
        quote_identifier_path(progress_table),
        quote_sql_literal(table),
        quote_sql_literal(&error.to_string())
    )
}

fn parse_progress_row(
    table: &str,
    output: &str,
) -> Result<Option<SyncTableProgress>, TableSyncError> {
    let Some(line) = output.lines().find(|line| !line.trim().is_empty()) else {
        return Ok(None);
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() != 10 {
        return Err(TableSyncError::Progress(format!(
            "progress row has {} fields, expected 10",
            fields.len()
        )));
    }

    Ok(Some(SyncTableProgress {
        table: table.to_string(),
        last_primary_key: parse_primary_key_json(fields[0])?,
        chunks: parse_u64("chunks", fields[1])?,
        rows_scanned: parse_u64("rows_scanned", fields[2])?,
        total_rows: parse_optional_u64("total_rows", fields[3])?,
        inserts: parse_u64("inserts_applied", fields[4])?,
        updates: parse_u64("updates_applied", fields[5])?,
        extra_target_rows: parse_u64("extra_target_rows", fields[6])?,
        mode: SyncMode::parse(fields[7])?,
        status: SyncProgressStatus::parse(fields[8])?,
        last_error: non_empty(fields[9]),
    }))
}

fn parse_primary_key_json(value: &str) -> Result<Option<Vec<String>>, TableSyncError> {
    if value.is_empty() {
        return Ok(None);
    }

    serde_json::from_str(value)
        .map(Some)
        .map_err(|error| TableSyncError::Progress(format!("invalid primary key json: {error}")))
}

fn parse_u64(field: &str, value: &str) -> Result<u64, TableSyncError> {
    value
        .parse()
        .map_err(|_| TableSyncError::Progress(format!("{field} must be an integer")))
}

fn parse_optional_u64(field: &str, value: &str) -> Result<Option<u64>, TableSyncError> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_u64(field, value).map(Some)
}

fn json_string(values: &[String]) -> String {
    serde_json::to_string(values).expect("primary key JSON serialization cannot fail")
}

fn non_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

impl SyncMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry-run",
            Self::Apply => "apply",
        }
    }

    fn parse(value: &str) -> Result<Self, TableSyncError> {
        match value {
            "dry-run" => Ok(Self::DryRun),
            "apply" => Ok(Self::Apply),
            other => Err(TableSyncError::Progress(format!(
                "unknown sync mode in progress: {other}"
            ))),
        }
    }
}

impl SyncProgressStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Error => "error",
        }
    }

    fn parse(value: &str) -> Result<Self, TableSyncError> {
        match value {
            "running" => Ok(Self::Running),
            "complete" => Ok(Self::Complete),
            "error" => Ok(Self::Error),
            other => Err(TableSyncError::Progress(format!(
                "unknown sync progress status: {other}"
            ))),
        }
    }
}

fn nullable_sql_literal(value: &str) -> String {
    if value.is_empty() {
        "NULL".to_string()
    } else {
        quote_sql_literal(value)
    }
}

fn nullable_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_progress_table_sql_allows_cdc_schema_prefix() {
        let sql = build_create_progress_table_sql("cdc.table_sync_progress");

        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `cdc`.`table_sync_progress`"));
        assert!(sql.contains("last_primary_key_json TEXT"));
        assert!(sql.contains("total_rows BIGINT UNSIGNED NULL"));
        assert!(sql.contains("status VARCHAR(16)"));
    }

    #[test]
    fn add_total_rows_column_sql_is_conditional_for_existing_tables() {
        let sql = build_add_total_rows_column_sql("globalcomix", "cdc.table_sync_progress");

        assert!(sql.contains("information_schema.columns"));
        assert!(sql.contains("column_name = 'total_rows'"));
        assert!(sql.contains("ALTER TABLE `cdc`.`table_sync_progress` ADD COLUMN total_rows"));
    }

    #[test]
    fn create_progress_schema_sql_uses_dotted_prefix() {
        let sql =
            build_create_progress_schema_sql("cdc.table_sync_progress").expect("schema create sql");

        assert_eq!(sql, "CREATE DATABASE IF NOT EXISTS `cdc`");
        assert_eq!(
            build_create_progress_schema_sql("table_sync_progress"),
            None
        );
    }

    #[test]
    fn upsert_progress_sql_stores_last_primary_key_and_counts() {
        let progress = SyncTableProgress {
            table: "releases".to_string(),
            last_primary_key: Some(vec!["42".to_string()]),
            chunks: 2,
            rows_scanned: 2000,
            total_rows: Some(5000),
            inserts: 3,
            updates: 4,
            extra_target_rows: 5,
            mode: SyncMode::Apply,
            status: SyncProgressStatus::Running,
            last_error: None,
        };

        let sql = build_progress_upsert_sql("cdc.table_sync_progress", &progress);

        assert!(sql.contains("'releases'"));
        assert!(sql.contains("'[\"42\"]'"));
        assert!(sql.contains("2,2000,5000,3,4,5"));
        assert!(sql.contains("'apply','running'"));
    }

    #[test]
    fn parse_progress_row_restores_resume_state() {
        let row = "[\"42\"]\t2\t2000\t5000\t3\t4\t5\tapply\trunning\t";

        let progress = parse_progress_row("releases", row)
            .expect("parse progress")
            .expect("progress row");

        assert_eq!(progress.table, "releases");
        assert_eq!(progress.last_primary_key, Some(vec!["42".to_string()]));
        assert_eq!(progress.chunks, 2);
        assert_eq!(progress.rows_scanned, 2000);
        assert_eq!(progress.total_rows, Some(5000));
        assert_eq!(progress.inserts, 3);
        assert_eq!(progress.updates, 4);
        assert_eq!(progress.extra_target_rows, 5);
        assert_eq!(progress.mode, SyncMode::Apply);
        assert_eq!(progress.status, SyncProgressStatus::Running);
    }
}
