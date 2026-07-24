use crate::snapshot::SnapshotRow;
use serde::Serialize;
use std::fmt;

#[derive(Clone, Debug)]
pub struct SyncTableConfig {
    pub source: crate::mysql_snapshot::MySqlConnectionConfig,
    pub target: crate::live::TargetMySqlConfig,
    pub table: SyncTable,
    pub chunk_size: usize,
    pub mode: SyncMode,
    pub progress_table: String,
    pub run_id: String,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
    pub max_deletes: Option<u64>,
    pub updated_since: Option<UpdatedSince>,
    pub plan_hash: Option<String>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SyncTable {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncChunkRequest {
    pub table: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
    pub updated_since: Option<UpdatedSince>,
    pub limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct UpdatedSince {
    pub column: String,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    DryRun,
    Apply,
    MissingPrimaryKeys,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPhase {
    All,
    DeleteExtras,
    InsertMissing,
    UpdateDivergent,
    Verify,
    VerifyNoTargetExtras,
}

impl SyncPhase {
    pub(crate) fn is_verification(self) -> bool {
        matches!(self, Self::Verify | Self::VerifyNoTargetExtras)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncTableReport {
    pub table: String,
    pub chunks: u64,
    pub rows_scanned: u64,
    pub inserts: u64,
    pub updates: u64,
    pub extra_target_rows: u64,
}

pub trait SyncTableReader {
    fn read_rows(&self, request: &SyncChunkRequest) -> Result<Vec<SnapshotRow>, TableSyncError>;

    fn requires_full_rows_for_missing_primary_keys(&self) -> bool {
        false
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum TableSyncError {
    InvalidTable(String),
    Read(String),
    Duplicate(String),
    Repair(String),
    Verification(String),
    Progress(String),
}

impl fmt::Display for TableSyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTable(message) => write!(formatter, "invalid sync table: {message}"),
            Self::Read(message) => write!(formatter, "sync read failed: {message}"),
            Self::Duplicate(message) => write!(formatter, "sync duplicate detected: {message}"),
            Self::Repair(message) => write!(formatter, "sync repair failed: {message}"),
            Self::Verification(message) => write!(formatter, "sync verification failed: {message}"),
            Self::Progress(message) => write!(formatter, "sync progress failed: {message}"),
        }
    }
}

impl std::error::Error for TableSyncError {}

#[derive(Serialize)]
pub(crate) struct SyncRunScope<'a> {
    pub(crate) source_host: &'a str,
    pub(crate) source_port: u16,
    pub(crate) source_database: &'a str,
    pub(crate) target_host: &'a str,
    pub(crate) target_port: u16,
    pub(crate) target_database: &'a str,
    pub(crate) insert_conflict_policy: &'a str,
    pub(crate) plan_hash: Option<&'a str>,
}

#[derive(Serialize)]
pub(crate) struct SyncRunSpec<'a> {
    pub(crate) scope: &'a str,
    pub(crate) table: &'a SyncTable,
    pub(crate) chunk_size: usize,
    pub(crate) mode: SyncMode,
    pub(crate) start_after: &'a Option<Vec<String>>,
    pub(crate) end_at: &'a Option<Vec<String>>,
    pub(crate) max_deletes: Option<u64>,
    pub(crate) updated_since: Option<&'a UpdatedSince>,
}

pub struct SyncRunOptions {
    pub run_id: String,
    pub run_scope: String,
    pub chunk_size: usize,
    pub mode: SyncMode,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
    pub max_deletes: Option<u64>,
}

pub(crate) fn validate_sync_table_config(config: &SyncTableConfig) -> Result<(), TableSyncError> {
    if config.mode == SyncMode::MissingPrimaryKeys && config.updated_since.is_some() {
        return Err(TableSyncError::InvalidTable(
            "missing-primary-keys mode cannot use updated_since".to_string(),
        ));
    }
    if config.updated_since.is_some() && (config.start_after.is_some() || config.end_at.is_some()) {
        return Err(TableSyncError::InvalidTable(
            "updated_since cannot be combined with start_after or end_at".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn sync_insert_mode(config: &SyncTableConfig) -> crate::target::SnapshotInsertMode {
    if config.updated_since.is_some() {
        crate::target::SnapshotInsertMode::Upsert
    } else {
        crate::target::SnapshotInsertMode::Insert
    }
}

pub(crate) fn target_connection_config(
    config: &SyncTableConfig,
) -> crate::mysql_snapshot::MySqlConnectionConfig {
    crate::mysql_snapshot::MySqlConnectionConfig {
        host: config.target.host.clone(),
        port: config.target.port,
        user: config.target.user.clone(),
        password: config.target.password.clone(),
        database: config.target.database.clone(),
    }
}

pub(crate) fn snapshot_table(table: &SyncTable) -> crate::snapshot::SnapshotTable {
    crate::snapshot::SnapshotTable {
        name: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: table.columns.clone(),
    }
}

pub(crate) fn validate_sync_range(
    table: &SyncTable,
    start_after: Option<&Vec<String>>,
    end_at: Option<&Vec<String>>,
) -> Result<(), TableSyncError> {
    validate_bound_arity(&table.primary_key, start_after, "start_after")?;
    validate_bound_arity(&table.primary_key, end_at, "end_at")?;
    Ok(())
}

pub(crate) fn validate_bound_arity(
    primary_key: &[String],
    values: Option<&Vec<String>>,
    label: &str,
) -> Result<(), TableSyncError> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.len() != primary_key.len() {
        return Err(TableSyncError::InvalidTable(format!(
            "{label} has {} values for {} primary-key columns",
            values.len(),
            primary_key.len()
        )));
    }
    Ok(())
}

pub(crate) fn validate_sync_table(
    table: &SyncTable,
    chunk_size: usize,
) -> Result<(), TableSyncError> {
    if table.name.is_empty() {
        return Err(TableSyncError::InvalidTable(
            "table name is required".to_string(),
        ));
    }
    if table.primary_key.is_empty() {
        return Err(TableSyncError::InvalidTable(
            "primary key is required".to_string(),
        ));
    }
    if table.columns.is_empty() {
        return Err(TableSyncError::InvalidTable(
            "columns are required".to_string(),
        ));
    }
    if chunk_size == 0 {
        return Err(TableSyncError::InvalidTable(
            "chunk size must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn sync_chunk_request_with_updated_since(
    table: &SyncTable,
    start_after: Option<Vec<String>>,
    limit: usize,
    updated_since: UpdatedSince,
) -> SyncChunkRequest {
    SyncChunkRequest {
        updated_since: Some(updated_since),
        ..sync_chunk_request(table, start_after, None, limit)
    }
}

pub(crate) fn sync_chunk_request(
    table: &SyncTable,
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    limit: usize,
) -> SyncChunkRequest {
    SyncChunkRequest {
        table: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: table.columns.clone(),
        start_after,
        end_at,
        updated_since: None,
        limit,
    }
}

pub(crate) fn last_primary_key(rows: &[SnapshotRow]) -> Result<Vec<String>, TableSyncError> {
    rows.last()
        .map(|row| row.primary_key.clone())
        .ok_or_else(|| TableSyncError::Read("source chunk unexpectedly empty".to_string()))
}
