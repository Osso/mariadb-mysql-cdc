use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

const SNAPSHOT_RETRY_ATTEMPTS: u32 = 3;
const SNAPSHOT_RETRY_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotTable {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
}

impl From<&crate::inventory::TableInventory> for SnapshotTable {
    fn from(table: &crate::inventory::TableInventory) -> Self {
        Self {
            name: table.name.clone(),
            primary_key: table.primary_key.clone(),
            columns: table
                .columns
                .iter()
                .filter(|column| column.generated.is_none())
                .map(|column| column.name.clone())
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotRow {
    pub primary_key: Vec<String>,
    pub values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRequest {
    pub table: String,
    pub primary_key: Vec<String>,
    pub selected_columns: Vec<String>,
    pub start_after: Option<Vec<String>>,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotProgress {
    pub tables: BTreeMap<String, TableSnapshotProgress>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TableSnapshotProgress {
    pub last_primary_key: Option<Vec<String>>,
    pub rows_copied: u64,
    pub complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotResult {
    pub table: String,
    pub rows_copied: u64,
}

pub trait SnapshotProgressStore {
    fn load(&self) -> Result<SnapshotProgress, SnapshotError>;
    fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError>;
}

pub trait SnapshotSource {
    fn read_chunk(&self, request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError>;
}

pub trait SnapshotTarget {
    fn write_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), SnapshotError>;
}

#[derive(Clone, Debug)]
pub struct FileSnapshotProgressStore {
    path: PathBuf,
}

impl FileSnapshotProgressStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl SnapshotProgressStore for FileSnapshotProgressStore {
    fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => decode_progress(&contents),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(SnapshotProgress::default())
            }
            Err(error) => Err(SnapshotError::Read {
                path: self.path.clone(),
                source: error,
            }),
        }
    }

    fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
        let encoded = encode_progress(progress)?;
        let temp_path = temp_progress_path(&self.path);

        fs::write(&temp_path, encoded).map_err(|error| SnapshotError::Write {
            path: temp_path.clone(),
            source: error,
        })?;

        fs::rename(&temp_path, &self.path).map_err(|error| SnapshotError::Rename {
            from: temp_path,
            to: self.path.clone(),
            source: error,
        })
    }
}

#[derive(Debug)]
pub enum SnapshotError {
    Decode(serde_json::Error),
    Encode(serde_json::Error),
    InvalidTable(String),
    Read {
        path: PathBuf,
        source: io::Error,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
        source: io::Error,
    },
    Write {
        path: PathBuf,
        source: io::Error,
    },
    Retry {
        operation: String,
        table: String,
        attempts: u32,
        start_after: String,
        source: Box<SnapshotError>,
    },
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(source) => {
                write!(formatter, "failed to decode snapshot progress: {source}")
            }
            Self::Encode(source) => {
                write!(formatter, "failed to encode snapshot progress: {source}")
            }
            Self::InvalidTable(message) => formatter.write_str(message),
            Self::Read { path, source } => {
                write!(formatter, "failed to read {}: {source}", path.display())
            }
            Self::Rename { from, to, source } => write!(
                formatter,
                "failed to move {} to {}: {source}",
                from.display(),
                to.display()
            ),
            Self::Write { path, source } => {
                write!(formatter, "failed to write {}: {source}", path.display())
            }
            Self::Retry {
                operation,
                table,
                attempts,
                start_after,
                source,
            } => write!(
                formatter,
                "snapshot {operation} failed table={table} attempts={attempts} start_after={start_after}: {source}"
            ),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl SnapshotProgress {
    pub fn table(&self, table: &str) -> Option<&TableSnapshotProgress> {
        self.tables.get(table)
    }

    pub fn mark_chunk(&mut self, table: &str, last_primary_key: Vec<String>, row_count: u64) {
        let progress = self.tables.entry(table.to_string()).or_default();
        progress.last_primary_key = Some(last_primary_key);
        progress.rows_copied += row_count;
        progress.complete = false;
    }

    pub fn mark_complete(&mut self, table: &str) {
        self.tables.entry(table.to_string()).or_default().complete = true;
    }
}

pub fn build_chunk_request(
    table: &SnapshotTable,
    limit: usize,
    progress: &SnapshotProgress,
) -> Result<ChunkRequest, SnapshotError> {
    validate_table(table)?;
    let table_progress = progress.table(&table.name);
    let start_after = table_progress.and_then(|progress| progress.last_primary_key.clone());

    Ok(ChunkRequest {
        table: table.name.clone(),
        primary_key: table.primary_key.clone(),
        selected_columns: table.columns.clone(),
        start_after,
        limit,
    })
}

pub fn snapshot_table(
    table: &SnapshotTable,
    chunk_size: usize,
    progress_store: &impl SnapshotProgressStore,
    source: &impl SnapshotSource,
    target: &mut impl SnapshotTarget,
) -> Result<SnapshotResult, SnapshotError> {
    validate_table(table)?;
    let mut progress = progress_store.load()?;
    let starting_rows_copied = rows_copied_for_table(&progress, &table.name);
    if is_table_complete(&progress, &table.name) {
        return Ok(snapshot_result(table, 0));
    }

    copy_table_chunks(
        table,
        chunk_size,
        &mut progress,
        progress_store,
        source,
        target,
    )?;
    let rows_copied = rows_copied_for_table(&progress, &table.name) - starting_rows_copied;

    Ok(snapshot_result(table, rows_copied))
}

fn copy_table_chunks(
    table: &SnapshotTable,
    chunk_size: usize,
    progress: &mut SnapshotProgress,
    progress_store: &impl SnapshotProgressStore,
    source: &impl SnapshotSource,
    target: &mut impl SnapshotTarget,
) -> Result<(), SnapshotError> {
    loop {
        let request = build_chunk_request(table, chunk_size, progress)?;
        let rows = read_chunk_with_retry(source, &request)?;

        if rows.is_empty() {
            progress.mark_complete(&table.name);
            save_progress_with_retry(progress_store, progress, &request)?;
            return Ok(());
        }

        write_rows_with_retry(target, &request, &rows)?;
        let last_primary_key = last_primary_key(&rows)?;
        progress.mark_chunk(&table.name, last_primary_key, rows.len() as u64);

        if rows.len() < chunk_size {
            progress.mark_complete(&table.name);
            save_progress_with_retry(progress_store, progress, &request)?;
            return Ok(());
        }

        save_progress_with_retry(progress_store, progress, &request)?;
    }
}

fn read_chunk_with_retry(
    source: &impl SnapshotSource,
    request: &ChunkRequest,
) -> Result<Vec<SnapshotRow>, SnapshotError> {
    retry_snapshot_operation("source_read", request, || source.read_chunk(request))
}

fn write_rows_with_retry(
    target: &mut impl SnapshotTarget,
    request: &ChunkRequest,
    rows: &[SnapshotRow],
) -> Result<(), SnapshotError> {
    retry_snapshot_operation("target_write", request, || target.write_rows(rows))
}

fn save_progress_with_retry(
    progress_store: &impl SnapshotProgressStore,
    progress: &SnapshotProgress,
    request: &ChunkRequest,
) -> Result<(), SnapshotError> {
    retry_snapshot_operation("progress_save", request, || progress_store.save(progress))
}

fn retry_snapshot_operation<T>(
    operation: &str,
    request: &ChunkRequest,
    mut attempt_operation: impl FnMut() -> Result<T, SnapshotError>,
) -> Result<T, SnapshotError> {
    let mut last_error = None;
    for attempt in 1..=SNAPSHOT_RETRY_ATTEMPTS {
        match attempt_operation() {
            Ok(value) => return Ok(value),
            Err(error) => {
                log_snapshot_retry(operation, request, attempt, &error);
                last_error = Some(error);
            }
        }

        if attempt < SNAPSHOT_RETRY_ATTEMPTS {
            thread::sleep(SNAPSHOT_RETRY_BACKOFF);
        }
    }

    Err(retry_error(
        operation,
        request,
        last_error.expect("retry error"),
    ))
}

fn log_snapshot_retry(
    operation: &str,
    request: &ChunkRequest,
    attempt: u32,
    error: &SnapshotError,
) {
    eprintln!(
        "snapshot_retry operation={} table={} attempt={} attempts={} start_after={} error={}",
        operation,
        request.table,
        attempt,
        SNAPSHOT_RETRY_ATTEMPTS,
        format_start_after(&request.start_after),
        error
    );
}

fn retry_error(operation: &str, request: &ChunkRequest, source: SnapshotError) -> SnapshotError {
    SnapshotError::Retry {
        operation: operation.to_string(),
        table: request.table.clone(),
        attempts: SNAPSHOT_RETRY_ATTEMPTS,
        start_after: format_start_after(&request.start_after),
        source: Box::new(source),
    }
}

fn format_start_after(start_after: &Option<Vec<String>>) -> String {
    start_after
        .as_ref()
        .map(|values| values.join(","))
        .unwrap_or_else(|| "-".to_string())
}

fn is_table_complete(progress: &SnapshotProgress, table: &str) -> bool {
    progress.table(table).is_some_and(|table| table.complete)
}

fn rows_copied_for_table(progress: &SnapshotProgress, table: &str) -> u64 {
    progress
        .table(table)
        .map_or(0, |table_progress| table_progress.rows_copied)
}

fn snapshot_result(table: &SnapshotTable, rows_copied: u64) -> SnapshotResult {
    SnapshotResult {
        table: table.name.clone(),
        rows_copied,
    }
}

pub fn format_progress(progress: &SnapshotProgress) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "snapshot_progress tables={}",
        progress.tables.len()
    ));

    for (table, table_progress) in &progress.tables {
        let last_primary_key = table_progress
            .last_primary_key
            .as_ref()
            .map(|values| values.join(","))
            .unwrap_or_default();
        lines.push(format!(
            "snapshot_table_progress table={} rows_copied={} complete={} last_primary_key={}",
            table, table_progress.rows_copied, table_progress.complete, last_primary_key
        ));
    }

    lines.join("\n")
}

pub fn encode_progress(progress: &SnapshotProgress) -> Result<String, SnapshotError> {
    serde_json::to_string_pretty(progress)
        .map(|json| format!("{json}\n"))
        .map_err(SnapshotError::Encode)
}

fn decode_progress(contents: &str) -> Result<SnapshotProgress, SnapshotError> {
    serde_json::from_str(contents).map_err(SnapshotError::Decode)
}

fn validate_table(table: &SnapshotTable) -> Result<(), SnapshotError> {
    if table.name.is_empty() {
        return Err(SnapshotError::InvalidTable(
            "table name is required".to_string(),
        ));
    }

    if table.primary_key.is_empty() {
        return Err(SnapshotError::InvalidTable(format!(
            "{} needs a primary key for deterministic snapshot chunks",
            table.name
        )));
    }

    Ok(())
}

fn last_primary_key(rows: &[SnapshotRow]) -> Result<Vec<String>, SnapshotError> {
    rows.last()
        .map(|row| row.primary_key.clone())
        .ok_or_else(|| SnapshotError::InvalidTable("chunk had no rows".to_string()))
}

fn temp_progress_path(path: &Path) -> PathBuf {
    let mut temp_path = path.to_path_buf();
    let extension = match path.extension().and_then(|value| value.to_str()) {
        Some(extension) => format!("{extension}.tmp"),
        None => "tmp".to_string(),
    };
    temp_path.set_extension(extension);
    temp_path
}

#[cfg(test)]
mod tests;
