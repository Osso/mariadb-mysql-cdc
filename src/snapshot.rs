use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

pub use crate::snapshot_ranges::{SnapshotRange, plan_snapshot_ranges};

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
    pub values: BTreeMap<String, Option<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRequest {
    pub table: String,
    pub primary_key: Vec<String>,
    pub selected_columns: Vec<String>,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunkProgress {
    pub table: String,
    pub chunk_start: Option<Vec<String>>,
    pub chunk_end: Vec<String>,
    pub chunk_rows: u64,
    pub rows_copied: u64,
}

pub trait SnapshotObserver {
    fn chunk_copied(&self, progress: &SnapshotChunkProgress);
}

pub struct NoopSnapshotObserver;

impl SnapshotObserver for NoopSnapshotObserver {
    fn chunk_copied(&self, _progress: &SnapshotChunkProgress) {}
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
    ProgressSchemaEnsure {
        progress_table: String,
        source: Box<SnapshotError>,
    },
    ProgressRowRead {
        progress_table: String,
        source: Box<SnapshotError>,
    },
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
            Self::ProgressSchemaEnsure {
                progress_table,
                source,
            } => write!(
                formatter,
                "progress schema ensure failed progress_table={progress_table}: {source}"
            ),
            Self::ProgressRowRead {
                progress_table,
                source,
            } => write!(
                formatter,
                "progress row read failed progress_table={progress_table}: {source}"
            ),
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
        end_at: None,
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
    snapshot_table_with_observer(
        table,
        chunk_size,
        progress_store,
        source,
        target,
        &NoopSnapshotObserver,
    )
}

pub fn snapshot_table_range_with_observer(
    table: &SnapshotTable,
    range: &SnapshotRange,
    chunk_size: usize,
    progress_store: &impl SnapshotProgressStore,
    source: &impl SnapshotSource,
    target: &mut impl SnapshotTarget,
    observer: &impl SnapshotObserver,
) -> Result<SnapshotResult, SnapshotError> {
    validate_table(table)?;
    let progress_key = range_progress_key(&table.name, range.worker);
    let mut progress = load_progress_with_retry(progress_store, &progress_key)?;
    let starting_rows_copied = rows_copied_for_table(&progress, &progress_key);
    if is_table_complete(&progress, &progress_key) {
        return Ok(snapshot_result(table, 0));
    }

    copy_table_range_chunks(TableRangeCopy {
        table,
        range,
        progress_key: &progress_key,
        chunk_size,
        progress: &mut progress,
        progress_store,
        source,
        target,
        observer,
    })?;
    let rows_copied = rows_copied_for_table(&progress, &progress_key) - starting_rows_copied;

    Ok(snapshot_result(table, rows_copied))
}

pub fn snapshot_table_with_observer(
    table: &SnapshotTable,
    chunk_size: usize,
    progress_store: &impl SnapshotProgressStore,
    source: &impl SnapshotSource,
    target: &mut impl SnapshotTarget,
    observer: &impl SnapshotObserver,
) -> Result<SnapshotResult, SnapshotError> {
    validate_table(table)?;
    let mut progress = load_progress_with_retry(progress_store, &table.name)?;
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
        observer,
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
    observer: &impl SnapshotObserver,
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
        observer.chunk_copied(&snapshot_chunk_progress(
            &request,
            rows.len() as u64,
            progress,
        )?);

        if rows.len() < chunk_size {
            progress.mark_complete(&table.name);
            save_progress_with_retry(progress_store, progress, &request)?;
            return Ok(());
        }

        save_progress_with_retry(progress_store, progress, &request)?;
    }
}

struct TableRangeCopy<'a, S, T, O, P>
where
    S: SnapshotSource,
    T: SnapshotTarget,
    O: SnapshotObserver,
    P: SnapshotProgressStore,
{
    table: &'a SnapshotTable,
    range: &'a SnapshotRange,
    progress_key: &'a str,
    chunk_size: usize,
    progress: &'a mut SnapshotProgress,
    progress_store: &'a P,
    source: &'a S,
    target: &'a mut T,
    observer: &'a O,
}

fn copy_table_range_chunks<S, T, O, P>(
    context: TableRangeCopy<'_, S, T, O, P>,
) -> Result<(), SnapshotError>
where
    S: SnapshotSource,
    T: SnapshotTarget,
    O: SnapshotObserver,
    P: SnapshotProgressStore,
{
    let mut context = context;
    loop {
        let request = build_range_chunk_request(
            context.table,
            context.range,
            context.progress_key,
            context.chunk_size,
            context.progress,
        )?;
        let rows = read_chunk_with_retry(context.source, &request)?;

        if rows.is_empty() {
            mark_range_complete(&mut context, &request)?;
            return Ok(());
        }

        write_rows_with_retry(context.target, &request, &rows)?;
        let last_primary_key = last_primary_key(&rows)?;
        context
            .progress
            .mark_chunk(context.progress_key, last_primary_key, rows.len() as u64);
        context
            .observer
            .chunk_copied(&snapshot_chunk_progress_for_key(
                &request,
                context.progress_key,
                rows.len() as u64,
                context.progress,
            )?);

        if rows.len() < context.chunk_size {
            mark_range_complete(&mut context, &request)?;
            return Ok(());
        }

        save_progress_with_retry(context.progress_store, context.progress, &request)?;
    }
}

fn mark_range_complete<S, T, O, P>(
    context: &mut TableRangeCopy<'_, S, T, O, P>,
    request: &ChunkRequest,
) -> Result<(), SnapshotError>
where
    S: SnapshotSource,
    T: SnapshotTarget,
    O: SnapshotObserver,
    P: SnapshotProgressStore,
{
    context.progress.mark_complete(context.progress_key);
    save_progress_with_retry(context.progress_store, context.progress, request)
}

fn build_range_chunk_request(
    table: &SnapshotTable,
    range: &SnapshotRange,
    progress_key: &str,
    limit: usize,
    progress: &SnapshotProgress,
) -> Result<ChunkRequest, SnapshotError> {
    validate_table(table)?;
    let table_progress = progress.table(progress_key);
    let start_after = table_progress
        .and_then(|progress| progress.last_primary_key.clone())
        .or_else(|| range.start_after.clone());

    Ok(ChunkRequest {
        table: table.name.clone(),
        primary_key: table.primary_key.clone(),
        selected_columns: table.columns.clone(),
        start_after,
        end_at: range.end_at.clone(),
        limit,
    })
}

fn range_progress_key(table: &str, worker: usize) -> String {
    format!("{table}#range{worker}")
}

fn snapshot_chunk_progress(
    request: &ChunkRequest,
    chunk_rows: u64,
    progress: &SnapshotProgress,
) -> Result<SnapshotChunkProgress, SnapshotError> {
    snapshot_chunk_progress_for_key(request, &request.table, chunk_rows, progress)
}

fn snapshot_chunk_progress_for_key(
    request: &ChunkRequest,
    progress_key: &str,
    chunk_rows: u64,
    progress: &SnapshotProgress,
) -> Result<SnapshotChunkProgress, SnapshotError> {
    let table_progress = progress.table(progress_key).ok_or_else(|| {
        SnapshotError::InvalidTable(format!("{progress_key} progress was not recorded"))
    })?;
    let chunk_end = table_progress.last_primary_key.clone().ok_or_else(|| {
        SnapshotError::InvalidTable(format!("{progress_key} progress has no chunk end"))
    })?;
    Ok(SnapshotChunkProgress {
        table: request.table.clone(),
        chunk_start: request.start_after.clone(),
        chunk_end,
        chunk_rows,
        rows_copied: table_progress.rows_copied,
    })
}

fn load_progress_with_retry(
    progress_store: &impl SnapshotProgressStore,
    table: &str,
) -> Result<SnapshotProgress, SnapshotError> {
    let context = RetryContext::new("progress_load", table, None);
    retry_snapshot_operation(&context, || progress_store.load())
}

fn read_chunk_with_retry(
    source: &impl SnapshotSource,
    request: &ChunkRequest,
) -> Result<Vec<SnapshotRow>, SnapshotError> {
    let context = RetryContext::from_request("source_read", request);
    retry_snapshot_operation(&context, || source.read_chunk(request))
}

fn write_rows_with_retry(
    target: &mut impl SnapshotTarget,
    request: &ChunkRequest,
    rows: &[SnapshotRow],
) -> Result<(), SnapshotError> {
    let context = RetryContext::from_request("target_write", request);
    retry_snapshot_operation(&context, || target.write_rows(rows))
}

fn save_progress_with_retry(
    progress_store: &impl SnapshotProgressStore,
    progress: &SnapshotProgress,
    request: &ChunkRequest,
) -> Result<(), SnapshotError> {
    let context = RetryContext::from_request("progress_save", request);
    retry_snapshot_operation(&context, || progress_store.save(progress))
}

fn retry_snapshot_operation<T>(
    context: &RetryContext,
    mut attempt_operation: impl FnMut() -> Result<T, SnapshotError>,
) -> Result<T, SnapshotError> {
    let mut last_error = None;
    for attempt in 1..=SNAPSHOT_RETRY_ATTEMPTS {
        match attempt_operation() {
            Ok(value) => return Ok(value),
            Err(error) => {
                log_snapshot_retry(context, attempt, &error);
                last_error = Some(error);
            }
        }

        if attempt < SNAPSHOT_RETRY_ATTEMPTS {
            thread::sleep(SNAPSHOT_RETRY_BACKOFF);
        }
    }

    Err(retry_error(context, last_error.expect("retry error")))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RetryContext {
    operation: &'static str,
    table: String,
    start_after: String,
}

impl RetryContext {
    fn new(operation: &'static str, table: &str, start_after: Option<Vec<String>>) -> Self {
        Self {
            operation,
            table: table.to_string(),
            start_after: format_start_after(&start_after),
        }
    }

    fn from_request(operation: &'static str, request: &ChunkRequest) -> Self {
        Self::new(operation, &request.table, request.start_after.clone())
    }
}

fn format_snapshot_retry(context: &RetryContext, attempt: u32, error: &SnapshotError) -> String {
    let (operation, progress_table, phase) = retry_metadata(context.operation, error);
    let progress_context = match (progress_table, phase) {
        (Some(progress_table), Some(phase)) => {
            format!(" progress_table={progress_table} phase={phase}")
        }
        _ => String::new(),
    };
    format!(
        "snapshot_retry operation={operation} table={}{} attempt={} attempts={} start_after={} error={error}",
        context.table, progress_context, attempt, SNAPSHOT_RETRY_ATTEMPTS, context.start_after,
    )
}

fn log_snapshot_retry(context: &RetryContext, attempt: u32, error: &SnapshotError) {
    eprintln!("{}", format_snapshot_retry(context, attempt, error));
}

fn retry_error(context: &RetryContext, source: SnapshotError) -> SnapshotError {
    let (operation, _, _) = retry_metadata(context.operation, &source);
    SnapshotError::Retry {
        operation: operation.to_string(),
        table: context.table.clone(),
        attempts: SNAPSHOT_RETRY_ATTEMPTS,
        start_after: context.start_after.clone(),
        source: Box::new(source),
    }
}

fn retry_metadata<'a>(
    default_operation: &'static str,
    error: &'a SnapshotError,
) -> (&'static str, Option<&'a str>, Option<&'static str>) {
    match error {
        SnapshotError::ProgressSchemaEnsure { progress_table, .. } => (
            "progress_ensure",
            Some(progress_table),
            Some("schema_ensure"),
        ),
        SnapshotError::ProgressRowRead { progress_table, .. } => {
            ("progress_load", Some(progress_table), Some("row_read"))
        }
        _ => (default_operation, None, None),
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
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, VecDeque};

    #[test]
    fn builds_first_chunk_request_from_table_metadata() {
        let table = accounts_table();
        let progress = SnapshotProgress::default();

        let request = build_chunk_request(&table, 500, &progress).expect("request");

        assert_eq!(request.table, "accounts");
        assert_eq!(request.primary_key, vec!["id"]);
        assert_eq!(request.selected_columns, vec!["id", "name"]);
        assert_eq!(request.start_after, None);
        assert_eq!(request.end_at, None);
        assert_eq!(request.limit, 500);
    }

    #[test]
    fn resumes_chunk_request_after_last_primary_key() {
        let table = accounts_table();
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts", vec!["42".to_string()], 42);

        let request = build_chunk_request(&table, 100, &progress).expect("request");

        assert_eq!(request.start_after, Some(vec!["42".to_string()]));
    }

    #[test]
    fn snapshots_table_in_chunks_and_saves_progress() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let source = FakeSnapshotSource::new(vec![
            vec![row("1", "alpha"), row("2", "beta")],
            vec![row("3", "gamma")],
        ]);
        let mut target = FakeSnapshotTarget::default();

        let result =
            snapshot_table(&table, 2, &progress_store, &source, &mut target).expect("snapshot");

        assert_eq!(result.rows_copied, 3);
        assert_eq!(target.rows.len(), 3);

        let saved = progress_store.load().expect("load progress");
        let table_progress = saved.table("accounts").expect("table progress");
        assert_eq!(table_progress.last_primary_key, Some(vec!["3".to_string()]));
        assert_eq!(table_progress.rows_copied, 3);
        assert!(table_progress.complete);
    }

    #[test]
    fn reports_chunk_progress_with_bounds_and_copied_rows() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let source = FakeSnapshotSource::new(vec![
            vec![row("1", "alpha"), row("2", "beta")],
            vec![row("3", "gamma")],
        ]);
        let observer = RecordingSnapshotObserver::default();
        let mut target = FakeSnapshotTarget::default();

        snapshot_table_with_observer(&table, 2, &progress_store, &source, &mut target, &observer)
            .expect("snapshot");

        assert_eq!(
            observer.events.borrow().as_slice(),
            &[
                SnapshotChunkProgress {
                    table: "accounts".to_string(),
                    chunk_start: None,
                    chunk_end: vec!["2".to_string()],
                    chunk_rows: 2,
                    rows_copied: 2,
                },
                SnapshotChunkProgress {
                    table: "accounts".to_string(),
                    chunk_start: Some(vec!["2".to_string()]),
                    chunk_end: vec!["3".to_string()],
                    chunk_rows: 1,
                    rows_copied: 3,
                },
            ]
        );
    }

    #[test]
    fn snapshots_range_with_worker_bounds_and_progress_key() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let range = SnapshotRange {
            worker: 2,
            start_after: Some(pk("100")),
            end_at: Some(pk("200")),
        };
        let source = FakeSnapshotSource::new(vec![vec![row("150", "middle")]]);
        let mut target = FakeSnapshotTarget::default();

        let result = snapshot_table_range_with_observer(
            &table,
            &range,
            10,
            &progress_store,
            &source,
            &mut target,
            &NoopSnapshotObserver,
        )
        .expect("snapshot range");

        assert_eq!(result.table, "accounts");
        assert_eq!(result.rows_copied, 1);
        assert_eq!(source.requests.borrow()[0].start_after, Some(pk("100")));
        assert_eq!(source.requests.borrow()[0].end_at, Some(pk("200")));

        let saved = progress_store.load().expect("progress");
        assert!(saved.table("accounts").is_none());
        assert!(saved.table("accounts#range2").expect("range").complete);
    }

    #[test]
    fn resumes_snapshot_range_from_worker_progress() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts#range2", pk("150"), 50);
        progress_store.save(&progress).expect("save progress");
        let range = SnapshotRange {
            worker: 2,
            start_after: Some(pk("100")),
            end_at: Some(pk("200")),
        };
        let source = FakeSnapshotSource::new(vec![Vec::new()]);
        let mut target = FakeSnapshotTarget::default();

        snapshot_table_range_with_observer(
            &table,
            &range,
            10,
            &progress_store,
            &source,
            &mut target,
            &NoopSnapshotObserver,
        )
        .expect("snapshot range");

        assert_eq!(source.requests.borrow()[0].start_after, Some(pk("150")));
        assert_eq!(source.requests.borrow()[0].end_at, Some(pk("200")));
    }

    #[test]
    fn retries_temporary_source_read_failure() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let source = FlakySnapshotSource::fail_then_return(1, vec![row("1", "alpha")]);
        let mut target = FakeSnapshotTarget::default();

        snapshot_table(&table, 2, &progress_store, &source, &mut target).expect("snapshot");

        assert_eq!(source.attempts(), 2);
        assert_eq!(target.rows.len(), 1);
    }

    #[test]
    fn retries_temporary_target_write_failure() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let source = FakeSnapshotSource::new(vec![vec![row("1", "alpha")]]);
        let mut target = FlakySnapshotTarget::fail_then_write(1);

        snapshot_table(&table, 2, &progress_store, &source, &mut target).expect("snapshot");

        assert_eq!(target.attempts, 2);
        assert_eq!(target.rows.len(), 1);
    }

    #[test]
    fn retries_temporary_progress_save_failure() {
        let table = accounts_table();
        let progress_store = FlakyProgressStore::fail_then_save(1);
        let source = FakeSnapshotSource::new(vec![vec![row("1", "alpha")]]);
        let mut target = FakeSnapshotTarget::default();

        snapshot_table(&table, 2, &progress_store, &source, &mut target).expect("snapshot");

        assert_eq!(progress_store.save_attempts(), 2);
        assert_eq!(target.rows.len(), 1);
    }

    #[test]
    fn retries_temporary_progress_load_failure_before_resuming() {
        let table = accounts_table();
        let mut saved_progress = SnapshotProgress::default();
        saved_progress.mark_chunk("accounts", vec!["1".to_string()], 1);
        let progress_store = FlakyProgressStore::fail_load_then_return(1, saved_progress);
        let source = FakeSnapshotSource::new(vec![vec![row("2", "bravo")]]);
        let mut target = FakeSnapshotTarget::default();

        snapshot_table(&table, 2, &progress_store, &source, &mut target).expect("snapshot");

        assert_eq!(progress_store.load_attempts(), 2);
        assert_eq!(
            source.requests.borrow()[0].start_after,
            Some(vec!["1".to_string()])
        );
    }

    #[test]
    fn reports_retry_context_after_repeated_failure() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let source = FlakySnapshotSource::always_fail();
        let mut target = FakeSnapshotTarget::default();

        let error =
            snapshot_table(&table, 2, &progress_store, &source, &mut target).expect_err("error");
        let message = error.to_string();

        assert!(message.contains("snapshot source_read failed"));
        assert!(message.contains("table=accounts"));
        assert!(message.contains("attempts=3"));
        assert!(message.contains("start_after=-"));
    }

    #[test]
    fn formats_progress_schema_ensure_retry_as_distinct_operation() {
        let context = RetryContext::new("progress_load", "accounts", None);
        let error = SnapshotError::ProgressSchemaEnsure {
            progress_table: "cdc.snapshot_progress".to_string(),
            source: Box::new(test_error("permission denied")),
        };

        let line = format_snapshot_retry(&context, 1, &error);

        assert!(line.contains("operation=progress_ensure"));
        assert!(!line.contains("operation=progress_load"));
        assert!(line.contains("progress_table=cdc.snapshot_progress"));
        assert!(line.contains("phase=schema_ensure"));
        assert!(line.contains("error=progress schema ensure failed"));
    }

    #[test]
    fn formats_progress_row_read_retry_as_progress_load() {
        let context = RetryContext::new("progress_load", "accounts", None);
        let error = SnapshotError::ProgressRowRead {
            progress_table: "cdc.snapshot_progress".to_string(),
            source: Box::new(test_error("connection reset")),
        };

        let line = format_snapshot_retry(&context, 1, &error);

        assert!(line.contains("operation=progress_load"));
        assert!(line.contains("progress_table=cdc.snapshot_progress"));
        assert!(line.contains("phase=row_read"));
        assert!(line.contains("error=progress row read failed"));
    }

    #[test]
    fn skips_completed_table_on_rerun() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts", vec!["3".to_string()], 3);
        progress.mark_complete("accounts");
        progress_store.save(&progress).expect("save progress");
        let source = FakeSnapshotSource::new(vec![vec![row("4", "delta")]]);
        let mut target = FakeSnapshotTarget::default();

        let result =
            snapshot_table(&table, 2, &progress_store, &source, &mut target).expect("snapshot");

        assert_eq!(result.rows_copied, 0);
        assert!(target.rows.is_empty());
    }

    #[test]
    fn formats_snapshot_progress_for_operators() {
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts", vec!["42".to_string()], 42);

        assert_eq!(
            format_progress(&progress),
            "snapshot_progress tables=1\nsnapshot_table_progress table=accounts rows_copied=42 complete=false last_primary_key=42"
        );
    }

    #[test]
    fn file_progress_store_round_trips_table_progress() {
        let path = unique_path("snapshot-progress.json");
        let store = FileSnapshotProgressStore::new(path.clone());
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts", vec!["9".to_string()], 9);

        store.save(&progress).expect("save progress");
        let loaded = store.load().expect("load progress");

        assert_eq!(loaded, progress);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn builds_snapshot_table_from_inventory_table() {
        let inventory_table = crate::inventory::TableInventory {
            name: "accounts".to_string(),
            table_type: "BASE TABLE".to_string(),
            engine: Some("InnoDB".to_string()),
            collation: None,
            primary_key: vec!["id".to_string()],
            columns: vec![
                inventory_column("id"),
                inventory_column("name"),
                generated_inventory_column("name_length"),
            ],
        };

        let table = SnapshotTable::from(&inventory_table);

        assert_eq!(table.name, "accounts");
        assert_eq!(table.primary_key, vec!["id"]);
        assert_eq!(table.columns, vec!["id", "name"]);
    }

    #[test]
    fn plans_four_disjoint_snapshot_ranges_from_three_boundaries() {
        let ranges =
            plan_snapshot_ranges(vec![pk("100"), pk("200"), pk("300")], 4).expect("ranges");

        assert_eq!(
            ranges,
            vec![
                SnapshotRange {
                    worker: 0,
                    start_after: None,
                    end_at: Some(pk("100")),
                },
                SnapshotRange {
                    worker: 1,
                    start_after: Some(pk("100")),
                    end_at: Some(pk("200")),
                },
                SnapshotRange {
                    worker: 2,
                    start_after: Some(pk("200")),
                    end_at: Some(pk("300")),
                },
                SnapshotRange {
                    worker: 3,
                    start_after: Some(pk("300")),
                    end_at: None,
                },
            ]
        );
    }

    #[test]
    fn plans_numeric_snapshot_ranges_across_string_digit_widths() {
        let ranges =
            plan_snapshot_ranges(vec![pk("99999"), pk("100000"), pk("200000")], 4).expect("ranges");

        assert_eq!(
            ranges[1],
            SnapshotRange {
                worker: 1,
                start_after: Some(pk("99999")),
                end_at: Some(pk("100000")),
            }
        );
    }

    #[test]
    fn plans_single_snapshot_range_without_boundaries() {
        let ranges = plan_snapshot_ranges(Vec::new(), 1).expect("ranges");

        assert_eq!(
            ranges,
            vec![SnapshotRange {
                worker: 0,
                start_after: None,
                end_at: None,
            }]
        );
    }

    #[test]
    fn rejects_snapshot_ranges_with_unordered_boundaries() {
        let error = plan_snapshot_ranges(vec![pk("200"), pk("100")], 3).expect_err("error");

        assert_eq!(
            error.to_string(),
            "snapshot range boundaries must be strictly ascending"
        );
    }

    #[test]
    fn rejects_snapshot_range_count_that_does_not_match_workers() {
        let error = plan_snapshot_ranges(vec![pk("100")], 4).expect_err("error");

        assert_eq!(
            error.to_string(),
            "snapshot range planning needs exactly workers - 1 boundaries"
        );
    }

    fn accounts_table() -> SnapshotTable {
        SnapshotTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        }
    }

    fn row(id: &str, name: &str) -> SnapshotRow {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), Some(id.to_string()));
        values.insert("name".to_string(), Some(name.to_string()));

        SnapshotRow {
            primary_key: vec![id.to_string()],
            values,
        }
    }

    fn pk(value: &str) -> Vec<String> {
        vec![value.to_string()]
    }

    fn inventory_column(name: &str) -> crate::inventory::ColumnInventory {
        crate::inventory::ColumnInventory {
            name: name.to_string(),
            ordinal_position: 1,
            column_type: "varchar(64)".to_string(),
            data_type: "varchar".to_string(),
            is_nullable: false,
            character_set: None,
            collation: None,
            default_value: None,
            extra: String::new(),
            comment: String::new(),
            generated: None,
        }
    }

    fn generated_inventory_column(name: &str) -> crate::inventory::ColumnInventory {
        crate::inventory::ColumnInventory {
            generated: Some(crate::inventory::GeneratedColumn {
                expression: "`name`".to_string(),
                generation_kind: "VIRTUAL".to_string(),
            }),
            ..inventory_column(name)
        }
    }

    fn unique_path(file_name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        path.push(format!("mariadb-mysql-cdc-{nanos}-{file_name}"));
        path
    }

    #[derive(Default)]
    struct MemoryProgressStore {
        progress: RefCell<SnapshotProgress>,
    }

    impl SnapshotProgressStore for MemoryProgressStore {
        fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
            Ok(self.progress.borrow().clone())
        }

        fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
            *self.progress.borrow_mut() = progress.clone();
            Ok(())
        }
    }

    struct FlakyProgressStore {
        load_failures_remaining: RefCell<u32>,
        load_attempts: RefCell<u32>,
        failures_remaining: RefCell<u32>,
        progress: RefCell<SnapshotProgress>,
        save_attempts: RefCell<u32>,
    }

    impl FlakyProgressStore {
        fn fail_then_save(failures: u32) -> Self {
            Self {
                load_failures_remaining: RefCell::new(0),
                load_attempts: RefCell::new(0),
                failures_remaining: RefCell::new(failures),
                progress: RefCell::new(SnapshotProgress::default()),
                save_attempts: RefCell::new(0),
            }
        }

        fn fail_load_then_return(failures: u32, progress: SnapshotProgress) -> Self {
            Self {
                load_failures_remaining: RefCell::new(failures),
                load_attempts: RefCell::new(0),
                failures_remaining: RefCell::new(0),
                progress: RefCell::new(progress),
                save_attempts: RefCell::new(0),
            }
        }

        fn load_attempts(&self) -> u32 {
            *self.load_attempts.borrow()
        }

        fn save_attempts(&self) -> u32 {
            *self.save_attempts.borrow()
        }
    }

    impl SnapshotProgressStore for FlakyProgressStore {
        fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
            *self.load_attempts.borrow_mut() += 1;
            if take_failure(&self.load_failures_remaining) {
                return Err(test_error("progress load timeout"));
            }

            Ok(self.progress.borrow().clone())
        }

        fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
            *self.save_attempts.borrow_mut() += 1;
            if take_failure(&self.failures_remaining) {
                return Err(test_error("progress write timeout"));
            }

            *self.progress.borrow_mut() = progress.clone();
            Ok(())
        }
    }

    struct FakeSnapshotSource {
        chunks: RefCell<VecDeque<Vec<SnapshotRow>>>,
        requests: RefCell<Vec<ChunkRequest>>,
    }

    impl FakeSnapshotSource {
        fn new(chunks: Vec<Vec<SnapshotRow>>) -> Self {
            Self {
                chunks: RefCell::new(chunks.into()),
                requests: RefCell::new(Vec::new()),
            }
        }
    }

    impl SnapshotSource for FakeSnapshotSource {
        fn read_chunk(&self, request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError> {
            self.requests.borrow_mut().push(request.clone());
            Ok(self.chunks.borrow_mut().pop_front().unwrap_or_default())
        }
    }

    struct FlakySnapshotSource {
        attempts: RefCell<u32>,
        failures_remaining: RefCell<Option<u32>>,
        rows: Vec<SnapshotRow>,
    }

    impl FlakySnapshotSource {
        fn fail_then_return(failures: u32, rows: Vec<SnapshotRow>) -> Self {
            Self {
                attempts: RefCell::new(0),
                failures_remaining: RefCell::new(Some(failures)),
                rows,
            }
        }

        fn always_fail() -> Self {
            Self {
                attempts: RefCell::new(0),
                failures_remaining: RefCell::new(None),
                rows: Vec::new(),
            }
        }

        fn attempts(&self) -> u32 {
            *self.attempts.borrow()
        }
    }

    impl SnapshotSource for FlakySnapshotSource {
        fn read_chunk(&self, _request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError> {
            *self.attempts.borrow_mut() += 1;
            let mut failures_remaining = self.failures_remaining.borrow_mut();
            match failures_remaining.as_mut() {
                None => Err(test_error("source read timeout")),
                Some(failures) if *failures > 0 => {
                    *failures -= 1;
                    Err(test_error("source read timeout"))
                }
                Some(_) => Ok(self.rows.clone()),
            }
        }
    }

    #[derive(Default)]
    struct FakeSnapshotTarget {
        rows: Vec<SnapshotRow>,
    }

    impl SnapshotTarget for FakeSnapshotTarget {
        fn write_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), SnapshotError> {
            self.rows.extend_from_slice(rows);
            Ok(())
        }
    }

    struct FlakySnapshotTarget {
        attempts: u32,
        failures_remaining: u32,
        rows: Vec<SnapshotRow>,
    }

    impl FlakySnapshotTarget {
        fn fail_then_write(failures: u32) -> Self {
            Self {
                attempts: 0,
                failures_remaining: failures,
                rows: Vec::new(),
            }
        }
    }

    impl SnapshotTarget for FlakySnapshotTarget {
        fn write_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), SnapshotError> {
            self.attempts += 1;
            if self.failures_remaining > 0 {
                self.failures_remaining -= 1;
                return Err(test_error("target write timeout"));
            }

            self.rows.extend_from_slice(rows);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingSnapshotObserver {
        events: RefCell<Vec<SnapshotChunkProgress>>,
    }

    impl SnapshotObserver for RecordingSnapshotObserver {
        fn chunk_copied(&self, progress: &SnapshotChunkProgress) {
            self.events.borrow_mut().push(progress.clone());
        }
    }

    fn take_failure(failures_remaining: &RefCell<u32>) -> bool {
        let mut failures = failures_remaining.borrow_mut();
        if *failures == 0 {
            return false;
        }

        *failures -= 1;
        true
    }

    fn test_error(message: &str) -> SnapshotError {
        SnapshotError::InvalidTable(message.to_string())
    }
}
