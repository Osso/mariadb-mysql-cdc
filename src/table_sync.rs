use crate::snapshot::SnapshotRow;
use std::collections::BTreeMap;
use std::fmt;

mod mysql;
pub(crate) mod progress;
mod target;

use mysql::MySqlSyncReader;
#[cfg(test)]
pub(crate) use mysql::build_sync_select_sql;
pub use progress::{
    MySqlSyncProgressStore, NoopSyncProgressStore, SyncProgressStatus, SyncProgressStore,
    SyncTableProgress,
};
pub use target::SyncRepairTarget;

#[derive(Clone, Debug)]
pub struct SyncTableConfig {
    pub source: crate::mysql_snapshot::MySqlConnectionConfig,
    pub target: crate::live::TargetMySqlConfig,
    pub table: SyncTable,
    pub chunk_size: usize,
    pub mode: SyncMode,
    pub progress_table: String,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
    pub max_deletes: Option<u64>,
    pub updated_since: Option<UpdatedSince>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdatedSince {
    pub column: String,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncMode {
    DryRun,
    Apply,
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
}

#[derive(Debug, Eq, PartialEq)]
pub enum TableSyncError {
    InvalidTable(String),
    Read(String),
    Repair(String),
    Progress(String),
}

impl fmt::Display for TableSyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTable(message) => write!(formatter, "invalid sync table: {message}"),
            Self::Read(message) => write!(formatter, "sync read failed: {message}"),
            Self::Repair(message) => write!(formatter, "sync repair failed: {message}"),
            Self::Progress(message) => write!(formatter, "sync progress failed: {message}"),
        }
    }
}

impl std::error::Error for TableSyncError {}

pub fn sync_table(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<SyncTableReport, TableSyncError> {
    let mut progress_store = NoopSyncProgressStore;
    sync_table_with_progress(
        table,
        chunk_size,
        mode,
        source,
        target,
        repair_target,
        &mut progress_store,
    )
}

pub fn sync_table_with_progress(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableReport, TableSyncError> {
    sync_table_with_progress_range(
        table,
        SyncRunOptions {
            chunk_size,
            mode,
            start_after: None,
            end_at: None,
            max_deletes: Some(0),
        },
        source,
        target,
        repair_target,
        progress_store,
    )
}

pub fn sync_recent_updates(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    updated_since: UpdatedSince,
) -> Result<SyncTableReport, TableSyncError> {
    validate_sync_table(table, chunk_size)?;
    let mut report = SyncTableReport {
        table: table.name.clone(),
        ..SyncTableReport::default()
    };
    let mut start_after = None;

    loop {
        let source_rows = read_recent_update_chunk(
            table,
            chunk_size,
            source,
            start_after.clone(),
            updated_since.clone(),
        )?;
        if source_rows.is_empty() {
            return Ok(report);
        }
        apply_recent_update_chunk(&source_rows, mode, repair_target, &mut report)?;
        start_after = Some(last_primary_key(&source_rows)?);
        if source_rows.len() < chunk_size {
            return Ok(report);
        }
    }
}

fn read_recent_update_chunk(
    table: &SyncTable,
    chunk_size: usize,
    source: &impl SyncTableReader,
    start_after: Option<Vec<String>>,
    updated_since: UpdatedSince,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    source.read_rows(&sync_chunk_request_with_updated_since(
        table,
        start_after,
        chunk_size,
        updated_since,
    ))
}

pub struct SyncRunOptions {
    pub chunk_size: usize,
    pub mode: SyncMode,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
    pub max_deletes: Option<u64>,
}

pub fn sync_table_with_progress_range(
    table: &SyncTable,
    options: SyncRunOptions,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableReport, TableSyncError> {
    let SyncRunOptions {
        chunk_size,
        mode,
        start_after: range_start_after,
        end_at: range_end_at,
        max_deletes,
    } = options;
    validate_sync_table(table, chunk_size)?;
    validate_sync_range(table, range_start_after.as_ref(), range_end_at.as_ref())?;
    let mut progress = load_sync_progress(table, mode, progress_store)?;
    let mut report = progress.report();
    let mut start_after = progress.last_primary_key.clone().or(range_start_after);

    loop {
        let Some(next_start_after) = sync_next_chunk(SyncChunkContext {
            table,
            chunk_size,
            mode,
            start_after: start_after.clone(),
            source,
            target,
            repair_target,
            progress_store,
            progress: &mut progress,
            report: &mut report,
            range_end_at: range_end_at.clone(),
            max_deletes,
        })?
        else {
            complete_sync_progress(&mut progress, progress_store)?;
            return Ok(report);
        };
        start_after = Some(next_start_after);
    }
}

struct SyncChunkContext<'a, S, T, R, P>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    table: &'a SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    start_after: Option<Vec<String>>,
    source: &'a S,
    target: &'a T,
    repair_target: &'a mut R,
    progress_store: &'a mut P,
    progress: &'a mut SyncTableProgress,
    report: &'a mut SyncTableReport,
    range_end_at: Option<Vec<String>>,
    max_deletes: Option<u64>,
}

fn sync_next_chunk<S, T, R, P>(
    mut context: SyncChunkContext<'_, S, T, R, P>,
) -> Result<Option<Vec<String>>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let source_rows = read_source_chunk(&context)?;
    if source_rows.is_empty() {
        let tail_start_after = context.start_after.clone();
        repair_target_tail(&mut context, tail_start_after)?;
        return Ok(None);
    }

    let end_at = repair_source_chunk(&mut context, &source_rows)?;

    if source_rows.len() < context.chunk_size {
        repair_target_tail(&mut context, Some(end_at))?;
        Ok(None)
    } else {
        Ok(Some(end_at))
    }
}

fn repair_source_chunk<S, T, R, P>(
    context: &mut SyncChunkContext<'_, S, T, R, P>,
    source_rows: &[SnapshotRow],
) -> Result<Vec<String>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let end_at = last_primary_key(source_rows)?;
    let target_rows = read_source_bounded_target_window(context, &end_at)?;
    repair_chunk(
        source_rows,
        &target_rows,
        context.mode,
        context.repair_target,
        context.report,
        context.max_deletes,
    )?;
    record_repaired_source_chunk(context, source_rows.len(), end_at.clone())?;
    Ok(end_at)
}

fn read_source_bounded_target_window<S, T, R, P>(
    context: &SyncChunkContext<'_, S, T, R, P>,
    end_at: &[String],
) -> Result<Vec<SnapshotRow>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    read_target_window(
        context.table,
        context.start_after.clone(),
        Some(end_at.to_vec()),
        context.chunk_size,
        context.target,
    )
}

fn record_repaired_source_chunk<S, T, R, P>(
    context: &mut SyncChunkContext<'_, S, T, R, P>,
    row_count: usize,
    end_at: Vec<String>,
) -> Result<(), TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    record_sync_chunk(
        context.progress,
        context.report,
        row_count,
        end_at,
        context.progress_store,
    )
}

fn repair_target_tail<S, T, R, P>(
    context: &mut SyncChunkContext<'_, S, T, R, P>,
    start_after: Option<Vec<String>>,
) -> Result<(), TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let target_rows = read_target_window(
        context.table,
        start_after,
        context.range_end_at.clone(),
        context.chunk_size,
        context.target,
    )?;
    repair_chunk(
        &[],
        &target_rows,
        context.mode,
        context.repair_target,
        context.report,
        context.max_deletes,
    )
}

fn read_source_chunk<S, T, R, P>(
    context: &SyncChunkContext<'_, S, T, R, P>,
) -> Result<Vec<SnapshotRow>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let request = sync_chunk_request(
        context.table,
        context.start_after.clone(),
        context.range_end_at.clone(),
        context.chunk_size,
    );
    context.source.read_rows(&request)
}

fn record_sync_chunk(
    progress: &mut SyncTableProgress,
    report: &mut SyncTableReport,
    row_count: usize,
    end_at: Vec<String>,
    progress_store: &mut impl SyncProgressStore,
) -> Result<(), TableSyncError> {
    report.chunks += 1;
    report.rows_scanned += row_count as u64;
    progress.record_chunk(report, end_at);
    progress_store.save(progress)
}

fn load_sync_progress(
    table: &SyncTable,
    mode: SyncMode,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableProgress, TableSyncError> {
    progress_store.ensure()?;
    let mut progress = progress_store
        .load(&table.name)?
        .filter(|progress| progress.status != SyncProgressStatus::Complete)
        .unwrap_or_else(|| SyncTableProgress::started(table.name.clone(), mode));
    progress.mark_running(mode);
    progress_store.save(&progress)?;
    Ok(progress)
}

fn complete_sync_progress(
    progress: &mut SyncTableProgress,
    progress_store: &mut impl SyncProgressStore,
) -> Result<(), TableSyncError> {
    progress.mark_complete();
    progress_store.save(progress)
}

fn read_target_window(
    table: &SyncTable,
    mut start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    chunk_size: usize,
    target: &impl SyncTableReader,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    let mut rows = Vec::new();

    loop {
        let page = target.read_rows(&sync_chunk_request(
            table,
            start_after.clone(),
            end_at.clone(),
            chunk_size,
        ))?;
        if page.is_empty() {
            return Ok(rows);
        }

        let page_is_complete = page.len() < chunk_size;
        start_after = Some(last_primary_key(&page)?);
        rows.extend(page);

        if page_is_complete {
            return Ok(rows);
        }
    }
}

pub fn run_sync_table(config: &SyncTableConfig) -> Result<SyncTableReport, TableSyncError> {
    validate_sync_table_config(config)?;
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new(target_connection_config(config));
    let mut progress_store =
        progress::MySqlSyncProgressStore::new(config.target.clone(), config.progress_table.clone());
    let mut repair_target = mysql_repair_target(config)?;
    let result = run_sync_table_with_targets(
        config,
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    );
    if let Err(error) = &result {
        progress_store.save_error(&config.table.name, error)?;
    }
    result
}

fn mysql_repair_target(
    config: &SyncTableConfig,
) -> Result<
    crate::target::TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
    TableSyncError,
> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new(&config.target)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    Ok(crate::target::TargetMySqlWriter::from_snapshot_table(
        &snapshot_table(&config.table),
        executor,
        sync_insert_mode(config),
    ))
}

fn run_sync_table_with_targets(
    config: &SyncTableConfig,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableReport, TableSyncError> {
    if let Some(updated_since) = &config.updated_since {
        sync_recent_updates(
            &config.table,
            config.chunk_size,
            config.mode,
            source,
            repair_target,
            updated_since.clone(),
        )
    } else {
        sync_table_with_progress_range(
            &config.table,
            SyncRunOptions {
                chunk_size: config.chunk_size,
                mode: config.mode,
                start_after: config.start_after.clone(),
                end_at: config.end_at.clone(),
                max_deletes: config.max_deletes,
            },
            source,
            target,
            repair_target,
            progress_store,
        )
    }
}

fn validate_sync_table_config(config: &SyncTableConfig) -> Result<(), TableSyncError> {
    if config.updated_since.is_some() && (config.start_after.is_some() || config.end_at.is_some()) {
        return Err(TableSyncError::InvalidTable(
            "updated_since cannot be combined with start_after or end_at".to_string(),
        ));
    }
    Ok(())
}

fn sync_insert_mode(config: &SyncTableConfig) -> crate::target::SnapshotInsertMode {
    if config.updated_since.is_some() {
        crate::target::SnapshotInsertMode::Upsert
    } else {
        crate::target::SnapshotInsertMode::IgnoreDuplicate
    }
}

fn target_connection_config(
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

fn snapshot_table(table: &SyncTable) -> crate::snapshot::SnapshotTable {
    crate::snapshot::SnapshotTable {
        name: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: table.columns.clone(),
    }
}

fn validate_sync_range(
    table: &SyncTable,
    start_after: Option<&Vec<String>>,
    end_at: Option<&Vec<String>>,
) -> Result<(), TableSyncError> {
    validate_bound_arity(&table.primary_key, start_after, "start_after")?;
    validate_bound_arity(&table.primary_key, end_at, "end_at")?;
    Ok(())
}

fn validate_bound_arity(
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

fn validate_sync_table(table: &SyncTable, chunk_size: usize) -> Result<(), TableSyncError> {
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

fn sync_chunk_request_with_updated_since(
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

fn sync_chunk_request(
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

fn last_primary_key(rows: &[SnapshotRow]) -> Result<Vec<String>, TableSyncError> {
    rows.last()
        .map(|row| row.primary_key.clone())
        .ok_or_else(|| TableSyncError::Read("source chunk unexpectedly empty".to_string()))
}

fn repair_chunk(
    source_rows: &[SnapshotRow],
    target_rows: &[SnapshotRow],
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
    max_deletes: Option<u64>,
) -> Result<(), TableSyncError> {
    let source_by_key = rows_by_key(source_rows);
    let target_by_key = rows_by_key(target_rows);

    repair_extra_rows(
        &source_by_key,
        &target_by_key,
        mode,
        repair_target,
        report,
        max_deletes,
    )?;
    repair_changed_rows(&source_by_key, &target_by_key, mode, repair_target, report)?;
    repair_missing_rows(&source_by_key, &target_by_key, mode, repair_target, report)?;

    Ok(())
}

fn repair_extra_rows(
    source_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
    max_deletes: Option<u64>,
) -> Result<(), TableSyncError> {
    for primary_key in target_by_key
        .keys()
        .filter(|primary_key| !source_by_key.contains_key(*primary_key))
    {
        ensure_delete_allowed(report.extra_target_rows, max_deletes, mode)?;
        apply_delete(primary_key, mode, repair_target)?;
        report.extra_target_rows += 1;
    }
    Ok(())
}

fn repair_changed_rows(
    source_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
) -> Result<(), TableSyncError> {
    for (primary_key, source) in source_by_key {
        if target_by_key
            .get(primary_key)
            .is_some_and(|target| source.values != target.values)
        {
            apply_update(source, mode, repair_target)?;
            report.updates += 1;
        }
    }
    Ok(())
}

fn repair_missing_rows(
    source_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
) -> Result<(), TableSyncError> {
    for (primary_key, source) in source_by_key {
        if !target_by_key.contains_key(primary_key) {
            apply_insert(source, mode, repair_target)?;
            report.inserts += 1;
        }
    }
    Ok(())
}

fn apply_recent_update_chunk(
    source_rows: &[SnapshotRow],
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
) -> Result<(), TableSyncError> {
    report.chunks += 1;
    report.rows_scanned += source_rows.len() as u64;
    report.updates += source_rows.len() as u64;
    if mode == SyncMode::Apply {
        for row in source_rows {
            repair_target.insert_row(row)?;
        }
    }
    Ok(())
}

fn rows_by_key(rows: &[SnapshotRow]) -> BTreeMap<Vec<String>, &SnapshotRow> {
    rows.iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect()
}

fn ensure_delete_allowed(
    existing_deletes: u64,
    max_deletes: Option<u64>,
    mode: SyncMode,
) -> Result<(), TableSyncError> {
    if mode == SyncMode::Apply && max_deletes.is_some_and(|limit| existing_deletes >= limit) {
        return Err(TableSyncError::Repair(format!(
            "delete safety threshold exceeded: max_deletes={}",
            max_deletes.expect("checked max deletes")
        )));
    }
    Ok(())
}

fn apply_insert(
    row: &SnapshotRow,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    if mode == SyncMode::Apply {
        repair_target.insert_row(row)?;
    }
    Ok(())
}

fn apply_update(
    row: &SnapshotRow,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    if mode == SyncMode::Apply {
        repair_target.update_row(row)?;
    }
    Ok(())
}

fn apply_delete(
    primary_key: &[String],
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    if mode == SyncMode::Apply {
        repair_target.delete_row(primary_key)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn dry_run_reports_repairs_without_applying_them() {
        let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
        let target = FakeReader::new(vec![row("0", "extra"), row("1", "old")]);
        let mut repair_target = RecordingRepairTarget::default();

        let report = sync_table(
            &account_table(),
            10,
            SyncMode::DryRun,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        assert_eq!(report.inserts, 1);
        assert_eq!(report.updates, 1);
        assert_eq!(report.extra_target_rows, 1);
        assert!(repair_target.inserts.borrow().is_empty());
        assert!(repair_target.updates.borrow().is_empty());
    }

    #[test]
    fn apply_repairs_missing_different_and_extra_target_rows() {
        let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
        let target = FakeReader::new(vec![row("0", "extra"), row("1", "old")]);
        let mut repair_target = RecordingRepairTarget::default();

        let mut progress_store = RecordingProgressStore::default();
        let report = sync_table_with_progress_range(
            &account_table(),
            SyncRunOptions {
                chunk_size: 10,
                mode: SyncMode::Apply,
                start_after: None,
                end_at: None,
                max_deletes: Some(1),
            },
            &source,
            &target,
            &mut repair_target,
            &mut progress_store,
        )
        .expect("sync report");

        assert_eq!(report.inserts, 1);
        assert_eq!(report.updates, 1);
        assert_eq!(report.extra_target_rows, 1);
        assert_eq!(
            repair_target.inserts.borrow().as_slice(),
            &[row("2", "bravo")]
        );
        assert_eq!(
            repair_target.updates.borrow().as_slice(),
            &[row("1", "alpha")]
        );
        assert_eq!(
            repair_target.deletes.borrow().as_slice(),
            &[vec!["0".to_string()]]
        );
    }

    #[test]
    fn apply_stops_before_deleting_above_safety_threshold() {
        let source = FakeReader::new(vec![row("1", "alpha")]);
        let target = FakeReader::new(vec![row("0", "extra"), row("1", "alpha")]);
        let mut repair_target = RecordingRepairTarget::default();
        let mut progress_store = RecordingProgressStore::default();

        let error = sync_table_with_progress_range(
            &account_table(),
            SyncRunOptions {
                chunk_size: 10,
                mode: SyncMode::Apply,
                start_after: None,
                end_at: None,
                max_deletes: Some(0),
            },
            &source,
            &target,
            &mut repair_target,
            &mut progress_store,
        )
        .expect_err("delete threshold");

        assert_eq!(
            error.to_string(),
            "sync repair failed: delete safety threshold exceeded: max_deletes=0"
        );
        assert!(repair_target.deletes.borrow().is_empty());
    }

    #[test]
    fn recent_update_sync_upserts_filtered_source_rows_without_deletes() {
        let source = FakeReader::new(vec![
            row_with_updated_at("1", "alpha", "2026-05-01 00:00:00"),
            row_with_updated_at("2", "bravo", "2026-06-02 00:00:00"),
        ]);
        let mut repair_target = RecordingRepairTarget::default();

        let report = sync_recent_updates(
            &account_table_with_updated_at(),
            10,
            SyncMode::Apply,
            &source,
            &mut repair_target,
            UpdatedSince {
                column: "updated_at".to_string(),
                value: "2026-06-01 00:00:00".to_string(),
            },
        )
        .expect("recent sync");

        assert_eq!(report.rows_scanned, 1);
        assert_eq!(report.updates, 1);
        assert_eq!(
            repair_target.inserts.borrow().as_slice(),
            &[row_with_updated_at("2", "bravo", "2026-06-02 00:00:00")]
        );
        assert!(repair_target.deletes.borrow().is_empty());
    }

    #[test]
    fn core_config_rejects_updated_since_with_primary_key_bounds() {
        let config = SyncTableConfig {
            source: crate::mysql_snapshot::MySqlConnectionConfig::default(),
            target: crate::live::TargetMySqlConfig::default(),
            table: account_table_with_updated_at(),
            chunk_size: 10,
            mode: SyncMode::DryRun,
            progress_table: "cdc.table_sync_progress".to_string(),
            start_after: Some(vec!["10".to_string()]),
            end_at: None,
            max_deletes: Some(0),
            updated_since: Some(UpdatedSince {
                column: "updated_at".to_string(),
                value: "2026-06-01 00:00:00".to_string(),
            }),
        };

        let error = validate_sync_table_config(&config).expect_err("conflicting config");

        assert_eq!(
            error.to_string(),
            "invalid sync table: updated_since cannot be combined with start_after or end_at"
        );
    }

    #[test]
    fn rejects_range_bounds_with_wrong_composite_primary_key_arity() {
        let source = FakeReader::new(vec![]);
        let target = FakeReader::new(vec![]);
        let mut repair_target = RecordingRepairTarget::default();
        let mut progress_store = RecordingProgressStore::default();
        let table = SyncTable {
            name: "accounts".to_string(),
            primary_key: vec!["tenant_id".to_string(), "id".to_string()],
            columns: vec!["tenant_id".to_string(), "id".to_string()],
        };

        let error = sync_table_with_progress_range(
            &table,
            SyncRunOptions {
                chunk_size: 10,
                mode: SyncMode::DryRun,
                start_after: Some(vec!["1".to_string()]),
                end_at: None,
                max_deletes: Some(0),
            },
            &source,
            &target,
            &mut repair_target,
            &mut progress_store,
        )
        .expect_err("bad arity");

        assert_eq!(
            error.to_string(),
            "invalid sync table: start_after has 1 values for 2 primary-key columns"
        );
    }

    #[test]
    fn apply_repairs_target_tail_after_last_source_row() {
        let source = FakeReader::new(vec![row("1", "alpha")]);
        let target = FakeReader::new(vec![row("1", "alpha"), row("2", "extra")]);
        let mut repair_target = RecordingRepairTarget::default();
        let mut progress_store = RecordingProgressStore::default();

        let report = sync_table_with_progress_range(
            &account_table(),
            SyncRunOptions {
                chunk_size: 10,
                mode: SyncMode::Apply,
                start_after: None,
                end_at: None,
                max_deletes: Some(1),
            },
            &source,
            &target,
            &mut repair_target,
            &mut progress_store,
        )
        .expect("sync report");

        assert_eq!(report.extra_target_rows, 1);
        assert_eq!(
            repair_target.deletes.borrow().as_slice(),
            &[vec!["2".to_string()]]
        );
    }

    #[test]
    fn apply_repairs_source_empty_target_range() {
        let source = FakeReader::new(vec![]);
        let target = FakeReader::new(vec![row("2", "extra")]);
        let mut repair_target = RecordingRepairTarget::default();
        let mut progress_store = RecordingProgressStore::default();

        let report = sync_table_with_progress_range(
            &account_table(),
            SyncRunOptions {
                chunk_size: 10,
                mode: SyncMode::Apply,
                start_after: Some(vec!["1".to_string()]),
                end_at: Some(vec!["3".to_string()]),
                max_deletes: Some(1),
            },
            &source,
            &target,
            &mut repair_target,
            &mut progress_store,
        )
        .expect("sync report");

        assert_eq!(report.extra_target_rows, 1);
        assert_eq!(
            repair_target.deletes.borrow().as_slice(),
            &[vec!["2".to_string()]]
        );
    }

    #[test]
    fn apply_releases_unique_conflicts_before_inserting_missing_rows() {
        let source = FakeReader::new(vec![row("10", "shared"), row("20", "correct")]);
        let target = FakeReader::new(vec![row("20", "shared")]);
        let mut repair_target = RecordingRepairTarget::default();

        sync_table(
            &account_table(),
            10,
            SyncMode::Apply,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        assert_eq!(
            repair_target.operations.borrow().as_slice(),
            &["update:20".to_string(), "insert:10".to_string()]
        );
    }

    #[test]
    fn target_read_is_bounded_by_source_chunk_end() {
        let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo"), row("3", "coda")]);
        let target = FakeReader::new(vec![]);
        let mut repair_target = RecordingRepairTarget::default();

        sync_table(
            &account_table(),
            2,
            SyncMode::DryRun,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        let target_requests = target.requests.borrow();
        assert_eq!(target_requests[0].end_at, Some(vec!["2".to_string()]));
        assert_eq!(target_requests[1].start_after, Some(vec!["2".to_string()]));
        assert_eq!(target_requests[1].end_at, Some(vec!["3".to_string()]));
    }

    #[test]
    fn target_read_allows_extra_rows_inside_source_window() {
        let source = FakeReader::new(vec![row("4", "delta")]);
        let target = FakeReader::new(vec![
            row("1", "extra"),
            row("2", "extra"),
            row("3", "extra"),
            row("4", "delta"),
        ]);
        let mut repair_target = RecordingRepairTarget::default();

        let report = sync_table(
            &account_table(),
            1,
            SyncMode::DryRun,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        assert_eq!(report.extra_target_rows, 3);
        assert!(target.requests.borrow().len() > 1);
    }

    #[test]
    fn resumes_from_saved_table_progress_and_saves_each_chunk() {
        let source = FakeReader::new(vec![row("1", "old"), row("2", "bravo"), row("3", "coda")]);
        let target = FakeReader::new(vec![row("2", "bravo"), row("3", "coda")]);
        let mut repair_target = RecordingRepairTarget::default();
        let mut progress_store = RecordingProgressStore::with_progress(SyncTableProgress {
            table: "accounts".to_string(),
            last_primary_key: Some(vec!["1".to_string()]),
            chunks: 1,
            rows_scanned: 1,
            total_rows: None,
            inserts: 0,
            updates: 0,
            extra_target_rows: 0,
            mode: SyncMode::Apply,
            status: progress::SyncProgressStatus::Running,
            last_error: None,
        });

        let report = sync_table_with_progress(
            &account_table(),
            1,
            SyncMode::Apply,
            &source,
            &target,
            &mut repair_target,
            &mut progress_store,
        )
        .expect("sync report");

        assert_eq!(
            source.requests.borrow()[0].start_after,
            Some(vec!["1".to_string()])
        );
        assert_eq!(report.rows_scanned, 3);
        let saved = progress_store.saved.borrow();
        assert_eq!(
            saved.last().expect("saved progress").last_primary_key,
            Some(vec!["3".to_string()])
        );
        assert_eq!(
            saved.last().expect("saved progress").status,
            progress::SyncProgressStatus::Complete
        );
    }

    #[test]
    fn completed_progress_starts_a_fresh_full_scan() {
        let source = FakeReader::new(vec![row("1", "alpha")]);
        let target = FakeReader::new(vec![]);
        let mut repair_target = RecordingRepairTarget::default();
        let mut progress_store = RecordingProgressStore::with_progress(SyncTableProgress {
            table: "accounts".to_string(),
            last_primary_key: Some(vec!["99".to_string()]),
            chunks: 4,
            rows_scanned: 99,
            total_rows: Some(99),
            inserts: 10,
            updates: 20,
            extra_target_rows: 3,
            mode: SyncMode::Apply,
            status: progress::SyncProgressStatus::Complete,
            last_error: None,
        });

        let report = sync_table_with_progress(
            &account_table(),
            10,
            SyncMode::Apply,
            &source,
            &target,
            &mut repair_target,
            &mut progress_store,
        )
        .expect("fresh sync report");

        assert_eq!(source.requests.borrow()[0].start_after, None);
        assert_eq!(report.rows_scanned, 1);
        assert_eq!(report.inserts, 1);
        assert_eq!(report.updates, 0);
        assert_eq!(report.extra_target_rows, 0);
    }

    #[test]
    fn builds_sync_select_with_start_and_end_bounds() {
        let sql = build_sync_select_sql(&SyncChunkRequest {
            table: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
            start_after: Some(vec!["10".to_string()]),
            end_at: Some(vec!["20".to_string()]),
            updated_since: None,
            limit: 100,
        });

        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `accounts` WHERE (`id` > '10') AND NOT ((`id` > '20')) ORDER BY `id` LIMIT 100"
        );
    }

    #[test]
    fn builds_sync_select_with_updated_since_filter() {
        let sql = build_sync_select_sql(&SyncChunkRequest {
            table: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "updated_at".to_string()],
            start_after: Some(vec!["10".to_string()]),
            end_at: None,
            updated_since: Some(UpdatedSince {
                column: "updated_at".to_string(),
                value: "2026-06-01 00:00:00".to_string(),
            }),
            limit: 100,
        });

        assert_eq!(
            sql,
            "SELECT `id`, `updated_at` FROM `accounts` WHERE (`id` > '10') AND `updated_at` >= '2026-06-01 00:00:00' ORDER BY `id` LIMIT 100"
        );
    }

    fn account_table() -> SyncTable {
        SyncTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        }
    }

    fn account_table_with_updated_at() -> SyncTable {
        SyncTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "updated_at".to_string(),
            ],
        }
    }

    fn row(id: &str, name: &str) -> SnapshotRow {
        SnapshotRow {
            primary_key: vec![id.to_string()],
            values: BTreeMap::from([
                ("id".to_string(), id.to_string()),
                ("name".to_string(), name.to_string()),
            ]),
        }
    }

    fn row_with_updated_at(id: &str, name: &str, updated_at: &str) -> SnapshotRow {
        SnapshotRow {
            primary_key: vec![id.to_string()],
            values: BTreeMap::from([
                ("id".to_string(), id.to_string()),
                ("name".to_string(), name.to_string()),
                ("updated_at".to_string(), updated_at.to_string()),
            ]),
        }
    }

    struct FakeReader {
        rows: Vec<SnapshotRow>,
        requests: RefCell<Vec<SyncChunkRequest>>,
    }

    impl FakeReader {
        fn new(rows: Vec<SnapshotRow>) -> Self {
            Self {
                rows,
                requests: RefCell::new(Vec::new()),
            }
        }
    }

    impl SyncTableReader for FakeReader {
        fn read_rows(
            &self,
            request: &SyncChunkRequest,
        ) -> Result<Vec<SnapshotRow>, TableSyncError> {
            self.requests.borrow_mut().push(request.clone());
            Ok(self
                .rows
                .iter()
                .filter(|row| row_in_window(row, request))
                .take(request.limit)
                .cloned()
                .collect())
        }
    }

    fn row_in_window(row: &SnapshotRow, request: &SyncChunkRequest) -> bool {
        let after_start = request
            .start_after
            .as_ref()
            .is_none_or(|start| row.primary_key > *start);
        let before_end = request
            .end_at
            .as_ref()
            .is_none_or(|end| row.primary_key <= *end);
        let after_update = request.updated_since.as_ref().is_none_or(|updated_since| {
            row.values
                .get(&updated_since.column)
                .is_some_and(|value| value >= &updated_since.value)
        });
        after_start && before_end && after_update
    }

    #[derive(Default)]
    struct RecordingRepairTarget {
        inserts: RefCell<Vec<SnapshotRow>>,
        updates: RefCell<Vec<SnapshotRow>>,
        deletes: RefCell<Vec<Vec<String>>>,
        operations: RefCell<Vec<String>>,
    }

    impl SyncRepairTarget for RecordingRepairTarget {
        fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
            self.inserts.borrow_mut().push(row.clone());
            self.operations
                .borrow_mut()
                .push(format!("insert:{}", row.primary_key.join(",")));
            Ok(())
        }

        fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
            self.updates.borrow_mut().push(row.clone());
            self.operations
                .borrow_mut()
                .push(format!("update:{}", row.primary_key.join(",")));
            Ok(())
        }

        fn delete_row(&mut self, primary_key: &[String]) -> Result<(), TableSyncError> {
            self.deletes.borrow_mut().push(primary_key.to_vec());
            self.operations
                .borrow_mut()
                .push(format!("delete:{}", primary_key.join(",")));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingProgressStore {
        loaded: Option<SyncTableProgress>,
        saved: RefCell<Vec<SyncTableProgress>>,
    }

    impl RecordingProgressStore {
        fn with_progress(progress: SyncTableProgress) -> Self {
            Self {
                loaded: Some(progress),
                saved: RefCell::new(Vec::new()),
            }
        }
    }

    impl SyncProgressStore for RecordingProgressStore {
        fn ensure(&mut self) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn load(&self, _table: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
            Ok(self.loaded.clone())
        }

        fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
            self.saved.borrow_mut().push(progress.clone());
            Ok(())
        }

        fn save_error(
            &mut self,
            _table: &str,
            _error: &TableSyncError,
        ) -> Result<(), TableSyncError> {
            Ok(())
        }
    }
}
