use crate::snapshot::SnapshotRow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

mod mysql;
mod progress;
mod target;

use mysql::MySqlSyncReader;
#[cfg(test)]
pub(crate) use mysql::build_sync_select_sql;
pub use progress::{NoopSyncProgressStore, SyncProgressStore, SyncTableProgress};
pub use target::SyncRepairTarget;

#[derive(Clone, Debug)]
pub struct SyncTableConfig {
    pub source: crate::mysql_snapshot::MySqlConnectionConfig,
    pub target: crate::live::TargetMySqlConfig,
    pub mariadb: String,
    pub table: SyncTable,
    pub chunk_size: usize,
    pub mode: SyncMode,
    pub progress_table: String,
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
    pub limit: usize,
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
    validate_sync_table(table, chunk_size)?;
    let mut progress = load_sync_progress(table, mode, progress_store)?;
    let mut report = progress.report();
    let mut start_after = progress.last_primary_key.clone();

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
}

fn sync_next_chunk<S, T, R, P>(
    context: SyncChunkContext<'_, S, T, R, P>,
) -> Result<Option<Vec<String>>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let source_rows = read_source_chunk(&context)?;
    if source_rows.is_empty() {
        return Ok(None);
    }

    let end_at = last_primary_key(&source_rows)?;
    let target_rows = read_target_window(
        context.table,
        context.start_after,
        Some(end_at.clone()),
        context.chunk_size,
        context.target,
    )?;
    repair_chunk(
        &source_rows,
        &target_rows,
        context.mode,
        context.repair_target,
        context.report,
    )?;
    record_sync_chunk(
        context.progress,
        context.report,
        source_rows.len(),
        end_at.clone(),
        context.progress_store,
    )?;

    if source_rows.len() < context.chunk_size {
        Ok(None)
    } else {
        Ok(Some(end_at))
    }
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
        None,
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
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new(target_connection_config(config));
    let executor =
        crate::live::MysqlCliExecutor::new(config.mariadb.clone(), config.target.clone());
    let mut progress_store = progress::MySqlSyncProgressStore::new(
        config.mariadb.clone(),
        config.target.clone(),
        config.progress_table.clone(),
    );
    let mut repair_target = crate::target::TargetMySqlWriter::from_snapshot_table(
        &snapshot_table(&config.table),
        executor,
        crate::target::SnapshotInsertMode::IgnoreDuplicate,
    );
    let result = sync_table_with_progress(
        &config.table,
        config.chunk_size,
        config.mode,
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

fn target_connection_config(
    config: &SyncTableConfig,
) -> crate::mysql_snapshot::MySqlConnectionConfig {
    crate::mysql_snapshot::MySqlConnectionConfig {
        host: config.target.host.clone(),
        port: config.target.port,
        user: config.target.user.clone(),
        password: config.target.password.clone(),
        database: config.target.database.clone(),
        mariadb: config.mariadb.clone(),
    }
}

fn snapshot_table(table: &SyncTable) -> crate::snapshot::SnapshotTable {
    crate::snapshot::SnapshotTable {
        name: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: table.columns.clone(),
    }
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
) -> Result<(), TableSyncError> {
    let source_by_key = rows_by_key(source_rows);
    let target_by_key = rows_by_key(target_rows);

    for primary_key in row_keys(&source_by_key, &target_by_key) {
        match (
            source_by_key.get(&primary_key),
            target_by_key.get(&primary_key),
        ) {
            (Some(source), Some(target)) if source.values != target.values => {
                apply_update(source, mode, repair_target)?;
                report.updates += 1;
            }
            (Some(source), None) => {
                apply_insert(source, mode, repair_target)?;
                report.inserts += 1;
            }
            (None, Some(_target)) => report.extra_target_rows += 1,
            _ => {}
        }
    }

    Ok(())
}

fn rows_by_key(rows: &[SnapshotRow]) -> BTreeMap<Vec<String>, &SnapshotRow> {
    rows.iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect()
}

fn row_keys(
    source: &BTreeMap<Vec<String>, &SnapshotRow>,
    target: &BTreeMap<Vec<String>, &SnapshotRow>,
) -> BTreeSet<Vec<String>> {
    source.keys().chain(target.keys()).cloned().collect()
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
    fn apply_repairs_missing_and_different_target_rows() {
        let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
        let target = FakeReader::new(vec![row("1", "old")]);
        let mut repair_target = RecordingRepairTarget::default();

        let report = sync_table(
            &account_table(),
            10,
            SyncMode::Apply,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        assert_eq!(report.inserts, 1);
        assert_eq!(report.updates, 1);
        assert_eq!(
            repair_target.inserts.borrow().as_slice(),
            &[row("2", "bravo")]
        );
        assert_eq!(
            repair_target.updates.borrow().as_slice(),
            &[row("1", "alpha")]
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
    fn builds_sync_select_with_start_and_end_bounds() {
        let sql = build_sync_select_sql(&SyncChunkRequest {
            table: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
            start_after: Some(vec!["10".to_string()]),
            end_at: Some(vec!["20".to_string()]),
            limit: 100,
        });

        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `accounts` WHERE (`id` > '10') AND NOT ((`id` > '20')) ORDER BY `id` LIMIT 100"
        );
    }

    fn account_table() -> SyncTable {
        SyncTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
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
        after_start && before_end
    }

    #[derive(Default)]
    struct RecordingRepairTarget {
        inserts: RefCell<Vec<SnapshotRow>>,
        updates: RefCell<Vec<SnapshotRow>>,
    }

    impl SyncRepairTarget for RecordingRepairTarget {
        fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
            self.inserts.borrow_mut().push(row.clone());
            Ok(())
        }

        fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
            self.updates.borrow_mut().push(row.clone());
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
