use super::{
    CatchupSnapshotConfig, CatchupSnapshotError, CatchupTableLogContext, MysqlProgressSaveState,
    save_mysql_snapshot_progress, snapshot_target_for_table,
};
use crate::mysql_client::{PersistentMySqlSource, PersistentProgressWriter};
use crate::snapshot::{
    SnapshotError, SnapshotProgress, SnapshotProgressStore, SnapshotRange, SnapshotTable,
    plan_snapshot_ranges, snapshot_table_range_with_observer,
};
use crate::table_sync::{SyncMode, SyncProgressStatus, SyncTableProgress};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::thread;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CatchupTableMode {
    Sequential,
    Parallel { workers: usize },
}

pub(super) fn catchup_table_mode(total_rows: u64, requested_workers: usize) -> CatchupTableMode {
    let workers = parallel_worker_count(total_rows, requested_workers);
    if workers <= 1 {
        CatchupTableMode::Sequential
    } else {
        CatchupTableMode::Parallel { workers }
    }
}

pub(super) fn parallel_worker_count(total_rows: u64, requested_workers: usize) -> usize {
    let table_workers = usize::try_from(total_rows).unwrap_or(usize::MAX);
    requested_workers.min(table_workers).max(1)
}

pub(super) fn copy_catchup_table_parallel(
    config: &CatchupSnapshotConfig,
    source: &PersistentMySqlSource,
    table: &SnapshotTable,
    workers: usize,
    log_context: CatchupTableLogContext,
    total_rows: u64,
) -> Result<crate::snapshot::SnapshotResult, CatchupSnapshotError> {
    let started_at = Instant::now();
    let boundaries = source.read_range_boundaries(table, workers, total_rows)?;
    let ranges = plan_snapshot_ranges(boundaries, workers)?;
    let rows_copied = copy_catchup_table_ranges(config, table, ranges, log_context)?;
    record_parallel_table_complete(config, table, rows_copied, total_rows)?;
    println!(
        "{}",
        super::format_catchup_table_complete(
            &table.name,
            log_context.table_number,
            log_context.total_tables,
            log_context.completed_tables + 1,
            rows_copied,
            started_at.elapsed().as_secs()
        )
    );
    Ok(crate::snapshot::SnapshotResult {
        table: table.name.clone(),
        rows_copied,
    })
}

fn copy_catchup_table_ranges(
    config: &CatchupSnapshotConfig,
    table: &SnapshotTable,
    ranges: Vec<SnapshotRange>,
    log_context: CatchupTableLogContext,
) -> Result<u64, CatchupSnapshotError> {
    std::thread::scope(|scope| {
        let handles = ranges
            .into_iter()
            .map(|range| {
                let worker_config = config.clone();
                let worker_table = table.clone();
                scope.spawn(move || {
                    copy_catchup_table_range(&worker_config, &worker_table, &range, log_context)
                })
            })
            .collect::<Vec<_>>();
        collect_range_results(handles)
    })
}

fn copy_catchup_table_range(
    config: &CatchupSnapshotConfig,
    table: &SnapshotTable,
    range: &SnapshotRange,
    log_context: CatchupTableLogContext,
) -> Result<crate::snapshot::SnapshotResult, CatchupSnapshotError> {
    let source = PersistentMySqlSource::new(&config.source)?;
    let mut target = snapshot_target_for_table(config, &source, table)?;
    let progress_store = mysql_only_progress_store(config)?;
    let observer = super::CatchupSnapshotLogger::new(
        log_context.table_number,
        log_context.total_tables,
        log_context.completed_tables,
        config.throttle,
    );
    snapshot_table_range_with_observer(
        table,
        range,
        config.chunk_size,
        &progress_store,
        &source,
        &mut target,
        &observer,
    )
    .map_err(CatchupSnapshotError::from)
}

fn collect_range_results(
    handles: Vec<
        thread::ScopedJoinHandle<'_, Result<crate::snapshot::SnapshotResult, CatchupSnapshotError>>,
    >,
) -> Result<u64, CatchupSnapshotError> {
    let mut rows_copied = 0;
    for handle in handles {
        let result = handle.join().map_err(|_| {
            CatchupSnapshotError::Config("catchup range worker panicked".to_string())
        })?;
        rows_copied += result?.rows_copied;
    }
    Ok(rows_copied)
}

fn mysql_only_progress_store(
    config: &CatchupSnapshotConfig,
) -> Result<MysqlOnlyCatchupProgressStore, CatchupSnapshotError> {
    let mysql_store = PersistentProgressWriter::new(&config.target, config.progress_table.clone())
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))?;
    Ok(MysqlOnlyCatchupProgressStore {
        mysql_store,
        total_rows: BTreeMap::new(),
        mysql_save_state: RefCell::new(BTreeMap::new()),
    })
}

fn record_parallel_table_complete(
    config: &CatchupSnapshotConfig,
    table: &SnapshotTable,
    rows_copied: u64,
    total_rows: u64,
) -> Result<(), CatchupSnapshotError> {
    let mysql_store = PersistentProgressWriter::new(&config.target, config.progress_table.clone())
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))?;
    mysql_store
        .ensure()
        .and_then(|_| {
            mysql_store.save(&SyncTableProgress {
                table: table.name.clone(),
                last_primary_key: None,
                chunks: 0,
                rows_scanned: rows_copied,
                total_rows: Some(total_rows),
                inserts: rows_copied,
                updates: 0,
                extra_target_rows: 0,
                mode: SyncMode::Apply,
                status: SyncProgressStatus::Complete,
                last_error: None,
            })
        })
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))?;
    Ok(())
}

struct MysqlOnlyCatchupProgressStore {
    mysql_store: PersistentProgressWriter,
    total_rows: BTreeMap<String, u64>,
    mysql_save_state: RefCell<BTreeMap<String, MysqlProgressSaveState>>,
}

impl SnapshotProgressStore for MysqlOnlyCatchupProgressStore {
    fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
        self.mysql_store
            .ensure()
            .and_then(|_| self.mysql_store.load_snapshot_progress())
            .map_err(|error| SnapshotError::InvalidTable(error.to_string()))
    }

    fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
        save_mysql_snapshot_progress(
            &self.mysql_store,
            &self.total_rows,
            &self.mysql_save_state,
            progress,
        )
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))
    }
}
