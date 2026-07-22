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
    let rows_copied = copy_catchup_table_ranges(config, table, ranges, log_context, total_rows)?;
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
    total_rows: u64,
) -> Result<u64, CatchupSnapshotError> {
    let workers = ranges.len();
    std::thread::scope(|scope| {
        let handles = ranges
            .into_iter()
            .map(|range| {
                let worker_config = config.clone();
                let worker_table = table.clone();
                scope.spawn(move || {
                    let range_total_rows = range_total_rows(total_rows, workers, range.worker);
                    copy_catchup_table_range(
                        &worker_config,
                        &worker_table,
                        &range,
                        range_total_rows,
                        log_context,
                    )
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
    range_total_rows: u64,
    log_context: CatchupTableLogContext,
) -> Result<crate::snapshot::SnapshotResult, CatchupSnapshotError> {
    let source = PersistentMySqlSource::new(&config.source)?;
    let mut target = snapshot_target_for_table(config, &source, table)?;
    let progress_store = mysql_only_progress_store(config, table, range, range_total_rows)?;
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
    table: &SnapshotTable,
    range: &SnapshotRange,
    range_total_rows: u64,
) -> Result<MysqlOnlyCatchupProgressStore, CatchupSnapshotError> {
    let mysql_store = PersistentProgressWriter::new(&config.target, config.progress_table.clone())
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))?;
    let mut total_rows = BTreeMap::new();
    total_rows.insert(
        range_progress_key(&table.name, range.worker),
        range_total_rows,
    );
    Ok(MysqlOnlyCatchupProgressStore {
        mysql_store,
        progress_table: config.progress_table.clone(),
        total_rows,
        mysql_save_state: RefCell::new(BTreeMap::new()),
    })
}

fn range_progress_key(table: &str, worker: usize) -> String {
    format!("{table}#range{worker}")
}

fn range_total_rows(total_rows: u64, workers: usize, worker: usize) -> u64 {
    let offsets = snapshot_boundary_offsets(total_rows, workers);
    match (
        worker,
        offsets.get(worker),
        worker.checked_sub(1).and_then(|index| offsets.get(index)),
    ) {
        (0, Some(end), _) => end + 1,
        (_, Some(end), Some(previous)) => end - previous,
        (_, None, Some(previous)) => total_rows.saturating_sub(previous + 1),
        _ => total_rows,
    }
}

fn snapshot_boundary_offsets(total_rows: u64, workers: usize) -> Vec<u64> {
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
                run_id: None,
                run_spec_json: None,
                table: table.name.clone(),
                last_primary_key: None,
                chunks: 0,
                rows_scanned: rows_copied,
                total_rows: Some(total_rows),
                inserts: rows_copied,
                updates: 0,
                extra_target_rows: 0,
                delete_preflight_complete: false,
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
    progress_table: String,
    total_rows: BTreeMap<String, u64>,
    mysql_save_state: RefCell<BTreeMap<String, MysqlProgressSaveState>>,
}

impl SnapshotProgressStore for MysqlOnlyCatchupProgressStore {
    fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
        self.mysql_store
            .ensure()
            .map_err(|error| SnapshotError::ProgressSchemaEnsure {
                progress_table: self.progress_table.clone(),
                source: Box::new(SnapshotError::InvalidTable(error.to_string())),
            })?;
        self.mysql_store
            .load_snapshot_progress()
            .map_err(|error| SnapshotError::ProgressRowRead {
                progress_table: self.progress_table.clone(),
                source: Box::new(SnapshotError::InvalidTable(error.to_string())),
            })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_totals_sum_to_table_total() {
        let totals = (0..4)
            .map(|worker| range_total_rows(10, 4, worker))
            .collect::<Vec<_>>();

        assert_eq!(totals, vec![3, 2, 3, 2]);
        assert_eq!(totals.iter().sum::<u64>(), 10);
    }

    #[test]
    fn range_totals_handle_more_workers_than_rows() {
        let totals = (0..2)
            .map(|worker| range_total_rows(2, 2, worker))
            .collect::<Vec<_>>();

        assert_eq!(totals, vec![1, 1]);
    }
}
