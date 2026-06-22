use crate::snapshot::{SnapshotChunkProgress, SnapshotObserver};
use std::time::{Duration, Instant};

pub(super) struct CatchupSnapshotLogger {
    table_number: usize,
    total_tables: usize,
    completed_tables: usize,
    started_at: Instant,
    throttle: Duration,
}

impl CatchupSnapshotLogger {
    pub(super) fn new(
        table_number: usize,
        total_tables: usize,
        completed_tables: usize,
        throttle: Duration,
    ) -> Self {
        Self {
            table_number,
            total_tables,
            completed_tables,
            started_at: Instant::now(),
            throttle,
        }
    }

    pub(super) fn elapsed_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }
}

impl SnapshotObserver for CatchupSnapshotLogger {
    fn chunk_copied(&self, progress: &SnapshotChunkProgress) {
        println!(
            "{}",
            format_catchup_chunk_progress(
                progress,
                self.table_number,
                self.total_tables,
                self.completed_tables,
                self.elapsed_seconds(),
            )
        );
        throttle_after_chunk(self.throttle);
    }
}

fn throttle_after_chunk(throttle: Duration) {
    if throttle.is_zero() {
        return;
    }

    std::thread::sleep(throttle);
}

pub(super) fn format_catchup_table_start(
    table: &str,
    table_number: usize,
    total_tables: usize,
    total_rows: Option<u64>,
) -> String {
    format!(
        "catchup_table_start table={} table_number={} total_tables={} completed_tables={} total_rows={}",
        table,
        table_number,
        total_tables,
        table_number - 1,
        display_optional_u64(total_rows)
    )
}

pub(super) fn format_catchup_chunk_progress(
    progress: &SnapshotChunkProgress,
    table_number: usize,
    total_tables: usize,
    completed_tables: usize,
    elapsed_seconds: u64,
) -> String {
    format!(
        "catchup_table_progress table={} table_number={} total_tables={} completed_tables={} chunk_start={} chunk_end={} chunk_rows={} imported_rows={} skipped_rows={} elapsed_seconds={}",
        progress.table,
        table_number,
        total_tables,
        completed_tables,
        display_primary_key(&progress.chunk_start),
        display_primary_key(&Some(progress.chunk_end.clone())),
        progress.chunk_rows,
        progress.rows_copied,
        0,
        elapsed_seconds
    )
}

pub(super) fn format_catchup_table_complete(
    table: &str,
    table_number: usize,
    total_tables: usize,
    completed_tables: usize,
    rows_copied: u64,
    elapsed_seconds: u64,
) -> String {
    format!(
        "catchup_table_complete table={} table_number={} total_tables={} completed_tables={} rows_copied={} elapsed_seconds={}",
        table, table_number, total_tables, completed_tables, rows_copied, elapsed_seconds
    )
}

fn display_primary_key(value: &Option<Vec<String>>) -> String {
    value
        .as_ref()
        .map(|values| values.join(","))
        .unwrap_or_else(|| "-".to_string())
}

fn display_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}
