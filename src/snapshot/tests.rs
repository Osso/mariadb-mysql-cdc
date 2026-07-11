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
    let ranges = plan_snapshot_ranges(vec![pk("100"), pk("200"), pk("300")], 4).expect("ranges");

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
        default_value: None,
        extra: String::new(),
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
