use super::progress_log::format_catchup_chunk_progress;
use super::*;
use crate::mysql_client::PersistentMySqlSource;
use crate::mysql_support::target_mysql_opts;
use crate::snapshot::SnapshotChunkProgress;

#[test]
fn builds_first_chunk_select() {
    let sql = build_select_chunk_sql(&ChunkRequest {
        table: "accounts".to_string(),
        primary_key: vec!["id".to_string()],
        selected_columns: vec!["id".to_string(), "name".to_string()],
        start_after: None,
        end_at: None,
        limit: 100,
    });

    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `accounts` ORDER BY `id` LIMIT 100"
    );
}

#[test]
fn builds_resume_select_for_composite_primary_key() {
    let sql = build_select_chunk_sql(&ChunkRequest {
        table: "edges".to_string(),
        primary_key: vec!["left_id".to_string(), "right_id".to_string()],
        selected_columns: vec!["left_id".to_string(), "right_id".to_string()],
        start_after: Some(vec!["10".to_string(), "20".to_string()]),
        end_at: None,
        limit: 50,
    });

    assert_eq!(
        sql,
        "SELECT `left_id`, `right_id` FROM `edges` WHERE (`left_id` > '10') OR (`left_id` = '10' AND `right_id` > '20') ORDER BY `left_id`, `right_id` LIMIT 50"
    );
}

#[test]
fn builds_bounded_select_for_snapshot_range() {
    let sql = build_select_chunk_sql(&ChunkRequest {
        table: "accounts".to_string(),
        primary_key: vec!["id".to_string()],
        selected_columns: vec!["id".to_string(), "name".to_string()],
        start_after: Some(vec!["100".to_string()]),
        end_at: Some(vec!["200".to_string()]),
        limit: 100,
    });

    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `accounts` WHERE (`id` > '100') AND NOT ((`id` > '200')) ORDER BY `id` LIMIT 100"
    );
}

#[test]
fn parses_snapshot_rows_with_primary_key_values() {
    let rows = parse_snapshot_rows(
        &["id".to_string(), "name".to_string()],
        &["id".to_string()],
        "1\talpha\n2\tbeta\n",
    )
    .expect("rows");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].primary_key, vec!["1"]);
    assert_eq!(rows[1].values["name"], Some("beta".to_string()));
}

#[test]
fn rejects_null_snapshot_primary_key() {
    let values = BTreeMap::from([
        ("id".to_string(), None),
        ("name".to_string(), Some("value".to_string())),
    ]);

    let error = primary_key_values(&["id".to_string()], &values).expect_err("null primary key");

    assert_eq!(error.to_string(), "primary-key column `id` was NULL");
}

#[test]
fn mysql_progress_save_is_throttled_until_interval_or_completion() {
    let now = std::time::Instant::now();
    let mut state = MysqlProgressSaveState::default();
    let running = SyncProgressStatus::Running;
    let complete = SyncProgressStatus::Complete;

    assert!(state.should_save(1_000, running, now));
    state.record_save(1_000, running, now);

    assert!(!state.should_save(2_000, running, now + MYSQL_PROGRESS_SAVE_INTERVAL / 2));
    assert!(state.should_save(3_000, running, now + MYSQL_PROGRESS_SAVE_INTERVAL));
    assert!(state.should_save(3_000, complete, now + MYSQL_PROGRESS_SAVE_INTERVAL / 2));
}

#[test]
fn parallel_worker_count_is_bounded_by_table_rows() {
    assert_eq!(parallel_worker_count(0, 4), 1);
    assert_eq!(parallel_worker_count(2, 4), 2);
    assert_eq!(parallel_worker_count(100, 4), 4);
}

#[test]
fn catchup_table_mode_uses_parallel_only_when_requested() {
    assert_eq!(catchup_table_mode(100, 1), CatchupTableMode::Sequential);
    assert_eq!(
        catchup_table_mode(100, 4),
        CatchupTableMode::Parallel { workers: 4 }
    );
    assert_eq!(
        catchup_table_mode(2, 4),
        CatchupTableMode::Parallel { workers: 2 }
    );
}

#[test]
fn catchup_snapshot_accepts_plaintext_source_without_tls_ca() {
    let config = valid_catchup_config();

    validate_config(&config).expect("plaintext source without CA should be accepted");
}

#[test]
fn catchup_source_inventory_uses_plaintext_without_tls_ca() {
    let mut config = valid_catchup_config();
    config.source.host = "127.0.0.1".to_string();
    config.source.port = 1;

    let error = read_snapshot_tables(&config).expect_err("source inventory connection should fail");
    let message = error.to_string();

    assert!(message.contains("inventory failed"));
    assert!(!message.contains("TLS CA file"));
}

#[test]
fn persistent_snapshot_source_uses_plaintext_without_tls_ca() {
    let mut source = valid_catchup_config().source;
    source.host = "127.0.0.1".to_string();
    source.port = 1;

    let error = match PersistentMySqlSource::new(&source) {
        Ok(_) => panic!("source connection should fail"),
        Err(error) => error,
    };
    let message = error.to_string();

    assert!(message.contains("failed to connect to source mysql"));
    assert!(!message.contains("TLS CA file"));
}

#[test]
fn catchup_target_still_requires_tls_ca() {
    let mut config = valid_catchup_config();
    config.target.tls_ca_file = String::new();

    let error = validate_config(&config).expect_err("missing target TLS CA");

    assert_eq!(error.to_string(), "target TLS CA file is required");
}

#[test]
fn catchup_target_dns_keeps_hostname_verification() {
    let target = valid_catchup_config().target;

    let opts = target_mysql_opts(&target).expect("target TLS opts");
    let ssl = opts.get_ssl_opts().expect("target TLS configured");

    assert!(!ssl.skip_domain_validation());
    assert!(!ssl.accept_invalid_certs());
}

#[test]
fn rejects_zero_parallel_workers() {
    let mut config = valid_catchup_config();
    config.parallel_workers = 0;

    let error = validate_config(&config).expect_err("zero workers");

    assert_eq!(
        error.to_string(),
        "parallel workers must be greater than zero"
    );
}

#[test]
fn rejects_target_schema_missing_snapshot_column() {
    let table = SnapshotTable {
        name: "accounts".to_string(),
        primary_key: vec!["id".to_string()],
        columns: vec!["id".to_string(), "name".to_string()],
    };
    let target_columns = vec!["id".to_string()];

    let error = validate_target_table_columns(&table, &target_columns).expect_err("schema error");

    assert_eq!(
        error.to_string(),
        "target table accounts is missing source columns: name"
    );
}

#[test]
fn accepts_target_schema_with_extra_columns() {
    let table = SnapshotTable {
        name: "accounts".to_string(),
        primary_key: vec!["id".to_string()],
        columns: vec!["id".to_string(), "name".to_string()],
    };
    let target_columns = vec!["id".to_string(), "name".to_string(), "extra".to_string()];

    validate_target_table_columns(&table, &target_columns).expect("compatible schema");
}

#[test]
fn catchup_snapshot_uses_persistent_clients_for_chunk_io() {
    let source = include_str!("../mysql_snapshot.rs");
    let target_schema = include_str!("target_schema.rs");

    assert!(source.contains("PersistentMySqlSource::new(&config.source)"));
    assert!(target_schema.contains("PersistentTargetExecutor::new(&config.target)"));
    assert!(source.contains("PersistentProgressWriter::new(&config.target"));
    assert!(!source.contains("MysqlCliExecutor"));
    assert!(!source.contains("Command::new"));
    assert!(!target_schema.contains("MysqlCliExecutor"));
    assert!(!target_schema.contains("Command::new"));
}

#[test]
fn catchup_snapshot_parallel_path_uses_threaded_range_workers() {
    let source = include_str!("parallel.rs");

    assert!(source.contains("std::thread::scope"));
    assert!(source.contains("read_range_boundaries"));
    assert!(source.contains("snapshot_table_range_with_observer"));
}

fn valid_catchup_config() -> CatchupSnapshotConfig {
    CatchupSnapshotConfig {
        source: MySqlConnectionConfig {
            host: "source".to_string(),
            port: 3306,
            user: "reader".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
        },
        target: TargetMySqlConfig {
            host: "target".to_string(),
            port: 25060,
            user: "writer".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
            insert_conflict_policy: crate::live::InsertConflictPolicy::IgnoreDuplicate,
        },
        progress_file: std::path::PathBuf::from("progress.json"),
        progress_table: "cdc.table_sync_progress".to_string(),
        chunk_size: 10_000,
        throttle: Duration::ZERO,
        parallel_workers: 1,
        table: None,
    }
}

#[test]
fn formats_catchup_table_start_with_total_rows() {
    let line = format_catchup_table_start("accounts", 2, 10, Some(42_000));

    assert_eq!(
        line,
        "catchup_table_start table=accounts table_number=2 total_tables=10 completed_tables=1 total_rows=42000"
    );
}

#[test]
fn formats_catchup_chunk_progress_with_bounds_and_elapsed_time() {
    let progress = SnapshotChunkProgress {
        table: "accounts".to_string(),
        chunk_start: Some(vec!["100".to_string()]),
        chunk_end: vec!["200".to_string()],
        chunk_rows: 100,
        rows_copied: 1_200,
    };

    let line = format_catchup_chunk_progress(&progress, 2, 10, 1, 35);

    assert_eq!(
        line,
        "catchup_table_progress table=accounts table_number=2 total_tables=10 completed_tables=1 chunk_start=100 chunk_end=200 chunk_rows=100 imported_rows=1200 skipped_rows=0 elapsed_seconds=35"
    );
}

#[test]
fn formats_catchup_table_completion_with_completed_count() {
    let line = format_catchup_table_complete("accounts", 2, 10, 2, 1_200, 35);

    assert_eq!(
        line,
        "catchup_table_complete table=accounts table_number=2 total_tables=10 completed_tables=2 rows_copied=1200 elapsed_seconds=35"
    );
}

#[test]
fn catchup_progress_loads_mysql_progress_when_file_is_empty() {
    let path = unique_path("empty-progress.json");
    let mysql_progress = snapshot_progress("accounts", Some(vec!["42".to_string()]), 42, false);
    let store = catchup_progress_store_for_test(&path, mysql_progress.clone());

    let loaded = store.load().expect("progress");

    assert_eq!(loaded, mysql_progress);
    assert_eq!(*store.mysql_store.ensure_calls.borrow(), 1);
    assert_eq!(*store.mysql_store.load_calls.borrow(), 1);
}

#[test]
fn catchup_progress_load_classifies_schema_ensure_failure() {
    let path = unique_path("ensure-error.json");
    let store = catchup_progress_store_for_test(&path, SnapshotProgress::default());
    *store.mysql_store.ensure_error.borrow_mut() = Some("permission denied".to_string());

    let error = store.load().expect_err("ensure error");

    assert_eq!(
        error.to_string(),
        "progress schema ensure failed progress_table=cdc.snapshot_progress: sync progress failed: permission denied"
    );
}

#[test]
fn catchup_progress_load_classifies_row_read_failure() {
    let path = unique_path("row-read-error.json");
    let store = catchup_progress_store_for_test(&path, SnapshotProgress::default());
    *store.mysql_store.load_error.borrow_mut() = Some("connection reset".to_string());

    let error = store.load().expect_err("row read error");

    assert_eq!(
        error.to_string(),
        "progress row read failed progress_table=cdc.snapshot_progress: sync progress failed: connection reset"
    );
}

#[test]
fn catchup_progress_prefers_file_progress_over_mysql_progress() {
    let path = unique_path("file-progress.json");
    let file_store = FileSnapshotProgressStore::new(path.clone());
    let file_progress = snapshot_progress("accounts", Some(vec!["9".to_string()]), 9, false);
    file_store.save(&file_progress).expect("save file progress");
    let mysql_progress = snapshot_progress("accounts", Some(vec!["42".to_string()]), 42, false);
    let store = catchup_progress_store_for_test(&path, mysql_progress);

    let loaded = store.load().expect("progress");

    assert_eq!(loaded, file_progress);
    assert_eq!(*store.mysql_store.ensure_calls.borrow(), 0);
    assert_eq!(*store.mysql_store.load_calls.borrow(), 0);

    let _ = std::fs::remove_file(path);
}

#[test]
fn catchup_progress_records_table_error_for_run_history() {
    let path = unique_path("error-progress.json");
    let store = catchup_progress_store_for_test(&path, SnapshotProgress::default());
    let error = CatchupSnapshotError::Config("source disconnected".to_string());

    store
        .save_table_error("accounts", &error)
        .expect("save table error");

    assert_eq!(
        store.mysql_store.errors.borrow().as_slice(),
        &[("accounts".to_string(), "source disconnected".to_string())]
    );
}

fn catchup_progress_store_for_test(
    path: &std::path::Path,
    mysql_progress: SnapshotProgress,
) -> CatchupProgressStore<FakeMysqlProgressStore> {
    CatchupProgressStore {
        file_store: FileSnapshotProgressStore::new(path),
        mysql_store: FakeMysqlProgressStore::new(mysql_progress),
        progress_table: "cdc.snapshot_progress".to_string(),
        total_rows: RefCell::new(BTreeMap::new()),
        mysql_save_state: RefCell::new(BTreeMap::new()),
    }
}

fn snapshot_progress(
    table: &str,
    last_primary_key: Option<Vec<String>>,
    rows_copied: u64,
    complete: bool,
) -> SnapshotProgress {
    let table_progress = crate::snapshot::TableSnapshotProgress {
        last_primary_key,
        rows_copied,
        complete,
    };
    SnapshotProgress {
        tables: BTreeMap::from([(table.to_string(), table_progress)]),
    }
}

fn unique_path(file_name: &str) -> std::path::PathBuf {
    let mut fixture_path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    fixture_path.push(format!(
        "mariadb-mysql-cdc-mysql-snapshot-{nanos}-{file_name}"
    ));
    fixture_path
}

struct FakeMysqlProgressStore {
    progress: SnapshotProgress,
    ensure_calls: RefCell<u32>,
    load_calls: RefCell<u32>,
    ensure_error: RefCell<Option<String>>,
    load_error: RefCell<Option<String>>,
    saved: RefCell<Vec<SyncTableProgress>>,
    errors: RefCell<Vec<(String, String)>>,
}

impl FakeMysqlProgressStore {
    fn new(progress: SnapshotProgress) -> Self {
        Self {
            progress,
            ensure_calls: RefCell::new(0),
            load_calls: RefCell::new(0),
            ensure_error: RefCell::new(None),
            load_error: RefCell::new(None),
            saved: RefCell::new(Vec::new()),
            errors: RefCell::new(Vec::new()),
        }
    }
}

impl CatchupMysqlProgressStore for FakeMysqlProgressStore {
    fn ensure(&self) -> Result<(), TableSyncError> {
        *self.ensure_calls.borrow_mut() += 1;
        if let Some(message) = self.ensure_error.borrow().as_deref() {
            return Err(TableSyncError::Progress(message.to_string()));
        }
        Ok(())
    }

    fn load_snapshot_progress(&self) -> Result<SnapshotProgress, TableSyncError> {
        *self.load_calls.borrow_mut() += 1;
        if let Some(message) = self.load_error.borrow().as_deref() {
            return Err(TableSyncError::Progress(message.to_string()));
        }
        Ok(self.progress.clone())
    }

    fn save(&self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.saved.borrow_mut().push(progress.clone());
        Ok(())
    }

    fn save_error_message(&self, table: &str, error: &str) -> Result<(), TableSyncError> {
        self.errors
            .borrow_mut()
            .push((table.to_string(), error.to_string()));
        Ok(())
    }
}
