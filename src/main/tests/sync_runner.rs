use crate::live::TargetMySqlConfig;
use crate::mysql_config::MySqlConnectionConfig;
use crate::database_row::DatabaseRow;
use crate::sync::{
    SyncChunkConfig, SyncChunkProgress, SyncChunkProgressStore, SyncChunkReadRequest,
    SyncChunkSource, SyncChunkTargetSession, SyncConfig, SyncPrimaryKeyOrdering, SyncRunIdentity,
    SyncTable, build_sync_run_identity, run_sync_tables_bounded, sync_table_to_completion,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

#[test]
fn sync_runner_resumes_and_runs_chunks_until_durable_completion() {
    let table = table("episodes");
    let chunk = chunk_config(&table);
    let mut source = EmptySource::default();
    let mut target = TailTarget::new(vec![vec![row("6")], Vec::new()]);
    let mut progress = MemoryProgress::with_progress(SyncChunkProgress {
        run_id: chunk.run_id.clone(),
        table: table.name.clone(),
        run_spec_json: chunk.run_spec_json.clone(),
        last_primary_key: Some(strings(["5"])),
        complete: false,
        chunks: 4,
        rows_scanned: 100,
        inserts: 2,
        updates: 3,
        deletes: 4,
    });

    let completed = sync_table_to_completion(&chunk, &mut source, &mut target, &mut progress)
        .expect("complete resumed table sync");

    assert!(completed.complete);
    assert_eq!(completed.last_primary_key, Some(strings(["5"])));
    assert_eq!(completed.chunks, 6);
    assert_eq!(completed.deletes, 5);
    assert_eq!(source.requests.len(), 2);
    assert_eq!(target.read_requests.len(), 2);
    assert_eq!(target.deleted, vec![strings(["6"])]);
    assert_eq!(target.commits, 2);
    assert_eq!(progress.saves.len(), 2);
    assert!(!progress.saves[0].complete);
    assert!(progress.saves[1].complete);
}

#[test]
fn sync_runner_returns_the_first_chunk_error_without_retrying() {
    let table = table("episodes");
    let chunk = chunk_config(&table);
    let mut source = EmptySource::default();
    let mut target = TailTarget::new(Vec::new());
    let mut progress = FailingProgress::default();

    let error = sync_table_to_completion(&chunk, &mut source, &mut target, &mut progress)
        .expect_err("progress load failure");

    assert_eq!(
        error,
        "load sync progress for run `sync-run-42` table `episodes`: progress unavailable"
    );
    assert_eq!(progress.loads, 1);
    assert!(source.requests.is_empty());
    assert!(target.read_requests.is_empty());
}

#[test]
fn sync_runner_bounds_concurrency_and_returns_reports_by_table_name() {
    let tables = vec![table("delta"), table("beta"), table("alpha"), table("gamma")];
    let (config, identity) = config_and_identity(&tables, 2);
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));

    let reports = run_sync_tables_bounded(&config, &identity, tables, {
        let active = Arc::clone(&active);
        let max_active = Arc::clone(&max_active);
        let barrier = Arc::clone(&barrier);
        move |_, identity, table| {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(current, Ordering::SeqCst);
            barrier.wait();
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(completed_progress(identity, &table.name))
        }
    })
    .expect("bounded table sync");

    assert_eq!(max_active.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(
        reports
            .iter()
            .map(|report| report.table.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta", "delta", "gamma"]
    );
}

#[test]
fn sync_runner_stops_unscheduled_tables_after_first_failure() {
    let tables = vec![table("alpha"), table("beta"), table("delta"), table("gamma")];
    let (config, identity) = config_and_identity(&tables, 2);
    let started = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(2));

    let error = run_sync_tables_bounded(&config, &identity, tables, {
        let started = Arc::clone(&started);
        let barrier = Arc::clone(&barrier);
        move |_, identity, table| {
            started.lock().expect("started lock").push(table.name.clone());
            barrier.wait();
            if table.name == "alpha" {
                Err("forced row-stage failure".to_string())
            } else {
                Ok(completed_progress(identity, &table.name))
            }
        }
    })
    .expect_err("first table failure");

    assert_eq!(
        error,
        "sync table `alpha` failed: forced row-stage failure"
    );
    let mut started = started.lock().expect("started lock").clone();
    started.sort();
    assert_eq!(started, ["alpha", "beta"]);
}

#[test]
fn sync_runner_revalidates_table_scope_and_parallelism_at_execution() {
    let tables = vec![table("episodes")];
    let (config, identity) = config_and_identity(&tables, 1);
    let never_run = |_: &SyncConfig,
                     _: &SyncRunIdentity,
                     _: SyncTable|
     -> Result<SyncChunkProgress, String> { panic!("runner should not start") };

    assert_eq!(
        run_sync_tables_bounded(&config, &identity, Vec::new(), never_run)
            .expect_err("empty table scope"),
        "sync row stage requires at least one table"
    );

    let mut invalid = config;
    invalid.parallelism = 0;
    assert_eq!(
        run_sync_tables_bounded(&invalid, &identity, tables, never_run)
            .expect_err("zero parallelism"),
        "sync row stage parallelism must be greater than zero"
    );
}

#[derive(Default)]
struct EmptySource {
    requests: Vec<SyncChunkReadRequest>,
}

impl SyncChunkSource for EmptySource {
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<DatabaseRow>, String> {
        self.requests.push(request.clone());
        Ok(Vec::new())
    }
}

struct TailTarget {
    tail_rows: VecDeque<Vec<DatabaseRow>>,
    read_requests: Vec<SyncChunkReadRequest>,
    deleted: Vec<Vec<String>>,
    commits: usize,
}

impl TailTarget {
    fn new(tail_rows: Vec<Vec<DatabaseRow>>) -> Self {
        Self {
            tail_rows: tail_rows.into(),
            read_requests: Vec::new(),
            deleted: Vec::new(),
            commits: 0,
        }
    }
}

impl SyncChunkTargetSession for TailTarget {
    fn set_autocommit(&mut self, _enabled: bool) -> Result<(), String> {
        Ok(())
    }

    fn lock_table_write(&mut self, _database: &str, _table: &str) -> Result<(), String> {
        Ok(())
    }

    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<DatabaseRow>, String> {
        self.read_requests.push(request.clone());
        Ok(self.tail_rows.pop_front().unwrap_or_default())
    }

    fn delete_rows(&mut self, primary_keys: &[Vec<String>]) -> Result<(), String> {
        self.deleted.extend_from_slice(primary_keys);
        Ok(())
    }

    fn update_rows(&mut self, _rows: &[DatabaseRow]) -> Result<(), String> {
        Ok(())
    }

    fn insert_rows(&mut self, _rows: &[DatabaseRow]) -> Result<(), String> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), String> {
        self.commits += 1;
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn unlock_tables(&mut self) -> Result<(), String> {
        Ok(())
    }
}

struct MemoryProgress {
    current: Option<SyncChunkProgress>,
    saves: Vec<SyncChunkProgress>,
}

impl MemoryProgress {
    fn with_progress(progress: SyncChunkProgress) -> Self {
        Self {
            current: Some(progress),
            saves: Vec::new(),
        }
    }
}

impl SyncChunkProgressStore for MemoryProgress {
    fn load(&mut self, _run_id: &str, _table: &str) -> Result<Option<SyncChunkProgress>, String> {
        Ok(self.current.clone())
    }

    fn save(&mut self, progress: &SyncChunkProgress) -> Result<(), String> {
        self.current = Some(progress.clone());
        self.saves.push(progress.clone());
        Ok(())
    }
}

#[derive(Default)]
struct FailingProgress {
    loads: usize,
}

impl SyncChunkProgressStore for FailingProgress {
    fn load(&mut self, _run_id: &str, _table: &str) -> Result<Option<SyncChunkProgress>, String> {
        self.loads += 1;
        Err("progress unavailable".to_string())
    }

    fn save(&mut self, _progress: &SyncChunkProgress) -> Result<(), String> {
        panic!("failed progress must not save")
    }
}

fn config_and_identity(
    tables: &[SyncTable],
    parallelism: usize,
) -> (SyncConfig, SyncRunIdentity) {
    let source = MySqlConnectionConfig {
        host: "source".to_string(),
        user: "source-user".to_string(),
        password: "source-password".to_string(),
        database: "source-db".to_string(),
        ..MySqlConnectionConfig::default()
    };
    let target = TargetMySqlConfig {
        host: "target".to_string(),
        user: "target-user".to_string(),
        password: "target-password".to_string(),
        database: "target-db".to_string(),
        tls_ca_file: "/tmp/test-ca.pem".to_string(),
        ..TargetMySqlConfig::default()
    };
    let config = SyncConfig {
        source,
        target,
        tables: tables.iter().map(|table| table.name.clone()).collect(),
        chunk_size: 1,
        parallelism,
        progress_table: "cdc.sync_runs".to_string(),
        run_id: Some("sync-run-42".to_string()),
        run_id_prefix: None,
    };
    let identity = build_sync_run_identity(&config, tables.to_vec()).expect("sync run identity");
    (config, identity)
}

fn chunk_config(table: &SyncTable) -> SyncChunkConfig {
    SyncChunkConfig {
        run_id: "sync-run-42".to_string(),
        run_spec_json: r#"{"tables":["episodes"]}"#.to_string(),
        target_database: "target-db".to_string(),
        table: table.clone(),
        chunk_size: 1,
    }
}

fn completed_progress(identity: &SyncRunIdentity, table: &str) -> SyncChunkProgress {
    SyncChunkProgress {
        run_id: identity.run_id.clone(),
        table: table.to_string(),
        run_spec_json: identity.run_spec_json.clone(),
        last_primary_key: Some(strings(["9"])),
        complete: true,
        chunks: 1,
        rows_scanned: 1,
        inserts: 0,
        updates: 0,
        deletes: 0,
    }
}

fn table(name: &str) -> SyncTable {
    SyncTable {
        name: name.to_string(),
        primary_key: strings(["id"]),
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns: strings(["id", "title"]),
    }
}

fn row(id: &str) -> DatabaseRow {
    DatabaseRow {
        primary_key: strings([id]),
        values: BTreeMap::from([
            ("id".to_string(), Some(id.to_string())),
            ("title".to_string(), Some(format!("title-{id}"))),
        ]),
    }
}

fn strings<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}
