use crate::live::{ApplyBinlogConfig, ApplyBinlogError, ApplyBinlogReport, apply_remote_binlog};
use crate::snapshot::{
    SnapshotError, SnapshotFence, SnapshotProgress, SnapshotProgressStore, SnapshotResult,
    SnapshotSource, SnapshotTable, SnapshotTarget, snapshot_table,
};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatchupPlan {
    pub tables: Vec<SnapshotTable>,
    pub chunk_size: usize,
    pub start_file: String,
    pub start_position: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatchupReport {
    pub snapshot_fence: SnapshotFence,
    pub snapshot_results: Vec<SnapshotResult>,
    pub replay_report: ApplyBinlogReport,
}

pub trait CdcReplay {
    fn replay_from_start(
        &self,
        start_file: &str,
        start_position: u64,
    ) -> Result<ApplyBinlogReport, CatchupError>;
}

pub struct BinlogCdcReplay {
    config: ApplyBinlogConfig,
}

impl BinlogCdcReplay {
    pub fn new(config: ApplyBinlogConfig) -> Self {
        Self { config }
    }
}

impl CdcReplay for BinlogCdcReplay {
    fn replay_from_start(
        &self,
        start_file: &str,
        start_position: u64,
    ) -> Result<ApplyBinlogReport, CatchupError> {
        let mut config = self.config.clone();
        config.source.binlog_file = start_file.to_string();
        config.source.start_position = start_position;
        apply_remote_binlog(&config).map_err(CatchupError::Replay)
    }
}

#[derive(Debug)]
pub enum CatchupError {
    InvalidPlan(String),
    Snapshot(SnapshotError),
    Replay(ApplyBinlogError),
}

impl fmt::Display for CatchupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan(message) => formatter.write_str(message),
            Self::Snapshot(source) => write!(formatter, "catchup snapshot failed: {source}"),
            Self::Replay(source) => write!(formatter, "catchup replay failed: {source}"),
        }
    }
}

impl std::error::Error for CatchupError {}

impl From<SnapshotError> for CatchupError {
    fn from(source: SnapshotError) -> Self {
        Self::Snapshot(source)
    }
}

pub fn run_catchup<P, S, T, R>(
    plan: &CatchupPlan,
    progress_store: &P,
    snapshot_source: &S,
    snapshot_target: &mut T,
    replay: &R,
) -> Result<CatchupReport, CatchupError>
where
    P: SnapshotProgressStore,
    S: SnapshotSource,
    T: SnapshotTarget,
    R: CdcReplay,
{
    validate_plan(plan)?;
    let snapshot_fence = load_or_capture_snapshot_fence(progress_store, snapshot_source)?;
    let snapshot_results = snapshot_tables(plan, progress_store, snapshot_source, snapshot_target)?;
    let mut progress = progress_store.load()?;
    let completed_fence = finalize_snapshot_fence(plan, &progress, snapshot_fence.clone())?;
    progress.snapshot_fence = Some(completed_fence.clone());
    progress_store.save(&progress)?;
    let replay_report =
        replay.replay_from_start(&snapshot_fence.source_file, snapshot_fence.source_position)?;

    Ok(CatchupReport {
        snapshot_fence: completed_fence,
        snapshot_results,
        replay_report,
    })
}

fn load_or_capture_snapshot_fence<P, S>(
    progress_store: &P,
    snapshot_source: &S,
) -> Result<SnapshotFence, CatchupError>
where
    P: SnapshotProgressStore,
    S: SnapshotSource,
{
    let mut progress = progress_store.load()?;
    if let Some(fence) = progress.snapshot_fence {
        fence.validate().map_err(CatchupError::Snapshot)?;
        return Ok(fence);
    }
    if !progress.tables.is_empty() {
        return Err(CatchupError::InvalidPlan(
            "snapshot progress is missing source fencing metadata".to_string(),
        ));
    }

    let fence = snapshot_source.capture_start_coordinate()?;
    fence.validate().map_err(CatchupError::Snapshot)?;
    progress.snapshot_fence = Some(fence.clone());
    progress_store.save(&progress)?;
    Ok(fence)
}

fn snapshot_tables<P, S, T>(
    plan: &CatchupPlan,
    progress_store: &P,
    snapshot_source: &S,
    snapshot_target: &mut T,
) -> Result<Vec<SnapshotResult>, CatchupError>
where
    P: SnapshotProgressStore,
    S: SnapshotSource,
    T: SnapshotTarget,
{
    let mut results = Vec::new();

    for table in &plan.tables {
        let result = snapshot_table(
            table,
            plan.chunk_size,
            progress_store,
            snapshot_source,
            snapshot_target,
        )?;
        results.push(result);
    }

    Ok(results)
}

fn finalize_snapshot_fence(
    plan: &CatchupPlan,
    progress: &SnapshotProgress,
    mut fence: SnapshotFence,
) -> Result<SnapshotFence, CatchupError> {
    if !plan.tables.iter().all(|table| {
        progress
            .table(&table.name)
            .is_some_and(|table_progress| table_progress.complete)
    }) {
        return Err(CatchupError::InvalidPlan(
            "snapshot progress is incomplete for planned tables".to_string(),
        ));
    }

    fence.complete = true;
    Ok(fence)
}

fn validate_plan(plan: &CatchupPlan) -> Result<(), CatchupError> {
    if plan.tables.is_empty() {
        return Err(CatchupError::InvalidPlan(
            "catchup needs at least one table".to_string(),
        ));
    }
    if plan.chunk_size == 0 {
        return Err(CatchupError::InvalidPlan(
            "catchup chunk size must be greater than zero".to_string(),
        ));
    }
    if plan.start_file.is_empty() {
        return Err(CatchupError::InvalidPlan(
            "catchup start binlog file is required".to_string(),
        ));
    }
    if plan.start_position == 0 {
        return Err(CatchupError::InvalidPlan(
            "catchup start binlog position must be greater than zero".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{
        ChunkRequest, FileSnapshotProgressStore, SnapshotError, SnapshotProgress,
        SnapshotProgressStore, SnapshotRow, TableSnapshotProgress,
    };
    use std::cell::RefCell;
    use std::collections::{BTreeMap, VecDeque};

    #[test]
    fn file_progress_reload_replays_from_persisted_fence_after_all_planned_tables_complete() {
        let path = unique_path("catchup-progress.json");
        let plan = CatchupPlan {
            tables: vec![accounts_table(), orders_table()],
            chunk_size: 2,
            start_file: "stale-binlog.000001".to_string(),
            start_position: 4,
        };
        let fence = SnapshotFence {
            source_file: "mysqld-bin.000009".to_string(),
            source_position: 321,
            complete: false,
        };
        let first_store = FileSnapshotProgressStore::new(&path);
        let source = QueueSnapshotSource::with_start(
            fence.clone(),
            vec![vec![row("1", "account")], vec![row("2", "order")]],
        );
        let mut target = RecordingSnapshotTarget::default();

        run_catchup(
            &plan,
            &first_store,
            &source,
            &mut target,
            &RecordingReplay::default(),
        )
        .expect("initial catchup");

        let reloaded_store = FileSnapshotProgressStore::new(&path);
        let persisted = reloaded_store.load().expect("reload progress");
        assert_eq!(
            persisted.snapshot_fence,
            Some(SnapshotFence {
                complete: true,
                ..fence.clone()
            })
        );
        assert!(plan.tables.iter().all(|table| {
            persisted
                .table(&table.name)
                .is_some_and(|progress| progress.complete)
        }));

        let restarted_source = QueueSnapshotSource::with_start(
            SnapshotFence {
                source_file: "wrong-binlog.000001".to_string(),
                source_position: 999,
                complete: false,
            },
            Vec::new(),
        );
        let replay = RecordingReplay::default();
        run_catchup(
            &plan,
            &reloaded_store,
            &restarted_source,
            &mut RecordingSnapshotTarget::default(),
            &replay,
        )
        .expect("restarted catchup");

        assert_eq!(
            replay.start.borrow().as_ref(),
            Some(&(fence.source_file, fence.source_position))
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn file_progress_does_not_complete_fence_for_partial_planned_tables() {
        let path = unique_path("partial-catchup-progress.json");
        let plan = CatchupPlan {
            tables: vec![accounts_table(), orders_table()],
            chunk_size: 2,
            start_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
        };
        let fence = SnapshotFence {
            source_file: "mysqld-bin.000009".to_string(),
            source_position: 321,
            complete: false,
        };
        let store = FileSnapshotProgressStore::new(&path);
        store
            .save(&SnapshotProgress {
                snapshot_fence: Some(fence.clone()),
                tables: BTreeMap::from([
                    (
                        "accounts".to_string(),
                        TableSnapshotProgress {
                            complete: true,
                            ..TableSnapshotProgress::default()
                        },
                    ),
                    ("orders".to_string(), TableSnapshotProgress::default()),
                ]),
            })
            .expect("save partial progress");

        let reloaded = FileSnapshotProgressStore::new(&path);
        let progress = reloaded.load().expect("reload partial progress");
        let error = finalize_snapshot_fence(&plan, &progress, fence).expect_err("partial plan");

        assert_eq!(
            error.to_string(),
            "snapshot progress is incomplete for planned tables"
        );
        assert!(!progress.snapshot_fence.expect("fence").complete);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn snapshots_capture_and_replay_from_the_persisted_source_fence() {
        let plan = CatchupPlan {
            tables: vec![accounts_table()],
            chunk_size: 2,
            start_file: "stale-binlog.000001".to_string(),
            start_position: 4,
        };
        let progress_store = MemoryProgressStore::default();
        let source = QueueSnapshotSource::with_start(
            SnapshotFence {
                source_file: "mysqld-bin.000009".to_string(),
                source_position: 321,
                complete: false,
            },
            vec![vec![row("1", "snapshot")], Vec::new()],
        );
        let mut target = RecordingSnapshotTarget::default();
        let replay = RecordingReplay::default();

        let report =
            run_catchup(&plan, &progress_store, &source, &mut target, &replay).expect("catchup");

        assert_eq!(
            report.snapshot_results,
            vec![SnapshotResult {
                table: "accounts".to_string(),
                rows_copied: 1,
            }]
        );
        assert_eq!(
            report.snapshot_fence,
            SnapshotFence {
                source_file: "mysqld-bin.000009".to_string(),
                source_position: 321,
                complete: true,
            }
        );
        assert_eq!(target.rows.borrow().as_slice(), &[row("1", "snapshot")]);
        assert_eq!(
            progress_store
                .load()
                .expect("persisted snapshot progress")
                .snapshot_fence,
            Some(SnapshotFence {
                source_file: "mysqld-bin.000009".to_string(),
                source_position: 321,
                complete: true,
            })
        );
        assert_eq!(
            replay.start.borrow().as_ref(),
            Some(&("mysqld-bin.000009".to_string(), 321))
        );
    }

    #[test]
    fn parent_created_after_snapshot_start_is_replayed() {
        let plan = CatchupPlan {
            tables: vec![accounts_table()],
            chunk_size: 2,
            start_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
        };
        let progress_store = MemoryProgressStore::default();
        let source = QueueSnapshotSource::with_start(
            SnapshotFence {
                source_file: "mysqld-bin.000001".to_string(),
                source_position: 100,
                complete: false,
            },
            vec![Vec::new()],
        );
        let mut target = RecordingSnapshotTarget::default();
        let replay = FilteringReplay::new(vec![
            ReplayEvent {
                position: 99,
                row: row("parent", "before"),
            },
            ReplayEvent {
                position: 100,
                row: row("parent", "created-after-start"),
            },
        ]);

        run_catchup(&plan, &progress_store, &source, &mut target, &replay).expect("catchup");

        assert_eq!(
            replay.applied.borrow().as_slice(),
            &[row("parent", "created-after-start")]
        );
    }

    #[test]
    fn child_later_than_snapshot_fence_is_replayed_from_exact_boundary() {
        let plan = CatchupPlan {
            tables: vec![accounts_table()],
            chunk_size: 2,
            start_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
        };
        let progress_store = MemoryProgressStore::default();
        let source = QueueSnapshotSource::with_start(
            SnapshotFence {
                source_file: "mysqld-bin.000001".to_string(),
                source_position: 200,
                complete: false,
            },
            vec![Vec::new()],
        );
        let mut target = RecordingSnapshotTarget::default();
        let replay = FilteringReplay::new(vec![
            ReplayEvent {
                position: 199,
                row: row("child", "before"),
            },
            ReplayEvent {
                position: 200,
                row: row("child", "at-fence"),
            },
            ReplayEvent {
                position: 201,
                row: row("child", "later"),
            },
        ]);

        run_catchup(&plan, &progress_store, &source, &mut target, &replay).expect("catchup");

        assert_eq!(
            replay.applied.borrow().as_slice(),
            &[row("child", "at-fence"), row("child", "later")]
        );
    }

    #[test]
    fn rejects_progress_with_rows_but_without_snapshot_fence() {
        let plan = CatchupPlan {
            tables: vec![accounts_table()],
            chunk_size: 2,
            start_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
        };
        let progress_store = MemoryProgressStore::with_progress(SnapshotProgress {
            snapshot_fence: None,
            tables: BTreeMap::from([(
                "accounts".to_string(),
                crate::snapshot::TableSnapshotProgress {
                    last_primary_key: Some(vec!["1".to_string()]),
                    rows_copied: 1,
                    complete: false,
                },
            )]),
        });
        let source = QueueSnapshotSource::with_start(
            SnapshotFence {
                source_file: "mysqld-bin.000001".to_string(),
                source_position: 100,
                complete: false,
            },
            vec![],
        );
        let mut target = RecordingSnapshotTarget::default();
        let replay = RecordingReplay::default();

        let error = run_catchup(&plan, &progress_store, &source, &mut target, &replay)
            .expect_err("missing fence must reject resumed snapshot");

        assert_eq!(
            error.to_string(),
            "snapshot progress is missing source fencing metadata"
        );
    }

    #[test]
    fn rejects_invalid_catchup_plans_before_snapshot() {
        let mut plan = CatchupPlan {
            tables: vec![accounts_table()],
            chunk_size: 2,
            start_file: "mysqld-bin.000001".to_string(),
            start_position: 123,
        };

        plan.tables.clear();
        assert_eq!(
            validate_plan(&plan)
                .expect_err("missing tables")
                .to_string(),
            "catchup needs at least one table"
        );

        plan.tables = vec![accounts_table()];
        plan.chunk_size = 0;
        assert_eq!(
            validate_plan(&plan).expect_err("zero chunk").to_string(),
            "catchup chunk size must be greater than zero"
        );

        plan.chunk_size = 2;
        plan.start_file.clear();
        assert_eq!(
            validate_plan(&plan)
                .expect_err("missing binlog file")
                .to_string(),
            "catchup start binlog file is required"
        );

        plan.start_file = "mysqld-bin.000001".to_string();
        plan.start_position = 0;
        assert_eq!(
            validate_plan(&plan).expect_err("zero position").to_string(),
            "catchup start binlog position must be greater than zero"
        );
    }

    fn accounts_table() -> SnapshotTable {
        table("accounts")
    }

    fn orders_table() -> SnapshotTable {
        table("orders")
    }

    fn table(name: &str) -> SnapshotTable {
        SnapshotTable {
            name: name.to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        }
    }

    fn unique_path(file_name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        path.push(format!("mariadb-mysql-cdc-catchup-{nanos}-{file_name}"));
        path
    }

    fn row(id: &str, name: &str) -> SnapshotRow {
        SnapshotRow {
            primary_key: vec![id.to_string()],
            values: BTreeMap::from([
                ("id".to_string(), Some(id.to_string())),
                ("name".to_string(), Some(name.to_string())),
            ]),
        }
    }

    #[derive(Default)]
    struct MemoryProgressStore {
        progress: RefCell<SnapshotProgress>,
    }

    impl MemoryProgressStore {
        fn with_progress(progress: SnapshotProgress) -> Self {
            Self {
                progress: RefCell::new(progress),
            }
        }
    }

    impl SnapshotProgressStore for MemoryProgressStore {
        fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
            Ok(self.progress.borrow().clone())
        }

        fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
            self.progress.replace(progress.clone());
            Ok(())
        }
    }

    struct QueueSnapshotSource {
        start: SnapshotFence,
        chunks: RefCell<VecDeque<Vec<SnapshotRow>>>,
    }

    impl QueueSnapshotSource {
        fn with_start(start: SnapshotFence, chunks: Vec<Vec<SnapshotRow>>) -> Self {
            Self {
                start,
                chunks: RefCell::new(chunks.into()),
            }
        }
    }

    impl SnapshotSource for QueueSnapshotSource {
        fn read_chunk(&self, _request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError> {
            Ok(self.chunks.borrow_mut().pop_front().unwrap_or_default())
        }

        fn capture_start_coordinate(&self) -> Result<SnapshotFence, SnapshotError> {
            Ok(self.start.clone())
        }
    }

    #[derive(Default)]
    struct RecordingSnapshotTarget {
        rows: RefCell<Vec<SnapshotRow>>,
    }

    impl SnapshotTarget for RecordingSnapshotTarget {
        fn write_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), SnapshotError> {
            self.rows.borrow_mut().extend_from_slice(rows);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingReplay {
        start: RefCell<Option<(String, u64)>>,
    }

    struct ReplayEvent {
        position: u64,
        row: SnapshotRow,
    }

    struct FilteringReplay {
        events: Vec<ReplayEvent>,
        applied: RefCell<Vec<SnapshotRow>>,
    }

    impl FilteringReplay {
        fn new(events: Vec<ReplayEvent>) -> Self {
            Self {
                events,
                applied: RefCell::new(Vec::new()),
            }
        }
    }

    impl CdcReplay for FilteringReplay {
        fn replay_from_start(
            &self,
            start_file: &str,
            start_position: u64,
        ) -> Result<ApplyBinlogReport, CatchupError> {
            for event in &self.events {
                if crate::snapshot::compare_source_coordinates(
                    "mysqld-bin.000001",
                    event.position,
                    start_file,
                    start_position,
                ) != std::cmp::Ordering::Less
                {
                    self.applied.borrow_mut().push(event.row.clone());
                }
            }
            Ok(ApplyBinlogReport {
                applied_statements: self.applied.borrow().len() as u64,
                quarantined_statements: 0,
            })
        }
    }

    impl CdcReplay for RecordingReplay {
        fn replay_from_start(
            &self,
            start_file: &str,
            start_position: u64,
        ) -> Result<ApplyBinlogReport, CatchupError> {
            self.start
                .replace(Some((start_file.to_string(), start_position)));
            Ok(ApplyBinlogReport {
                applied_statements: 3,
                quarantined_statements: 0,
            })
        }
    }
}
