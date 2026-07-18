use crate::live::{ApplyBinlogConfig, ApplyBinlogError, ApplyBinlogReport, apply_remote_binlog};
use crate::snapshot::{
    SnapshotError, SnapshotProgressStore, SnapshotResult, SnapshotSource, SnapshotTable,
    SnapshotTarget, snapshot_table,
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
    let snapshot_results = snapshot_tables(plan, progress_store, snapshot_source, snapshot_target)?;
    let replay_report = replay.replay_from_start(&plan.start_file, plan.start_position)?;

    Ok(CatchupReport {
        snapshot_results,
        replay_report,
    })
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
        ChunkRequest, SnapshotError, SnapshotProgress, SnapshotProgressStore, SnapshotRow,
    };
    use std::cell::RefCell;
    use std::collections::{BTreeMap, VecDeque};

    #[test]
    fn snapshots_chunks_then_replays_from_global_start() {
        let plan = CatchupPlan {
            tables: vec![accounts_table()],
            chunk_size: 2,
            start_file: "mysqld-bin.000001".to_string(),
            start_position: 123,
        };
        let progress_store = MemoryProgressStore::default();
        let source = QueueSnapshotSource::new(vec![vec![row("1", "snapshot")], Vec::new()]);
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
        assert_eq!(target.rows.borrow().as_slice(), &[row("1", "snapshot")]);
        assert_eq!(
            replay.start.borrow().as_ref(),
            Some(&("mysqld-bin.000001".to_string(), 123))
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
        SnapshotTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        }
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
        chunks: RefCell<VecDeque<Vec<SnapshotRow>>>,
    }

    impl QueueSnapshotSource {
        fn new(chunks: Vec<Vec<SnapshotRow>>) -> Self {
            Self {
                chunks: RefCell::new(chunks.into()),
            }
        }
    }

    impl SnapshotSource for QueueSnapshotSource {
        fn read_chunk(&self, _request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError> {
            Ok(self.chunks.borrow_mut().pop_front().unwrap_or_default())
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
