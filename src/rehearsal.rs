use crate::snapshot::{
    SnapshotError, SnapshotProgressStore, SnapshotResult, SnapshotSource, SnapshotTable,
    SnapshotTarget, snapshot_table,
};
use crate::validation::{
    ChecksumComparison, CountComparison, DivergenceReport, ValidationError, ValidationReader,
    ValidationTable, report_row_divergence, validate_sampled_checksums, validate_table_counts,
};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RehearsalPlan {
    pub tables: Vec<SnapshotTable>,
    pub chunk_size: usize,
    pub checksum_sample_size: usize,
    pub divergence_limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CdcRehearsalSummary {
    pub applied_events: u64,
    pub quarantined_events: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RehearsalReport {
    pub snapshot_results: Vec<SnapshotResult>,
    pub cdc_summary: CdcRehearsalSummary,
    pub count_comparisons: Vec<CountComparison>,
    pub checksum_differences: Vec<ChecksumComparison>,
    pub divergence_reports: Vec<DivergenceReport>,
}

impl RehearsalReport {
    pub fn passed(&self) -> bool {
        counts_match(&self.count_comparisons)
            && self.checksum_differences.is_empty()
            && divergences_match(&self.divergence_reports)
            && self.cdc_summary.quarantined_events == 0
    }
}

pub trait TargetTrafficGuard {
    fn assert_not_serving_traffic(&self) -> Result<(), RehearsalError>;
}

pub trait CdcRehearsal {
    fn apply_changes(&self) -> Result<CdcRehearsalSummary, RehearsalError>;
}

pub struct RehearsalInputs<'a, P, S, T, VS, VT, G, C> {
    pub progress_store: &'a P,
    pub snapshot_source: &'a S,
    pub snapshot_target: &'a mut T,
    pub validation_source: &'a VS,
    pub validation_target: &'a VT,
    pub traffic_guard: &'a G,
    pub cdc: &'a C,
}

#[derive(Debug)]
pub enum RehearsalError {
    InvalidPlan(String),
    TargetServingTraffic(String),
    Snapshot(SnapshotError),
    Validation(ValidationError),
}

impl fmt::Display for RehearsalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan(message) => formatter.write_str(message),
            Self::TargetServingTraffic(message) => {
                write!(formatter, "target is serving traffic: {message}")
            }
            Self::Snapshot(source) => write!(formatter, "snapshot failed: {source}"),
            Self::Validation(source) => write!(formatter, "validation failed: {source}"),
        }
    }
}

impl std::error::Error for RehearsalError {}

impl From<SnapshotError> for RehearsalError {
    fn from(source: SnapshotError) -> Self {
        Self::Snapshot(source)
    }
}

impl From<ValidationError> for RehearsalError {
    fn from(source: ValidationError) -> Self {
        Self::Validation(source)
    }
}

pub fn run_rehearsal<P, S, T, VS, VT, G, C>(
    plan: &RehearsalPlan,
    inputs: RehearsalInputs<'_, P, S, T, VS, VT, G, C>,
) -> Result<RehearsalReport, RehearsalError>
where
    P: SnapshotProgressStore,
    S: SnapshotSource,
    T: SnapshotTarget,
    VS: ValidationReader,
    VT: ValidationReader,
    G: TargetTrafficGuard,
    C: CdcRehearsal,
{
    validate_plan(plan)?;
    inputs.traffic_guard.assert_not_serving_traffic()?;

    let snapshot_results = snapshot_tables(
        plan,
        inputs.progress_store,
        inputs.snapshot_source,
        inputs.snapshot_target,
    )?;
    let cdc_summary = inputs.cdc.apply_changes()?;
    let validation_tables = validation_tables(&plan.tables);
    let count_comparisons = validate_table_counts(
        &validation_tables,
        inputs.validation_source,
        inputs.validation_target,
    )?;
    let checksum_differences = validate_sampled_checksums(
        &validation_tables,
        plan.checksum_sample_size,
        inputs.validation_source,
        inputs.validation_target,
    )?;
    let divergence_reports = divergence_reports(
        plan,
        &validation_tables,
        inputs.validation_source,
        inputs.validation_target,
    )?;

    Ok(RehearsalReport {
        snapshot_results,
        cdc_summary,
        count_comparisons,
        checksum_differences,
        divergence_reports,
    })
}

fn snapshot_tables<P, S, T>(
    plan: &RehearsalPlan,
    progress_store: &P,
    snapshot_source: &S,
    snapshot_target: &mut T,
) -> Result<Vec<SnapshotResult>, RehearsalError>
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

fn divergence_reports<VS, VT>(
    plan: &RehearsalPlan,
    tables: &[ValidationTable],
    validation_source: &VS,
    validation_target: &VT,
) -> Result<Vec<DivergenceReport>, RehearsalError>
where
    VS: ValidationReader,
    VT: ValidationReader,
{
    tables
        .iter()
        .map(|table| {
            report_row_divergence(
                table,
                None,
                plan.divergence_limit,
                validation_source,
                validation_target,
            )
            .map_err(RehearsalError::from)
        })
        .collect()
}

fn validation_tables(tables: &[SnapshotTable]) -> Vec<ValidationTable> {
    tables.iter().map(ValidationTable::from).collect()
}

fn validate_plan(plan: &RehearsalPlan) -> Result<(), RehearsalError> {
    if plan.tables.is_empty() {
        return Err(RehearsalError::InvalidPlan(
            "rehearsal needs at least one table".to_string(),
        ));
    }
    if plan.chunk_size == 0 {
        return Err(RehearsalError::InvalidPlan(
            "rehearsal chunk size must be greater than zero".to_string(),
        ));
    }
    if plan.checksum_sample_size == 0 {
        return Err(RehearsalError::InvalidPlan(
            "rehearsal checksum sample size must be greater than zero".to_string(),
        ));
    }
    if plan.divergence_limit == 0 {
        return Err(RehearsalError::InvalidPlan(
            "rehearsal divergence limit must be greater than zero".to_string(),
        ));
    }

    Ok(())
}

fn counts_match(comparisons: &[CountComparison]) -> bool {
    comparisons.iter().all(CountComparison::matches)
}

fn divergences_match(reports: &[DivergenceReport]) -> bool {
    reports.iter().all(DivergenceReport::matches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{
        ChunkRequest, SnapshotError, SnapshotProgress, SnapshotProgressStore, SnapshotRow,
    };
    use crate::validation::{ChecksumRequest, ChecksumSample, DivergenceRequest};
    use std::cell::{Cell, RefCell};
    use std::collections::{BTreeMap, VecDeque};

    #[test]
    fn runs_rehearsal_without_serving_target_traffic() {
        let progress_store = MemoryProgressStore::default();
        let source = QueueSnapshotSource::new(vec![vec![row("1", "alpha")], Vec::new()]);
        let mut target = RecordingSnapshotTarget::default();
        let validation_source = ValidationFixture::matching();
        let validation_target = ValidationFixture::matching();
        let guard = FakeTrafficGuard::not_serving();
        let cdc = FakeCdc::clean();
        let plan = rehearsal_plan();

        let report = run_rehearsal(
            &plan,
            RehearsalInputs {
                progress_store: &progress_store,
                snapshot_source: &source,
                snapshot_target: &mut target,
                validation_source: &validation_source,
                validation_target: &validation_target,
                traffic_guard: &guard,
                cdc: &cdc,
            },
        )
        .expect("rehearsal");

        assert!(report.passed());
        assert_eq!(
            report.snapshot_results,
            vec![SnapshotResult {
                table: "accounts".to_string(),
                rows_copied: 1,
            }]
        );
        assert_eq!(target.rows.borrow().as_slice(), &[row("1", "alpha")]);
        assert_eq!(cdc.apply_calls.get(), 1);
    }

    #[test]
    fn blocks_rehearsal_when_target_serves_traffic() {
        let progress_store = MemoryProgressStore::default();
        let source = QueueSnapshotSource::new(vec![Vec::new()]);
        let mut target = RecordingSnapshotTarget::default();
        let validation_source = ValidationFixture::matching();
        let validation_target = ValidationFixture::matching();
        let guard = FakeTrafficGuard::serving();
        let cdc = FakeCdc::clean();

        let error = run_rehearsal(
            &rehearsal_plan(),
            RehearsalInputs {
                progress_store: &progress_store,
                snapshot_source: &source,
                snapshot_target: &mut target,
                validation_source: &validation_source,
                validation_target: &validation_target,
                traffic_guard: &guard,
                cdc: &cdc,
            },
        )
        .expect_err("traffic guard should block")
        .to_string();

        assert!(error.contains("target is serving traffic"));
        assert!(target.rows.borrow().is_empty());
        assert_eq!(cdc.apply_calls.get(), 0);
    }

    #[test]
    fn reports_failed_validation_gates() {
        let progress_store = MemoryProgressStore::default();
        let source = QueueSnapshotSource::new(vec![Vec::new()]);
        let mut target = RecordingSnapshotTarget::default();
        let validation_source = ValidationFixture::new(2, vec![row("1", "source")]);
        let validation_target = ValidationFixture::new(1, vec![row("1", "target")]);
        let guard = FakeTrafficGuard::not_serving();
        let cdc = FakeCdc {
            summary: CdcRehearsalSummary {
                applied_events: 10,
                quarantined_events: 1,
            },
            apply_calls: Cell::new(0),
        };

        let report = run_rehearsal(
            &rehearsal_plan(),
            RehearsalInputs {
                progress_store: &progress_store,
                snapshot_source: &source,
                snapshot_target: &mut target,
                validation_source: &validation_source,
                validation_target: &validation_target,
                traffic_guard: &guard,
                cdc: &cdc,
            },
        )
        .expect("rehearsal");

        assert!(!report.passed());
        assert_eq!(report.count_comparisons[0].source_count, 2);
        assert_eq!(report.count_comparisons[0].target_count, 1);
        assert_eq!(report.checksum_differences.len(), 1);
        assert_eq!(report.divergence_reports[0].divergences.len(), 1);
        assert_eq!(report.cdc_summary.quarantined_events, 1);
    }

    fn rehearsal_plan() -> RehearsalPlan {
        RehearsalPlan {
            tables: vec![accounts_table()],
            chunk_size: 2,
            checksum_sample_size: 16,
            divergence_limit: 100,
        }
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
                ("id".to_string(), id.to_string()),
                ("name".to_string(), name.to_string()),
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

    struct ValidationFixture {
        count: u64,
        rows: Vec<SnapshotRow>,
    }

    impl ValidationFixture {
        fn matching() -> Self {
            Self::new(1, vec![row("1", "alpha")])
        }

        fn new(count: u64, rows: Vec<SnapshotRow>) -> Self {
            Self { count, rows }
        }
    }

    impl ValidationReader for ValidationFixture {
        fn count_rows(&self, _table: &str) -> Result<u64, ValidationError> {
            Ok(self.count)
        }

        fn sampled_checksums(
            &self,
            _request: &ChecksumRequest,
        ) -> Result<Vec<ChecksumSample>, ValidationError> {
            Ok(vec![ChecksumSample {
                sample_key: vec!["all".to_string()],
                row_count: self.count,
                checksum: self
                    .rows
                    .iter()
                    .map(|row| row.values.get("name").cloned().unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join("|"),
            }])
        }

        fn read_rows(
            &self,
            _request: &DivergenceRequest,
        ) -> Result<Vec<SnapshotRow>, ValidationError> {
            Ok(self.rows.clone())
        }
    }

    struct FakeTrafficGuard {
        serving: bool,
    }

    impl FakeTrafficGuard {
        fn not_serving() -> Self {
            Self { serving: false }
        }

        fn serving() -> Self {
            Self { serving: true }
        }
    }

    impl TargetTrafficGuard for FakeTrafficGuard {
        fn assert_not_serving_traffic(&self) -> Result<(), RehearsalError> {
            if self.serving {
                Err(RehearsalError::TargetServingTraffic(
                    "target endpoint is enabled".to_string(),
                ))
            } else {
                Ok(())
            }
        }
    }

    struct FakeCdc {
        summary: CdcRehearsalSummary,
        apply_calls: Cell<u64>,
    }

    impl FakeCdc {
        fn clean() -> Self {
            Self {
                summary: CdcRehearsalSummary {
                    applied_events: 5,
                    quarantined_events: 0,
                },
                apply_calls: Cell::new(0),
            }
        }
    }

    impl CdcRehearsal for FakeCdc {
        fn apply_changes(&self) -> Result<CdcRehearsalSummary, RehearsalError> {
            self.apply_calls.set(self.apply_calls.get() + 1);
            Ok(self.summary.clone())
        }
    }
}
