use crate::validation::{
    ChecksumComparison, CountComparison, DivergenceReport, ValidationError, ValidationReader,
    ValidationTable, report_row_divergence, validate_sampled_checksums, validate_table_counts,
};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CutoverPlan {
    pub tables: Vec<ValidationTable>,
    pub checksum_sample_size: usize,
    pub divergence_limit: usize,
    pub max_allowed_lag_events: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CdcDrainSummary {
    pub applied_events: u64,
    pub remaining_lag_events: u64,
    pub quarantined_events: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CutoverValidationReport {
    pub count_comparisons: Vec<CountComparison>,
    pub checksum_differences: Vec<ChecksumComparison>,
    pub divergence_reports: Vec<DivergenceReport>,
}

impl CutoverValidationReport {
    pub fn passed(&self) -> bool {
        self.count_comparisons.iter().all(CountComparison::matches)
            && self.checksum_differences.is_empty()
            && self
                .divergence_reports
                .iter()
                .all(DivergenceReport::matches)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CutoverReport {
    pub drain_summary: CdcDrainSummary,
    pub validation: CutoverValidationReport,
    pub endpoint_switched: bool,
    pub writes_resumed: bool,
}

impl CutoverReport {
    pub fn passed(&self) -> bool {
        self.drain_summary.remaining_lag_events == 0
            && self.drain_summary.quarantined_events == 0
            && self.validation.passed()
            && self.endpoint_switched
            && self.writes_resumed
    }
}

pub trait WriteController {
    fn stop_writes(&self) -> Result<(), CutoverStepError>;
    fn resume_writes(&self) -> Result<(), CutoverStepError>;
}

pub trait CdcLagDrainer {
    fn drain_lag(&self) -> Result<CdcDrainSummary, CutoverStepError>;
}

pub trait EndpointSwitcher {
    fn switch_to_target(&self) -> Result<(), CutoverStepError>;
}

pub struct CutoverInputs<'a, VS, VT, W, D, E> {
    pub validation_source: &'a VS,
    pub validation_target: &'a VT,
    pub writes: &'a W,
    pub cdc: &'a D,
    pub endpoint: &'a E,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CutoverStepError {
    pub message: String,
}

impl CutoverStepError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CutoverStepError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CutoverStepError {}

#[derive(Debug)]
pub enum CutoverError {
    InvalidPlan(String),
    StopWrites(CutoverStepError),
    DrainLag(CutoverStepError),
    LagNotDrained(CdcDrainSummary),
    Validation(ValidationError),
    ValidationGate(Box<CutoverValidationReport>),
    SwitchEndpoint(CutoverStepError),
    ResumeWrites(CutoverStepError),
    ResumeAfterFailure {
        original: Box<CutoverError>,
        resume: CutoverStepError,
    },
}

impl fmt::Display for CutoverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan(message) => formatter.write_str(message),
            Self::StopWrites(source) => write!(formatter, "failed to stop writes: {source}"),
            Self::DrainLag(source) => write!(formatter, "failed to drain CDC lag: {source}"),
            Self::LagNotDrained(summary) => write!(
                formatter,
                "CDC lag not drained: {} remaining, {} quarantined",
                summary.remaining_lag_events, summary.quarantined_events
            ),
            Self::Validation(source) => write!(formatter, "cutover validation failed: {source}"),
            Self::ValidationGate(report) => write!(
                formatter,
                "cutover validation gates failed: {} count checks, {} checksum differences, {} divergence reports",
                report.count_comparisons.len(),
                report.checksum_differences.len(),
                report.divergence_reports.len()
            ),
            Self::SwitchEndpoint(source) => {
                write!(formatter, "failed to switch endpoint: {source}")
            }
            Self::ResumeWrites(source) => write!(formatter, "failed to resume writes: {source}"),
            Self::ResumeAfterFailure { original, resume } => write!(
                formatter,
                "cutover failed ({original}) and failed to resume writes: {resume}"
            ),
        }
    }
}

impl std::error::Error for CutoverError {}

pub fn run_cutover<VS, VT, W, D, E>(
    plan: &CutoverPlan,
    inputs: CutoverInputs<'_, VS, VT, W, D, E>,
) -> Result<CutoverReport, CutoverError>
where
    VS: ValidationReader,
    VT: ValidationReader,
    W: WriteController,
    D: CdcLagDrainer,
    E: EndpointSwitcher,
{
    validate_plan(plan)?;
    inputs
        .writes
        .stop_writes()
        .map_err(CutoverError::StopWrites)?;

    match run_stopped_cutover(plan, &inputs) {
        Ok(report) => Ok(report),
        Err(error) => resume_after_failure(inputs.writes, error),
    }
}

fn run_stopped_cutover<VS, VT, W, D, E>(
    plan: &CutoverPlan,
    inputs: &CutoverInputs<'_, VS, VT, W, D, E>,
) -> Result<CutoverReport, CutoverError>
where
    VS: ValidationReader,
    VT: ValidationReader,
    W: WriteController,
    D: CdcLagDrainer,
    E: EndpointSwitcher,
{
    let drain_summary = drain_lag(plan, inputs.cdc)?;
    let validation = validate_cutover(plan, inputs.validation_source, inputs.validation_target)?;
    inputs
        .endpoint
        .switch_to_target()
        .map_err(CutoverError::SwitchEndpoint)?;
    inputs
        .writes
        .resume_writes()
        .map_err(CutoverError::ResumeWrites)?;

    Ok(CutoverReport {
        drain_summary,
        validation,
        endpoint_switched: true,
        writes_resumed: true,
    })
}

fn drain_lag(
    plan: &CutoverPlan,
    cdc: &impl CdcLagDrainer,
) -> Result<CdcDrainSummary, CutoverError> {
    let summary = cdc.drain_lag().map_err(CutoverError::DrainLag)?;
    let lag_ok = summary.remaining_lag_events <= plan.max_allowed_lag_events;
    let quarantine_ok = summary.quarantined_events == 0;

    if lag_ok && quarantine_ok {
        Ok(summary)
    } else {
        Err(CutoverError::LagNotDrained(summary))
    }
}

fn validate_cutover(
    plan: &CutoverPlan,
    source: &impl ValidationReader,
    target: &impl ValidationReader,
) -> Result<CutoverValidationReport, CutoverError> {
    let report = CutoverValidationReport {
        count_comparisons: validate_table_counts(&plan.tables, source, target)?,
        checksum_differences: validate_sampled_checksums(
            &plan.tables,
            plan.checksum_sample_size,
            source,
            target,
        )?,
        divergence_reports: divergence_reports(plan, source, target)?,
    };

    if report.passed() {
        Ok(report)
    } else {
        Err(CutoverError::ValidationGate(Box::new(report)))
    }
}

fn divergence_reports(
    plan: &CutoverPlan,
    source: &impl ValidationReader,
    target: &impl ValidationReader,
) -> Result<Vec<DivergenceReport>, ValidationError> {
    plan.tables
        .iter()
        .map(|table| report_row_divergence(table, None, plan.divergence_limit, source, target))
        .collect()
}

fn resume_after_failure<W: WriteController>(
    writes: &W,
    error: CutoverError,
) -> Result<CutoverReport, CutoverError> {
    match writes.resume_writes() {
        Ok(()) => Err(error),
        Err(resume) => Err(CutoverError::ResumeAfterFailure {
            original: Box::new(error),
            resume,
        }),
    }
}

fn validate_plan(plan: &CutoverPlan) -> Result<(), CutoverError> {
    if plan.tables.is_empty() {
        return Err(CutoverError::InvalidPlan(
            "cutover needs at least one table".to_string(),
        ));
    }
    if plan.checksum_sample_size == 0 {
        return Err(CutoverError::InvalidPlan(
            "cutover checksum sample size must be greater than zero".to_string(),
        ));
    }
    if plan.divergence_limit == 0 {
        return Err(CutoverError::InvalidPlan(
            "cutover divergence limit must be greater than zero".to_string(),
        ));
    }

    Ok(())
}

impl From<ValidationError> for CutoverError {
    fn from(source: ValidationError) -> Self {
        Self::Validation(source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::SnapshotRow;
    use crate::validation::{ChecksumRequest, ChecksumSample, DivergenceRequest};
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;

    #[test]
    fn runs_cutover_sequence_when_gates_pass() {
        let writes = RecordingWrites::default();
        let cdc = FakeDrain::clean();
        let endpoint = RecordingEndpoint::default();
        let source = ValidationFixture::matching();
        let target = ValidationFixture::matching();

        let report = run_cutover(
            &cutover_plan(),
            inputs(&source, &target, &writes, &cdc, &endpoint),
        )
        .expect("cutover");

        assert!(report.passed());
        assert_eq!(writes.calls.borrow().as_slice(), &["stop", "resume"]);
        assert_eq!(cdc.calls.get(), 1);
        assert_eq!(endpoint.calls.get(), 1);
    }

    #[test]
    fn resumes_writes_without_switching_when_lag_remains() {
        let writes = RecordingWrites::default();
        let cdc = FakeDrain {
            summary: CdcDrainSummary {
                applied_events: 10,
                remaining_lag_events: 1,
                quarantined_events: 0,
            },
            calls: Cell::new(0),
        };
        let endpoint = RecordingEndpoint::default();
        let source = ValidationFixture::matching();
        let target = ValidationFixture::matching();

        let error = run_cutover(
            &cutover_plan(),
            inputs(&source, &target, &writes, &cdc, &endpoint),
        )
        .expect_err("lag should block")
        .to_string();

        assert!(error.contains("CDC lag not drained"));
        assert_eq!(writes.calls.borrow().as_slice(), &["stop", "resume"]);
        assert_eq!(endpoint.calls.get(), 0);
    }

    #[test]
    fn resumes_writes_without_switching_when_validation_fails() {
        let writes = RecordingWrites::default();
        let cdc = FakeDrain::clean();
        let endpoint = RecordingEndpoint::default();
        let source = ValidationFixture::new(2, vec![row("1", "source")]);
        let target = ValidationFixture::new(1, vec![row("1", "target")]);

        let error = run_cutover(
            &cutover_plan(),
            inputs(&source, &target, &writes, &cdc, &endpoint),
        )
        .expect_err("validation should block")
        .to_string();

        assert!(error.contains("validation gates failed"));
        assert_eq!(writes.calls.borrow().as_slice(), &["stop", "resume"]);
        assert_eq!(endpoint.calls.get(), 0);
    }

    #[test]
    fn reports_resume_failure_after_blocked_cutover() {
        let writes = RecordingWrites {
            fail_resume: true,
            ..RecordingWrites::default()
        };
        let cdc = FakeDrain {
            summary: CdcDrainSummary {
                applied_events: 0,
                remaining_lag_events: 0,
                quarantined_events: 1,
            },
            calls: Cell::new(0),
        };
        let endpoint = RecordingEndpoint::default();
        let source = ValidationFixture::matching();
        let target = ValidationFixture::matching();

        let error = run_cutover(
            &cutover_plan(),
            inputs(&source, &target, &writes, &cdc, &endpoint),
        )
        .expect_err("resume should fail")
        .to_string();

        assert!(error.contains("failed to resume writes"));
        assert!(error.contains("CDC lag not drained"));
    }

    fn inputs<'a>(
        source: &'a ValidationFixture,
        target: &'a ValidationFixture,
        writes: &'a RecordingWrites,
        cdc: &'a FakeDrain,
        endpoint: &'a RecordingEndpoint,
    ) -> CutoverInputs<
        'a,
        ValidationFixture,
        ValidationFixture,
        RecordingWrites,
        FakeDrain,
        RecordingEndpoint,
    > {
        CutoverInputs {
            validation_source: source,
            validation_target: target,
            writes,
            cdc,
            endpoint,
        }
    }

    fn cutover_plan() -> CutoverPlan {
        CutoverPlan {
            tables: vec![accounts_table()],
            checksum_sample_size: 16,
            divergence_limit: 100,
            max_allowed_lag_events: 0,
        }
    }

    fn accounts_table() -> ValidationTable {
        ValidationTable {
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
    struct RecordingWrites {
        calls: RefCell<Vec<&'static str>>,
        fail_resume: bool,
    }

    impl WriteController for RecordingWrites {
        fn stop_writes(&self) -> Result<(), CutoverStepError> {
            self.calls.borrow_mut().push("stop");
            Ok(())
        }

        fn resume_writes(&self) -> Result<(), CutoverStepError> {
            self.calls.borrow_mut().push("resume");
            if self.fail_resume {
                Err(CutoverStepError::new("resume hook failed"))
            } else {
                Ok(())
            }
        }
    }

    struct FakeDrain {
        summary: CdcDrainSummary,
        calls: Cell<u64>,
    }

    impl FakeDrain {
        fn clean() -> Self {
            Self {
                summary: CdcDrainSummary {
                    applied_events: 10,
                    remaining_lag_events: 0,
                    quarantined_events: 0,
                },
                calls: Cell::new(0),
            }
        }
    }

    impl CdcLagDrainer for FakeDrain {
        fn drain_lag(&self) -> Result<CdcDrainSummary, CutoverStepError> {
            self.calls.set(self.calls.get() + 1);
            Ok(self.summary.clone())
        }
    }

    #[derive(Default)]
    struct RecordingEndpoint {
        calls: Cell<u64>,
    }

    impl EndpointSwitcher for RecordingEndpoint {
        fn switch_to_target(&self) -> Result<(), CutoverStepError> {
            self.calls.set(self.calls.get() + 1);
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
}
