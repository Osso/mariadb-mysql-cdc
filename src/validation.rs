use crate::database_row::DatabaseRow;
use crate::inventory::TableInventory;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationTable {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
}

impl From<&TableInventory> for ValidationTable {
    fn from(table: &TableInventory) -> Self {
        Self {
            name: table.name.clone(),
            primary_key: table.primary_key.clone(),
            columns: table
                .columns
                .iter()
                .filter(|column| column.generated.is_none())
                .map(|column| column.name.clone())
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountComparison {
    pub table: String,
    pub source_count: u64,
    pub target_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChecksumRequest {
    pub table: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
    pub sample_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChecksumSample {
    pub sample_key: Vec<String>,
    pub row_count: u64,
    pub checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChecksumComparison {
    pub table: String,
    pub sample_key: Vec<String>,
    pub source: Option<ChecksumSample>,
    pub target: Option<ChecksumSample>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DivergenceRequest {
    pub table: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
    pub start_after: Option<Vec<String>>,
    pub limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowDivergence {
    pub primary_key: Vec<String>,
    pub kind: RowDivergenceKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowDivergenceKind {
    MissingSource,
    MissingTarget,
    ValueMismatch {
        differing_columns: Vec<String>,
        source: BTreeMap<String, Option<String>>,
        target: BTreeMap<String, Option<String>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DivergenceReport {
    pub table: String,
    pub divergences: Vec<RowDivergence>,
}

pub trait ValidationReader {
    fn count_rows(&self, table: &str) -> Result<u64, ValidationError>;
    fn sampled_checksums(
        &self,
        request: &ChecksumRequest,
    ) -> Result<Vec<ChecksumSample>, ValidationError>;
    fn read_rows(&self, request: &DivergenceRequest) -> Result<Vec<DatabaseRow>, ValidationError>;
}

#[derive(Debug)]
pub enum ValidationError {
    InvalidTable(String),
    Reader { table: String, message: String },
}

impl ValidationError {
    pub fn reader(table: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Reader {
            table: table.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTable(message) => formatter.write_str(message),
            Self::Reader { table, message } => {
                write!(formatter, "validation reader failed for {table}: {message}")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

pub fn validate_table_counts(
    tables: &[ValidationTable],
    source: &impl ValidationReader,
    target: &impl ValidationReader,
) -> Result<Vec<CountComparison>, ValidationError> {
    tables
        .iter()
        .map(|table| compare_table_count(table, source, target))
        .collect()
}

pub fn validate_sampled_checksums(
    tables: &[ValidationTable],
    sample_size: usize,
    source: &impl ValidationReader,
    target: &impl ValidationReader,
) -> Result<Vec<ChecksumComparison>, ValidationError> {
    let mut comparisons = Vec::new();

    for table in tables {
        let request = checksum_request(table, sample_size)?;
        let source_samples = source.sampled_checksums(&request)?;
        let target_samples = target.sampled_checksums(&request)?;
        comparisons.extend(compare_checksum_samples(
            table,
            source_samples,
            target_samples,
        ));
    }

    Ok(comparisons)
}

pub fn report_row_divergence(
    table: &ValidationTable,
    start_after: Option<Vec<String>>,
    limit: usize,
    source: &impl ValidationReader,
    target: &impl ValidationReader,
) -> Result<DivergenceReport, ValidationError> {
    validate_table(table)?;
    let request = DivergenceRequest {
        table: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: table.columns.clone(),
        start_after,
        limit,
    };
    let source_rows = source.read_rows(&request)?;
    let target_rows = target.read_rows(&request)?;

    Ok(DivergenceReport {
        table: table.name.clone(),
        divergences: compare_rows(source_rows, target_rows),
    })
}

impl CountComparison {
    pub fn matches(&self) -> bool {
        self.source_count == self.target_count
    }
}

impl ChecksumComparison {
    pub fn matches(&self) -> bool {
        match (&self.source, &self.target) {
            (Some(source), Some(target)) => {
                source.row_count == target.row_count && source.checksum == target.checksum
            }
            _ => false,
        }
    }
}

impl DivergenceReport {
    pub fn matches(&self) -> bool {
        self.divergences.is_empty()
    }
}

fn compare_table_count(
    table: &ValidationTable,
    source: &impl ValidationReader,
    target: &impl ValidationReader,
) -> Result<CountComparison, ValidationError> {
    validate_table(table)?;

    Ok(CountComparison {
        table: table.name.clone(),
        source_count: source.count_rows(&table.name)?,
        target_count: target.count_rows(&table.name)?,
    })
}

fn checksum_request(
    table: &ValidationTable,
    sample_size: usize,
) -> Result<ChecksumRequest, ValidationError> {
    validate_table(table)?;

    Ok(ChecksumRequest {
        table: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: table.columns.clone(),
        sample_size,
    })
}

fn compare_checksum_samples(
    table: &ValidationTable,
    source_samples: Vec<ChecksumSample>,
    target_samples: Vec<ChecksumSample>,
) -> Vec<ChecksumComparison> {
    let source_by_key = samples_by_key(source_samples);
    let target_by_key = samples_by_key(target_samples);

    sample_keys(&source_by_key, &target_by_key)
        .into_iter()
        .filter_map(|sample_key| {
            build_checksum_difference(&table.name, sample_key, &source_by_key, &target_by_key)
        })
        .collect()
}

fn build_checksum_difference(
    table: &str,
    sample_key: Vec<String>,
    source_by_key: &BTreeMap<Vec<String>, ChecksumSample>,
    target_by_key: &BTreeMap<Vec<String>, ChecksumSample>,
) -> Option<ChecksumComparison> {
    let source = source_by_key.get(&sample_key).cloned();
    let target = target_by_key.get(&sample_key).cloned();
    let comparison = ChecksumComparison {
        table: table.to_string(),
        sample_key,
        source,
        target,
    };

    if comparison.matches() {
        None
    } else {
        Some(comparison)
    }
}

fn samples_by_key(samples: Vec<ChecksumSample>) -> BTreeMap<Vec<String>, ChecksumSample> {
    samples
        .into_iter()
        .map(|sample| (sample.sample_key.clone(), sample))
        .collect()
}

fn sample_keys(
    source: &BTreeMap<Vec<String>, ChecksumSample>,
    target: &BTreeMap<Vec<String>, ChecksumSample>,
) -> BTreeSet<Vec<String>> {
    source.keys().chain(target.keys()).cloned().collect()
}

fn compare_rows(
    source_rows: Vec<DatabaseRow>,
    target_rows: Vec<DatabaseRow>,
) -> Vec<RowDivergence> {
    let source_by_key = rows_by_key(source_rows);
    let target_by_key = rows_by_key(target_rows);

    row_keys(&source_by_key, &target_by_key)
        .into_iter()
        .filter_map(|primary_key| build_row_divergence(primary_key, &source_by_key, &target_by_key))
        .collect()
}

fn build_row_divergence(
    primary_key: Vec<String>,
    source_by_key: &BTreeMap<Vec<String>, DatabaseRow>,
    target_by_key: &BTreeMap<Vec<String>, DatabaseRow>,
) -> Option<RowDivergence> {
    match (
        source_by_key.get(&primary_key),
        target_by_key.get(&primary_key),
    ) {
        (Some(source), Some(target)) => value_mismatch(primary_key, source, target),
        (Some(_), None) => Some(row_divergence(
            primary_key,
            RowDivergenceKind::MissingTarget,
        )),
        (None, Some(_)) => Some(row_divergence(
            primary_key,
            RowDivergenceKind::MissingSource,
        )),
        (None, None) => None,
    }
}

fn value_mismatch(
    primary_key: Vec<String>,
    source: &DatabaseRow,
    target: &DatabaseRow,
) -> Option<RowDivergence> {
    let differing_columns = differing_columns(&source.values, &target.values);

    if differing_columns.is_empty() {
        return None;
    }

    Some(row_divergence(
        primary_key,
        RowDivergenceKind::ValueMismatch {
            differing_columns,
            source: source.values.clone(),
            target: target.values.clone(),
        },
    ))
}

fn differing_columns(
    source: &BTreeMap<String, Option<String>>,
    target: &BTreeMap<String, Option<String>>,
) -> Vec<String> {
    source
        .keys()
        .chain(target.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|column| source.get(column) != target.get(column))
        .collect()
}

fn row_divergence(primary_key: Vec<String>, kind: RowDivergenceKind) -> RowDivergence {
    RowDivergence { primary_key, kind }
}

fn rows_by_key(rows: Vec<DatabaseRow>) -> BTreeMap<Vec<String>, DatabaseRow> {
    rows.into_iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect()
}

fn row_keys(
    source: &BTreeMap<Vec<String>, DatabaseRow>,
    target: &BTreeMap<Vec<String>, DatabaseRow>,
) -> BTreeSet<Vec<String>> {
    source.keys().chain(target.keys()).cloned().collect()
}

fn validate_table(table: &ValidationTable) -> Result<(), ValidationError> {
    if table.name.is_empty() {
        return Err(ValidationError::InvalidTable(
            "validation table needs a name".to_string(),
        ));
    }
    if table.primary_key.is_empty() {
        return Err(ValidationError::InvalidTable(format!(
            "{} needs a primary key for validation",
            table.name
        )));
    }
    if table.columns.is_empty() {
        return Err(ValidationError::InvalidTable(format!(
            "{} needs selected columns for validation",
            table.name
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn compares_table_counts() {
        let tables = vec![accounts_table()];
        let source = FakeReader::with_count("accounts", 3);
        let target = FakeReader::with_count("accounts", 2);

        let report = validate_table_counts(&tables, &source, &target).expect("counts");

        assert_eq!(
            report,
            vec![CountComparison {
                table: "accounts".to_string(),
                source_count: 3,
                target_count: 2,
            }]
        );
        assert!(!report[0].matches());
    }

    #[test]
    fn reports_sampled_checksum_differences() {
        let tables = vec![accounts_table()];
        let source = FakeReader::with_checksums(vec![
            checksum(&["a"], 10, "source-a"),
            checksum(&["b"], 10, "same"),
        ]);
        let target = FakeReader::with_checksums(vec![
            checksum(&["a"], 10, "target-a"),
            checksum(&["b"], 10, "same"),
            checksum(&["c"], 1, "target-only"),
        ]);

        let report = validate_sampled_checksums(&tables, 128, &source, &target).expect("checksums");

        assert_eq!(report.len(), 2);
        assert_eq!(report[0].sample_key, vec!["a"]);
        assert_eq!(
            report[0].source.as_ref().expect("source").checksum,
            "source-a"
        );
        assert_eq!(
            report[0].target.as_ref().expect("target").checksum,
            "target-a"
        );
        assert_eq!(report[1].sample_key, vec!["c"]);
        assert!(report[1].source.is_none());
    }

    #[test]
    fn reports_row_level_divergence() {
        let table = accounts_table();
        let source = FakeReader::with_rows(vec![
            row("1", "same"),
            row("2", "source value"),
            row("3", "source only"),
        ]);
        let target = FakeReader::with_rows(vec![
            row("1", "same"),
            row("2", "target value"),
            row("4", "target only"),
        ]);

        let report =
            report_row_divergence(&table, None, 100, &source, &target).expect("row divergence");

        assert!(!report.matches());
        assert_eq!(report.divergences.len(), 3);
        assert_eq!(report.divergences[0].primary_key, vec!["2"]);
        assert_eq!(
            report.divergences[0].kind,
            RowDivergenceKind::ValueMismatch {
                differing_columns: vec!["name".to_string()],
                source: row("2", "source value").values,
                target: row("2", "target value").values,
            }
        );
        assert_eq!(report.divergences[1].kind, RowDivergenceKind::MissingTarget);
        assert_eq!(report.divergences[2].kind, RowDivergenceKind::MissingSource);
    }

    #[test]
    fn passes_divergence_request_to_readers() {
        let table = accounts_table();
        let source = FakeReader::default();
        let target = FakeReader::default();

        report_row_divergence(&table, Some(vec!["9".to_string()]), 50, &source, &target)
            .expect("row divergence");

        let request = source.last_request.borrow().clone().expect("request");
        assert_eq!(request.table, "accounts");
        assert_eq!(request.primary_key, vec!["id"]);
        assert_eq!(request.columns, vec!["id", "name"]);
        assert_eq!(request.start_after, Some(vec!["9".to_string()]));
        assert_eq!(request.limit, 50);
    }

    fn accounts_table() -> ValidationTable {
        ValidationTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        }
    }

    fn checksum(key: &[&str], row_count: u64, checksum: &str) -> ChecksumSample {
        ChecksumSample {
            sample_key: key.iter().map(|value| value.to_string()).collect(),
            row_count,
            checksum: checksum.to_string(),
        }
    }

    fn row(id: &str, name: &str) -> DatabaseRow {
        DatabaseRow {
            primary_key: vec![id.to_string()],
            values: BTreeMap::from([
                ("id".to_string(), Some(id.to_string())),
                ("name".to_string(), Some(name.to_string())),
            ]),
        }
    }

    #[derive(Default)]
    struct FakeReader {
        count: u64,
        checksums: Vec<ChecksumSample>,
        rows: Vec<DatabaseRow>,
        last_request: RefCell<Option<DivergenceRequest>>,
    }

    impl FakeReader {
        fn with_count(table: &str, count: u64) -> Self {
            let reader = Self {
                count,
                ..Self::default()
            };
            assert_eq!(table, "accounts");
            reader
        }

        fn with_checksums(checksums: Vec<ChecksumSample>) -> Self {
            Self {
                checksums,
                ..Self::default()
            }
        }

        fn with_rows(rows: Vec<DatabaseRow>) -> Self {
            Self {
                rows,
                ..Self::default()
            }
        }
    }

    impl ValidationReader for FakeReader {
        fn count_rows(&self, _table: &str) -> Result<u64, ValidationError> {
            Ok(self.count)
        }

        fn sampled_checksums(
            &self,
            _request: &ChecksumRequest,
        ) -> Result<Vec<ChecksumSample>, ValidationError> {
            Ok(self.checksums.clone())
        }

        fn read_rows(
            &self,
            request: &DivergenceRequest,
        ) -> Result<Vec<DatabaseRow>, ValidationError> {
            self.last_request.replace(Some(request.clone()));
            Ok(self.rows.clone())
        }
    }
}
