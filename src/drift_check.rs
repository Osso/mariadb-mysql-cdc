use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use std::fmt;

#[cfg(test)]
pub(crate) use crate::checksum::ChecksumColumn;

const MIN_REPAIR_RANGE_ROWS: u64 = 100;
const MAX_MISMATCH_RANGES: usize = 1000;

mod content;
mod query;
mod report;
#[cfg(test)]
mod tests;

pub use query::{
    build_checksum_columns_sql, build_count_sql, build_json_check_clauses_sql,
    build_list_tables_sql, build_primary_key_endpoints_range_sql, build_primary_key_endpoints_sql,
    build_primary_key_sql,
};
pub use report::{format_drift_report, run_drift_check, run_drift_check_with_observer};

#[cfg(test)]
pub(crate) use content::partition_checksum_columns;
#[cfg(test)]
pub(crate) use query::{
    QueryConnectionConfig, connection_opts, is_missing_table_error, mark_json_alias_columns,
    source_query_config, target_query_config,
};
#[cfg(test)]
pub(crate) use report::format_content_summary;
pub(crate) use report::{format_count, format_drift_comparison, format_key_bound_json};

#[derive(Clone, Debug)]
pub struct DriftCheckConfig {
    pub source: MySqlConnectionConfig,
    pub target: TargetMySqlConfig,
    pub tables: Vec<String>,
    pub content_check: bool,
    pub chunk_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriftComparison {
    pub table: String,
    pub source_count: Option<u64>,
    pub target_count: Option<u64>,
    pub content: Option<ContentDriftSummary>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ContentDriftSummary {
    pub chunks: u64,
    pub mismatched_chunks: u64,
    pub mismatched_ranges: Vec<ContentDriftRange>,
    pub range_limit_exceeded: bool,
    pub skipped_columns: Vec<String>,
    pub skipped_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentDriftRange {
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
    pub source_count: u64,
    pub target_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriftCheckReport {
    pub comparisons: Vec<DriftComparison>,
}

#[derive(Debug)]
pub enum DriftCheckError {
    Config(String),
    Query(String),
}

impl fmt::Display for DriftCheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => formatter.write_str(message),
            Self::Query(message) => write!(formatter, "drift check query failed: {message}"),
        }
    }
}

impl std::error::Error for DriftCheckError {}

pub trait DriftCheckObserver {
    fn table_started(&self, _table: &str, _index: usize, _total: usize) {}
    fn count_started(&self, _table: &str, _side: &str) {}
    fn count_completed(
        &self,
        _table: &str,
        _source_count: Option<u64>,
        _target_count: Option<u64>,
    ) {
    }
    fn content_started(&self, _table: &str, _chunk_size: usize) {}
    fn content_skipped(&self, _table: &str, _reason: &str) {}
    fn content_chunk_started(
        &self,
        _table: &str,
        _start_after: Option<&Vec<String>>,
        _end_at: Option<&Vec<String>>,
    ) {
    }
    fn content_chunk_completed(
        &self,
        _table: &str,
        _source_count: u64,
        _target_count: u64,
        _mismatch: bool,
    ) {
    }
    fn table_completed(&self, _comparison: &DriftComparison) {}
}

pub struct NoopDriftCheckObserver;
impl DriftCheckObserver for NoopDriftCheckObserver {}

pub struct StderrDriftCheckObserver;
impl DriftCheckObserver for StderrDriftCheckObserver {
    fn table_started(&self, table: &str, index: usize, total: usize) {
        eprintln!("drift_check_table_start table={table} index={index} total={total}");
    }

    fn count_started(&self, table: &str, side: &str) {
        eprintln!("drift_check_count_start table={table} side={side}");
    }

    fn count_completed(&self, table: &str, source_count: Option<u64>, target_count: Option<u64>) {
        eprintln!(
            "drift_check_count_complete table={table} source_count={} target_count={}",
            format_count(source_count),
            format_count(target_count)
        );
    }

    fn content_started(&self, table: &str, chunk_size: usize) {
        eprintln!("drift_check_content_start table={table} chunk_size={chunk_size}");
    }

    fn content_skipped(&self, table: &str, reason: &str) {
        eprintln!(
            "drift_check_content_skipped table={table} reason={}",
            serde_json::to_string(reason).expect("serialize log value")
        );
    }

    fn content_chunk_started(
        &self,
        table: &str,
        start_after: Option<&Vec<String>>,
        end_at: Option<&Vec<String>>,
    ) {
        eprintln!(
            "drift_check_chunk_start table={table} start_after_json={} end_at_json={}",
            format_key_bound_json(start_after),
            format_key_bound_json(end_at)
        );
    }

    fn content_chunk_completed(
        &self,
        table: &str,
        source_count: u64,
        target_count: u64,
        mismatch: bool,
    ) {
        eprintln!(
            "drift_check_chunk_complete table={table} source_count={source_count} target_count={target_count} mismatch={mismatch}"
        );
    }

    fn table_completed(&self, comparison: &DriftComparison) {
        eprintln!("{}", format_drift_comparison(comparison));
    }
}
