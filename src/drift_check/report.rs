use super::content::compare_table_content;
use super::query::{
    build_count_sql, query_count, query_list, source_query_config, target_query_config,
};
use super::{
    ContentDriftRange, ContentDriftSummary, DriftCheckConfig, DriftCheckError, DriftCheckObserver,
    DriftCheckReport, DriftComparison, MySqlConnectionConfig, NoopDriftCheckObserver,
    TargetMySqlConfig,
};

pub fn run_drift_check(config: &DriftCheckConfig) -> Result<DriftCheckReport, DriftCheckError> {
    run_drift_check_with_observer(config, &NoopDriftCheckObserver)
}

pub fn run_drift_check_with_observer(
    config: &DriftCheckConfig,
    observer: &impl DriftCheckObserver,
) -> Result<DriftCheckReport, DriftCheckError> {
    validate_config(config)?;
    let tables = drift_tables(config)?;
    let total = tables.len();
    let comparisons = tables
        .iter()
        .enumerate()
        .map(|(index, table)| compare_table(config, table, index + 1, total, observer))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DriftCheckReport { comparisons })
}

pub fn format_drift_report(report: &DriftCheckReport) -> String {
    let mismatches = report.mismatches();
    let mut lines = vec![format!(
        "drift_check tables={} mismatches={mismatches}",
        report.comparisons.len()
    )];

    for comparison in &report.comparisons {
        lines.push(format_drift_comparison(comparison));
        lines.extend(format_content_ranges(comparison));
    }
    lines.join("\n")
}

impl DriftCheckReport {
    pub fn has_mismatches(&self) -> bool {
        self.mismatches() > 0
    }

    pub fn is_clean(&self) -> bool {
        !self.has_mismatches()
    }

    fn mismatches(&self) -> usize {
        self.comparisons
            .iter()
            .filter(|comparison| !comparison.matches())
            .count()
    }
}

impl DriftComparison {
    pub fn matches(&self) -> bool {
        let counts_match = matches!(
            (self.source_count, self.target_count),
            (Some(source_count), Some(target_count)) if source_count == target_count
        );
        let content_matches = self
            .content
            .as_ref()
            .is_none_or(|content| content.mismatched_chunks == 0);
        counts_match && content_matches
    }

    fn delta(&self) -> Option<i128> {
        let source_count = self.source_count?;
        let target_count = self.target_count?;
        Some(i128::from(target_count) - i128::from(source_count))
    }

    fn status(&self) -> &'static str {
        match (self.source_count, self.target_count) {
            (None, _) => "source_missing",
            (_, None) => "target_missing",
            (Some(source_count), Some(target_count))
                if source_count == target_count
                    && self
                        .content
                        .as_ref()
                        .is_none_or(|content| content.mismatched_chunks == 0) =>
            {
                "ok"
            }
            (Some(_), Some(_)) => "drift",
        }
    }
}

fn validate_config(config: &DriftCheckConfig) -> Result<(), DriftCheckError> {
    validate_source_connection(&config.source)?;
    validate_target_connection(&config.target)?;
    if config.chunk_size == 0 {
        return Err(DriftCheckError::Config(
            "chunk size must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_source_connection(config: &MySqlConnectionConfig) -> Result<(), DriftCheckError> {
    if config.host.is_empty() {
        return Err(DriftCheckError::Config(
            "source host is required".to_string(),
        ));
    }
    if config.user.is_empty() {
        return Err(DriftCheckError::Config(
            "source user is required".to_string(),
        ));
    }
    if config.password.is_empty() {
        return Err(DriftCheckError::Config(
            "source password is required".to_string(),
        ));
    }
    if config.database.is_empty() {
        return Err(DriftCheckError::Config(
            "source database is required".to_string(),
        ));
    }
    Ok(())
}

fn validate_target_connection(target: &TargetMySqlConfig) -> Result<(), DriftCheckError> {
    if target.host.is_empty() {
        return Err(DriftCheckError::Config(
            "target host is required".to_string(),
        ));
    }
    if target.user.is_empty() {
        return Err(DriftCheckError::Config(
            "target user is required".to_string(),
        ));
    }
    if target.password.is_empty() {
        return Err(DriftCheckError::Config(
            "target password is required".to_string(),
        ));
    }
    if target.database.is_empty() {
        return Err(DriftCheckError::Config(
            "target database is required".to_string(),
        ));
    }
    Ok(())
}

fn drift_tables(config: &DriftCheckConfig) -> Result<Vec<String>, DriftCheckError> {
    if !config.tables.is_empty() {
        return Ok(config.tables.clone());
    }

    query_list(
        &source_query_config(&config.source),
        &super::build_list_tables_sql(),
    )
}

fn compare_table(
    config: &DriftCheckConfig,
    table: &str,
    index: usize,
    total: usize,
    observer: &impl DriftCheckObserver,
) -> Result<DriftComparison, DriftCheckError> {
    observer.table_started(table, index, total);
    let sql = build_count_sql(table);
    observer.count_started(table, "source");
    let source_count = query_count(&source_query_config(&config.source), table, &sql)?;
    observer.count_started(table, "target");
    let target_count = query_count(&target_query_config(&config.target), table, &sql)?;
    observer.count_completed(table, source_count, target_count);
    let content = if should_check_content(config, source_count, target_count) {
        observer.content_started(table, config.chunk_size);
        Some(compare_table_content(config, table, observer)?)
    } else {
        None
    };

    let comparison = DriftComparison {
        table: table.to_string(),
        source_count,
        target_count,
        content,
    };
    observer.table_completed(&comparison);
    Ok(comparison)
}

fn should_check_content(
    config: &DriftCheckConfig,
    source_count: Option<u64>,
    target_count: Option<u64>,
) -> bool {
    config.content_check
        && matches!((source_count, target_count), (Some(source), Some(target)) if source == target)
}

pub(crate) fn format_drift_comparison(comparison: &DriftComparison) -> String {
    format!(
        "drift_check_table table={} source_count={} target_count={} delta={} status={}{}",
        comparison.table,
        format_count(comparison.source_count),
        format_count(comparison.target_count),
        format_delta(comparison.delta()),
        comparison.status(),
        format_content_summary(comparison.content.as_ref())
    )
}

fn format_content_ranges(comparison: &DriftComparison) -> Vec<String> {
    comparison
        .content
        .as_ref()
        .map(|content| {
            content
                .mismatched_ranges
                .iter()
                .map(|range| format_content_range(&comparison.table, range))
                .collect()
        })
        .unwrap_or_default()
}

fn format_content_range(table: &str, range: &ContentDriftRange) -> String {
    format!(
        "drift_check_range table={} start_after_json={} end_at_json={} source_count={} target_count={}",
        table,
        format_key_bound_json(range.start_after.as_ref()),
        format_key_bound_json(range.end_at.as_ref()),
        range.source_count,
        range.target_count
    )
}

pub(crate) fn format_key_bound_json(bound: Option<&Vec<String>>) -> String {
    bound
        .map(|values| serde_json::to_string(values).expect("serialize key bound"))
        .unwrap_or_else(|| "null".to_string())
}

pub(crate) fn format_content_summary(content: Option<&ContentDriftSummary>) -> String {
    content
        .map(|content| {
            if let Some(reason) = &content.skipped_reason {
                return format!(" content_skipped={}", reason.replace(char::is_whitespace, "_"));
            }
            format!(
                " content_chunks={} content_mismatches={} content_ranges={} content_range_limit_exceeded={}{}",
                content.chunks,
                content.mismatched_chunks,
                content.mismatched_ranges.len(),
                content.range_limit_exceeded,
                format_skipped_columns(&content.skipped_columns)
            )
        })
        .unwrap_or_default()
}

fn format_skipped_columns(skipped_columns: &[String]) -> String {
    if skipped_columns.is_empty() {
        return String::new();
    }
    format!(" content_skipped_columns={}", skipped_columns.join(","))
}

pub(crate) fn format_count(count: Option<u64>) -> String {
    count
        .map(|count| count.to_string())
        .unwrap_or_else(|| "missing".to_string())
}

fn format_delta(delta: Option<i128>) -> String {
    delta
        .map(|delta| delta.to_string())
        .unwrap_or_else(|| "missing".to_string())
}
