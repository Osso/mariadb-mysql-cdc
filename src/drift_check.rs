use crate::checksum::{ChecksumColumn, ChecksumRequest, build_chunk_checksum_sql};
use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::{quote_ident, quote_sql_literal};
use mysql::prelude::{FromRow, Queryable};
use mysql::{Conn, Opts, OptsBuilder, Row, SslOpts, Value};
use std::fmt;

const MIN_REPAIR_RANGE_ROWS: u64 = 100;
const MAX_MISMATCH_RANGES: usize = 1000;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentDriftSummary {
    pub chunks: u64,
    pub mismatched_chunks: u64,
    pub mismatched_ranges: Vec<ContentDriftRange>,
    pub range_limit_exceeded: bool,
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

pub fn run_drift_check(config: &DriftCheckConfig) -> Result<DriftCheckReport, DriftCheckError> {
    validate_config(config)?;
    let tables = drift_tables(config)?;
    let comparisons = tables
        .iter()
        .map(|table| compare_table(config, table))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DriftCheckReport { comparisons })
}

pub fn build_count_sql(table: &str) -> String {
    format!("SELECT COUNT(*) FROM {}", quote_ident(table))
}

pub fn build_list_tables_sql() -> String {
    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME".to_string()
}

pub fn build_primary_key_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
        quote_sql_literal(table)
    )
}

pub fn build_checksum_columns_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} AND COALESCE(GENERATION_EXPRESSION, '') = '' ORDER BY ORDINAL_POSITION",
        quote_sql_literal(table)
    )
}

pub fn build_json_check_clauses_sql(table: &str) -> String {
    format!(
        "SELECT cc.CHECK_CLAUSE FROM information_schema.TABLE_CONSTRAINTS tc JOIN information_schema.CHECK_CONSTRAINTS cc ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = {} AND tc.CONSTRAINT_TYPE = 'CHECK' AND cc.CHECK_CLAUSE LIKE '%json_valid%'",
        quote_sql_literal(table)
    )
}

pub fn build_primary_key_endpoints_sql(
    table: &str,
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    limit: usize,
) -> Result<String, DriftCheckError> {
    build_primary_key_endpoints_range_sql(table, primary_key, start_after, None, limit)
}

pub fn build_primary_key_endpoints_range_sql(
    table: &str,
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    limit: usize,
) -> Result<String, DriftCheckError> {
    validate_bound_arity(primary_key, start_after.as_ref(), "start_after")?;
    validate_bound_arity(primary_key, end_at.as_ref(), "end_at")?;
    let columns = primary_key
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let order_by = columns.clone();
    let bounds = endpoint_bounds(primary_key, start_after, end_at);
    Ok(format!(
        "SELECT {columns} FROM {}{bounds} ORDER BY {order_by} LIMIT {limit}",
        quote_ident(table)
    ))
}

fn endpoint_bounds(
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> String {
    let mut predicates = Vec::new();
    if let Some(start) = start_after {
        predicates.push(primary_key_bound_predicate(primary_key, &start, ">"));
    }
    if let Some(end) = end_at {
        predicates.push(format!(
            "NOT ({})",
            primary_key_bound_predicate(primary_key, &end, ">")
        ));
    }
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
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
        &build_list_tables_sql(),
    )
}

fn compare_table(
    config: &DriftCheckConfig,
    table: &str,
) -> Result<DriftComparison, DriftCheckError> {
    let sql = build_count_sql(table);
    let source_count = query_count(&source_query_config(&config.source), table, &sql)?;
    let target_count = query_count(&target_query_config(&config.target), table, &sql)?;
    let content = if should_check_content(config, source_count, target_count) {
        Some(compare_table_content(config, table)?)
    } else {
        None
    };

    Ok(DriftComparison {
        table: table.to_string(),
        source_count,
        target_count,
        content,
    })
}

fn should_check_content(
    config: &DriftCheckConfig,
    source_count: Option<u64>,
    target_count: Option<u64>,
) -> bool {
    config.content_check
        && matches!((source_count, target_count), (Some(source), Some(target)) if source == target)
}

fn compare_table_content(
    config: &DriftCheckConfig,
    table: &str,
) -> Result<ContentDriftSummary, DriftCheckError> {
    let context = ChecksumCompareContext::load(config, table)?;
    compare_checksum_chunks(&context)
}

struct ChecksumCompareContext {
    source: QueryConnectionConfig,
    target: QueryConnectionConfig,
    table: String,
    primary_key: Vec<String>,
    columns: Vec<ChecksumColumn>,
    chunk_size: usize,
}

impl ChecksumCompareContext {
    fn load(config: &DriftCheckConfig, table: &str) -> Result<Self, DriftCheckError> {
        let source = source_query_config(&config.source);
        let target = target_query_config(&config.target);
        let primary_key = query_primary_key(&source, table)?;
        let columns = query_checksum_columns(&source, table)?;
        Ok(Self {
            source,
            target,
            table: table.to_string(),
            primary_key,
            columns,
            chunk_size: config.chunk_size,
        })
    }
}

fn compare_checksum_chunks(
    context: &ChecksumCompareContext,
) -> Result<ContentDriftSummary, DriftCheckError> {
    let mut summary = ContentDriftSummary {
        chunks: 0,
        mismatched_chunks: 0,
        mismatched_ranges: Vec::new(),
        range_limit_exceeded: false,
    };
    let mut start_after = None;

    loop {
        let endpoints = query_primary_key_endpoints(
            &context.source,
            &context.table,
            &context.primary_key,
            start_after.clone(),
            context.chunk_size,
        )?;
        let end_at = endpoints.last().cloned();
        record_checksum_comparison(&mut summary, context, start_after.clone(), end_at.clone())?;

        if endpoints.len() < context.chunk_size {
            record_target_tail_checksum(&mut summary, context, end_at)?;
            return Ok(summary);
        }
        start_after = end_at;
    }
}

fn record_checksum_comparison(
    summary: &mut ContentDriftSummary,
    context: &ChecksumCompareContext,
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> Result<(), DriftCheckError> {
    let comparison = compare_checksum_range(context, start_after, end_at)?;
    summary.chunks += 1;
    if comparison.is_mismatch() {
        summary.mismatched_chunks += 1;
        split_or_record_mismatch(summary, context, comparison)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChecksumRangeComparison {
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    source: (u64, u64),
    target: (u64, u64),
}

impl ChecksumRangeComparison {
    fn is_mismatch(&self) -> bool {
        self.source != self.target
    }

    fn source_count(&self) -> u64 {
        self.source.0
    }

    fn target_count(&self) -> u64 {
        self.target.0
    }

    fn drift_range(&self) -> ContentDriftRange {
        ContentDriftRange {
            start_after: self.start_after.clone(),
            end_at: self.end_at.clone(),
            source_count: self.source_count(),
            target_count: self.target_count(),
        }
    }
}

fn compare_checksum_range(
    context: &ChecksumCompareContext,
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> Result<ChecksumRangeComparison, DriftCheckError> {
    let source = checksum_for_range(
        context,
        &context.source,
        start_after.clone(),
        end_at.clone(),
    )?;
    let target = checksum_for_range(
        context,
        &context.target,
        start_after.clone(),
        end_at.clone(),
    )?;
    Ok(ChecksumRangeComparison {
        start_after,
        end_at,
        source,
        target,
    })
}

fn split_or_record_mismatch(
    summary: &mut ContentDriftSummary,
    context: &ChecksumCompareContext,
    comparison: ChecksumRangeComparison,
) -> Result<(), DriftCheckError> {
    let Some(midpoint) = mismatch_midpoint(summary, context, &comparison)? else {
        record_mismatched_range(summary, comparison.drift_range());
        return Ok(());
    };

    record_checksum_comparison(
        summary,
        context,
        comparison.start_after.clone(),
        Some(midpoint.clone()),
    )?;
    record_checksum_comparison(summary, context, Some(midpoint), comparison.end_at)?;
    Ok(())
}

fn record_mismatched_range(summary: &mut ContentDriftSummary, range: ContentDriftRange) {
    if summary.mismatched_ranges.len() >= MAX_MISMATCH_RANGES {
        summary.range_limit_exceeded = true;
    } else {
        summary.mismatched_ranges.push(range);
    }
}

fn mismatch_midpoint(
    summary: &ContentDriftSummary,
    context: &ChecksumCompareContext,
    comparison: &ChecksumRangeComparison,
) -> Result<Option<Vec<String>>, DriftCheckError> {
    if summary.range_limit_exceeded
        || summary.mismatched_ranges.len() >= MAX_MISMATCH_RANGES
        || comparison.source_count() <= MIN_REPAIR_RANGE_ROWS
    {
        return Ok(None);
    }
    let split_size = (comparison.source_count() / 2) as usize;
    let endpoints = query_primary_key_endpoints_in_range(
        &context.source,
        &context.table,
        &context.primary_key,
        comparison.start_after.clone(),
        comparison.end_at.clone(),
        split_size.max(1),
    )?;
    let midpoint = endpoints.last().cloned();
    Ok(midpoint.filter(|value| Some(value.clone()) != comparison.end_at))
}

fn record_target_tail_checksum(
    summary: &mut ContentDriftSummary,
    context: &ChecksumCompareContext,
    end_at: Option<Vec<String>>,
) -> Result<(), DriftCheckError> {
    if end_at.is_some() {
        record_checksum_comparison(summary, context, end_at, None)?;
    }
    Ok(())
}

fn checksum_for_range(
    context: &ChecksumCompareContext,
    config: &QueryConnectionConfig,
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> Result<(u64, u64), DriftCheckError> {
    query_chunk_checksum(
        config,
        &context.table,
        &context.primary_key,
        &context.columns,
        start_after,
        end_at,
    )
}

fn query_count(
    config: &QueryConnectionConfig,
    table: &str,
    sql: &str,
) -> Result<Option<u64>, DriftCheckError> {
    match query_scalar::<u64>(config, sql) {
        Ok(Some(count)) => Ok(Some(count)),
        Ok(None) => Err(DriftCheckError::Query(format!(
            "{table} count query returned no rows"
        ))),
        Err(DriftCheckError::Query(message)) if is_missing_table_error(&message) => Ok(None),
        Err(error) => Err(error),
    }
}

fn is_missing_table_error(message: &str) -> bool {
    (message.contains("ERROR 1146") || message.contains("error 1146"))
        && message.contains("doesn't exist")
}

fn query_primary_key(
    config: &QueryConnectionConfig,
    table: &str,
) -> Result<Vec<String>, DriftCheckError> {
    let primary_key = query_list(config, &build_primary_key_sql(table))?;
    if primary_key.is_empty() {
        return Err(DriftCheckError::Config(format!(
            "table `{table}` has no primary key for content drift check"
        )));
    }
    Ok(primary_key)
}

fn query_checksum_columns(
    config: &QueryConnectionConfig,
    table: &str,
) -> Result<Vec<ChecksumColumn>, DriftCheckError> {
    let mut conn = open_connection(config)?;
    let mut columns = conn
        .query_map(
            build_checksum_columns_sql(table),
            |(name, data_type, column_type): (String, String, String)| ChecksumColumn {
                name,
                data_type,
                column_type,
            },
        )
        .map_err(query_error)?;
    let json_checks = query_json_check_clauses(&mut conn, table)?;
    mark_json_alias_columns(&mut columns, &json_checks);
    Ok(columns)
}

fn query_json_check_clauses(conn: &mut Conn, table: &str) -> Result<Vec<String>, DriftCheckError> {
    conn.query::<String, _>(build_json_check_clauses_sql(table))
        .map_err(query_error)
}

fn mark_json_alias_columns(columns: &mut [ChecksumColumn], check_clauses: &[String]) {
    for column in columns {
        if check_clauses
            .iter()
            .any(|clause| check_clause_references_json_column(clause, &column.name))
        {
            column.data_type = "json".to_string();
        }
    }
}

fn check_clause_references_json_column(clause: &str, column: &str) -> bool {
    let lower_clause = clause.to_ascii_lowercase();
    let lower_column = quote_ident(column).to_ascii_lowercase();
    lower_clause.contains("json_valid") && lower_clause.contains(&lower_column)
}

fn query_primary_key_endpoints(
    config: &QueryConnectionConfig,
    table: &str,
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    limit: usize,
) -> Result<Vec<Vec<String>>, DriftCheckError> {
    query_primary_key_endpoints_in_range(config, table, primary_key, start_after, None, limit)
}

fn query_primary_key_endpoints_in_range(
    config: &QueryConnectionConfig,
    table: &str,
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    limit: usize,
) -> Result<Vec<Vec<String>>, DriftCheckError> {
    let sql =
        build_primary_key_endpoints_range_sql(table, primary_key, start_after, end_at, limit)?;
    query_rows(config, &sql)
}

fn query_chunk_checksum(
    config: &QueryConnectionConfig,
    table: &str,
    primary_key: &[String],
    columns: &[ChecksumColumn],
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> Result<(u64, u64), DriftCheckError> {
    let sql = build_chunk_checksum_sql(&ChecksumRequest {
        table: table.to_string(),
        primary_key: primary_key.to_vec(),
        columns: columns.to_vec(),
        start_after,
        end_at,
    })
    .map_err(DriftCheckError::Config)?;
    query_required_scalar(config, &sql)
}

fn query_list(config: &QueryConnectionConfig, sql: &str) -> Result<Vec<String>, DriftCheckError> {
    let mut conn = open_connection(config)?;
    conn.query::<String, _>(sql).map_err(query_error)
}

fn query_rows(
    config: &QueryConnectionConfig,
    sql: &str,
) -> Result<Vec<Vec<String>>, DriftCheckError> {
    let mut conn = open_connection(config)?;
    let rows = conn.query::<Row, _>(sql).map_err(query_error)?;
    Ok(rows.into_iter().map(row_to_strings).collect())
}

fn query_required_scalar<T>(config: &QueryConnectionConfig, sql: &str) -> Result<T, DriftCheckError>
where
    T: FromRow,
{
    query_scalar(config, sql)?
        .ok_or_else(|| DriftCheckError::Query("checksum query returned no rows".to_string()))
}

fn query_scalar<T>(config: &QueryConnectionConfig, sql: &str) -> Result<Option<T>, DriftCheckError>
where
    T: FromRow,
{
    let mut conn = open_connection(config)?;
    conn.query_first::<T, _>(sql).map_err(query_error)
}

fn row_to_strings(row: Row) -> Vec<String> {
    row.unwrap().into_iter().map(value_to_string).collect()
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::NULL => String::new(),
        Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            format_date(year, month, day, hour, minute, second, micros)
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            format_time(negative, days, hours, minutes, seconds, micros)
        }
    }
}

fn open_connection(config: &QueryConnectionConfig) -> Result<Conn, DriftCheckError> {
    Conn::new(connection_opts(config)).map_err(query_error)
}

fn query_error(error: mysql::Error) -> DriftCheckError {
    DriftCheckError::Query(error.to_string())
}

fn connection_opts(config: &QueryConnectionConfig) -> Opts {
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(&config.host))
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .db_name(Some(&config.database))
        .prefer_socket(false)
        .ssl_opts(
            SslOpts::default()
                .with_danger_skip_domain_validation(true)
                .with_danger_accept_invalid_certs(true),
        );
    Opts::from(builder)
}

#[derive(Clone, Debug)]
struct QueryConnectionConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

fn source_query_config(config: &MySqlConnectionConfig) -> QueryConnectionConfig {
    QueryConnectionConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.user.clone(),
        password: config.password.clone(),
        database: config.database.clone(),
    }
}

fn target_query_config(target: &TargetMySqlConfig) -> QueryConnectionConfig {
    QueryConnectionConfig {
        host: target.host.clone(),
        port: target.port,
        user: target.user.clone(),
        password: target.password.clone(),
        database: target.database.clone(),
    }
}

fn format_drift_comparison(comparison: &DriftComparison) -> String {
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

fn format_key_bound_json(bound: Option<&Vec<String>>) -> String {
    bound
        .map(|values| serde_json::to_string(values).expect("serialize key bound"))
        .unwrap_or_else(|| "null".to_string())
}

fn format_content_summary(content: Option<&ContentDriftSummary>) -> String {
    content
        .map(|content| {
            format!(
                " content_chunks={} content_mismatches={} content_ranges={} content_range_limit_exceeded={}",
                content.chunks,
                content.mismatched_chunks,
                content.mismatched_ranges.len(),
                content.range_limit_exceeded
            )
        })
        .unwrap_or_default()
}

fn format_count(count: Option<u64>) -> String {
    count
        .map(|count| count.to_string())
        .unwrap_or_else(|| "missing".to_string())
}

fn format_delta(delta: Option<i128>) -> String {
    delta
        .map(|delta| delta.to_string())
        .unwrap_or_else(|| "missing".to_string())
}

fn validate_bound_arity(
    primary_key: &[String],
    values: Option<&Vec<String>>,
    label: &str,
) -> Result<(), DriftCheckError> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.len() != primary_key.len() {
        return Err(DriftCheckError::Config(format!(
            "{label} has {} values for {} primary-key columns",
            values.len(),
            primary_key.len()
        )));
    }
    Ok(())
}

fn primary_key_bound_predicate(columns: &[String], values: &[String], operator: &str) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, _column)| primary_key_bound_branch(columns, values, index, operator))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn primary_key_bound_branch(
    columns: &[String],
    values: &[String],
    index: usize,
    operator: &str,
) -> String {
    let mut parts = Vec::new();
    for equal_index in 0..index {
        parts.push(format!(
            "{} = {}",
            quote_ident(&columns[equal_index]),
            quote_sql_literal(&values[equal_index])
        ));
    }
    parts.push(format!(
        "{} {operator} {}",
        quote_ident(&columns[index]),
        quote_sql_literal(&values[index])
    ));
    format!("({})", parts.join(" AND "))
}

fn format_date(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> String {
    if hour == 0 && minute == 0 && second == 0 && micros == 0 {
        format!("{year:04}-{month:02}-{day:02}")
    } else if micros == 0 {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}")
    }
}

fn format_time(
    negative: bool,
    days: u32,
    hours: u8,
    minutes: u8,
    seconds: u8,
    micros: u32,
) -> String {
    let sign = if negative { "-" } else { "" };
    let total_hours = days * 24 + u32::from(hours);
    if micros == 0 {
        format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_sql_quotes_table_identifier() {
        assert_eq!(
            build_count_sql("accounts"),
            "SELECT COUNT(*) FROM `accounts`"
        );
        assert_eq!(
            build_count_sql("weird`table"),
            "SELECT COUNT(*) FROM `weird``table`"
        );
    }

    #[test]
    fn list_tables_sql_is_read_only_and_bounded_to_current_database() {
        assert_eq!(
            build_list_tables_sql(),
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
        );
    }

    #[test]
    fn primary_key_endpoint_sql_rejects_bad_bound_arity() {
        let error = build_primary_key_endpoints_sql(
            "accounts",
            &["tenant_id".to_string(), "id".to_string()],
            Some(vec!["10".to_string()]),
            100,
        )
        .expect_err("bad arity");

        assert_eq!(
            error.to_string(),
            "start_after has 1 values for 2 primary-key columns"
        );
    }

    #[test]
    fn marks_mariadb_json_alias_columns_from_json_valid_checks() {
        let mut columns = vec![ChecksumColumn {
            name: "payload".to_string(),
            data_type: "longtext".to_string(),
            column_type: "longtext".to_string(),
        }];

        mark_json_alias_columns(&mut columns, &["json_valid(`payload`)".to_string()]);

        assert_eq!(columns[0].data_type, "json");
    }

    #[test]
    fn formats_drift_report_with_match_and_mismatch_status() {
        let report = DriftCheckReport {
            comparisons: vec![
                DriftComparison {
                    table: "accounts".to_string(),
                    source_count: Some(10),
                    target_count: Some(10),
                    content: None,
                },
                DriftComparison {
                    table: "releases".to_string(),
                    source_count: Some(7),
                    target_count: Some(5),
                    content: None,
                },
            ],
        };

        assert!(report.has_mismatches());
        assert!(!report.is_clean());
        assert_eq!(
            format_drift_report(&report),
            [
                "drift_check tables=2 mismatches=1",
                "drift_check_table table=accounts source_count=10 target_count=10 delta=0 status=ok",
                "drift_check_table table=releases source_count=7 target_count=5 delta=-2 status=drift",
            ]
            .join("\n")
        );
    }

    #[test]
    fn formats_content_drift_as_mismatch_even_when_counts_match() {
        let report = DriftCheckReport {
            comparisons: vec![DriftComparison {
                table: "accounts".to_string(),
                source_count: Some(10),
                target_count: Some(10),
                content: Some(ContentDriftSummary {
                    chunks: 3,
                    mismatched_chunks: 1,
                    mismatched_ranges: vec![ContentDriftRange {
                        start_after: Some(vec!["10,tenant".to_string()]),
                        end_at: Some(vec!["11".to_string()]),
                        source_count: 1,
                        target_count: 1,
                    }],
                    range_limit_exceeded: false,
                }),
            }],
        };

        assert!(report.has_mismatches());
        assert_eq!(
            format_drift_report(&report),
            [
                "drift_check tables=1 mismatches=1",
                "drift_check_table table=accounts source_count=10 target_count=10 delta=0 status=drift content_chunks=3 content_mismatches=1 content_ranges=1 content_range_limit_exceeded=false",
                "drift_check_range table=accounts start_after_json=[\"10,tenant\"] end_at_json=[\"11\"] source_count=1 target_count=1",
            ]
            .join("\n")
        );
    }

    #[test]
    fn clean_report_has_no_mismatches() {
        let report = DriftCheckReport {
            comparisons: vec![DriftComparison {
                table: "accounts".to_string(),
                source_count: Some(10),
                target_count: Some(10),
                content: None,
            }],
        };

        assert!(!report.has_mismatches());
        assert!(report.is_clean());
    }

    #[test]
    fn only_1146_42s02_errors_are_treated_as_missing_tables() {
        assert!(is_missing_table_error(
            "ERROR 1146 (42S02): Table 'db.accounts' doesn't exist"
        ));
        assert!(!is_missing_table_error(
            "ERROR 1146 (HY000): Table metadata lock failed"
        ));
        assert!(!is_missing_table_error(
            "ERROR 1051 (42S02): Unknown table 'db.accounts'"
        ));
    }
}
