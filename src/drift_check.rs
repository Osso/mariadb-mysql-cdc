use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::quote_ident;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, SslOpts};
use std::fmt;

#[derive(Clone, Debug)]
pub struct DriftCheckConfig {
    pub source: MySqlConnectionConfig,
    pub target: TargetMySqlConfig,
    pub tables: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriftComparison {
    pub table: String,
    pub source_count: Option<u64>,
    pub target_count: Option<u64>,
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
        .map(|table| compare_table_count(config, table))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DriftCheckReport { comparisons })
}

pub fn build_count_sql(table: &str) -> String {
    format!("SELECT COUNT(*) FROM {}", quote_ident(table))
}

pub fn build_list_tables_sql() -> String {
    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME".to_string()
}

pub fn format_drift_report(report: &DriftCheckReport) -> String {
    let mismatches = report.mismatches();
    let mut lines = vec![format!(
        "drift_check tables={} mismatches={mismatches}",
        report.comparisons.len()
    )];

    lines.extend(report.comparisons.iter().map(format_drift_comparison));
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
        matches!(
            (self.source_count, self.target_count),
            (Some(source_count), Some(target_count)) if source_count == target_count
        )
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
            (Some(source_count), Some(target_count)) if source_count == target_count => "ok",
            (Some(_), Some(_)) => "drift",
        }
    }
}

fn validate_config(config: &DriftCheckConfig) -> Result<(), DriftCheckError> {
    validate_source_connection(&config.source)?;
    validate_target_connection(&config.target)
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

fn compare_table_count(
    config: &DriftCheckConfig,
    table: &str,
) -> Result<DriftComparison, DriftCheckError> {
    let sql = build_count_sql(table);
    let source_count = query_count(&source_query_config(&config.source), table, &sql)?;
    let target_count = query_count(&target_query_config(&config.target), table, &sql)?;

    Ok(DriftComparison {
        table: table.to_string(),
        source_count,
        target_count,
    })
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

fn query_list(config: &QueryConnectionConfig, sql: &str) -> Result<Vec<String>, DriftCheckError> {
    let mut conn = open_connection(config)?;
    conn.query::<String, _>(sql).map_err(query_error)
}

fn query_scalar<T>(config: &QueryConnectionConfig, sql: &str) -> Result<Option<T>, DriftCheckError>
where
    T: mysql::prelude::FromValue,
{
    let mut conn = open_connection(config)?;
    conn.query_first::<T, _>(sql).map_err(query_error)
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
        "drift_check_table table={} source_count={} target_count={} delta={} status={}",
        comparison.table,
        format_count(comparison.source_count),
        format_count(comparison.target_count),
        format_delta(comparison.delta()),
        comparison.status()
    )
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
    fn formats_drift_report_with_match_and_mismatch_status() {
        let report = DriftCheckReport {
            comparisons: vec![
                DriftComparison {
                    table: "accounts".to_string(),
                    source_count: Some(10),
                    target_count: Some(10),
                },
                DriftComparison {
                    table: "releases".to_string(),
                    source_count: Some(7),
                    target_count: Some(5),
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
    fn clean_report_has_no_mismatches() {
        let report = DriftCheckReport {
            comparisons: vec![DriftComparison {
                table: "accounts".to_string(),
                source_count: Some(10),
                target_count: Some(10),
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
