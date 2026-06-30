use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::quote_ident;
use std::fmt;
use std::process::Command;

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

    let output = query_connection(
        &source_query_config(&config.source),
        &build_list_tables_sql(),
    )?;
    Ok(output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn compare_table_count(
    config: &DriftCheckConfig,
    table: &str,
) -> Result<DriftComparison, DriftCheckError> {
    let sql = build_count_sql(table);
    let source_count = query_count(&source_query_config(&config.source), table, &sql)?;
    let target_count = query_count(
        &target_query_config(&config.target, &config.source.mariadb),
        table,
        &sql,
    )?;

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
    let output = match query_connection(config, sql) {
        Ok(output) => output,
        Err(DriftCheckError::Query(message)) if is_missing_table_error(&message) => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    output
        .trim()
        .parse::<u64>()
        .map(Some)
        .map_err(|_| DriftCheckError::Query(format!("{table} count was not numeric: {output:?}")))
}

fn is_missing_table_error(message: &str) -> bool {
    message.contains("ERROR 1146 (42S02)") && message.contains("doesn't exist")
}

fn query_connection(config: &QueryConnectionConfig, sql: &str) -> Result<String, DriftCheckError> {
    let output = Command::new(&config.mariadb)
        .args([
            "--batch",
            "--raw",
            "--skip-column-names",
            "--default-character-set=utf8mb4",
            "--host",
            &config.host,
            "--port",
            &config.port.to_string(),
            "--user",
            &config.user,
            &config.database,
            "-e",
            sql,
        ])
        .env("MYSQL_PWD", &config.password)
        .output()
        .map_err(|error| DriftCheckError::Query(format!("failed to run mariadb: {error}")))?;

    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(DriftCheckError::Query(format!(
        "mariadb exited with {}: {}",
        output.status,
        stderr.trim()
    )))
}

#[derive(Clone, Debug)]
struct QueryConnectionConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    mariadb: String,
}

fn source_query_config(config: &MySqlConnectionConfig) -> QueryConnectionConfig {
    QueryConnectionConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.user.clone(),
        password: config.password.clone(),
        database: config.database.clone(),
        mariadb: config.mariadb.clone(),
    }
}

fn target_query_config(target: &TargetMySqlConfig, mariadb: &str) -> QueryConnectionConfig {
    QueryConnectionConfig {
        host: target.host.clone(),
        port: target.port,
        user: target.user.clone(),
        password: target.password.clone(),
        database: target.database.clone(),
        mariadb: mariadb.to_string(),
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
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn selected_table_missing_on_target_is_reported_as_drift() {
        let mariadb = write_fake_mariadb(
            "target_missing",
            r#"#!/usr/bin/env bash
if [[ "$*" == *"target_db"* ]]; then
  echo "ERROR 1146 (42S02) at line 1: Table 'target_db.missing_target' doesn't exist" >&2
  exit 1
fi
echo 4
"#,
        );
        let config = drift_check_config(&mariadb, &["missing_target"]);

        let report = run_drift_check(&config).expect("missing target table should not abort");

        assert_eq!(
            format_drift_report(&report),
            [
                "drift_check tables=1 mismatches=1",
                "drift_check_table table=missing_target source_count=4 target_count=missing delta=missing status=target_missing",
            ]
            .join("\n")
        );
    }

    #[test]
    fn selected_table_missing_on_source_is_reported_as_drift() {
        let mariadb = write_fake_mariadb(
            "source_missing",
            r#"#!/usr/bin/env bash
if [[ "$*" == *"source_db"* ]]; then
  echo "ERROR 1146 (42S02) at line 1: Table 'source_db.missing_source' doesn't exist" >&2
  exit 1
fi
echo 9
"#,
        );
        let config = drift_check_config(&mariadb, &["missing_source"]);

        let report = run_drift_check(&config).expect("missing source table should not abort");

        assert_eq!(
            format_drift_report(&report),
            [
                "drift_check tables=1 mismatches=1",
                "drift_check_table table=missing_source source_count=missing target_count=9 delta=missing status=source_missing",
            ]
            .join("\n")
        );
    }

    #[test]
    fn non_missing_table_query_error_stays_hard_failure() {
        let mariadb = write_fake_mariadb(
            "auth_error",
            r#"#!/usr/bin/env bash
echo "ERROR 1045 (28000): Access denied for user" >&2
exit 1
"#,
        );
        let config = drift_check_config(&mariadb, &["accounts"]);

        let error = run_drift_check(&config).expect_err("auth errors must abort drift check");

        assert!(error.to_string().contains("ERROR 1045"));
    }

    #[test]
    fn only_1146_42s02_errors_are_treated_as_missing_tables() {
        assert!(is_missing_table_error(
            "mariadb exited with exit status: 1: ERROR 1146 (42S02) at line 1: Table 'db.accounts' doesn't exist"
        ));
        assert!(!is_missing_table_error(
            "mariadb exited with exit status: 1: ERROR 1146 (HY000) at line 1: Table metadata lock failed"
        ));
        assert!(!is_missing_table_error(
            "mariadb exited with exit status: 1: ERROR 1051 (42S02) at line 1: Unknown table 'db.accounts'"
        ));
    }

    fn drift_check_config(mariadb: &str, tables: &[&str]) -> DriftCheckConfig {
        DriftCheckConfig {
            source: MySqlConnectionConfig {
                host: "source-host".to_string(),
                port: 3306,
                user: "source-user".to_string(),
                password: "source-password".to_string(),
                database: "source_db".to_string(),
                mariadb: mariadb.to_string(),
            },
            target: TargetMySqlConfig {
                host: "target-host".to_string(),
                port: 3306,
                user: "target-user".to_string(),
                password: "target-password".to_string(),
                database: "target_db".to_string(),
                ..TargetMySqlConfig::default()
            },
            tables: tables.iter().map(|table| table.to_string()).collect(),
        }
    }

    fn write_fake_mariadb(name: &str, script: &str) -> String {
        let path = temp_script_path(name);
        fs::write(&path, script).expect("write fake mariadb script");
        let mut permissions = fs::metadata(&path)
            .expect("fake mariadb metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).expect("mark fake mariadb executable");
        path.to_string_lossy().into_owned()
    }

    fn temp_script_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("mariadb-mysql-cdc-{name}-{unique}.sh"))
    }
}
