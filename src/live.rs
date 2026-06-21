use crate::probe::BinlogCoordinate;
use crate::statement::{
    QuarantineError, QuarantinedStatement, StatementApplier, StatementEvent, StatementOutcome,
    StatementQuarantine,
};
use crate::target::{SqlStatement, TargetExecuteError, TargetExecutor};
use std::cell::RefCell;
use std::fmt;
use std::process::Command;

#[derive(Clone, Debug)]
pub struct ApplyBinlogConfig {
    pub source: SourceBinlogConfig,
    pub target: TargetMySqlConfig,
    pub mariadb: String,
    pub mariadb_binlog: String,
}

impl Default for ApplyBinlogConfig {
    fn default() -> Self {
        Self {
            source: SourceBinlogConfig::default(),
            target: TargetMySqlConfig::default(),
            mariadb: "mariadb".to_string(),
            mariadb_binlog: "mariadb-binlog".to_string(),
        }
    }
}

impl ApplyBinlogConfig {
    pub fn validate(&self) -> Result<(), ApplyBinlogError> {
        if self.source.host.is_empty() {
            return Err(config_error("source host is required"));
        }
        if self.source.user.is_empty() {
            return Err(config_error("source user is required"));
        }
        if self.source.password.is_empty() {
            return Err(config_error("source password is required"));
        }
        if self.source.binlog_file.is_empty() {
            return Err(config_error("binlog file is required"));
        }
        if self.source.start_position == 0 {
            return Err(config_error("start position must be greater than zero"));
        }
        self.target.validate()
    }
}

#[derive(Clone, Debug)]
pub struct SourceBinlogConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub binlog_file: String,
    pub start_position: u64,
    pub stop_position: Option<u64>,
}

impl Default for SourceBinlogConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            binlog_file: String::new(),
            start_position: 4,
            stop_position: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TargetMySqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl Default for TargetMySqlConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            database: String::new(),
        }
    }
}

impl TargetMySqlConfig {
    fn validate(&self) -> Result<(), ApplyBinlogError> {
        if self.host.is_empty() {
            return Err(config_error("target host is required"));
        }
        if self.user.is_empty() {
            return Err(config_error("target user is required"));
        }
        if self.password.is_empty() {
            return Err(config_error("target password is required"));
        }
        if self.database.is_empty() {
            return Err(config_error("target database is required"));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyBinlogReport {
    pub applied_statements: u64,
    pub quarantined_statements: u64,
}

#[derive(Debug)]
pub enum ApplyBinlogError {
    Config(String),
    SourceCommand(String),
    Target(String),
    Statement(String),
    Quarantined(Vec<QuarantinedStatement>),
}

impl fmt::Display for ApplyBinlogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => formatter.write_str(message),
            Self::SourceCommand(message) => {
                write!(formatter, "source binlog command failed: {message}")
            }
            Self::Target(message) => write!(formatter, "target apply failed: {message}"),
            Self::Statement(message) => write!(formatter, "statement apply failed: {message}"),
            Self::Quarantined(statements) => write!(
                formatter,
                "{} statement(s) quarantined; refusing to continue",
                statements.len()
            ),
        }
    }
}

impl std::error::Error for ApplyBinlogError {}

pub fn apply_remote_binlog(
    config: &ApplyBinlogConfig,
) -> Result<ApplyBinlogReport, ApplyBinlogError> {
    config.validate()?;
    let output = read_remote_binlog(config)?;
    let events = extract_statement_events(
        &output,
        &BinlogCoordinate {
            file: config.source.binlog_file.clone(),
            position: config.source.start_position,
        },
    );
    let executor = MysqlCliExecutor {
        mariadb: config.mariadb.clone(),
        target: config.target.clone(),
    };
    apply_statement_events(events, executor, RecordingQuarantine::default())
}

pub fn apply_statement_events<E, Q>(
    events: Vec<StatementEvent>,
    executor: E,
    quarantine: Q,
) -> Result<ApplyBinlogReport, ApplyBinlogError>
where
    E: TargetExecutor,
    Q: StatementQuarantine + QuarantineRecorder,
{
    let applier = StatementApplier::new(executor, quarantine);
    let mut applied_statements = 0;
    let mut quarantined_statements = 0;

    for event in &events {
        match applier.apply(event) {
            Ok(StatementOutcome::Replayed) => applied_statements += 1,
            Ok(StatementOutcome::Quarantined(_)) => quarantined_statements += 1,
            Err(error) => return Err(ApplyBinlogError::Statement(error.to_string())),
        }
    }

    let quarantined = applier.quarantine_recorder().recorded_statements();
    if !quarantined.is_empty() {
        return Err(ApplyBinlogError::Quarantined(quarantined));
    }

    Ok(ApplyBinlogReport {
        applied_statements,
        quarantined_statements,
    })
}

pub trait QuarantineRecorder {
    fn recorded_statements(&self) -> Vec<QuarantinedStatement>;
}

#[derive(Default)]
pub struct RecordingQuarantine {
    statements: RefCell<Vec<QuarantinedStatement>>,
}

impl StatementQuarantine for RecordingQuarantine {
    fn record(&self, statement: &QuarantinedStatement) -> Result<(), QuarantineError> {
        self.statements.borrow_mut().push(statement.clone());
        Ok(())
    }
}

impl QuarantineRecorder for RecordingQuarantine {
    fn recorded_statements(&self) -> Vec<QuarantinedStatement> {
        self.statements.borrow().clone()
    }
}

pub struct MysqlCliExecutor {
    mariadb: String,
    target: TargetMySqlConfig,
}

impl TargetExecutor for MysqlCliExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        let output = Command::new(&self.mariadb)
            .args([
                "--batch",
                "--raw",
                "--skip-column-names",
                "--host",
                &self.target.host,
                "--port",
                &self.target.port.to_string(),
                "--user",
                &self.target.user,
                &self.target.database,
                "-e",
                &statement.sql,
            ])
            .env("MYSQL_PWD", &self.target.password)
            .output()
            .map_err(|error| TargetExecuteError::new(format!("failed to run mariadb: {error}")))?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(TargetExecuteError::new(format!(
                "mariadb exited with {}: {}",
                output.status,
                stderr.trim()
            )))
        }
    }
}

pub fn extract_statement_events(output: &str, start: &BinlogCoordinate) -> Vec<StatementEvent> {
    let mut current_file = start.file.clone();
    let mut current_position = start.position;
    let mut default_database = None;
    let mut events = Vec::new();

    for line in output.lines() {
        let line = line.trim();

        if let Some(position) = parse_at_position(line) {
            current_position = position;
            continue;
        }
        if let Some(file) = parse_rotate_file(line) {
            current_file = file;
            continue;
        }
        if let Some(database) = parse_use_database(line) {
            default_database = Some(database);
            continue;
        }
        if let Some(sql) = parse_sql_statement(line) {
            events.push(StatementEvent {
                coordinate: BinlogCoordinate {
                    file: current_file.clone(),
                    position: current_position,
                },
                default_database: default_database.clone(),
                sql,
            });
        }
    }

    events
}

fn read_remote_binlog(config: &ApplyBinlogConfig) -> Result<String, ApplyBinlogError> {
    let args = binlog_args(&config.source);
    let output = Command::new(&config.mariadb_binlog)
        .args(args)
        .env("MYSQL_PWD", &config.source.password)
        .output()
        .map_err(|error| {
            ApplyBinlogError::SourceCommand(format!("failed to run mariadb-binlog: {error}"))
        })?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(ApplyBinlogError::SourceCommand(format!(
            "mariadb-binlog exited with {}: {}",
            output.status,
            stderr.trim()
        )))
    }
}

fn binlog_args(source: &SourceBinlogConfig) -> Vec<String> {
    let mut args = vec![
        "--read-from-remote-server".to_string(),
        "--verbose".to_string(),
        "--base64-output=decode-rows".to_string(),
        "--host".to_string(),
        source.host.clone(),
        "--port".to_string(),
        source.port.to_string(),
        "--user".to_string(),
        source.user.clone(),
        "--start-position".to_string(),
        source.start_position.to_string(),
    ];

    if let Some(stop_position) = source.stop_position {
        args.push("--stop-position".to_string());
        args.push(stop_position.to_string());
    }

    args.push(source.binlog_file.clone());
    args
}

fn parse_at_position(line: &str) -> Option<u64> {
    let rest = line.strip_prefix("# at ")?;
    rest.split_whitespace().next()?.parse().ok()
}

fn parse_rotate_file(line: &str) -> Option<String> {
    let tail = line.split_once("Rotate to ")?.1;
    let file = tail.split_whitespace().next()?.trim_matches('\'');

    if file.is_empty() {
        None
    } else {
        Some(file.to_string())
    }
}

fn parse_use_database(line: &str) -> Option<String> {
    let rest = line.strip_prefix("use `")?;
    let database = rest.split_once('`')?.0;

    if database.is_empty() {
        None
    } else {
        Some(database.to_string())
    }
}

fn parse_sql_statement(line: &str) -> Option<String> {
    let cleaned = cleanup_binlog_sql_line(line);
    let upper = cleaned.to_ascii_uppercase();
    let is_dml = upper.starts_with("INSERT INTO ")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("DELETE FROM ")
        || upper.starts_with("REPLACE INTO ");
    let is_ddl = upper.starts_with("CREATE ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("DROP ")
        || upper.starts_with("TRUNCATE ");

    if is_dml || is_ddl {
        Some(cleaned)
    } else {
        None
    }
}

fn cleanup_binlog_sql_line(line: &str) -> String {
    line.trim()
        .trim_end_matches("/*!*/;")
        .trim_end_matches(';')
        .trim()
        .to_string()
}

fn config_error(message: &str) -> ApplyBinlogError {
    ApplyBinlogError::Config(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn extracts_statement_events_with_coordinates_and_database() {
        let events = extract_statement_events(
            "\
# at 100
use `test_cdc`/*!*/;
SET TIMESTAMP=1/*!*/;
INSERT INTO accounts (id, name) VALUES (1, 'alpha')/*!*/;
# at 180
UPDATE accounts SET name = 'beta' WHERE id = 1/*!*/;
# at 220
# Rotate to mysqld-bin.000002  pos: 4
# at 4
DELETE FROM accounts WHERE id = 1/*!*/;
",
            &BinlogCoordinate {
                file: "mysqld-bin.000001".to_string(),
                position: 4,
            },
        );

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].coordinate.position, 100);
        assert_eq!(events[0].default_database, Some("test_cdc".to_string()));
        assert_eq!(
            events[0].sql,
            "INSERT INTO accounts (id, name) VALUES (1, 'alpha')"
        );
        assert_eq!(events[2].coordinate.file, "mysqld-bin.000002");
    }

    #[test]
    fn applies_extracted_compatible_statements() {
        let events = vec![StatementEvent {
            coordinate: BinlogCoordinate {
                file: "mysqld-bin.000001".to_string(),
                position: 100,
            },
            default_database: Some("test_cdc".to_string()),
            sql: "INSERT INTO accounts (id, name) VALUES (1, 'alpha')".to_string(),
        }];
        let executor = RecordingExecutor::default();

        let report = apply_statement_events(events, executor, RecordingQuarantine::default())
            .expect("apply");

        assert_eq!(
            report,
            ApplyBinlogReport {
                applied_statements: 1,
                quarantined_statements: 0,
            }
        );
    }

    #[test]
    fn refuses_quarantined_statements() {
        let events = vec![StatementEvent {
            coordinate: BinlogCoordinate {
                file: "mysqld-bin.000001".to_string(),
                position: 100,
            },
            default_database: Some("test_cdc".to_string()),
            sql: "CREATE TABLE accounts (id INT PRIMARY KEY)".to_string(),
        }];
        let executor = RecordingExecutor::default();

        let error = apply_statement_events(events, executor, RecordingQuarantine::default())
            .expect_err("ddl should quarantine")
            .to_string();

        assert!(error.contains("quarantined"));
    }

    #[derive(Default)]
    struct RecordingExecutor {
        statements: RefCell<Vec<SqlStatement>>,
    }

    impl TargetExecutor for RecordingExecutor {
        fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
            self.statements.borrow_mut().push(statement.clone());
            Ok(())
        }
    }
}
