use crate::probe::BinlogCoordinate;
use crate::statement::{
    QuarantineError, QuarantinedStatement, StatementApplier, StatementEvent, StatementOutcome,
    StatementQuarantine,
};
use crate::target::{SqlStatement, TargetExecuteError, TargetExecutor};
use std::cell::RefCell;
use std::fmt;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

mod insert_conflict;
pub use insert_conflict::{InsertConflictPolicy, should_ignore_duplicate_insert};

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
    pub database: Option<String>,
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
            database: None,
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
    pub insert_conflict_policy: InsertConflictPolicy,
}

impl Default for TargetMySqlConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            database: String::new(),
            insert_conflict_policy: InsertConflictPolicy::Error,
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

pub fn stream_remote_binlog(config: &ApplyBinlogConfig) -> Result<(), ApplyBinlogError> {
    config.validate()?;
    let executor = MysqlCliExecutor {
        mariadb: config.mariadb.clone(),
        target: config.target.clone(),
    };
    let quarantine = RecordingQuarantine::default();
    let applier = StatementApplier::new(executor, quarantine);

    stream_statement_events(config, &applier)
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
        let output = self.run_statement(statement)?;

        if output.status.success() {
            return Ok(());
        }

        self.handle_failed_statement(statement, &output)
    }
}

impl MysqlCliExecutor {
    fn run_statement(
        &self,
        statement: &SqlStatement,
    ) -> Result<std::process::Output, TargetExecuteError> {
        let password_arg = format!("--password={}", self.target.password);
        Command::new(&self.mariadb)
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
                &password_arg,
                "--ssl",
                "--ssl-verify-server-cert=0",
                &self.target.database,
                "-e",
                &statement.sql,
            ])
            .output()
            .map_err(|error| TargetExecuteError::new(format!("failed to run mariadb: {error}")))
    }

    fn handle_failed_statement(
        &self,
        statement: &SqlStatement,
        output: &std::process::Output,
    ) -> Result<(), TargetExecuteError> {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if self.can_ignore_duplicate_insert(&statement.sql, &stderr) {
            return Ok(());
        }

        Err(TargetExecuteError::new(format!(
            "mariadb exited with {}: {}",
            output.status,
            stderr.trim()
        )))
    }

    fn can_ignore_duplicate_insert(&self, sql: &str, stderr: &str) -> bool {
        should_ignore_duplicate_insert(self.target.insert_conflict_policy, sql, stderr)
    }
}

pub fn extract_statement_events(output: &str, start: &BinlogCoordinate) -> Vec<StatementEvent> {
    let mut extractor = StatementEventExtractor::new(start.clone());
    let mut events = Vec::new();

    for line in output.lines() {
        if let Some(event) = extractor.accept_line(line) {
            events.push(event);
        }
    }

    events
}

fn stream_statement_events<E, Q>(
    config: &ApplyBinlogConfig,
    applier: &StatementApplier<E, Q>,
) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
    Q: StatementQuarantine + QuarantineRecorder,
{
    let mut child = Command::new(&config.mariadb_binlog)
        .args(stop_never_args(&config.source))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| {
            ApplyBinlogError::SourceCommand(format!("failed to run mariadb-binlog: {error}"))
        })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        ApplyBinlogError::SourceCommand("mariadb-binlog stdout was not captured".to_string())
    })?;
    let start = BinlogCoordinate {
        file: config.source.binlog_file.clone(),
        position: config.source.start_position,
    };
    let mut extractor = StatementEventExtractor::new(start);

    for line in BufReader::new(stdout).lines() {
        let line = line.map_err(|error| {
            ApplyBinlogError::SourceCommand(format!("failed reading mariadb-binlog: {error}"))
        })?;
        if let Some(event) = extractor.accept_line(&line) {
            apply_stream_event(applier, &event)?;
        }
    }

    let status = child.wait().map_err(|error| {
        ApplyBinlogError::SourceCommand(format!("failed waiting for mariadb-binlog: {error}"))
    })?;
    if status.success() {
        Ok(())
    } else {
        Err(ApplyBinlogError::SourceCommand(format!(
            "mariadb-binlog exited with {status}"
        )))
    }
}

fn apply_stream_event<E, Q>(
    applier: &StatementApplier<E, Q>,
    event: &StatementEvent,
) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
    Q: StatementQuarantine + QuarantineRecorder,
{
    match applier.apply(event) {
        Ok(StatementOutcome::Replayed) => {
            println!(
                "applied statement at {}:{}",
                event.coordinate.file, event.coordinate.position
            );
            Ok(())
        }
        Ok(StatementOutcome::Quarantined(_)) => Err(ApplyBinlogError::Quarantined(
            applier.quarantine_recorder().recorded_statements(),
        )),
        Err(error) => Err(ApplyBinlogError::Statement(error.to_string())),
    }
}

fn read_remote_binlog(config: &ApplyBinlogConfig) -> Result<String, ApplyBinlogError> {
    let args = binlog_args(&config.source);
    let output = Command::new(&config.mariadb_binlog)
        .args(args)
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
        format!("--password={}", source.password),
        "--start-position".to_string(),
        source.start_position.to_string(),
    ];

    if let Some(database) = &source.database {
        args.push("--database".to_string());
        args.push(database.clone());
    }

    if let Some(stop_position) = source.stop_position {
        args.push("--stop-position".to_string());
        args.push(stop_position.to_string());
    }

    args.push(source.binlog_file.clone());
    args
}

fn stop_never_args(source: &SourceBinlogConfig) -> Vec<String> {
    let mut args = binlog_args(source);
    let binlog_file_index = args.len().saturating_sub(1);
    args.insert(binlog_file_index, "--stop-never".to_string());
    args
}

struct StatementEventExtractor {
    current_file: String,
    current_position: u64,
    default_database: Option<String>,
    pending_statement: Vec<String>,
}

impl StatementEventExtractor {
    fn new(start: BinlogCoordinate) -> Self {
        Self {
            current_file: start.file,
            current_position: start.position,
            default_database: None,
            pending_statement: Vec::new(),
        }
    }

    fn accept_line(&mut self, line: &str) -> Option<StatementEvent> {
        let line = line.trim();

        if self.is_collecting_statement() {
            return self.collect_statement_line(line);
        }

        if let Some(position) = parse_at_position(line) {
            self.current_position = position;
            return None;
        }
        if let Some(file) = parse_rotate_file(line) {
            self.current_file = file;
            return None;
        }
        if let Some(database) = parse_use_database(line) {
            self.default_database = Some(database);
            return None;
        }

        if starts_sql_statement(line) {
            return self.start_statement(line);
        }

        None
    }

    fn is_collecting_statement(&self) -> bool {
        !self.pending_statement.is_empty()
    }

    fn collect_statement_line(&mut self, line: &str) -> Option<StatementEvent> {
        if is_statement_terminator(line) {
            return Some(self.finish_statement());
        }

        self.push_statement_line(line)
    }

    fn start_statement(&mut self, line: &str) -> Option<StatementEvent> {
        self.push_statement_line(line)
    }

    fn push_statement_line(&mut self, line: &str) -> Option<StatementEvent> {
        self.pending_statement.push(line.to_string());

        if line_has_statement_terminator(line) {
            Some(self.finish_statement())
        } else {
            None
        }
    }

    fn finish_statement(&mut self) -> StatementEvent {
        let sql = cleanup_binlog_sql_line(&self.pending_statement.join("\n"));
        self.pending_statement.clear();

        StatementEvent {
            coordinate: BinlogCoordinate {
                file: self.current_file.clone(),
                position: self.current_position,
            },
            default_database: self.default_database.clone(),
            sql,
        }
    }
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

fn starts_sql_statement(line: &str) -> bool {
    let cleaned = cleanup_binlog_sql_line(line);
    let upper = cleaned.to_ascii_uppercase();
    upper.starts_with("INSERT INTO ")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("DELETE FROM ")
        || upper.starts_with("REPLACE INTO ")
        || upper.starts_with("CREATE ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("DROP ")
        || upper.starts_with("TRUNCATE ")
}

fn is_statement_terminator(line: &str) -> bool {
    line == "/*!*/;" || line == "/*!*/"
}

fn line_has_statement_terminator(line: &str) -> bool {
    line.ends_with("/*!*/;") || line.ends_with(';')
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

    #[test]
    fn stop_never_args_keep_binlog_file_last() {
        let source = SourceBinlogConfig {
            host: "10.0.0.1".to_string(),
            user: "cdc".to_string(),
            password: "secret".to_string(),
            database: Some("test".to_string()),
            binlog_file: "mysqld-bin.000001".to_string(),
            start_position: 4,
            ..SourceBinlogConfig::default()
        };

        let args = stop_never_args(&source);

        assert!(args.contains(&"--stop-never".to_string()));
        assert_eq!(args.last(), Some(&"mysqld-bin.000001".to_string()));
    }

    #[test]
    fn extracts_sanitized_production_query_shapes() {
        let fixture = include_str!("../fixtures/prod-derived/sanitized-query-events.txt");
        let events = extract_statement_events(
            fixture,
            &BinlogCoordinate {
                file: "mysqld-bin.002523".to_string(),
                position: 955857729,
            },
        );

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].coordinate.position, 955857729);
        assert_eq!(
            events[0].sql,
            "UPDATE `guests` `g`\nSET `supports_cookies` = 1\nWHERE `g`.`guest_id` = 1001"
        );
        assert_eq!(events[1].coordinate.position, 957812859);
        assert!(events[1].sql.contains("UPDATE phrases p set"));
        assert!(events[1].sql.contains("WHERE `p`.`id`"));
        assert_eq!(events[2].coordinate.position, 957812400);
        assert!(
            events[2]
                .sql
                .contains("INSERT INTO `users_search_queries_history`")
        );
        assert!(events[2].sql.contains("\\\"semantic\\\":true"));
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
