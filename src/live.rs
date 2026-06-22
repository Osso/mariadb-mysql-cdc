use crate::checkpoint::FileCheckpointStore;
use crate::probe::BinlogCoordinate;
use crate::statement::{
    QuarantineError, QuarantinedStatement, StatementApplier, StatementEvent, StatementOutcome,
    StatementQuarantine,
};
use crate::target::TargetExecutor;
use std::cell::RefCell;
use std::fmt;
use std::io::{BufRead, BufReader, Lines, Read};
use std::path::PathBuf;
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::thread;

mod binlog_command;
mod insert_conflict;
mod mysql_cli;
mod progress;
mod reconnect;
mod repair;
mod schema_recovery;
#[cfg(test)]
use crate::target::{SqlStatement, TargetExecuteError};
use binlog_command::{read_remote_binlog, stop_never_args};
pub use insert_conflict::{InsertConflictPolicy, should_ignore_duplicate_insert};
pub use mysql_cli::MysqlCliExecutor;
#[cfg(test)]
use mysql_cli::{
    format_slow_target_query_log, target_client_character_set_arg, target_session_init_command,
    truncate_sql_for_log,
};
use progress::{
    StreamProgress, format_stream_exit, format_stream_progress, format_stream_quarantine,
    format_stream_start,
};
use reconnect::{
    StreamCheckpointStore, format_reconnect_start, reconnect_delay, save_stream_checkpoint,
    should_reconnect,
};
use repair::{FailedStatementRepairer, TableSyncStatementRepairer, repair_failed_statement};
use schema_recovery::mysql_executor_with_recovery;

#[derive(Clone, Debug)]
pub struct ApplyBinlogConfig {
    pub source: SourceBinlogConfig,
    pub target: TargetMySqlConfig,
    pub mariadb: String,
    pub mariadb_binlog: String,
    pub checkpoint_file: Option<PathBuf>,
    pub max_reconnects: u32,
}

impl Default for ApplyBinlogConfig {
    fn default() -> Self {
        Self {
            source: SourceBinlogConfig::default(),
            target: TargetMySqlConfig::default(),
            mariadb: "mariadb".to_string(),
            mariadb_binlog: "mariadb-binlog".to_string(),
            checkpoint_file: None,
            max_reconnects: 12,
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
        if self.checkpoint_file.is_none() {
            self.source.validate_start_coordinate()?;
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

impl SourceBinlogConfig {
    fn validate_start_coordinate(&self) -> Result<(), ApplyBinlogError> {
        if self.binlog_file.is_empty() {
            return Err(config_error("binlog file is required"));
        }
        if self.start_position == 0 {
            return Err(config_error("start position must be greater than zero"));
        }

        Ok(())
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
    Checkpoint(String),
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
            Self::Checkpoint(message) => write!(formatter, "checkpoint failed: {message}"),
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
    let executor = mysql_executor_with_recovery(config);
    apply_statement_events(events, executor, RecordingQuarantine::default())
}

pub fn stream_remote_binlog(config: &ApplyBinlogConfig) -> Result<(), ApplyBinlogError> {
    config.validate()?;
    let executor = mysql_executor_with_recovery(config);
    let quarantine = RecordingQuarantine::default();
    let applier = StatementApplier::new(executor, quarantine);
    let repairer = TableSyncStatementRepairer::new(config.clone());

    match &config.checkpoint_file {
        Some(path) => {
            let checkpoint_store = FileCheckpointStore::new(path);
            stream_statement_events_with_reconnect(
                config,
                &applier,
                &repairer,
                Some(&checkpoint_store),
            )
        }
        None => stream_statement_events_with_reconnect::<_, _, _, FileCheckpointStore>(
            config, &applier, &repairer, None,
        ),
    }
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

fn stream_statement_events_with_reconnect<E, Q, R, C>(
    config: &ApplyBinlogConfig,
    applier: &StatementApplier<E, Q>,
    repairer: &R,
    checkpoint_store: Option<&C>,
) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
    Q: StatementQuarantine + QuarantineRecorder,
    R: FailedStatementRepairer,
    C: StreamCheckpointStore,
{
    run_stream_reconnect_loop(
        config,
        checkpoint_store,
        |attempt_config| {
            stream_statement_events_once(attempt_config, applier, repairer, checkpoint_store)
        },
        thread::sleep,
    )
}

fn run_stream_reconnect_loop<C, F, S>(
    config: &ApplyBinlogConfig,
    checkpoint_store: Option<&C>,
    mut run_attempt: F,
    sleep: S,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
    F: FnMut(&ApplyBinlogConfig) -> Result<(), ApplyBinlogError>,
    S: Fn(std::time::Duration),
{
    let mut attempt_config = config.clone();
    resume_from_checkpoint(&mut attempt_config, checkpoint_store)?;
    attempt_config.source.validate_start_coordinate()?;
    let mut attempt = 0;

    loop {
        match run_attempt(&attempt_config) {
            Ok(()) => return Ok(()),
            Err(error)
                if checkpoint_store.is_some()
                    && should_reconnect(&error, attempt, config.max_reconnects) =>
            {
                attempt += 1;
                resume_from_checkpoint(&mut attempt_config, checkpoint_store)?;
                attempt_config.source.validate_start_coordinate()?;
                println!(
                    "{}",
                    format_reconnect_start(&attempt_config, attempt, &error)
                );
                sleep(reconnect_delay(attempt));
            }
            Err(error) => return Err(error),
        }
    }
}

fn stream_statement_events_once<E, Q, R, C>(
    config: &ApplyBinlogConfig,
    applier: &StatementApplier<E, Q>,
    repairer: &R,
    checkpoint_store: Option<&C>,
) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
    Q: StatementQuarantine + QuarantineRecorder,
    R: FailedStatementRepairer,
    C: StreamCheckpointStore,
{
    let start = start_coordinate(&config.source);
    let stream = spawn_stream_reader(config)?;
    let StreamProcess {
        mut child,
        stdout,
        mut stderr,
    } = stream;
    let mut extractor = StatementEventExtractor::new(start.clone());
    let mut progress = StreamProgress::new(start);

    println!("{}", format_stream_start(config));
    process_stream_lines(
        BufReader::new(stdout).lines(),
        &mut extractor,
        applier,
        repairer,
        &mut progress,
        checkpoint_store,
    )?;
    wait_for_stream_exit(&mut child, &mut stderr, &progress)
}

fn start_coordinate(source: &SourceBinlogConfig) -> BinlogCoordinate {
    BinlogCoordinate {
        file: source.binlog_file.clone(),
        position: source.start_position,
    }
}

fn process_stream_lines<E, Q, R>(
    lines: Lines<BufReader<ChildStdout>>,
    extractor: &mut StatementEventExtractor,
    applier: &StatementApplier<E, Q>,
    repairer: &R,
    progress: &mut StreamProgress,
    checkpoint_store: Option<&impl StreamCheckpointStore>,
) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
    Q: StatementQuarantine + QuarantineRecorder,
    R: FailedStatementRepairer,
{
    for line in lines {
        let line = line.map_err(|error| {
            ApplyBinlogError::SourceCommand(format!("failed reading mariadb-binlog: {error}"))
        })?;
        if let Some(event) = extractor.accept_line(&line) {
            apply_stream_event(applier, repairer, &event, progress, checkpoint_store)?;
        }
    }

    Ok(())
}

struct StreamProcess {
    child: Child,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

fn wait_for_stream_exit(
    child: &mut Child,
    stderr: &mut ChildStderr,
    progress: &StreamProgress,
) -> Result<(), ApplyBinlogError> {
    let status = child.wait().map_err(|error| {
        ApplyBinlogError::SourceCommand(format!("failed waiting for mariadb-binlog: {error}"))
    })?;
    let mut stderr_output = String::new();
    stderr.read_to_string(&mut stderr_output).map_err(|error| {
        ApplyBinlogError::SourceCommand(format!("failed reading mariadb-binlog stderr: {error}"))
    })?;

    if status.success() {
        println!("{}", format_stream_exit(progress));
        Err(ApplyBinlogError::SourceCommand(
            "mariadb-binlog stream ended at EOF".to_string(),
        ))
    } else {
        Err(ApplyBinlogError::SourceCommand(format!(
            "mariadb-binlog exited with {status}: {}",
            stderr_output.trim()
        )))
    }
}

fn spawn_stream_reader(config: &ApplyBinlogConfig) -> Result<StreamProcess, ApplyBinlogError> {
    let mut child = Command::new(&config.mariadb_binlog)
        .args(stop_never_args(&config.source))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            ApplyBinlogError::SourceCommand(format!("failed to run mariadb-binlog: {error}"))
        })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        ApplyBinlogError::SourceCommand("mariadb-binlog stdout was not captured".to_string())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        ApplyBinlogError::SourceCommand("mariadb-binlog stderr was not captured".to_string())
    })?;

    Ok(StreamProcess {
        child,
        stdout,
        stderr,
    })
}

fn apply_stream_event<E, Q, R>(
    applier: &StatementApplier<E, Q>,
    repairer: &R,
    event: &StatementEvent,
    progress: &mut StreamProgress,
    checkpoint_store: Option<&impl StreamCheckpointStore>,
) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
    Q: StatementQuarantine + QuarantineRecorder,
    R: FailedStatementRepairer,
{
    match applier.apply(event) {
        Ok(StatementOutcome::Replayed) => {
            save_stream_checkpoint(checkpoint_store, event)?;
            if progress.record_applied(&event.coordinate) {
                println!("{}", format_stream_progress(progress));
            }
            Ok(())
        }
        Ok(StatementOutcome::Quarantined(reason)) => {
            progress.record_quarantined(&event.coordinate);
            println!("{}", format_stream_quarantine(progress, &reason));
            Err(ApplyBinlogError::Quarantined(
                applier.quarantine_recorder().recorded_statements(),
            ))
        }
        Err(error) => {
            if repair_failed_statement(repairer, event, &error)? {
                save_stream_checkpoint(checkpoint_store, event)?;
                if progress.record_applied(&event.coordinate) {
                    println!("{}", format_stream_progress(progress));
                }
                return Ok(());
            }
            Err(ApplyBinlogError::Statement(error.to_string()))
        }
    }
}

fn resume_from_checkpoint(
    config: &mut ApplyBinlogConfig,
    checkpoint_store: Option<&impl StreamCheckpointStore>,
) -> Result<(), ApplyBinlogError> {
    let Some(store) = checkpoint_store else {
        return Ok(());
    };
    let Some(checkpoint) = store.load_checkpoint()? else {
        return Ok(());
    };

    config.source.binlog_file = checkpoint.source_file;
    config.source.start_position = checkpoint.source_position;
    Ok(())
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
    line.ends_with("/*!*/;")
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
mod tests;
