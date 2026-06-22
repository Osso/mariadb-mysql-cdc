use super::{TargetMySqlConfig, should_ignore_duplicate_insert};
use crate::target::{SqlStatement, TargetExecuteError, TargetExecutor, render_sql_statement};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct MysqlCliExecutor {
    mariadb: String,
    target: TargetMySqlConfig,
}

const TARGET_SLOW_QUERY_AFTER: Duration = Duration::from_secs(20);
const TARGET_SLOW_QUERY_POLL: Duration = Duration::from_secs(1);
const TARGET_SLOW_QUERY_SQL_LIMIT: usize = 4_000;

impl MysqlCliExecutor {
    pub fn new(mariadb: impl Into<String>, target: TargetMySqlConfig) -> Self {
        Self {
            mariadb: mariadb.into(),
            target,
        }
    }
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
    fn run_statement(&self, statement: &SqlStatement) -> Result<Output, TargetExecuteError> {
        let child = self.spawn_statement(statement)?;
        wait_for_target_statement(child, statement)
    }

    fn spawn_statement(&self, statement: &SqlStatement) -> Result<Child, TargetExecuteError> {
        let password_arg = format!("--password={}", self.target.password);
        let rendered_statement = render_sql_statement(statement)?;
        let replay_sql = target_replay_sql(&rendered_statement);
        Command::new(&self.mariadb)
            .args([
                "--batch",
                "--raw",
                "--skip-column-names",
                target_client_character_set_arg(),
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
                &replay_sql,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| TargetExecuteError::new(format!("failed to run mariadb: {error}")))
    }

    fn handle_failed_statement(
        &self,
        statement: &SqlStatement,
        output: &Output,
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

fn wait_for_target_statement(
    mut child: Child,
    statement: &SqlStatement,
) -> Result<Output, TargetExecuteError> {
    let started_at = Instant::now();
    let mut logged_slow_query = false;

    while child
        .try_wait()
        .map_err(|error| TargetExecuteError::new(format!("failed to wait for mariadb: {error}")))?
        .is_none()
    {
        if !logged_slow_query && started_at.elapsed() >= TARGET_SLOW_QUERY_AFTER {
            println!("{}", format_slow_target_query_log(statement, started_at));
            logged_slow_query = true;
        }
        thread::sleep(TARGET_SLOW_QUERY_POLL);
    }

    child
        .wait_with_output()
        .map_err(|error| TargetExecuteError::new(format!("failed to read mariadb output: {error}")))
}

pub(super) fn format_slow_target_query_log(
    statement: &SqlStatement,
    started_at: Instant,
) -> String {
    let elapsed_seconds = started_at.elapsed().as_secs();
    let sql = truncate_sql_for_log(&statement.sql, TARGET_SLOW_QUERY_SQL_LIMIT);
    format!(
        "cdc_target_slow_query elapsed_seconds={} sql_bytes={} sql_truncated={} sql={}",
        elapsed_seconds,
        statement.sql.len(),
        sql.len() < statement.sql.len(),
        sql
    )
}

pub(super) fn truncate_sql_for_log(sql: &str, limit: usize) -> String {
    match sql.char_indices().nth(limit) {
        Some((index, _)) => sql[..index].to_string(),
        None => sql.to_string(),
    }
}

pub(super) fn target_session_init_command() -> &'static str {
    "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"
}

fn target_replay_sql(sql: &str) -> String {
    format!("{}; {}", target_session_init_command(), sql)
}

pub(super) fn target_client_character_set_arg() -> &'static str {
    "--default-character-set=utf8mb4"
}
