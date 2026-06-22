use super::{TargetMySqlConfig, should_ignore_duplicate_insert};
use crate::target::{SqlStatement, TargetExecuteError, TargetExecutor, render_sql_statement};
use std::io::Write;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct MysqlCliExecutor {
    mariadb: String,
    target: TargetMySqlConfig,
}

const TARGET_SLOW_QUERY_AFTER: Duration = Duration::from_secs(20);
const TARGET_SLOW_QUERY_POLL: Duration = Duration::from_millis(50);
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

        if let Some(retry_statement) = rewrite_generated_column_insert(statement, &output) {
            let retry_output = self.run_statement(&retry_statement)?;
            if retry_output.status.success() {
                return Ok(());
            }
            return self.handle_failed_statement(&retry_statement, &retry_output);
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
        let mut child = Command::new(&self.mariadb)
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
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| TargetExecuteError::new(format!("failed to run mariadb: {error}")))?;
        write_replay_sql(&mut child, &replay_sql)?;
        Ok(child)
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

fn write_replay_sql(child: &mut Child, replay_sql: &str) -> Result<(), TargetExecuteError> {
    let Some(mut stdin) = child.stdin.take() else {
        return Err(TargetExecuteError::new("failed to open mariadb stdin"));
    };
    stdin
        .write_all(replay_sql.as_bytes())
        .map_err(|error| TargetExecuteError::new(format!("failed to write mariadb stdin: {error}")))
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

fn rewrite_generated_column_insert(
    statement: &SqlStatement,
    output: &Output,
) -> Option<SqlStatement> {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let generated_column = generated_column_from_error(&stderr)?;
    let sql = strip_insert_column_for_retry(&statement.sql, &generated_column)?;
    println!(
        "cdc_target_rewrite_generated_column column={} original_sql_bytes={} rewritten_sql_bytes={}",
        generated_column,
        statement.sql.len(),
        sql.len()
    );
    Some(SqlStatement {
        sql,
        params: Vec::new(),
    })
}

fn generated_column_from_error(stderr: &str) -> Option<String> {
    let marker = "generated column '";
    let start = stderr.find(marker)? + marker.len();
    let rest = &stderr[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

pub(crate) fn strip_insert_column_for_retry(sql: &str, generated_column: &str) -> Option<String> {
    if !sql.trim_start().to_ascii_uppercase().starts_with("INSERT ") {
        return None;
    }

    let column_start = sql.find('(')?;
    let column_end = find_matching_parenthesis(sql, column_start)?;
    let columns = split_top_level_csv(&sql[column_start + 1..column_end]);
    let generated_index = columns
        .iter()
        .position(|column| unquote_identifier(column) == generated_column)?;
    let retained_columns = remove_index(&columns, generated_index);
    let values_start = find_values_start(&sql[column_end + 1..])? + column_end + 1;
    let value_tuples = strip_value_tuples(&sql[values_start..], generated_index, columns.len())?;

    Some(format!(
        "{}({}){}{}",
        &sql[..column_start],
        retained_columns.join(","),
        &sql[column_end + 1..values_start],
        value_tuples
    ))
}

fn find_values_start(input: &str) -> Option<usize> {
    let upper = input.to_ascii_uppercase();
    let values_index = upper.find("VALUES")?;
    Some(values_index + "VALUES".len())
}

fn strip_value_tuples(
    input: &str,
    value_index_to_remove: usize,
    expected_values: usize,
) -> Option<String> {
    let mut rest = input;
    let mut tuples = Vec::new();

    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }
        if rest.starts_with(',') {
            rest = &rest[1..];
            continue;
        }
        if !rest.starts_with('(') {
            return None;
        }

        let tuple_end = find_matching_parenthesis(rest, 0)?;
        let values = split_top_level_csv(&rest[1..tuple_end]);
        if values.len() != expected_values {
            return None;
        }
        let retained_values = remove_index(&values, value_index_to_remove);
        tuples.push(format!("({})", retained_values.join(",")));
        rest = &rest[tuple_end + 1..];
    }

    Some(tuples.join(","))
}

fn remove_index(items: &[String], remove_index: usize) -> Vec<String> {
    items
        .iter()
        .enumerate()
        .filter(|(index, _item)| *index != remove_index)
        .map(|(_index, item)| item.clone())
        .collect()
}

fn unquote_identifier(identifier: &str) -> &str {
    identifier
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
}

fn find_matching_parenthesis(input: &str, open_index: usize) -> Option<usize> {
    let mut scanner = SqlScanner::default();

    for (index, character) in input
        .char_indices()
        .skip_while(|(index, _)| *index < open_index)
    {
        if scanner.accept(character) == SqlScanEvent::BalancedClose {
            return Some(index);
        }
    }

    None
}

fn split_top_level_csv(input: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut scanner = SqlScanner::default();
    let mut value_start = 0;

    for (index, character) in input.char_indices() {
        if scanner.accept(character) == SqlScanEvent::TopLevelComma {
            values.push(input[value_start..index].trim().to_string());
            value_start = index + 1;
        }
    }

    values.push(input[value_start..].trim().to_string());
    values
}

#[derive(Default)]
struct SqlScanner {
    quote: Option<char>,
    escaped: bool,
    depth: i32,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SqlScanEvent {
    BalancedClose,
    TopLevelComma,
    Other,
}

impl SqlScanner {
    fn accept(&mut self, character: char) -> SqlScanEvent {
        if self.consume_escape() {
            return SqlScanEvent::Other;
        }
        if self.start_escape(character) {
            return SqlScanEvent::Other;
        }
        if self.update_quote(character) {
            return SqlScanEvent::Other;
        }
        self.accept_unquoted(character)
    }

    fn consume_escape(&mut self) -> bool {
        let was_escaped = self.escaped;
        self.escaped = false;
        was_escaped
    }

    fn start_escape(&mut self, character: char) -> bool {
        if self.quote.is_some() && character == '\\' {
            self.escaped = true;
            return true;
        }
        false
    }

    fn update_quote(&mut self, character: char) -> bool {
        match self.quote {
            Some(quote) if character == quote => {
                self.quote = None;
                true
            }
            Some(_) => true,
            None if matches!(character, '\'' | '"' | '`') => {
                self.quote = Some(character);
                true
            }
            None => false,
        }
    }

    fn accept_unquoted(&mut self, character: char) -> SqlScanEvent {
        match character {
            '(' => {
                self.depth += 1;
                SqlScanEvent::Other
            }
            ')' => {
                self.depth -= 1;
                self.close_event()
            }
            ',' if self.depth == 0 => SqlScanEvent::TopLevelComma,
            _ => SqlScanEvent::Other,
        }
    }

    fn close_event(&self) -> SqlScanEvent {
        if self.depth == 0 {
            SqlScanEvent::BalancedClose
        } else {
            SqlScanEvent::Other
        }
    }
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

pub(crate) fn target_session_init_command() -> &'static str {
    "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"
}

fn target_replay_sql(sql: &str) -> String {
    format!("{}; {}", target_session_init_command(), sql)
}

pub(super) fn target_client_character_set_arg() -> &'static str {
    "--default-character-set=utf8mb4"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::InsertConflictPolicy;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn strips_generated_column_from_insert_values() {
        let sql = "INSERT INTO `releases` (`slug`,`public_time`,`title`) VALUES (\"a\",NULL,\"hello\"),(\"b\",NULL,\"world\")";

        let rewritten = strip_insert_column_for_retry(sql, "public_time").expect("rewrite");

        assert_eq!(
            rewritten,
            "INSERT INTO `releases` (`slug`,`title`) VALUES(\"a\",\"hello\"),(\"b\",\"world\")"
        );
    }

    #[test]
    fn strips_generated_column_without_splitting_quoted_commas() {
        let sql = "INSERT INTO `releases` (`slug`,`public_time`,`title`) VALUES (\"a,b\",NULL,\"hello (world)\")";

        let rewritten = strip_insert_column_for_retry(sql, "public_time").expect("rewrite");

        assert_eq!(
            rewritten,
            "INSERT INTO `releases` (`slug`,`title`) VALUES(\"a,b\",\"hello (world)\")"
        );
    }

    #[test]
    fn extracts_generated_column_from_mysql_error() {
        let stderr = "ERROR 3105 (HY000): The value specified for generated column 'public_time' in table 'releases' is not allowed.";

        assert_eq!(
            generated_column_from_error(stderr),
            Some("public_time".to_string())
        );
    }

    #[test]
    fn execute_retries_insert_without_generated_column() {
        let fixture = FakeMariadb::new(
            "ERROR 3105 (HY000): The value specified for generated column 'public_time' in table 'releases' is not allowed.",
            1,
        );
        let executor = MysqlCliExecutor::new(fixture.script_path(), target_config());
        let statement = SqlStatement {
            sql: "INSERT INTO `releases` (`slug`,`public_time`,`title`) VALUES (\"a,b\",NULL,\"hello (world)\")"
                .to_string(),
            params: Vec::new(),
        };

        executor
            .execute(&statement)
            .expect("generated column retry");

        assert_eq!(fixture.call_count(), 2);
        let replay_sql = fixture.replay_sql();
        assert!(replay_sql.contains(target_session_init_command()));
        assert!(
            replay_sql.contains(
                "INSERT INTO `releases` (`slug`,`title`) VALUES(\"a,b\",\"hello (world)\")"
            )
        );
        assert!(!replay_sql.contains("`public_time`"));
    }

    #[test]
    fn execute_does_not_retry_unrelated_target_error() {
        let fixture = FakeMariadb::new("ERROR 1064 (42000): syntax error", 99);
        let executor = MysqlCliExecutor::new(fixture.script_path(), target_config());
        let statement = SqlStatement {
            sql: "INSERT INTO `releases` (`slug`) VALUES (\"alpha\")".to_string(),
            params: Vec::new(),
        };

        let error = executor.execute(&statement).expect_err("target failure");

        assert!(error.to_string().contains("ERROR 1064"));
        assert_eq!(fixture.call_count(), 1);
    }

    #[test]
    fn execute_sends_large_sql_through_stdin() {
        let fixture = FakeMariadb::new("", 0);
        let executor = MysqlCliExecutor::new(fixture.script_path(), target_config());
        let statement = SqlStatement {
            sql: format!(
                "INSERT INTO `events` (`body`) VALUES (\"{}\")",
                "x".repeat(200_000)
            ),
            params: Vec::new(),
        };

        executor
            .execute(&statement)
            .expect("large SQL through stdin");

        assert_eq!(fixture.call_count(), 1);
        assert!(fixture.replay_sql().contains(&statement.sql));
    }

    fn target_config() -> TargetMySqlConfig {
        TargetMySqlConfig {
            host: "target.db".to_string(),
            port: 25060,
            user: "target_user".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            insert_conflict_policy: InsertConflictPolicy::Error,
        }
    }

    struct FakeMariadb {
        dir: PathBuf,
        script: PathBuf,
        count_file: PathBuf,
        replay_sql_file: PathBuf,
    }

    impl FakeMariadb {
        fn new(first_error: &str, success_after_failures: usize) -> Self {
            let dir = temp_fixture_dir();
            fs::create_dir_all(&dir).expect("fixture dir");
            let script = dir.join("mariadb");
            let count_file = dir.join("count");
            let replay_sql_file = dir.join("replay.sql");
            let script_body = fake_mariadb_script(
                &count_file,
                &replay_sql_file,
                first_error,
                success_after_failures,
            );
            fs::write(&script, script_body).expect("fake mariadb script");
            let mut permissions = fs::metadata(&script)
                .expect("script metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).expect("script permissions");
            Self {
                dir,
                script,
                count_file,
                replay_sql_file,
            }
        }

        fn script_path(&self) -> String {
            self.script.to_string_lossy().into_owned()
        }

        fn call_count(&self) -> usize {
            fs::read_to_string(&self.count_file)
                .expect("call count")
                .trim()
                .parse()
                .expect("numeric call count")
        }

        fn replay_sql(&self) -> String {
            fs::read_to_string(&self.replay_sql_file).expect("replay sql")
        }
    }

    impl Drop for FakeMariadb {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    fn fake_mariadb_script(
        count_file: &Path,
        replay_sql_file: &Path,
        first_error: &str,
        success_after_failures: usize,
    ) -> String {
        format!(
            r#"#!/bin/sh
count_file={count_file}
replay_sql_file={replay_sql_file}
count="$(cat "$count_file" 2>/dev/null || echo 0)"
count=$((count + 1))
printf '%s' "$count" > "$count_file"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-e" ]; then
    printf '%s\n' "unexpected -e argument" >&2
    exit 2
  fi
  shift
done
cat > "$replay_sql_file"
if [ "$count" -le {success_after_failures} ]; then
  printf '%s\n' {first_error} >&2
  exit 1
fi
exit 0
"#,
            count_file = shell_quote_path(count_file),
            replay_sql_file = shell_quote_path(replay_sql_file),
            first_error = shell_quote(first_error),
        )
    }

    fn shell_quote_path(path: &Path) -> String {
        shell_quote(&path.to_string_lossy())
    }

    fn shell_quote(value: impl AsRef<str>) -> String {
        format!("'{}'", value.as_ref().replace('\'', "'\\''"))
    }

    fn temp_fixture_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let counter = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "mariadb-mysql-cdc-test-{}-{nanos}-{counter}",
            std::process::id()
        ))
    }
}
