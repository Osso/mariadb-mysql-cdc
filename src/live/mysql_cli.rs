use super::TargetMySqlConfig;
use crate::mysql_client::PersistentTargetExecutor;
use crate::target::{
    SqlStatement, TargetExecuteError, TargetExecutionOutcome, TargetExecutor, TargetRowChange,
};
use std::cell::RefCell;
#[cfg(test)]
use std::time::Instant;

pub struct MysqlCliExecutor {
    target: TargetMySqlConfig,
    executor: RefCell<Option<PersistentTargetExecutor>>,
}

#[cfg(test)]
const TARGET_SLOW_QUERY_SQL_LIMIT: usize = 4_000;

impl MysqlCliExecutor {
    pub fn new(target: TargetMySqlConfig) -> Self {
        Self {
            target,
            executor: RefCell::new(None),
        }
    }

    fn ensure_executor(
        &self,
    ) -> Result<std::cell::RefMut<'_, PersistentTargetExecutor>, TargetExecuteError> {
        if self.executor.borrow().is_none() {
            self.executor
                .replace(Some(PersistentTargetExecutor::new(&self.target)?));
        }
        Ok(std::cell::RefMut::map(
            self.executor.borrow_mut(),
            |executor| executor.as_mut().expect("target executor initialized"),
        ))
    }
}

impl TargetExecutor for MysqlCliExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        self.ensure_executor()?.execute(statement)
    }

    fn execute_row_change(
        &self,
        change: &TargetRowChange,
    ) -> Result<TargetExecutionOutcome, TargetExecuteError> {
        self.ensure_executor()?.execute_row_change(change)
    }
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

#[cfg(test)]
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

#[cfg(test)]
pub(super) fn truncate_sql_for_log(sql: &str, limit: usize) -> String {
    match sql.char_indices().nth(limit) {
        Some((index, _)) => sql[..index].to_string(),
        None => sql.to_string(),
    }
}

pub(crate) fn target_session_init_command() -> &'static str {
    "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"
}

#[cfg(test)]
fn target_replay_sql(sql: &str) -> String {
    format!("{}; {}", target_session_init_command(), sql)
}

#[cfg(test)]
pub(super) fn target_client_character_set_arg() -> &'static str {
    "--default-character-set=utf8mb4"
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn strips_generated_column_from_insert_values() {
        let sql = r#"INSERT INTO `releases` (`slug`,`public_time`,`title`) VALUES ("a",NULL,"hello"),("b",NULL,"world")"#;

        let rewritten = strip_insert_column_for_retry(sql, "public_time").expect("rewrite");

        assert_eq!(
            rewritten,
            r#"INSERT INTO `releases` (`slug`,`title`) VALUES("a","hello"),("b","world")"#
        );
    }

    #[test]
    fn strips_generated_column_without_splitting_quoted_commas() {
        let sql = r#"INSERT INTO `releases` (`slug`,`public_time`,`title`) VALUES ("a,b",NULL,"hello (world)")"#;

        let rewritten = strip_insert_column_for_retry(sql, "public_time").expect("rewrite");

        assert_eq!(
            rewritten,
            r#"INSERT INTO `releases` (`slug`,`title`) VALUES("a,b","hello (world)")"#
        );
    }

    #[test]
    fn slow_query_log_marks_untruncated_sql() {
        let statement = SqlStatement {
            sql: "SELECT 1".to_string(),
            params: Vec::new(),
        };
        let started_at = Instant::now() - Duration::from_secs(21);

        let log_line = format_slow_target_query_log(&statement, started_at);

        assert!(log_line.starts_with("cdc_target_slow_query elapsed_seconds="));
        assert!(log_line.contains("sql_truncated=false"));
    }

    #[test]
    fn truncate_sql_for_log_keeps_utf8_boundary() {
        assert_eq!(truncate_sql_for_log("éééSELECT", 3), "ééé");
    }

    #[test]
    fn target_replay_sql_keeps_session_init_prefix() {
        assert_eq!(
            target_replay_sql("INSERT INTO accounts VALUES (1)"),
            "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'; INSERT INTO accounts VALUES (1)"
        );
    }
}
