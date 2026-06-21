use crate::snapshot::{SnapshotRow, SnapshotTarget};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlStatement {
    pub sql: String,
    pub params: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryKey {
    pub values: Vec<String>,
}

impl PrimaryKey {
    pub fn new(values: Vec<String>) -> Self {
        Self { values }
    }
}

pub trait TargetExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError>;
}

#[derive(Clone, Debug)]
pub struct TargetMySqlWriter<E> {
    table: String,
    primary_key: Vec<String>,
    columns: Vec<String>,
    pub executor: E,
}

impl<E> TargetMySqlWriter<E>
where
    E: TargetExecutor,
{
    pub fn new(
        table: impl Into<String>,
        primary_key: Vec<&str>,
        columns: Vec<&str>,
        executor: E,
    ) -> Self {
        Self {
            table: table.into(),
            primary_key: primary_key.into_iter().map(str::to_string).collect(),
            columns: columns.into_iter().map(str::to_string).collect(),
            executor,
        }
    }

    pub fn insert_rows(&self, rows: &[SnapshotRow]) -> Result<(), TargetWriteError> {
        if rows.is_empty() {
            return Ok(());
        }

        let statement = build_insert_statement(&self.table, &self.primary_key, &self.columns, rows);
        self.execute_with_context("insert", rows.len(), statement)
    }

    pub fn update_row(&self, row: &SnapshotRow) -> Result<(), TargetWriteError> {
        let statement = build_update_statement(&self.table, &self.primary_key, &self.columns, row);
        self.execute_with_context("update", 1, statement)
    }

    pub fn delete_row(&self, primary_key: &PrimaryKey) -> Result<(), TargetWriteError> {
        let statement = build_delete_statement(&self.table, &self.primary_key, primary_key);
        self.execute_with_context("delete", 1, statement)
    }

    fn execute_with_context(
        &self,
        operation: &'static str,
        row_count: usize,
        statement: SqlStatement,
    ) -> Result<(), TargetWriteError> {
        self.executor
            .execute(&statement)
            .map_err(|source| TargetWriteError {
                operation,
                table: self.table.clone(),
                row_count,
                sql: statement.sql,
                source,
            })
    }
}

impl<E> SnapshotTarget for TargetMySqlWriter<E>
where
    E: TargetExecutor,
{
    fn write_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), crate::snapshot::SnapshotError> {
        self.insert_rows(rows)
            .map_err(|error| crate::snapshot::SnapshotError::InvalidTable(error.to_string()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetExecuteError {
    message: String,
}

impl TargetExecuteError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for TargetExecuteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TargetExecuteError {}

#[derive(Debug)]
pub struct TargetWriteError {
    operation: &'static str,
    table: String,
    row_count: usize,
    sql: String,
    source: TargetExecuteError,
}

impl fmt::Display for TargetWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rows = match self.row_count {
            1 => "1 row".to_string(),
            count => format!("{count} rows"),
        };

        write!(
            formatter,
            "target {} failed for {} on {}: {}; sql: {}",
            self.operation, rows, self.table, self.source, self.sql
        )
    }
}

impl std::error::Error for TargetWriteError {}

fn build_insert_statement(
    table: &str,
    primary_key: &[String],
    columns: &[String],
    rows: &[SnapshotRow],
) -> SqlStatement {
    let column_list = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>();
    let placeholders = row_placeholders(columns.len(), rows.len());
    let update_list = update_assignments(columns, primary_key);
    let params = rows
        .iter()
        .flat_map(|row| ordered_values(row, columns))
        .collect();

    SqlStatement {
        sql: format!(
            "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
            quote_ident(table),
            column_list.join(", "),
            placeholders,
            update_list.join(", ")
        ),
        params,
    }
}

fn build_update_statement(
    table: &str,
    primary_key: &[String],
    columns: &[String],
    row: &SnapshotRow,
) -> SqlStatement {
    let changed_columns = non_primary_columns(columns, primary_key);
    let assignments = changed_columns
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect::<Vec<_>>();
    let predicates = primary_key_predicates(primary_key);
    let mut params = ordered_values(row, &changed_columns);
    params.extend(row.primary_key.clone());

    SqlStatement {
        sql: format!(
            "UPDATE {} SET {} WHERE {}",
            quote_ident(table),
            assignments.join(", "),
            predicates.join(" AND ")
        ),
        params,
    }
}

fn build_delete_statement(
    table: &str,
    primary_key: &[String],
    row_primary_key: &PrimaryKey,
) -> SqlStatement {
    SqlStatement {
        sql: format!(
            "DELETE FROM {} WHERE {}",
            quote_ident(table),
            primary_key_predicates(primary_key).join(" AND ")
        ),
        params: row_primary_key.values.clone(),
    }
}

fn ordered_values(row: &SnapshotRow, columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|column| row.values.get(column).cloned().unwrap_or_default())
        .collect()
}

fn row_placeholders(column_count: usize, row_count: usize) -> String {
    let row_placeholder = format!("({})", vec!["?"; column_count].join(", "));
    vec![row_placeholder; row_count].join(", ")
}

fn update_assignments(columns: &[String], primary_key: &[String]) -> Vec<String> {
    non_primary_columns(columns, primary_key)
        .iter()
        .map(|column| format!("{} = VALUES({})", quote_ident(column), quote_ident(column)))
        .collect()
}

fn non_primary_columns(columns: &[String], primary_key: &[String]) -> Vec<String> {
    columns
        .iter()
        .filter(|column| !primary_key.contains(column))
        .cloned()
        .collect()
}

fn primary_key_predicates(primary_key: &[String]) -> Vec<String> {
    primary_key
        .iter()
        .map(|column| format!("{} = ?", quote_ident(column)))
        .collect()
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{SnapshotRow, SnapshotTarget};
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[test]
    fn writes_snapshot_rows_as_batched_upsert() {
        let executor = RecordingExecutor::default();
        let mut writer =
            TargetMySqlWriter::new("accounts", vec!["id"], vec!["id", "name"], executor);

        writer
            .write_rows(&[row("1", "alpha"), row("2", "beta")])
            .expect("write rows");

        let executed = writer.executor.statements.borrow();
        assert_eq!(executed.len(), 1);
        assert_eq!(
            executed[0].sql,
            "INSERT INTO `accounts` (`id`, `name`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
        assert_eq!(executed[0].params, vec!["1", "alpha", "2", "beta"]);
    }

    #[test]
    fn writes_updates_and_deletes_with_primary_key_predicates() {
        let executor = RecordingExecutor::default();
        let mut writer =
            TargetMySqlWriter::new("accounts", vec!["id"], vec!["id", "name"], executor);

        writer.update_row(&row("7", "updated")).expect("update row");
        writer
            .delete_row(&PrimaryKey::new(vec!["7".to_string()]))
            .expect("delete row");

        let executed = writer.executor.statements.borrow();
        assert_eq!(
            executed[0].sql,
            "UPDATE `accounts` SET `name` = ? WHERE `id` = ?"
        );
        assert_eq!(executed[0].params, vec!["updated", "7"]);
        assert_eq!(executed[1].sql, "DELETE FROM `accounts` WHERE `id` = ?");
        assert_eq!(executed[1].params, vec!["7"]);
    }

    #[test]
    fn error_contains_operation_table_row_count_and_sql() {
        let executor = RecordingExecutor::failing("duplicate key");
        let mut writer =
            TargetMySqlWriter::new("accounts", vec!["id"], vec!["id", "name"], executor);

        let error = writer
            .write_rows(&[row("1", "alpha")])
            .expect_err("write failure");
        let message = error.to_string();

        assert!(message.contains("insert"));
        assert!(message.contains("accounts"));
        assert!(message.contains("1 row"));
        assert!(message.contains("duplicate key"));
        assert!(message.contains("INSERT INTO `accounts`"));
    }

    fn row(id: &str, name: &str) -> SnapshotRow {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), id.to_string());
        values.insert("name".to_string(), name.to_string());

        SnapshotRow {
            primary_key: vec![id.to_string()],
            values,
        }
    }

    #[derive(Default)]
    struct RecordingExecutor {
        statements: RefCell<Vec<SqlStatement>>,
        failure: Option<String>,
    }

    impl RecordingExecutor {
        fn failing(message: &str) -> Self {
            Self {
                statements: RefCell::new(Vec::new()),
                failure: Some(message.to_string()),
            }
        }
    }

    impl TargetExecutor for RecordingExecutor {
        fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
            self.statements.borrow_mut().push(statement.clone());

            match &self.failure {
                Some(message) => Err(TargetExecuteError::new(message)),
                None => Ok(()),
            }
        }
    }
}
