use super::{ApplyBinlogConfig, MysqlCliExecutor, SourceBinlogConfig};
use crate::target::{SqlStatement, TargetExecuteError, TargetExecutor};
use std::process::Command;

pub trait SourceSchemaReader {
    fn read_create_table(&self, table: &str) -> Result<String, TargetExecuteError>;
}

pub struct SourceSchemaCliReader {
    mariadb: String,
    source: SourceBinlogConfig,
}

pub struct MissingTableRecoveringExecutor<E, S> {
    pub(super) target: E,
    source_schema: S,
}

impl<E, S> MissingTableRecoveringExecutor<E, S> {
    pub fn new(target: E, source_schema: S) -> Self {
        Self {
            target,
            source_schema,
        }
    }
}

impl<E, S> TargetExecutor for MissingTableRecoveringExecutor<E, S>
where
    E: TargetExecutor,
    S: SourceSchemaReader,
{
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        match self.target.execute(statement) {
            Ok(()) => Ok(()),
            Err(error) => self.create_missing_table_and_retry(statement, error),
        }
    }
}

impl<E, S> MissingTableRecoveringExecutor<E, S>
where
    E: TargetExecutor,
    S: SourceSchemaReader,
{
    fn create_missing_table_and_retry(
        &self,
        statement: &SqlStatement,
        error: TargetExecuteError,
    ) -> Result<(), TargetExecuteError> {
        let Some(table) = missing_target_table_name(&error.to_string()) else {
            return Err(error);
        };

        let source_ddl = self.source_schema.read_create_table(&table)?;
        let target_ddl = mysql_compatible_create_table(&source_ddl);
        self.create_target_table(&table, target_ddl)?;
        self.retry_statement_after_create(&table, statement)
    }

    fn create_target_table(&self, table: &str, sql: String) -> Result<(), TargetExecuteError> {
        self.target
            .execute(&SqlStatement {
                sql,
                params: Vec::new(),
            })
            .map_err(|source| {
                TargetExecuteError::new(format!(
                    "failed to create missing target table `{table}`: {source}"
                ))
            })
    }

    fn retry_statement_after_create(
        &self,
        table: &str,
        statement: &SqlStatement,
    ) -> Result<(), TargetExecuteError> {
        self.target.execute(statement).map_err(|source| {
            TargetExecuteError::new(format!(
                "statement still failed after creating missing target table `{table}`: {source}"
            ))
        })
    }
}

impl SourceSchemaReader for SourceSchemaCliReader {
    fn read_create_table(&self, table: &str) -> Result<String, TargetExecuteError> {
        let database = required_source_database(&self.source)?;
        let output = self.run_show_create_table(database, table)?;
        parse_show_create_table_output(&String::from_utf8_lossy(&output.stdout))
            .ok_or_else(|| TargetExecuteError::new(format!("source returned no DDL for `{table}`")))
    }
}

impl SourceSchemaCliReader {
    fn run_show_create_table(
        &self,
        database: &str,
        table: &str,
    ) -> Result<std::process::Output, TargetExecuteError> {
        let sql = format!(
            "SHOW CREATE TABLE {}.{}",
            quote_mysql_ident(database),
            quote_mysql_ident(table)
        );
        let password_arg = format!("--password={}", self.source.password);
        let output = Command::new(&self.mariadb)
            .args([
                "--batch",
                "--raw",
                "--skip-column-names",
                "--host",
                &self.source.host,
                "--port",
                &self.source.port.to_string(),
                "--user",
                &self.source.user,
                &password_arg,
                "-e",
                &sql,
            ])
            .output()
            .map_err(|error| TargetExecuteError::new(format!("failed to run mariadb: {error}")))?;

        show_create_table_result(output, table)
    }
}

pub(super) fn mysql_executor_with_recovery(
    config: &ApplyBinlogConfig,
) -> MissingTableRecoveringExecutor<MysqlCliExecutor, SourceSchemaCliReader> {
    MissingTableRecoveringExecutor::new(
        MysqlCliExecutor::new(config.mariadb.clone(), config.target.clone()),
        SourceSchemaCliReader {
            mariadb: config.mariadb.clone(),
            source: config.source.clone(),
        },
    )
}

fn required_source_database(source: &SourceBinlogConfig) -> Result<&str, TargetExecuteError> {
    source.database.as_deref().ok_or_else(|| {
        TargetExecuteError::new("source database is required to create missing target tables")
    })
}

fn show_create_table_result(
    output: std::process::Output,
    table: &str,
) -> Result<std::process::Output, TargetExecuteError> {
    if output.status.success() {
        return Ok(output);
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(TargetExecuteError::new(format!(
        "source SHOW CREATE TABLE failed for `{table}` with {}: {}",
        output.status,
        stderr.trim()
    )))
}

fn missing_target_table_name(error: &str) -> Option<String> {
    if !error.contains("1146") && !error.contains("doesn't exist") {
        return None;
    }

    let table_ref = error.split("Table '").nth(1)?.split('\'').next()?;
    table_ref.rsplit('.').next().map(str::to_string)
}

pub(crate) fn mysql_compatible_create_table(source_ddl: &str) -> String {
    let create_if_missing = source_ddl.replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1);
    let mysql_collations = create_if_missing
        .replace("utf8mb4_uca1400_ai_ci", "utf8mb4_0900_ai_ci")
        .replace("utf8mb3_uca1400_ai_ci", "utf8mb3_general_ci");
    remove_foreign_key_constraints(&mysql_collations)
}

fn remove_foreign_key_constraints(ddl: &str) -> String {
    let mut lines = ddl
        .lines()
        .filter(|line| !line.trim_start().starts_with("/*M!"))
        .filter(|line| {
            let upper = line.to_ascii_uppercase();
            !(upper.contains("CONSTRAINT") && upper.contains("FOREIGN KEY"))
        })
        .map(str::to_string)
        .collect::<Vec<_>>();

    if let Some(index) = line_before_table_options(&lines)
        && lines[index].trim_end().ends_with(',')
    {
        lines[index] = lines[index].trim_end_matches(',').to_string();
    }

    lines.join("\n")
}

fn line_before_table_options(lines: &[String]) -> Option<usize> {
    lines
        .iter()
        .position(|line| line.starts_with(") ENGINE"))
        .and_then(|index| index.checked_sub(1))
}

fn parse_show_create_table_output(output: &str) -> Option<String> {
    output
        .trim_end()
        .split_once('\t')
        .map(|(_, ddl)| ddl.to_string())
}

fn quote_mysql_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn creates_missing_target_table_from_source_ddl_and_retries_statement() {
        let executor = RecordingExecutor::missing_table_once();
        let source_schema = StaticSchemaReader {
            ddl: "CREATE TABLE `accounts` (
  `id` bigint NOT NULL,
  `name` varchar(255) COLLATE utf8mb4_uca1400_ai_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `accounts_ibfk_1` FOREIGN KEY (`id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci"
                .to_string(),
        };
        let recovering_executor = MissingTableRecoveringExecutor::new(executor, source_schema);
        let statement = SqlStatement {
            sql: "INSERT INTO `accounts` (`id`, `name`) VALUES (1, 'Ada')".to_string(),
            params: Vec::new(),
        };

        recovering_executor
            .execute(&statement)
            .expect("recover missing table");

        assert_eq!(
            recovering_executor.target.statements.borrow().as_slice(),
            &[
                "INSERT INTO `accounts` (`id`, `name`) VALUES (1, 'Ada')",
                "CREATE TABLE IF NOT EXISTS `accounts` (\n  `id` bigint NOT NULL,\n  `name` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
                "INSERT INTO `accounts` (`id`, `name`) VALUES (1, 'Ada')",
            ]
        );
    }

    #[derive(Default)]
    struct RecordingExecutor {
        statements: RefCell<Vec<String>>,
        fail_first_statement_as_missing_table: bool,
    }

    impl RecordingExecutor {
        fn missing_table_once() -> Self {
            Self {
                statements: RefCell::new(Vec::new()),
                fail_first_statement_as_missing_table: true,
            }
        }
    }

    impl TargetExecutor for RecordingExecutor {
        fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
            self.statements.borrow_mut().push(statement.sql.clone());
            if self.fail_first_statement_as_missing_table && self.statements.borrow().len() == 1 {
                return Err(TargetExecuteError::new(
                    "ERROR 1146 (42S02): Table 'globalcomix.accounts' doesn't exist",
                ));
            }
            Ok(())
        }
    }

    struct StaticSchemaReader {
        ddl: String,
    }

    impl SourceSchemaReader for StaticSchemaReader {
        fn read_create_table(&self, _table: &str) -> Result<String, TargetExecuteError> {
            Ok(self.ddl.clone())
        }
    }
}
