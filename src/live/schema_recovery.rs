use super::{ApplyBinlogConfig, MysqlCliExecutor, SourceBinlogConfig};
use crate::target::{
    SqlStatement, TargetExecuteError, TargetExecutionOutcome, TargetExecutor, TargetRowChange,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder};
use std::cell::RefCell;

pub trait SourceSchemaReader {
    fn read_create_table(&self, table: &str) -> Result<String, TargetExecuteError>;
}

pub struct SourceSchemaCliReader {
    source: SourceBinlogConfig,
    conn: RefCell<Option<Conn>>,
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

    fn execute_row_change(
        &self,
        change: &TargetRowChange,
    ) -> Result<TargetExecutionOutcome, TargetExecuteError> {
        match self.target.execute_row_change(change) {
            Ok(outcome) => Ok(outcome),
            Err(error) => self.create_missing_table_and_retry_row_change(change, error),
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

    fn create_missing_table_and_retry_row_change(
        &self,
        change: &TargetRowChange,
        error: TargetExecuteError,
    ) -> Result<TargetExecutionOutcome, TargetExecuteError> {
        let Some(table) = missing_target_table_name(&error.to_string()) else {
            return Err(error);
        };

        let source_ddl = self.source_schema.read_create_table(&table)?;
        let target_ddl = mysql_compatible_create_table(&source_ddl);
        self.create_target_table(&table, target_ddl)?;
        self.target.execute_row_change(change).map_err(|source| {
            TargetExecuteError::new(format!(
                "row change still failed after creating missing target table `{table}`: {source}"
            ))
        })
    }
}

impl SourceSchemaReader for SourceSchemaCliReader {
    fn read_create_table(&self, table: &str) -> Result<String, TargetExecuteError> {
        let database = required_source_database(&self.source)?;
        let sql = format!(
            "SHOW CREATE TABLE {}.{}",
            quote_mysql_ident(database),
            quote_mysql_ident(table)
        );
        self.ensure_conn()?
            .query_first::<(String, String), _>(sql)
            .map_err(|error| {
                TargetExecuteError::new(format!(
                    "source SHOW CREATE TABLE failed for `{table}`: {error}"
                ))
            })?
            .map(|(_table, ddl)| ddl)
            .ok_or_else(|| TargetExecuteError::new(format!("source returned no DDL for `{table}`")))
    }
}

impl SourceSchemaCliReader {
    fn ensure_conn(&self) -> Result<std::cell::RefMut<'_, Conn>, TargetExecuteError> {
        if self.conn.borrow().is_none() {
            self.conn.replace(Some(open_source_conn(&self.source)?));
        }
        Ok(std::cell::RefMut::map(self.conn.borrow_mut(), |conn| {
            conn.as_mut().expect("source schema connection initialized")
        }))
    }
}

pub(super) fn mysql_executor_with_recovery(
    config: &ApplyBinlogConfig,
) -> MissingTableRecoveringExecutor<MysqlCliExecutor, SourceSchemaCliReader> {
    MissingTableRecoveringExecutor::new(
        MysqlCliExecutor::new(config.target.clone()),
        SourceSchemaCliReader {
            source: config.source.clone(),
            conn: RefCell::new(None),
        },
    )
}

fn required_source_database(source: &SourceBinlogConfig) -> Result<&str, TargetExecuteError> {
    source.database.as_deref().ok_or_else(|| {
        TargetExecuteError::new("source database is required to create missing target tables")
    })
}

fn open_source_conn(source: &SourceBinlogConfig) -> Result<Conn, TargetExecuteError> {
    Conn::new(source_opts(source).map_err(TargetExecuteError::new)?).map_err(|error| {
        TargetExecuteError::new(format!("source schema connection failed: {error}"))
    })
}

fn source_opts(source: &SourceBinlogConfig) -> Result<Opts, String> {
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(&source.host))
        .tcp_port(source.port)
        .user(Some(&source.user))
        .pass(Some(&source.password))
        .prefer_socket(false);
    Ok(Opts::from(
        crate::mysql_support::apply_mysql_connection_liveness(builder),
    ))
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
    create_if_missing
        .replace("utf8mb4_uca1400_ai_ci", "utf8mb4_0900_ai_ci")
        .replace("utf8mb3_uca1400_ai_ci", "utf8mb3_general_ci")
}

fn quote_mysql_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn source_schema_recovery_connection_uses_plaintext_without_ca() {
        let source = SourceBinlogConfig {
            host: "source-db".to_string(),
            port: 3306,
            user: "cdc_reader".to_string(),
            password: "secret".to_string(),
            tls_ca_file: String::new(),
            ..SourceBinlogConfig::default()
        };

        let opts = source_opts(&source).expect("plaintext source schema connection opts");

        assert_eq!(opts.get_ip_or_hostname().as_ref(), "source-db");
        assert_eq!(opts.get_tcp_port(), 3306);
        assert!(opts.get_ssl_opts().is_none());
        assert_eq!(
            opts.get_tcp_connect_timeout(),
            Some(std::time::Duration::from_secs(10))
        );
        assert_eq!(opts.get_tcp_keepalive_time_ms(), Some(10_000));
        assert_eq!(opts.get_read_timeout(), None);
        assert_eq!(opts.get_write_timeout(), None);
        #[cfg(target_os = "linux")]
        assert_eq!(opts.get_tcp_user_timeout_ms(), Some(30_000));
    }

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
                "CREATE TABLE IF NOT EXISTS `accounts` (\n  `id` bigint NOT NULL,\n  `name` varchar(255) COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  CONSTRAINT `accounts_ibfk_1` FOREIGN KEY (`id`) REFERENCES `users` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
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
