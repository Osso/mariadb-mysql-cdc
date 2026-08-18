use crate::checkpoint::{Checkpoint, LastEvent};
use crate::live::{
    ApplyBinlogConfig, InsertConflictPolicy, TargetMySqlConfig, should_ignore_duplicate_insert,
};
use crate::mysql_config::MySqlConnectionConfig;
use crate::mysql_support::{
    apply_default_mysql_network_bounds, apply_mysql_connection_liveness, target_mysql_opts,
};
use crate::target::{
    SqlStatement, TargetExecuteError, TargetExecutor, TargetRowChange, TransactionalTargetExecutor,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Params};
use std::cell::RefCell;
use std::rc::Rc;

pub(crate) type SharedTargetConnection = Rc<RefCell<Option<Conn>>>;
type SharedParallelTargetWriter = Rc<
    RefCell<
        crate::live::parallel_writer::ParallelTargetWriter<
            crate::live::submitted_mysql::MariaDbSubmittedQueryFactory,
        >,
    >,
>;

mod connection;
pub(crate) mod missing_foreign_key;
mod query;
#[cfg(test)]
mod tests;

#[cfg(test)]
use connection::{NetworkTimeouts, apply_network_timeouts};
use connection::{base_opts, open_conn, source_connect_error, target_connect_error};
pub(crate) use query::value_to_string;
use query::{
    build_stream_lease_sql, build_target_column_select_sql, ensure_stream_lease_acquired,
    generated_column_retry_sql, row_to_strings, source_query_error, target_query_error,
};

#[derive(Debug, Eq, PartialEq)]
pub struct MySqlSourceError {
    message: String,
}

impl MySqlSourceError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for MySqlSourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MySqlSourceError {}

pub struct PersistentMySqlSource {
    conn: RefCell<Conn>,
}

#[derive(Clone)]
pub struct PersistentTargetExecutor {
    conn: SharedTargetConnection,
    source: Option<Rc<PersistentMySqlSource>>,
    insert_conflict_policy: InsertConflictPolicy,
    parallel_writer: Option<SharedParallelTargetWriter>,
}

pub(crate) fn sync_target_opts(target: &TargetMySqlConfig) -> Result<Opts, String> {
    let builder = OptsBuilder::from_opts(target_mysql_opts(target)?);
    Ok(Opts::from(apply_default_mysql_network_bounds(builder)))
}

pub(crate) fn open_stream_source(
    config: &ApplyBinlogConfig,
) -> Result<PersistentMySqlSource, TargetExecuteError> {
    let database =
        config.source.database.clone().ok_or_else(|| {
            TargetExecuteError::new("missing-FK repair requires a source database")
        })?;
    let source = MySqlConnectionConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        database,
    };
    let tls_ca_file =
        (!config.source.tls_ca_file.is_empty()).then_some(config.source.tls_ca_file.as_str());
    PersistentMySqlSource::new_with_tls_ca(&source, tls_ca_file)
        .map_err(|error| TargetExecuteError::new(error.to_string()))
}

impl PersistentMySqlSource {
    pub fn new(config: &MySqlConnectionConfig) -> Result<Self, MySqlSourceError> {
        Self::new_with_tls_ca(config, None)
    }

    pub(crate) fn new_with_opts(opts: Opts) -> Result<Self, MySqlSourceError> {
        let conn = open_conn(opts).map_err(source_connect_error)?;
        Ok(Self {
            conn: RefCell::new(conn),
        })
    }

    pub(crate) fn new_with_tls_ca(
        config: &MySqlConnectionConfig,
        tls_ca_file: Option<&str>,
    ) -> Result<Self, MySqlSourceError> {
        let opts = base_opts(
            &config.host,
            config.port,
            &config.user,
            &config.password,
            &config.database,
            tls_ca_file,
            &format!("source `{}`:{}", config.host, config.port),
        )
        .map_err(MySqlSourceError::new)?;
        Self::new_with_opts(opts)
    }

    pub(crate) fn new_without_operation_timeout(
        config: &MySqlConnectionConfig,
    ) -> Result<Self, MySqlSourceError> {
        let builder = OptsBuilder::default()
            .ip_or_hostname(Some(config.host.clone()))
            .tcp_port(config.port)
            .user(Some(config.user.clone()))
            .pass(Some(config.password.clone()))
            .db_name(Some(config.database.clone()))
            .prefer_socket(false);
        Self::new_with_opts(Opts::from(apply_mysql_connection_liveness(builder)))
    }

    pub(crate) fn query_rows_as_strings(
        &self,
        sql: &str,
    ) -> Result<Vec<Vec<Option<String>>>, MySqlSourceError> {
        let rows = self
            .conn
            .borrow_mut()
            .query::<mysql::Row, _>(sql)
            .map_err(source_query_error)?;
        Ok(rows.into_iter().map(row_to_strings).collect())
    }

    pub(crate) fn read_binlog_coordinate(&self) -> Result<Checkpoint, MySqlSourceError> {
        let rows = self.query_rows_as_strings(binlog_coordinate_query())?;
        parse_binlog_coordinate_checkpoint(rows)
    }
}

fn binlog_coordinate_query() -> &'static str {
    "SHOW MASTER STATUS"
}

fn parse_binlog_coordinate_checkpoint(
    rows: Vec<Vec<Option<String>>>,
) -> Result<Checkpoint, MySqlSourceError> {
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| MySqlSourceError::new("MariaDB binlog coordinate is missing".to_string()))?;
    let source_file = required_binlog_coordinate_value(&row, 0, "file")?;
    let source_position = parse_binlog_coordinate_position(&row)?;
    Ok(Checkpoint {
        source_file,
        source_position,
        gtid: None,
        event_timestamp: 0,
        last_event: LastEvent {
            event_type: "LostBinlogRecoveryCoordinate".to_string(),
            description: "MariaDB current committed binlog coordinate".to_string(),
        },
    })
}

fn parse_binlog_coordinate_position(row: &[Option<String>]) -> Result<u64, MySqlSourceError> {
    required_binlog_coordinate_value(row, 1, "position")?
        .parse::<u64>()
        .map_err(|error| {
            MySqlSourceError::new(format!(
                "invalid MariaDB binlog coordinate position: {error}"
            ))
        })
}

fn required_binlog_coordinate_value(
    row: &[Option<String>],
    index: usize,
    field: &str,
) -> Result<String, MySqlSourceError> {
    row.get(index)
        .and_then(Clone::clone)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            MySqlSourceError::new(format!("MariaDB binlog coordinate {field} is missing"))
        })
}

pub(crate) fn open_initialized_target_connection(opts: Opts) -> Result<Conn, TargetExecuteError> {
    let mut conn = open_conn(opts).map_err(target_connect_error)?;
    conn.query_drop(crate::live::target_session_init_command())
        .map_err(target_query_error)?;
    Ok(conn)
}

fn parallel_initial_checkpoint(config: &ApplyBinlogConfig) -> Checkpoint {
    Checkpoint {
        source_file: config.source.binlog_file.clone(),
        source_position: config.source.start_position,
        gtid: None,
        event_timestamp: 0,
        last_event: LastEvent {
            event_type: "ParallelTargetStart".to_string(),
            description: "source-scoped checkpoint loaded before parallel target dispatch"
                .to_string(),
        },
    }
}

impl PersistentTargetExecutor {
    pub fn new(config: &TargetMySqlConfig) -> Result<Self, TargetExecuteError> {
        Self::new_with_opts(
            target_mysql_opts(config).map_err(TargetExecuteError::new)?,
            config.insert_conflict_policy,
        )
    }

    pub(crate) fn new_for_stream(config: &ApplyBinlogConfig) -> Result<Self, TargetExecuteError> {
        let mut executor = Self::new_with_opts(
            target_mysql_opts(&config.target).map_err(TargetExecuteError::new)?,
            InsertConflictPolicy::Error,
        )?;
        if config.target_parallel_transactions <= 1 {
            executor.source = Some(Rc::new(open_stream_source(config)?));
            return Ok(executor);
        }
        let initial_checkpoint = parallel_initial_checkpoint(config);
        let factory = crate::live::submitted_mysql::MariaDbSubmittedQueryFactory::new(config);
        let writer = crate::live::parallel_writer::ParallelTargetWriter::new(
            config.target_parallel_transactions,
            factory,
            initial_checkpoint,
        )?;
        executor.parallel_writer = Some(Rc::new(RefCell::new(writer)));
        Ok(executor)
    }

    fn new_with_opts(
        opts: Opts,
        insert_conflict_policy: InsertConflictPolicy,
    ) -> Result<Self, TargetExecuteError> {
        let conn = open_initialized_target_connection(opts.clone())?;
        Ok(Self {
            conn: Rc::new(RefCell::new(Some(conn))),
            source: None,
            insert_conflict_policy,
            parallel_writer: None,
        })
    }

    fn parallel_transaction_active(&self) -> bool {
        self.parallel_writer
            .as_ref()
            .is_some_and(|writer| writer.borrow().is_active())
    }

    fn with_parallel_writer<T>(
        &self,
        operation: impl FnOnce(
            &mut crate::live::parallel_writer::ParallelTargetWriter<
                crate::live::submitted_mysql::MariaDbSubmittedQueryFactory,
            >,
        ) -> Result<T, TargetExecuteError>,
    ) -> Option<Result<T, TargetExecuteError>> {
        self.parallel_writer
            .as_ref()
            .map(|writer| operation(&mut writer.borrow_mut()))
    }

    fn wait_for_parallel_transactions(&self) -> Result<(), TargetExecuteError> {
        self.with_parallel_writer(|writer| writer.wait_for_all())
            .unwrap_or(Ok(()))
    }

    fn with_serial_connection<T>(
        &self,
        operation: impl FnOnce(&mut Conn) -> Result<T, TargetExecuteError>,
    ) -> Result<T, TargetExecuteError> {
        self.wait_for_parallel_transactions()?;
        self.with_connection(operation)
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut Conn) -> Result<T, TargetExecuteError>,
    ) -> Result<T, TargetExecuteError> {
        let mut connection = self.conn.borrow_mut();
        let connection = connection
            .as_mut()
            .ok_or_else(|| TargetExecuteError::new("target mysql connection is unavailable"))?;
        operation(connection)
    }

    pub fn read_column_names(&self, table: &str) -> Result<Vec<String>, TargetExecuteError> {
        self.with_connection(|conn| {
            conn.query(build_target_column_select_sql(table))
                .map_err(target_query_error)
        })
    }

    pub(crate) fn query_rows_as_strings(
        &self,
        sql: &str,
    ) -> Result<Vec<Vec<Option<String>>>, TargetExecuteError> {
        let rows = self
            .with_connection(|conn| conn.query::<mysql::Row, _>(sql).map_err(target_query_error))?;
        Ok(rows.into_iter().map(row_to_strings).collect())
    }

    pub(crate) fn execute_raw_sql(&self, sql: &str) -> Result<(), TargetExecuteError> {
        self.with_connection(|conn| conn.query_drop(sql).map_err(target_query_error))
    }
}

struct SerialRowChangeExecutor<'a> {
    target: &'a PersistentTargetExecutor,
}

impl missing_foreign_key::MissingForeignKeyRepairExecutor for SerialRowChangeExecutor<'_> {
    fn execute_row_change_statement(
        &mut self,
        change: &TargetRowChange,
    ) -> Result<(), TargetExecuteError> {
        self.target.execute_statement(&change.statement)
    }

    fn load_missing_foreign_key_parent(
        &mut self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<missing_foreign_key::MissingForeignKeyParent, TargetExecuteError> {
        self.target.load_missing_foreign_key_parent(change, error)
    }

    fn load_duplicate_parent_reconciliation(
        &mut self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<missing_foreign_key::DuplicateParentReconciliation, TargetExecuteError> {
        self.target
            .load_duplicate_parent_reconciliation(change, error)
    }

    fn verify_duplicate_parent_reconciliation(
        &mut self,
        change: &TargetRowChange,
        reconciliation: &missing_foreign_key::DuplicateParentReconciliation,
    ) -> Result<(), TargetExecuteError> {
        self.target
            .verify_duplicate_parent_reconciliation(change, reconciliation)
    }
}

impl TargetExecutor for PersistentTargetExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        let result = self.execute_statement(statement);
        match result {
            Ok(()) => Ok(()),
            Err(error) => self.retry_or_return_error(statement, error),
        }
    }

    fn execute_row_change(&self, change: &TargetRowChange) -> Result<(), TargetExecuteError> {
        if self.parallel_transaction_active() {
            return self
                .with_parallel_writer(|writer| writer.execute_row_change(change))
                .expect("parallel writer exists while its transaction is active");
        }
        let mut executor = SerialRowChangeExecutor { target: self };
        missing_foreign_key::execute_row_change_with_missing_foreign_key_repair(
            &mut executor,
            change,
        )
    }
}

impl TransactionalTargetExecutor for PersistentTargetExecutor {
    fn acquire_stream_lease(&self, lease_name: &str) -> Result<(), TargetExecuteError> {
        let acquired = self.with_connection(|conn| {
            conn.query_first::<u8, _>(build_stream_lease_sql(lease_name))
                .map_err(target_query_error)
        })?;
        ensure_stream_lease_acquired(lease_name, acquired)
    }

    fn begin_stream_transaction(&self) -> Result<(), TargetExecuteError> {
        self.with_parallel_writer(|writer| writer.begin())
            .unwrap_or_else(|| self.begin_transaction())
    }

    fn begin_transaction(&self) -> Result<(), TargetExecuteError> {
        self.wait_for_parallel_transactions()?;
        self.execute_transaction_control("BEGIN")
    }

    fn load_transaction_checkpoint_for_update(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<Option<Checkpoint>, TargetExecuteError> {
        if self.parallel_transaction_active() {
            return self
                .with_parallel_writer(|writer| Ok(Some(writer.logical_checkpoint())))
                .expect("parallel writer exists while its transaction is active");
        }
        let sql = crate::stream_checkpoint::build_checkpoint_select_for_update_sql(
            checkpoint_table,
            checkpoint_name,
        );
        let checkpoint_json = self.with_serial_connection(|conn| {
            conn.query_first::<String, _>(sql)
                .map_err(target_query_error)
        })?;
        checkpoint_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|error| {
                    TargetExecuteError::new(format!(
                        "invalid locked stream checkpoint JSON: {error}"
                    ))
                })
            })
            .transpose()
    }

    fn save_transaction_checkpoint(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), TargetExecuteError> {
        if self.parallel_transaction_active() {
            return self
                .with_parallel_writer(|writer| {
                    writer.save_checkpoint(checkpoint_table, checkpoint_name, checkpoint)
                })
                .expect("parallel writer exists while its transaction is active");
        }
        let sql = crate::stream_checkpoint::build_checkpoint_upsert_sql_for_checkpoint(
            checkpoint_table,
            checkpoint_name,
            checkpoint,
        )
        .map_err(TargetExecuteError::new)?;
        self.with_serial_connection(|conn| conn.query_drop(sql).map_err(target_query_error))
    }

    fn commit_transaction(&self) -> Result<(), TargetExecuteError> {
        if self.parallel_transaction_active() {
            return self
                .with_parallel_writer(|writer| writer.commit())
                .expect("parallel writer exists while its transaction is active");
        }
        self.execute_transaction_control("COMMIT")
    }

    fn rollback_transaction(&self) -> Result<(), TargetExecuteError> {
        if self.parallel_transaction_active() {
            return self
                .with_parallel_writer(|writer| writer.rollback())
                .expect("parallel writer exists while its transaction is active");
        }
        self.execute_transaction_control("ROLLBACK")
    }

    fn flush_pending_transactions(&self) -> Result<(), TargetExecuteError> {
        self.wait_for_parallel_transactions()
    }

    fn take_committed_checkpoints(&self) -> Result<Vec<Checkpoint>, TargetExecuteError> {
        self.with_parallel_writer(|writer| writer.take_committed_checkpoints())
            .unwrap_or(Ok(Vec::new()))
    }

    fn uses_parallel_transactions(&self) -> bool {
        self.parallel_writer.is_some()
    }
}

impl PersistentTargetExecutor {
    fn execute_transaction_control(&self, sql: &str) -> Result<(), TargetExecuteError> {
        self.with_connection(|conn| conn.query_drop(sql).map_err(target_query_error))
    }

    fn execute_statement(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        if self.parallel_transaction_active() {
            return self
                .with_parallel_writer(|writer| writer.execute(statement))
                .expect("parallel writer exists while its transaction is active");
        }
        let params = statement.params.clone();
        self.with_serial_connection(|conn| {
            conn.exec_drop(&statement.sql, Params::Positional(params))
                .map_err(target_query_error)
        })
    }

    fn retry_or_return_error(
        &self,
        statement: &SqlStatement,
        error: TargetExecuteError,
    ) -> Result<(), TargetExecuteError> {
        if self.can_ignore_duplicate_insert(&statement.sql, &error.to_string()) {
            return Ok(());
        }
        self.retry_generated_column_or_return_error(statement, error)
    }

    fn retry_generated_column_or_return_error(
        &self,
        statement: &SqlStatement,
        error: TargetExecuteError,
    ) -> Result<(), TargetExecuteError> {
        let Some(retry_sql) = generated_column_retry_sql(statement, &error.to_string()) else {
            return Err(error);
        };
        self.with_serial_connection(|conn| conn.query_drop(retry_sql).map_err(target_query_error))
    }

    fn can_ignore_duplicate_insert(&self, sql: &str, error: &str) -> bool {
        should_ignore_duplicate_insert(self.insert_conflict_policy, sql, error)
    }
}
