use crate::checkpoint::{Checkpoint, LastEvent};
use crate::live::{
    ApplyBinlogConfig, InsertConflictPolicy, TargetMySqlConfig, should_ignore_duplicate_insert,
};
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::{
    apply_default_mysql_network_bounds, apply_mysql_connection_liveness, target_mysql_opts,
};
use crate::snapshot::{ChunkRequest, SnapshotError, SnapshotProgress, SnapshotRow, SnapshotSource};
use crate::table_sync::progress::{
    build_add_total_rows_column_sql, build_create_progress_schema_sql,
    build_create_progress_table_sql, build_progress_upsert_sql,
};
use crate::table_sync::{SyncTableProgress, TableSyncError};
use crate::target::{
    SqlStatement, TargetExecuteError, TargetExecutor, TargetRowChange, TargetRowChangeKind,
    TransactionalTargetExecutor,
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
mod query;
#[cfg(test)]
mod tests;

#[cfg(test)]
use connection::{NetworkTimeouts, apply_network_timeouts};
use connection::{
    base_opts, open_conn, progress_connect_error, snapshot_connect_error, target_connect_error,
};
pub(crate) use query::value_to_string;
use query::{
    build_progress_error_message_sql, build_snapshot_boundary_select_sql,
    build_snapshot_progress_select_sql, build_stream_lease_sql, build_target_column_select_sql,
    ensure_stream_lease_acquired, generated_column_retry_sql, progress_query_error, row_to_strings,
    rows_to_tsv, snapshot_boundary_offsets, snapshot_progress_from_rows, snapshot_query_error,
    snapshot_row_from_mysql_row, target_query_error,
};

pub struct PersistentMySqlSource {
    conn: RefCell<Conn>,
}

#[derive(Clone)]
pub struct PersistentTargetExecutor {
    conn: SharedTargetConnection,
    insert_conflict_policy: InsertConflictPolicy,
    parallel_writer: Option<SharedParallelTargetWriter>,
}

pub struct PersistentProgressWriter {
    conn: RefCell<Conn>,
    default_database: String,
    progress_table: String,
}

pub(crate) fn sync_target_opts(target: &TargetMySqlConfig) -> Result<Opts, String> {
    let builder = OptsBuilder::from_opts(target_mysql_opts(target)?);
    Ok(Opts::from(apply_default_mysql_network_bounds(builder)))
}

pub(crate) fn target_reader_opts(target: &TargetMySqlConfig) -> Result<Opts, String> {
    base_opts(
        &target.host,
        target.port,
        &target.user,
        &target.password,
        &target.database,
        Some(&target.tls_ca_file),
        &format!("target `{}`:{}", target.host, target.port),
    )
}

impl PersistentMySqlSource {
    pub fn new(config: &MySqlConnectionConfig) -> Result<Self, SnapshotError> {
        Self::new_with_tls_ca(config, None)
    }

    pub(crate) fn new_with_opts(opts: Opts) -> Result<Self, SnapshotError> {
        let conn = open_conn(opts).map_err(snapshot_connect_error)?;
        Ok(Self {
            conn: RefCell::new(conn),
        })
    }

    pub(crate) fn new_with_tls_ca(
        config: &MySqlConnectionConfig,
        tls_ca_file: Option<&str>,
    ) -> Result<Self, SnapshotError> {
        let opts = base_opts(
            &config.host,
            config.port,
            &config.user,
            &config.password,
            &config.database,
            tls_ca_file,
            &format!("source `{}`:{}", config.host, config.port),
        )
        .map_err(SnapshotError::InvalidTable)?;
        Self::new_with_opts(opts)
    }

    pub(crate) fn new_without_operation_timeout(
        config: &MySqlConnectionConfig,
    ) -> Result<Self, SnapshotError> {
        let builder = OptsBuilder::default()
            .ip_or_hostname(Some(config.host.clone()))
            .tcp_port(config.port)
            .user(Some(config.user.clone()))
            .pass(Some(config.password.clone()))
            .db_name(Some(config.database.clone()))
            .prefer_socket(false);
        Self::new_with_opts(Opts::from(apply_mysql_connection_liveness(builder)))
    }

    pub fn count_rows(&self, table: &str) -> Result<u64, SnapshotError> {
        let sql = format!(
            "SELECT COUNT(*) FROM {}",
            crate::mysql_support::quote_ident(table)
        );
        self.conn
            .borrow_mut()
            .query_first::<u64, _>(sql)
            .map_err(snapshot_query_error)?
            .ok_or_else(|| SnapshotError::InvalidTable(format!("{table} row count was empty")))
    }

    pub fn read_range_boundaries(
        &self,
        table: &crate::snapshot::SnapshotTable,
        workers: usize,
        total_rows: u64,
    ) -> Result<Vec<Vec<String>>, SnapshotError> {
        snapshot_boundary_offsets(total_rows, workers)
            .into_iter()
            .map(|offset| self.read_range_boundary(table, offset))
            .collect()
    }

    fn read_range_boundary(
        &self,
        table: &crate::snapshot::SnapshotTable,
        offset: u64,
    ) -> Result<Vec<String>, SnapshotError> {
        let sql = build_snapshot_boundary_select_sql(table, offset);
        let row = self
            .conn
            .borrow_mut()
            .query_first::<mysql::Row, _>(sql)
            .map_err(snapshot_query_error)?
            .ok_or_else(|| {
                SnapshotError::InvalidTable(format!("{} boundary was empty", table.name))
            })?;
        row.unwrap()
            .into_iter()
            .map(value_to_string)
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| {
                    SnapshotError::InvalidTable(format!(
                        "primary-key column {} was NULL",
                        table.primary_key[index]
                    ))
                })
            })
            .collect()
    }

    pub fn read_create_table(&self, table: &str) -> Result<String, SnapshotError> {
        let sql = format!(
            "SHOW CREATE TABLE {}",
            crate::mysql_support::quote_ident(table)
        );
        self.conn
            .borrow_mut()
            .query_first::<(String, String), _>(sql)
            .map_err(snapshot_query_error)?
            .map(|(_, ddl)| ddl)
            .ok_or_else(|| SnapshotError::InvalidTable(format!("{table} DDL was empty")))
    }

    pub(crate) fn query_rows_as_strings(
        &self,
        sql: &str,
    ) -> Result<Vec<Vec<Option<String>>>, SnapshotError> {
        let rows = self
            .conn
            .borrow_mut()
            .query::<mysql::Row, _>(sql)
            .map_err(snapshot_query_error)?;
        Ok(rows.into_iter().map(row_to_strings).collect())
    }

    pub(crate) fn read_binlog_coordinate(&self) -> Result<Checkpoint, SnapshotError> {
        let rows = self.query_rows_as_strings(binlog_coordinate_query())?;
        parse_binlog_coordinate_checkpoint(rows)
    }
}

fn binlog_coordinate_query() -> &'static str {
    "SHOW MASTER STATUS"
}

fn parse_binlog_coordinate_checkpoint(
    rows: Vec<Vec<Option<String>>>,
) -> Result<Checkpoint, SnapshotError> {
    let row = rows.into_iter().next().ok_or_else(|| {
        SnapshotError::InvalidTable("MariaDB binlog coordinate is missing".to_string())
    })?;
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

fn parse_binlog_coordinate_position(row: &[Option<String>]) -> Result<u64, SnapshotError> {
    required_binlog_coordinate_value(row, 1, "position")?
        .parse::<u64>()
        .map_err(|error| {
            SnapshotError::InvalidTable(format!(
                "invalid MariaDB binlog coordinate position: {error}"
            ))
        })
}

fn required_binlog_coordinate_value(
    row: &[Option<String>],
    index: usize,
    field: &str,
) -> Result<String, SnapshotError> {
    row.get(index)
        .and_then(Clone::clone)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SnapshotError::InvalidTable(format!("MariaDB binlog coordinate {field} is missing"))
        })
}

impl SnapshotSource for PersistentMySqlSource {
    fn read_chunk(&self, request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError> {
        let sql = crate::mysql_snapshot::build_select_chunk_sql(request);
        let rows = self
            .conn
            .borrow_mut()
            .query::<mysql::Row, _>(sql)
            .map_err(snapshot_query_error)?;

        rows.into_iter()
            .map(|row| snapshot_row_from_mysql_row(request, row))
            .collect()
    }
}

fn open_initialized_target_connection(opts: Opts) -> Result<Conn, TargetExecuteError> {
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

    pub(crate) fn new_for_sync(config: &TargetMySqlConfig) -> Result<Self, TargetExecuteError> {
        Self::new_with_opts(
            sync_target_opts(config).map_err(TargetExecuteError::new)?,
            config.insert_conflict_policy,
        )
    }

    pub(crate) fn new_for_stream(config: &ApplyBinlogConfig) -> Result<Self, TargetExecuteError> {
        let mut executor = Self::new_with_opts(
            target_mysql_opts(&config.target).map_err(TargetExecuteError::new)?,
            InsertConflictPolicy::Error,
        )?;
        if config.target_parallel_transactions <= 1 {
            return Ok(executor);
        }
        let initial_checkpoint = parallel_initial_checkpoint(config);
        let factory =
            crate::live::submitted_mysql::MariaDbSubmittedQueryFactory::new(&config.target);
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

    pub(crate) fn begin_sync_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("BEGIN")
    }

    pub(crate) fn commit_sync_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("COMMIT")
    }

    pub(crate) fn rollback_sync_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("ROLLBACK")
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
        live_row_change_result(change.kind, self.execute_statement(&change.statement))
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

fn live_row_change_result(
    kind: TargetRowChangeKind,
    result: Result<(), TargetExecuteError>,
) -> Result<(), TargetExecuteError> {
    match result {
        Err(error) if error.mysql_code() == Some(1062) && kind == TargetRowChangeKind::Insert => {
            Ok(())
        }
        result => result,
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

impl PersistentProgressWriter {
    pub fn new(config: &TargetMySqlConfig, progress_table: String) -> Result<Self, TableSyncError> {
        let opts = sync_target_opts(config).map_err(TableSyncError::Progress)?;
        let mut conn = open_conn(opts).map_err(progress_connect_error)?;
        conn.query_drop(crate::live::target_session_init_command())
            .map_err(progress_query_error)?;
        Ok(Self {
            conn: RefCell::new(conn),
            default_database: config.database.clone(),
            progress_table,
        })
    }

    pub fn ensure(&self) -> Result<(), TableSyncError> {
        if let Some(sql) = build_create_progress_schema_sql(&self.progress_table) {
            self.execute_progress_sql(sql)?;
        }
        self.execute_progress_sql(build_create_progress_table_sql(&self.progress_table))?;
        self.execute_progress_sql(build_add_total_rows_column_sql(
            &self.default_database,
            &self.progress_table,
        ))
    }

    pub fn save(&self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.execute_progress_sql(build_progress_upsert_sql(&self.progress_table, progress))
    }

    pub fn save_error_message(&self, table: &str, error: &str) -> Result<(), TableSyncError> {
        self.execute_progress_sql(build_progress_error_message_sql(
            &self.progress_table,
            table,
            error,
        ))
    }

    pub fn load_snapshot_progress(&self) -> Result<SnapshotProgress, TableSyncError> {
        let sql = build_snapshot_progress_select_sql(&self.progress_table);
        let rows = self
            .conn
            .borrow_mut()
            .query::<(String, String, u64, String), _>(sql)
            .map_err(progress_query_error)?;
        snapshot_progress_from_rows(rows)
    }

    pub fn execute_table_sync_progress_sql(&self, sql: String) -> Result<(), TableSyncError> {
        self.execute_progress_sql(sql)
    }

    pub fn query_table_sync_progress_tsv(&self, sql: String) -> Result<String, TableSyncError> {
        let rows = self
            .conn
            .borrow_mut()
            .query::<mysql::Row, _>(sql)
            .map_err(progress_query_error)?;
        Ok(rows_to_tsv(rows))
    }

    fn execute_progress_sql(&self, sql: String) -> Result<(), TableSyncError> {
        self.conn
            .borrow_mut()
            .query_drop(sql)
            .map_err(progress_query_error)
    }
}
