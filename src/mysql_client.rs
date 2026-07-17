use crate::checkpoint::Checkpoint;
use crate::live::{InsertConflictPolicy, TargetMySqlConfig, should_ignore_duplicate_insert};
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::target_mysql_opts;
use crate::snapshot::{ChunkRequest, SnapshotError, SnapshotProgress, SnapshotRow, SnapshotSource};
use crate::table_sync::progress::{
    build_add_total_rows_column_sql, build_create_progress_schema_sql,
    build_create_progress_table_sql, build_progress_upsert_sql,
};
use crate::table_sync::{SyncTableProgress, TableSyncError};
use crate::target::{
    DuplicateConflict, SqlStatement, TargetExecuteError, TargetExecutionOutcome, TargetExecutor,
    TransactionalTargetExecutor,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, Params};
use std::cell::RefCell;
use std::rc::Rc;

pub(crate) type SharedTargetConnection = Rc<RefCell<Conn>>;

mod connection;
mod query;
#[cfg(test)]
mod tests;

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

pub struct PersistentTargetExecutor {
    conn: SharedTargetConnection,
    insert_conflict_policy: InsertConflictPolicy,
}

pub struct PersistentProgressWriter {
    conn: RefCell<Conn>,
    default_database: String,
    progress_table: String,
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

impl PersistentTargetExecutor {
    pub fn new(config: &TargetMySqlConfig) -> Result<Self, TargetExecuteError> {
        let opts = target_mysql_opts(config).map_err(TargetExecuteError::new)?;
        let mut conn = open_conn(opts).map_err(target_connect_error)?;
        conn.query_drop(crate::live::target_session_init_command())
            .map_err(target_query_error)?;
        Ok(Self {
            conn: Rc::new(RefCell::new(conn)),
            insert_conflict_policy: config.insert_conflict_policy,
        })
    }

    pub fn read_column_names(&self, table: &str) -> Result<Vec<String>, TargetExecuteError> {
        self.conn
            .borrow_mut()
            .query(build_target_column_select_sql(table))
            .map_err(target_query_error)
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

    fn execute_row_change(
        &self,
        statement: &SqlStatement,
    ) -> Result<TargetExecutionOutcome, TargetExecuteError> {
        match self.execute_statement(statement) {
            Ok(()) => Ok(TargetExecutionOutcome::Applied),
            Err(error) if error.mysql_code() == Some(1062) => Ok(
                TargetExecutionOutcome::DuplicateIgnored(DuplicateConflict {
                    error_code: 1062,
                    error_text: error.to_string(),
                    duplicate_index: crate::target::duplicate_index_from_error(&error.to_string()),
                }),
            ),
            Err(error) => {
                if let Some(conflict) = constraint_conflict_from_error(&error) {
                    return Ok(TargetExecutionOutcome::ConstraintConflict(conflict));
                }
                self.retry_or_return_error(statement, error)?;
                Ok(TargetExecutionOutcome::Applied)
            }
        }
    }
}

fn constraint_conflict_from_error(error: &TargetExecuteError) -> Option<DuplicateConflict> {
    let code = error.mysql_code()?;
    if !matches!(code, 1048 | 1451 | 1452 | 3819 | 4025) {
        return None;
    }
    Some(DuplicateConflict {
        error_code: code,
        error_text: error.to_string(),
        duplicate_index: None,
    })
}

impl TransactionalTargetExecutor for PersistentTargetExecutor {
    fn acquire_stream_lease(&self, lease_name: &str) -> Result<(), TargetExecuteError> {
        let acquired = self
            .conn
            .borrow_mut()
            .query_first::<u8, _>(build_stream_lease_sql(lease_name))
            .map_err(target_query_error)?;
        ensure_stream_lease_acquired(lease_name, acquired)
    }

    fn begin_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("BEGIN")
    }

    fn load_transaction_checkpoint_for_update(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<Option<Checkpoint>, TargetExecuteError> {
        let sql = crate::stream_checkpoint::build_checkpoint_select_for_update_sql(
            checkpoint_table,
            checkpoint_name,
        );
        let checkpoint_json = self
            .conn
            .borrow_mut()
            .query_first::<String, _>(sql)
            .map_err(target_query_error)?;
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
        let sql = crate::stream_checkpoint::build_checkpoint_upsert_sql_for_checkpoint(
            checkpoint_table,
            checkpoint_name,
            checkpoint,
        )
        .map_err(TargetExecuteError::new)?;
        self.conn
            .borrow_mut()
            .query_drop(sql)
            .map_err(target_query_error)
    }

    fn commit_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("COMMIT")
    }

    fn rollback_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("ROLLBACK")
    }
}

impl PersistentTargetExecutor {
    fn execute_transaction_control(&self, sql: &str) -> Result<(), TargetExecuteError> {
        self.conn
            .borrow_mut()
            .query_drop(sql)
            .map_err(target_query_error)
    }

    fn execute_statement(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        let params = statement.params.clone();
        self.conn
            .borrow_mut()
            .exec_drop(&statement.sql, Params::Positional(params))
            .map_err(target_query_error)
    }

    fn retry_or_return_error(
        &self,
        statement: &SqlStatement,
        error: TargetExecuteError,
    ) -> Result<(), TargetExecuteError> {
        if self.can_ignore_duplicate_insert(&statement.sql, &error.to_string()) {
            return Ok(());
        }
        let Some(retry_sql) = generated_column_retry_sql(statement, &error.to_string()) else {
            return Err(error);
        };
        self.conn
            .borrow_mut()
            .query_drop(retry_sql)
            .map_err(target_query_error)
    }

    fn can_ignore_duplicate_insert(&self, sql: &str, error: &str) -> bool {
        should_ignore_duplicate_insert(self.insert_conflict_policy, sql, error)
    }
}

impl PersistentProgressWriter {
    pub fn new(config: &TargetMySqlConfig, progress_table: String) -> Result<Self, TableSyncError> {
        let opts = target_mysql_opts(config).map_err(TableSyncError::Progress)?;
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
