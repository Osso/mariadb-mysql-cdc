use super::model::{
    SyncChunkProgress, SyncChunkProgressStore, SyncChunkReadRequest, SyncChunkSource,
    SyncChunkTargetSession, SyncProgressRow, SyncProgressStatus, SyncRunProgressStore, SyncStage,
    SyncTable,
};
use super::progress::{
    build_create_sync_progress_schema_sql, build_create_sync_progress_table_sql,
    build_sync_progress_select_sql, build_sync_progress_upsert_sql, parse_sync_progress_row,
};
use super::sql::{
    build_lock_table_write_sql, build_strict_delete_rows_statement, build_strict_insert_statement,
    build_strict_update_rows_statement, build_sync_select_sql,
};
use crate::database_row::DatabaseRow;
use crate::live::TargetMySqlConfig;
use crate::mysql_client::{
    PersistentMySqlSource, sync_source_opts, sync_target_opts, value_to_string,
};
use crate::mysql_config::MySqlConnectionConfig;
use crate::target::SqlStatement;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, Params};
use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MYSQL_MAX_PREPARED_STATEMENT_PLACEHOLDERS: usize = 65_535;
const MAX_SYNC_MUTATION_ROWS_PER_STATEMENT: usize = 128;
const MAX_SYNC_CONNECTION_RETRIES: u32 = 4;
const INITIAL_SYNC_CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(100);

pub(crate) struct MySqlSyncSource {
    source: PersistentMySqlSource,
    table: SyncTable,
}

pub(crate) struct MySqlSyncTargetSession {
    conn: Conn,
    database: String,
    table: SyncTable,
}

pub(crate) struct MySqlSyncProgressStore {
    conn: Conn,
    progress_table: String,
}

fn open_sync_connection(opts: Opts) -> mysql::Result<Conn> {
    retry_sync_connection_construction(
        || Conn::new(opts.clone()),
        thread::sleep,
        sample_connection_retry_jitter,
    )
}

pub(crate) fn retry_sync_connection_construction<T, C, S, J>(
    mut connect: C,
    mut sleep: S,
    mut sample_jitter: J,
) -> mysql::Result<T>
where
    C: FnMut() -> mysql::Result<T>,
    S: FnMut(Duration),
    J: FnMut(Duration) -> Duration,
{
    let mut retry = 0;
    loop {
        match connect() {
            Ok(connection) => return Ok(connection),
            Err(error) if error.is_connectivity_error() && retry < MAX_SYNC_CONNECTION_RETRIES => {
                let base_delay = INITIAL_SYNC_CONNECTION_RETRY_DELAY.saturating_mul(1 << retry);
                let maximum_jitter = base_delay / 2;
                let jitter = sample_jitter(base_delay).min(maximum_jitter);
                sleep(base_delay.saturating_add(jitter));
                retry += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

fn sample_connection_retry_jitter(base_delay: Duration) -> Duration {
    let maximum_jitter = base_delay / 2;
    let maximum_nanos = maximum_jitter.as_nanos();
    if maximum_nanos == 0 {
        return Duration::ZERO;
    }

    let clock_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let entropy = clock_nanos ^ u128::from(std::process::id());
    let jitter_nanos = entropy % (maximum_nanos + 1);
    Duration::from_nanos(jitter_nanos as u64)
}

impl MySqlSyncSource {
    pub(crate) fn new(config: &MySqlConnectionConfig, table: SyncTable) -> Result<Self, String> {
        let opts = sync_source_opts(config)?;
        let conn = open_sync_connection(opts)
            .map_err(|error| format!("failed to connect to source mysql: {error}"))?;
        let source = PersistentMySqlSource::from_sync_connection(conn);
        Ok(Self { source, table })
    }
}

impl SyncChunkSource for MySqlSyncSource {
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<DatabaseRow>, String> {
        let sql = build_sync_select_sql(&self.table, request);
        let rows = self
            .source
            .query_rows_as_strings(&sql)
            .map_err(|error| error.to_string())?;
        decode_sync_rows(&self.table, rows)
    }
}

impl MySqlSyncTargetSession {
    pub(crate) fn new(config: &TargetMySqlConfig, table: SyncTable) -> Result<Self, String> {
        let opts = sync_target_opts(config)?;
        let mut conn = open_sync_connection(opts)
            .map_err(|error| format!("failed to connect to target mysql: {error}"))?;
        initialize_target_session(&mut conn)?;
        Ok(Self {
            conn,
            database: config.database.clone(),
            table,
        })
    }

    fn execute_statement(&mut self, statement: SqlStatement) -> Result<(), String> {
        self.conn
            .exec_drop(&statement.sql, Params::Positional(statement.params))
            .map_err(|error| format!("target mysql statement failed: {error}"))
    }

    fn execute_control(&mut self, sql: &str) -> Result<(), String> {
        self.conn
            .query_drop(sql)
            .map_err(|error| format!("target mysql session command `{sql}` failed: {error}"))
    }

    fn query_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<DatabaseRow>, String> {
        let sql = build_sync_select_sql(&self.table, request);
        let rows = query_rows_as_strings(&mut self.conn, &sql, "target")?;
        decode_sync_rows(&self.table, rows)
    }
}

impl SyncChunkTargetSession for MySqlSyncTargetSession {
    fn set_autocommit(&mut self, enabled: bool) -> Result<(), String> {
        let value = if enabled { 1 } else { 0 };
        self.execute_control(&format!("SET autocommit={value}"))
    }

    fn lock_table_write(&mut self, database: &str, table: &str) -> Result<(), String> {
        validate_sync_target_lock_identity(&self.database, &self.table.name, database, table)?;
        let sql = build_lock_table_write_sql(&self.database, &self.table.name);
        self.execute_control(&sql)
    }

    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<DatabaseRow>, String> {
        self.query_rows(request)
    }

    fn delete_rows(&mut self, primary_keys: &[Vec<String>]) -> Result<(), String> {
        for statement in build_strict_delete_batches(&self.table, primary_keys) {
            self.execute_statement(statement)?;
        }
        Ok(())
    }

    fn update_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), String> {
        for statement in build_strict_update_batches(&self.table, rows) {
            self.execute_statement(statement)?;
        }
        Ok(())
    }

    fn insert_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), String> {
        for statement in build_strict_insert_batches(&self.table, rows) {
            self.execute_statement(statement)?;
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<(), String> {
        self.execute_control("COMMIT")
    }

    fn rollback(&mut self) -> Result<(), String> {
        self.execute_control("ROLLBACK")
    }

    fn unlock_tables(&mut self) -> Result<(), String> {
        self.execute_control("UNLOCK TABLES")
    }
}

impl MySqlSyncProgressStore {
    pub(crate) fn new(config: &TargetMySqlConfig, progress_table: String) -> Result<Self, String> {
        let opts = sync_target_opts(config)?;
        let mut conn = open_sync_connection(opts)
            .map_err(|error| format!("failed to connect to sync progress mysql: {error}"))?;
        initialize_target_session(&mut conn)?;
        let mut store = Self {
            conn,
            progress_table,
        };
        store.ensure()?;
        Ok(store)
    }

    fn ensure(&mut self) -> Result<(), String> {
        if let Some(sql) = build_create_sync_progress_schema_sql(&self.progress_table) {
            self.execute_progress_sql(&sql)?;
        }
        self.execute_progress_sql(&build_create_sync_progress_table_sql(&self.progress_table))
    }

    fn execute_progress_sql(&mut self, sql: &str) -> Result<(), String> {
        self.conn
            .query_drop(sql)
            .map_err(|error| format!("sync progress mysql command failed: {error}"))
    }

    fn execute_progress_statement(&mut self, statement: SqlStatement) -> Result<(), String> {
        self.conn
            .exec_drop(&statement.sql, Params::Positional(statement.params))
            .map_err(|error| format!("sync progress mysql statement failed: {error}"))
    }
}

impl SyncChunkProgressStore for MySqlSyncProgressStore {
    fn load(&mut self, run_id: &str, table: &str) -> Result<Option<SyncChunkProgress>, String> {
        self.load_stage(run_id, SyncStage::Rows, table)?
            .map(sync_chunk_progress_from_row)
            .transpose()
    }

    fn save(&mut self, progress: &SyncChunkProgress) -> Result<(), String> {
        self.save_stage(&sync_progress_row_from_chunk(progress))
    }
}

impl SyncRunProgressStore for MySqlSyncProgressStore {
    fn load_stage(
        &mut self,
        run_id: &str,
        stage: SyncStage,
        table_name: &str,
    ) -> Result<Option<SyncProgressRow>, String> {
        let sql = build_sync_progress_select_sql(&self.progress_table, run_id, stage, table_name);
        let rows = self
            .conn
            .query::<mysql::Row, _>(sql)
            .map_err(|error| format!("read sync progress row: {error}"))?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        parse_sync_progress_row(&mysql_row_to_tsv(row)).map(Some)
    }

    fn save_stage(&mut self, row: &SyncProgressRow) -> Result<(), String> {
        let statement = build_sync_progress_upsert_sql(&self.progress_table, row);
        self.execute_progress_statement(statement)
    }
}

pub(crate) fn decode_sync_rows(
    table: &SyncTable,
    rows: Vec<Vec<Option<String>>>,
) -> Result<Vec<DatabaseRow>, String> {
    rows.into_iter()
        .map(|fields| decode_sync_row(table, fields))
        .collect()
}

fn decode_sync_row(table: &SyncTable, fields: Vec<Option<String>>) -> Result<DatabaseRow, String> {
    if fields.len() != table.columns.len() {
        return Err(format!(
            "sync row has {} fields for {} selected columns",
            fields.len(),
            table.columns.len()
        ));
    }
    let values = table
        .columns
        .iter()
        .cloned()
        .zip(fields)
        .collect::<BTreeMap<_, _>>();
    let primary_key = table
        .primary_key
        .iter()
        .map(|column| required_primary_key_value(column, &values))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DatabaseRow {
        primary_key,
        values,
    })
}

fn required_primary_key_value(
    column: &str,
    values: &BTreeMap<String, Option<String>>,
) -> Result<String, String> {
    let value = values
        .get(column)
        .ok_or_else(|| format!("primary-key column `{column}` was not selected"))?;
    value
        .clone()
        .ok_or_else(|| format!("primary-key column `{column}` was NULL"))
}

pub(crate) fn strict_insert_batch_capacity(table: &SyncTable) -> usize {
    bounded_mutation_capacity(table.columns.len())
}

pub(crate) fn strict_update_batch_capacity(table: &SyncTable) -> usize {
    let changed_column_count = table.columns.len().saturating_sub(table.primary_key.len());
    let placeholders_per_row = changed_column_count
        .saturating_mul(table.primary_key.len().saturating_add(1))
        .saturating_add(table.primary_key.len());
    bounded_mutation_capacity(placeholders_per_row)
}

pub(crate) fn strict_delete_batch_capacity(table: &SyncTable) -> usize {
    bounded_mutation_capacity(table.primary_key.len())
}

fn bounded_mutation_capacity(placeholders_per_row: usize) -> usize {
    let capacity = MYSQL_MAX_PREPARED_STATEMENT_PLACEHOLDERS / placeholders_per_row.max(1);
    capacity.clamp(1, MAX_SYNC_MUTATION_ROWS_PER_STATEMENT)
}

pub(crate) fn build_strict_insert_batches(
    table: &SyncTable,
    rows: &[DatabaseRow],
) -> Vec<SqlStatement> {
    rows.chunks(strict_insert_batch_capacity(table))
        .map(|batch| build_strict_insert_statement(table, batch))
        .collect()
}

pub(crate) fn build_strict_update_batches(
    table: &SyncTable,
    rows: &[DatabaseRow],
) -> Vec<SqlStatement> {
    rows.chunks(strict_update_batch_capacity(table))
        .map(|batch| build_strict_update_rows_statement(table, batch))
        .collect()
}

pub(crate) fn build_strict_delete_batches(
    table: &SyncTable,
    primary_keys: &[Vec<String>],
) -> Vec<SqlStatement> {
    primary_keys
        .chunks(strict_delete_batch_capacity(table))
        .map(|batch| build_strict_delete_rows_statement(table, batch))
        .collect()
}

pub(crate) fn validate_sync_target_lock_identity(
    expected_database: &str,
    expected_table: &str,
    database: &str,
    table: &str,
) -> Result<(), String> {
    if database == expected_database && table == expected_table {
        return Ok(());
    }
    Err(format!(
        "sync target lock identity mismatch: expected `{expected_database}`.`{expected_table}`, found `{database}`.`{table}`"
    ))
}

pub(crate) fn sync_progress_row_from_chunk(progress: &SyncChunkProgress) -> SyncProgressRow {
    SyncProgressRow {
        run_id: progress.run_id.clone(),
        stage: SyncStage::Rows,
        table_name: progress.table.clone(),
        run_spec_json: progress.run_spec_json.clone(),
        last_primary_key: progress.last_primary_key.clone(),
        chunks: progress.chunks,
        rows_scanned: progress.rows_scanned,
        inserts: progress.inserts,
        updates: progress.updates,
        deletes: progress.deletes,
        status: if progress.complete {
            SyncProgressStatus::Complete
        } else {
            SyncProgressStatus::Running
        },
        last_error: None,
        created_at: String::new(),
        updated_at: String::new(),
        completed_at: None,
    }
}

pub(crate) fn sync_chunk_progress_from_row(
    progress: SyncProgressRow,
) -> Result<SyncChunkProgress, String> {
    if progress.stage != SyncStage::Rows {
        return Err(format!(
            "sync chunk progress requires `rows` stage, found `{}`",
            progress.stage.as_str()
        ));
    }
    let complete = match progress.status {
        SyncProgressStatus::Running => false,
        SyncProgressStatus::Complete => true,
        SyncProgressStatus::Error => {
            let message = progress
                .last_error
                .as_deref()
                .unwrap_or("unspecified sync progress error");
            return Err(format!(
                "sync progress for run `{}` table `{}` is in error: {message}",
                progress.run_id, progress.table_name
            ));
        }
    };
    Ok(SyncChunkProgress {
        run_id: progress.run_id,
        table: progress.table_name,
        run_spec_json: progress.run_spec_json,
        last_primary_key: progress.last_primary_key,
        complete,
        chunks: progress.chunks,
        rows_scanned: progress.rows_scanned,
        inserts: progress.inserts,
        updates: progress.updates,
        deletes: progress.deletes,
    })
}

fn initialize_target_session(conn: &mut Conn) -> Result<(), String> {
    conn.query_drop(crate::live::target_session_init_command())
        .map_err(|error| format!("initialize target mysql session: {error}"))
}

fn query_rows_as_strings(
    conn: &mut Conn,
    sql: &str,
    endpoint: &str,
) -> Result<Vec<Vec<Option<String>>>, String> {
    let rows = conn
        .query::<mysql::Row, _>(sql)
        .map_err(|error| format!("{endpoint} mysql query failed: {error}"))?;
    Ok(rows
        .into_iter()
        .map(|row| row.unwrap().into_iter().map(value_to_string).collect())
        .collect())
}

fn mysql_row_to_tsv(row: mysql::Row) -> String {
    row.unwrap()
        .into_iter()
        .map(value_to_string)
        .map(|value| value.unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\t")
}
