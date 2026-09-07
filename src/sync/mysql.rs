use super::model::{
    SyncChunkPage, SyncChunkProgress, SyncChunkProgressStore, SyncChunkReadRequest,
    SyncChunkSource, SyncChunkTargetSession, SyncMutationFailure, SyncProgressRow,
    SyncProgressStatus, SyncRunProgressStore, SyncStage, SyncTable, SyncUniqueIndex,
    SyncUniqueOwnerAction, SyncUniqueOwnerConflict,
};
use super::progress::{
    build_create_sync_progress_schema_sql, build_create_sync_progress_table_sql,
    build_sync_progress_select_sql, build_sync_progress_upsert_sql, parse_sync_progress_row,
};
use super::sql::{
    build_exact_primary_key_select_statement, build_lock_table_write_sql,
    build_strict_delete_rows_statement, build_strict_insert_statement,
    build_strict_update_rows_statement, build_sync_select_sql,
    build_unique_index_columns_statement, build_unique_owner_select_statement,
};
use crate::database_row::DatabaseRow;
use crate::live::TargetMySqlConfig;
use crate::mysql_client::{
    extend_session_wait_timeout, sync_source_opts, sync_target_opts, value_to_string,
};
use crate::mysql_config::MySqlConnectionConfig;
use crate::target::SqlStatement;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, Params};
use std::collections::{BTreeMap, BTreeSet};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod query;
mod unique_owner;

pub(crate) use query::{
    decode_optional_exact_row, mysql_rows_to_strings, query_statement_rows_as_strings,
};
pub(crate) use unique_owner::{
    SyncUniqueIndexColumn, build_sync_mutation_failure, format_unique_owner_reconciliation_event,
    resolve_sync_unique_index,
};
use unique_owner::{mysql_error_code, validate_unique_owner, verify_exact_row};

const MYSQL_MAX_PREPARED_STATEMENT_PLACEHOLDERS: usize = 65_535;
const MAX_SYNC_MUTATION_ROWS_PER_STATEMENT: usize = 128;
const MAX_SYNC_CONNECTION_RETRIES: u32 = 4;
const INITIAL_SYNC_CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(100);
// Bounds retained decoded row payloads, not MySQL wire bytes or a single oversized row.
const MAX_SYNC_PAGE_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

pub(crate) struct MySqlSyncSource {
    conn: Conn,
    table: SyncTable,
}

pub(crate) struct MySqlSyncTargetSession {
    conn: Conn,
    database: String,
    table: SyncTable,
    pending_reconciliation_events: Vec<String>,
}

pub(crate) struct MySqlSyncProgressStore {
    conn: Conn,
    progress_table: String,
}

pub(crate) fn open_sync_connection(opts: Opts) -> mysql::Result<Conn> {
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
        Ok(Self { conn, table })
    }
}

impl SyncChunkSource for MySqlSyncSource {
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<SyncChunkPage, String> {
        query_sync_page_rows(&mut self.conn, &self.table, request, "source")
    }

    fn read_row_by_primary_key(
        &mut self,
        primary_key: &[String],
    ) -> Result<Option<DatabaseRow>, String> {
        let statement = build_exact_primary_key_select_statement(&self.table, primary_key)?;
        let rows = query_statement_rows_as_strings(&mut self.conn, &statement, "source exact-row")?;
        decode_optional_exact_row(&self.table, rows, "source")
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
            pending_reconciliation_events: Vec::new(),
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

    fn query_rows(&mut self, request: &SyncChunkReadRequest) -> Result<SyncChunkPage, String> {
        query_sync_page_rows(&mut self.conn, &self.table, request, "target")
    }

    fn query_statement_rows(
        &mut self,
        statement: &SqlStatement,
        operation: &str,
    ) -> Result<Vec<Vec<Option<String>>>, String> {
        query_statement_rows_as_strings(&mut self.conn, statement, operation)
    }

    fn query_exact_row(&mut self, primary_key: &[String]) -> Result<Option<DatabaseRow>, String> {
        let statement = build_exact_primary_key_select_statement(&self.table, primary_key)?;
        let rows = self.query_statement_rows(&statement, "target exact-row")?;
        decode_optional_exact_row(&self.table, rows, "target")
    }

    fn execute_mutation_batches(
        &mut self,
        rows: &[DatabaseRow],
        capacity: usize,
        build: fn(&SyncTable, &[DatabaseRow]) -> Result<SqlStatement, String>,
    ) -> Result<(), SyncMutationFailure> {
        for (batch_index, batch) in rows.chunks(capacity).enumerate() {
            let start = batch_index * capacity;
            let statement = build(&self.table, batch).map_err(|message| {
                build_sync_mutation_failure(rows, start, batch.len(), None, message)
            })?;
            self.conn
                .exec_drop(&statement.sql, Params::Positional(statement.params))
                .map_err(|error| {
                    build_sync_mutation_failure(
                        rows,
                        start,
                        batch.len(),
                        mysql_error_code(&error),
                        format!("target mysql statement failed: {error}"),
                    )
                })?;
        }
        Ok(())
    }

    fn load_unique_index(&mut self, error: &str) -> Result<SyncUniqueIndex, String> {
        let statement = build_unique_index_columns_statement(&self.database, &self.table.name);
        let rows = self
            .conn
            .exec::<(String, Option<String>, u64, Option<u64>), _, _>(
                &statement.sql,
                Params::Positional(statement.params),
            )
            .map_err(|query_error| {
                format!(
                    "query target unique indexes for `{}`: {query_error}",
                    self.table.name
                )
            })?
            .into_iter()
            .map(
                |(index, column, sequence, prefix_length)| SyncUniqueIndexColumn {
                    index,
                    column,
                    sequence,
                    prefix_length,
                },
            )
            .collect();
        resolve_sync_unique_index(&self.table.name, error, rows)
    }

    fn query_unique_owner(
        &mut self,
        index: &SyncUniqueIndex,
        intended: &DatabaseRow,
    ) -> Result<Option<DatabaseRow>, String> {
        let statement = build_unique_owner_select_statement(&self.table, index, intended)?;
        let rows = self.query_statement_rows(&statement, "target unique-owner")?;
        decode_optional_exact_row(&self.table, rows, "target unique-owner")
    }

    fn verify_unique_identity_unowned(
        &mut self,
        conflict: &SyncUniqueOwnerConflict,
    ) -> Result<(), String> {
        if self
            .query_unique_owner(&conflict.index, &conflict.intended)?
            .is_none()
        {
            return Ok(());
        }
        Err(format!(
            "reconciled target owner still owns `{}` identity for `{}`",
            conflict.index.name, self.table.name
        ))
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

    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<SyncChunkPage, String> {
        self.query_rows(request)
    }

    fn delete_rows(&mut self, primary_keys: &[Vec<String>]) -> Result<(), String> {
        for statement in build_strict_delete_batches(&self.table, primary_keys)? {
            self.execute_statement(statement)?;
        }
        Ok(())
    }

    fn update_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), SyncMutationFailure> {
        self.execute_mutation_batches(
            rows,
            strict_update_batch_capacity(&self.table),
            build_strict_update_rows_statement,
        )
    }

    fn insert_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), SyncMutationFailure> {
        self.execute_mutation_batches(
            rows,
            strict_insert_batch_capacity(&self.table),
            build_strict_insert_statement,
        )
    }

    fn inspect_unique_owner_conflicts(
        &mut self,
        failure: &SyncMutationFailure,
    ) -> Result<Vec<SyncUniqueOwnerConflict>, String> {
        if failure.mysql_code != Some(1062) {
            return Err(failure.message.clone());
        }
        let index = self.load_unique_index(&failure.message)?;
        let mut owner_primary_keys = BTreeSet::new();
        let mut conflicts = Vec::new();
        for intended in &failure.failed_batch {
            let Some(owner) = self.query_unique_owner(&index, intended)? else {
                continue;
            };
            if owner.primary_key == intended.primary_key {
                continue;
            }
            validate_unique_owner(&self.table.name, &index, intended, &owner)?;
            if !owner_primary_keys.insert(owner.primary_key.clone()) {
                return Err(format!(
                    "secondary unique-owner evidence is ambiguous for `{}` index `{}`",
                    self.table.name, index.name
                ));
            }
            conflicts.push(SyncUniqueOwnerConflict {
                index: index.clone(),
                intended: intended.clone(),
                owner,
            });
        }
        if conflicts.is_empty() {
            return Err(format!(
                "secondary unique-owner evidence is absent for `{}` index `{}`",
                self.table.name, index.name
            ));
        }
        Ok(conflicts)
    }

    fn reconcile_unique_owner(
        &mut self,
        conflict: &SyncUniqueOwnerConflict,
        action: &SyncUniqueOwnerAction,
    ) -> Result<(), String> {
        match action {
            SyncUniqueOwnerAction::Update(row) => {
                self.update_rows(std::slice::from_ref(row))
                    .map_err(|error| error.to_string())?;
                verify_exact_row(
                    self.query_exact_row(&conflict.owner.primary_key)?,
                    Some(row),
                    "updated unique owner",
                )?;
            }
            SyncUniqueOwnerAction::Delete => {
                self.delete_rows(std::slice::from_ref(&conflict.owner.primary_key))?;
                verify_exact_row(
                    self.query_exact_row(&conflict.owner.primary_key)?,
                    None,
                    "deleted unique owner",
                )?;
            }
        }
        self.verify_unique_identity_unowned(conflict)?;
        self.pending_reconciliation_events
            .push(format_unique_owner_reconciliation_event(
                &self.table.name,
                conflict,
                action,
            ));
        Ok(())
    }

    fn verify_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), String> {
        for expected in rows {
            verify_exact_row(
                self.query_exact_row(&expected.primary_key)?,
                Some(expected),
                "mutated intended row",
            )?;
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<(), String> {
        if let Err(error) = self.execute_control("COMMIT") {
            self.pending_reconciliation_events.clear();
            return Err(error);
        }
        for event in self.pending_reconciliation_events.drain(..) {
            eprintln!("{event}");
        }
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), String> {
        self.pending_reconciliation_events.clear();
        self.execute_control("ROLLBACK")
    }

    fn unlock_tables(&mut self) -> Result<(), String> {
        self.execute_control("UNLOCK TABLES")
    }
}

impl MySqlSyncProgressStore {
    pub(crate) fn new(
        config: &TargetMySqlConfig,
        progress_table: String,
        coordinator_session_wait_timeout_seconds: Option<u32>,
    ) -> Result<Self, String> {
        let opts = sync_target_opts(config)?;
        let mut conn = open_sync_connection(opts)
            .map_err(|error| format!("failed to connect to sync progress mysql: {error}"))?;
        initialize_target_session(&mut conn)?;
        if let Some(seconds) = coordinator_session_wait_timeout_seconds {
            extend_session_wait_timeout(&mut conn, seconds, "recovery sync progress")?;
        }
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
        .map(|column| required_primary_key_value(table, column, &values))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DatabaseRow {
        primary_key,
        values,
    })
}

fn required_primary_key_value(
    table: &SyncTable,
    column: &str,
    values: &BTreeMap<String, Option<String>>,
) -> Result<String, String> {
    let value = values
        .get(column)
        .ok_or_else(|| format!("primary-key column `{column}` was not selected"))?;
    let value = value
        .as_deref()
        .ok_or_else(|| format!("primary-key column `{column}` was NULL"))?;
    let Some(labels) = table.enum_columns.get(column) else {
        return Ok(value.to_string());
    };
    let ordinal = value.parse::<usize>().map_err(|error| {
        format!(
            "ENUM primary-key column `{column}` in `{}` has invalid internal index `{value}`: {error}",
            table.name
        )
    })?;
    if ordinal == 0 {
        return Err(format!(
            "ENUM primary-key column `{column}` in `{}` has internal index 0 without a label cursor",
            table.name
        ));
    }
    labels.get(ordinal - 1).cloned().ok_or_else(|| {
        format!(
            "ENUM primary-key column `{column}` in `{}` has internal index `{ordinal}` outside its declaration",
            table.name
        )
    })
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

#[cfg(test)]
pub(crate) fn build_strict_update_batches(
    table: &SyncTable,
    rows: &[DatabaseRow],
) -> Result<Vec<SqlStatement>, String> {
    rows.chunks(strict_update_batch_capacity(table))
        .map(|batch| build_strict_update_rows_statement(table, batch))
        .collect()
}

pub(crate) fn build_strict_delete_batches(
    table: &SyncTable,
    primary_keys: &[Vec<String>],
) -> Result<Vec<SqlStatement>, String> {
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

fn query_sync_page_rows(
    conn: &mut Conn,
    table: &SyncTable,
    request: &SyncChunkReadRequest,
    endpoint: &str,
) -> Result<SyncChunkPage, String> {
    let sql = build_sync_select_sql(table, request);
    let mut result = conn
        .query_iter(sql)
        .map_err(|error| format!("{endpoint} mysql query failed: {error}"))?;
    let Some(mut rows) = result.iter() else {
        return Ok(SyncChunkPage {
            rows: Vec::new(),
            has_more: false,
        });
    };
    let mut collector = ByteBoundedRowCollector::new(request.limit, MAX_SYNC_PAGE_PAYLOAD_BYTES);
    while let Some(row) = rows.next() {
        let row = row.map_err(|error| format!("{endpoint} mysql row read failed: {error}"))?;
        let decoded = decode_sync_row(table, mysql_row_to_strings(row))?;
        if !collector.can_retain(&decoded) {
            while let Some(discarded) = rows.next() {
                discarded.map_err(|error| {
                    format!("{endpoint} mysql row read failed after byte boundary: {error}")
                })?;
            }
            return Ok(collector.finish(true));
        }
        collector.retain(decoded);
    }
    Ok(collector.finish(false))
}

fn mysql_row_to_strings(row: mysql::Row) -> Vec<Option<String>> {
    row.unwrap().into_iter().map(value_to_string).collect()
}

fn mysql_row_to_tsv(row: mysql::Row) -> String {
    row.unwrap()
        .into_iter()
        .map(value_to_string)
        .map(|value| value.unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\t")
}

struct ByteBoundedRowCollector {
    rows: Vec<DatabaseRow>,
    retained_bytes: usize,
    row_limit: usize,
    byte_budget: usize,
}

impl ByteBoundedRowCollector {
    fn new(row_limit: usize, byte_budget: usize) -> Self {
        Self {
            rows: Vec::new(),
            retained_bytes: 0,
            row_limit,
            byte_budget,
        }
    }

    fn can_retain(&self, row: &DatabaseRow) -> bool {
        if self.rows.len() == self.row_limit {
            return false;
        }
        self.rows.is_empty()
            || self
                .retained_bytes
                .saturating_add(projected_row_payload_bytes(row))
                <= self.byte_budget
    }

    fn retain(&mut self, row: DatabaseRow) {
        self.retained_bytes = self
            .retained_bytes
            .saturating_add(projected_row_payload_bytes(&row));
        self.rows.push(row);
    }

    fn finish(self, has_more: bool) -> SyncChunkPage {
        SyncChunkPage {
            rows: self.rows,
            has_more,
        }
    }
}

#[cfg(test)]
fn collect_byte_bounded_rows(
    rows: impl IntoIterator<Item = DatabaseRow>,
    row_limit: usize,
    byte_budget: usize,
) -> SyncChunkPage {
    let mut collector = ByteBoundedRowCollector::new(row_limit, byte_budget);
    for row in rows {
        if !collector.can_retain(&row) {
            return collector.finish(true);
        }
        collector.retain(row);
    }
    collector.finish(false)
}

fn projected_row_payload_bytes(row: &DatabaseRow) -> usize {
    row.values
        .values()
        .flatten()
        .map(String::len)
        .fold(0, usize::saturating_add)
}

#[cfg(test)]
mod tests {
    use super::collect_byte_bounded_rows;
    use crate::database_row::DatabaseRow;
    use std::collections::BTreeMap;

    #[test]
    fn byte_budget_keeps_the_next_row_for_the_next_cursor() {
        let rows = vec![row("1", "aaaa"), row("2", "bbbb")];

        let page = collect_byte_bounded_rows(rows, 10, 5);

        assert_eq!(page.rows, vec![row("1", "aaaa")]);
        assert!(page.has_more);
    }

    #[test]
    fn a_full_sql_row_limit_requires_another_page() {
        let page = collect_byte_bounded_rows(vec![row("1", "a"), row("2", "b")], 2, 100);
        assert!(page.has_more);
    }

    #[test]
    fn byte_budget_keeps_one_oversized_row() {
        let oversized = row("1", "abcdef");

        let page = collect_byte_bounded_rows(vec![oversized.clone()], 10, 5);

        assert_eq!(page.rows, vec![oversized]);
        assert!(!page.has_more);
    }

    fn row(id: &str, payload: &str) -> DatabaseRow {
        DatabaseRow {
            primary_key: vec![id.to_string()],
            values: BTreeMap::from([("payload".to_string(), Some(payload.to_string()))]),
        }
    }
}
