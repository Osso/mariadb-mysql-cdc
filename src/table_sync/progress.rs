use super::{SyncMode, SyncPhase, SyncTableReport, TableSyncError};
use crate::live::TargetMySqlConfig;
use crate::mysql_client::PersistentProgressWriter;
use crate::mysql_support::{
    qualified_table_parts, quote_ident, quote_identifier_path, quote_sql_literal,
};
use std::cell::RefCell;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncTableProgress {
    pub run_id: Option<String>,
    pub run_spec_json: Option<String>,
    pub table: String,
    pub last_primary_key: Option<Vec<String>>,
    pub chunks: u64,
    pub rows_scanned: u64,
    pub total_rows: Option<u64>,
    pub inserts: u64,
    pub updates: u64,
    pub extra_target_rows: u64,
    pub mode: SyncMode,
    pub status: SyncProgressStatus,
    pub last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncProgressStatus {
    Running,
    Complete,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncRunCandidate {
    pub(crate) run_id: String,
    pub(crate) table: String,
    pub(crate) run_spec_json: String,
    pub(crate) mode: SyncMode,
    pub(crate) status: SyncProgressStatus,
}

impl SyncRunCandidate {
    #[cfg(test)]
    pub(crate) fn new(
        run_id: &str,
        table: &str,
        run_spec_json: &str,
        mode: SyncMode,
        status: SyncProgressStatus,
    ) -> Self {
        Self {
            run_id: run_id.to_string(),
            table: table.to_string(),
            run_spec_json: run_spec_json.to_string(),
            mode,
            status,
        }
    }
}

pub(crate) fn select_compatible_failed_run(
    candidates: &[SyncRunCandidate],
    table: &str,
    phase: SyncPhase,
    expected_run_spec_json: &str,
) -> Result<Option<SyncRunCandidate>, TableSyncError> {
    if phase != SyncPhase::InsertMissing {
        return Ok(None);
    }

    let compatible = candidates
        .iter()
        .filter(|candidate| {
            candidate.table == table
                && candidate.mode == SyncMode::MissingPrimaryKeys
                && candidate.status == SyncProgressStatus::Error
                && candidate.run_spec_json == expected_run_spec_json
        })
        .collect::<Vec<_>>();
    match compatible.as_slice() {
        [] => Ok(None),
        [candidate] => Ok(Some((*candidate).clone())),
        _ => Err(TableSyncError::Progress(format!(
            "multiple compatible failed missing-primary-keys runs exist for table `{table}`"
        ))),
    }
}

pub trait SyncProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError>;
    fn acquire_run(&self, _run_id: &str) -> Result<(), TableSyncError> {
        Ok(())
    }
    fn release_run(&self, _run_id: &str) -> Result<(), TableSyncError> {
        Ok(())
    }
    fn load(&self, run_id: &str) -> Result<Option<SyncTableProgress>, TableSyncError>;
    fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError>;
    fn save_error(&mut self, run_id: &str, error: &TableSyncError) -> Result<(), TableSyncError>;
    fn transactional_save_sql(&self, _progress: &SyncTableProgress) -> Option<String> {
        None
    }
}

pub(crate) trait SyncRunSelectionStore {
    fn find_failed_run_candidates(
        &self,
        table: &str,
    ) -> Result<Vec<SyncRunCandidate>, TableSyncError>;

    fn acquire_selection_lock(
        &self,
        _table: &str,
        _run_spec_json: &str,
    ) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn release_selection_lock(
        &self,
        _table: &str,
        _run_spec_json: &str,
    ) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn begin_selection_transaction(&self) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn commit_selection_transaction(&self) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn rollback_selection_transaction(&self) -> Result<(), TableSyncError> {
        Ok(())
    }
}

pub(crate) fn claim_compatible_failed_run<P>(
    progress_store: &mut P,
    table: &str,
    phase: SyncPhase,
    expected_run_spec_json: &str,
) -> Result<Option<SyncRunCandidate>, TableSyncError>
where
    P: SyncProgressStore + SyncRunSelectionStore,
{
    if phase != SyncPhase::InsertMissing {
        return Ok(None);
    }

    progress_store.acquire_selection_lock(table, expected_run_spec_json)?;
    progress_store.begin_selection_transaction()?;
    let result = (|| {
        let candidates = progress_store.find_failed_run_candidates(table)?;
        if select_compatible_failed_run(&candidates, table, phase, expected_run_spec_json)?
            .is_none()
        {
            progress_store.commit_selection_transaction()?;
            return Ok(None);
        }
        let revalidated_candidates = progress_store.find_failed_run_candidates(table)?;
        let Some(candidate) = select_compatible_failed_run(
            &revalidated_candidates,
            table,
            phase,
            expected_run_spec_json,
        )?
        else {
            progress_store.commit_selection_transaction()?;
            return Ok(None);
        };
        let mut progress = progress_store.load(&candidate.run_id)?.ok_or_else(|| {
            TableSyncError::Progress(format!(
                "selected failed run `{}` disappeared before claim",
                candidate.run_id
            ))
        })?;
        progress.mark_running(candidate.mode);
        progress_store.save(&progress)?;
        progress_store.commit_selection_transaction()?;
        Ok(Some(candidate))
    })();
    if result.is_err() {
        let _ = progress_store.rollback_selection_transaction();
    }
    let release_result = progress_store.release_selection_lock(table, expected_run_spec_json);
    match (result, release_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(release_error)) => Err(TableSyncError::Progress(format!(
            "{error}; also failed to release compatible-run selection lock: {release_error}"
        ))),
    }
}

pub struct NoopSyncProgressStore;

impl SyncProgressStore for NoopSyncProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn load(&self, _run_id: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        Ok(None)
    }

    fn save(&mut self, _progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn save_error(&mut self, _run_id: &str, _error: &TableSyncError) -> Result<(), TableSyncError> {
        Ok(())
    }
}

pub struct MySqlSyncProgressStore {
    target: TargetMySqlConfig,
    table: String,
    writer: RefCell<Option<PersistentProgressWriter>>,
}

impl MySqlSyncProgressStore {
    pub fn new(target: TargetMySqlConfig, table: String) -> Self {
        Self {
            target,
            table,
            writer: RefCell::new(None),
        }
    }
}

impl SyncProgressStore for MySqlSyncProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        if let Some(schema_sql) = build_create_progress_schema_sql(&self.table) {
            self.execute(schema_sql)?;
        }
        self.execute(build_create_progress_table_sql(&self.table))?;
        self.execute(build_add_total_rows_column_sql(
            &self.target.database,
            &self.table,
        ))
    }

    fn load(&self, run_id: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        let output = self.query(build_progress_select_sql(&self.table, run_id))?;
        parse_progress_row(run_id, &output)
    }

    fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.execute(build_progress_upsert_sql(&self.table, progress))
    }

    fn save_error(&mut self, run_id: &str, error: &TableSyncError) -> Result<(), TableSyncError> {
        self.execute(build_progress_error_sql(&self.table, run_id, error))
    }
}

impl MySqlSyncProgressStore {
    fn execute(&self, statement: String) -> Result<(), TableSyncError> {
        self.writer()?.execute_table_sync_progress_sql(statement)
    }

    fn query(&self, sql: String) -> Result<String, TableSyncError> {
        self.writer()?.query_table_sync_progress_tsv(sql)
    }

    fn writer(&self) -> Result<std::cell::RefMut<'_, PersistentProgressWriter>, TableSyncError> {
        if self.writer.borrow().is_none() {
            let writer = PersistentProgressWriter::new(&self.target, self.table.clone())?;
            self.writer.replace(Some(writer));
        }
        Ok(std::cell::RefMut::map(self.writer.borrow_mut(), |writer| {
            writer.as_mut().expect("sync progress writer initialized")
        }))
    }
}

pub struct MySqlSyncRunProgressStore {
    inner: MySqlSyncProgressStore,
}

impl MySqlSyncRunProgressStore {
    pub fn new(target: TargetMySqlConfig, table: String) -> Self {
        Self {
            inner: MySqlSyncProgressStore::new(target, table),
        }
    }

    pub(crate) fn find_failed_run_candidates(
        &self,
        table: &str,
    ) -> Result<Vec<SyncRunCandidate>, TableSyncError> {
        let output = self
            .inner
            .query(build_failed_run_candidates_sql(&self.inner.table, table))?;
        parse_run_candidates(&output)
    }
}

impl SyncRunSelectionStore for MySqlSyncRunProgressStore {
    fn find_failed_run_candidates(
        &self,
        table: &str,
    ) -> Result<Vec<SyncRunCandidate>, TableSyncError> {
        self.find_failed_run_candidates(table)
    }

    fn acquire_selection_lock(
        &self,
        table: &str,
        run_spec_json: &str,
    ) -> Result<(), TableSyncError> {
        let result = self
            .inner
            .query(build_acquire_selection_lock_sql(table, run_spec_json))?;
        require_selection_lock_result(table, run_spec_json, &result)
    }

    fn release_selection_lock(
        &self,
        table: &str,
        run_spec_json: &str,
    ) -> Result<(), TableSyncError> {
        let result = self
            .inner
            .query(build_release_selection_lock_sql(table, run_spec_json))?;
        require_selection_lock_release(table, run_spec_json, &result)
    }

    fn begin_selection_transaction(&self) -> Result<(), TableSyncError> {
        self.inner.execute("START TRANSACTION".to_string())
    }

    fn commit_selection_transaction(&self) -> Result<(), TableSyncError> {
        self.inner.execute("COMMIT".to_string())
    }

    fn rollback_selection_transaction(&self) -> Result<(), TableSyncError> {
        self.inner.execute("ROLLBACK".to_string())
    }
}

impl SyncProgressStore for MySqlSyncRunProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        if let Some(schema_sql) = build_create_progress_schema_sql(&self.inner.table) {
            self.inner.execute(schema_sql)?;
        }
        self.inner
            .execute(build_create_sync_run_table_sql(&self.inner.table))?;
        let schema_count = self.inner.query(build_sync_run_schema_query(
            &self.inner.target.database,
            &self.inner.table,
        ))?;
        require_sync_run_schema(&self.inner.table, &schema_count)
    }

    fn acquire_run(&self, run_id: &str) -> Result<(), TableSyncError> {
        let result = self.inner.query(build_acquire_run_lock_sql(run_id))?;
        require_run_lock_result(run_id, &result)
    }

    fn release_run(&self, run_id: &str) -> Result<(), TableSyncError> {
        let result = self.inner.query(build_release_run_lock_sql(run_id))?;
        require_run_lock_release(run_id, &result)
    }

    fn load(&self, run_id: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        let output = self
            .inner
            .query(build_sync_run_select_sql(&self.inner.table, run_id))?;
        parse_sync_run_row(run_id, &output)
    }

    fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.inner
            .execute(build_sync_run_upsert_sql(&self.inner.table, progress))
    }

    fn save_error(&mut self, run_id: &str, error: &TableSyncError) -> Result<(), TableSyncError> {
        self.inner
            .execute(build_sync_run_error_sql(&self.inner.table, run_id, error))
    }

    fn transactional_save_sql(&self, progress: &SyncTableProgress) -> Option<String> {
        Some(build_sync_run_upsert_sql(&self.inner.table, progress))
    }
}

impl SyncTableProgress {
    pub fn started(run_id: String, run_spec_json: String, table: String, mode: SyncMode) -> Self {
        Self {
            run_id: Some(run_id),
            run_spec_json: Some(run_spec_json),
            table,
            last_primary_key: None,
            chunks: 0,
            rows_scanned: 0,
            total_rows: None,
            inserts: 0,
            updates: 0,
            extra_target_rows: 0,
            mode,
            status: SyncProgressStatus::Running,
            last_error: None,
        }
    }

    pub fn mark_running(&mut self, mode: SyncMode) {
        self.mode = mode;
        self.status = SyncProgressStatus::Running;
        self.last_error = None;
    }

    pub fn mark_complete(&mut self) {
        self.status = SyncProgressStatus::Complete;
        self.last_error = None;
    }

    pub fn record_chunk(&mut self, report: &SyncTableReport, last_primary_key: Vec<String>) {
        self.last_primary_key = Some(last_primary_key);
        self.chunks = report.chunks;
        self.rows_scanned = report.rows_scanned;
        self.inserts = report.inserts;
        self.updates = report.updates;
        self.extra_target_rows = report.extra_target_rows;
        self.status = SyncProgressStatus::Running;
    }

    pub fn report(&self) -> SyncTableReport {
        SyncTableReport {
            table: self.table.clone(),
            chunks: self.chunks,
            rows_scanned: self.rows_scanned,
            inserts: self.inserts,
            updates: self.updates,
            extra_target_rows: self.extra_target_rows,
        }
    }
}

const CREATE_PROGRESS_TABLE_NAME_COLUMN: &str = "table_name VARCHAR(255) NOT NULL PRIMARY KEY";
const CREATE_SYNC_RUN_TABLE_NAME_COLUMN: &str = "table_name VARCHAR(255) NOT NULL";
const CREATE_RUN_ID_COLUMN: &str = "run_id VARCHAR(128) NOT NULL PRIMARY KEY";
const CREATE_RUN_SPEC_COLUMN: &str = "run_spec_json LONGTEXT NOT NULL";
const CREATE_PROGRESS_TABLE_COLUMNS: &str = "last_primary_key_json TEXT NULL,\
chunks BIGINT UNSIGNED NOT NULL DEFAULT 0,\
rows_scanned BIGINT UNSIGNED NOT NULL DEFAULT 0,\
total_rows BIGINT UNSIGNED NULL,\
inserts_applied BIGINT UNSIGNED NOT NULL DEFAULT 0,\
updates_applied BIGINT UNSIGNED NOT NULL DEFAULT 0,\
extra_target_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,\
mode VARCHAR(16) NOT NULL,\
status VARCHAR(16) NOT NULL,\
last_error TEXT NULL,\
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";

fn build_create_table_sql(table: &str, columns: &[&str]) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_identifier_path(table),
        columns.join(",")
    )
}

pub(crate) fn build_create_progress_table_sql(table: &str) -> String {
    build_create_table_sql(
        table,
        &[
            CREATE_PROGRESS_TABLE_NAME_COLUMN,
            CREATE_PROGRESS_TABLE_COLUMNS,
        ],
    )
}

fn build_progress_select_sql(progress_table: &str, table: &str) -> String {
    format!(
        "SELECT COALESCE(last_primary_key_json, ''), chunks, rows_scanned, COALESCE(total_rows, ''), inserts_applied, updates_applied, extra_target_rows, mode, status, COALESCE(last_error, '') FROM {} WHERE table_name = {} LIMIT 1",
        quote_identifier_path(progress_table),
        quote_sql_literal(table)
    )
}

pub(crate) fn build_progress_upsert_sql(
    progress_table: &str,
    progress: &SyncTableProgress,
) -> String {
    let last_primary_key = progress
        .last_primary_key
        .as_ref()
        .map(|values| json_string(values))
        .unwrap_or_default();
    format!(
        "INSERT INTO {} (table_name,last_primary_key_json,chunks,rows_scanned,total_rows,inserts_applied,updates_applied,extra_target_rows,mode,status,last_error) VALUES ({},{},{},{},{},{},{},{},{},{},NULL) ON DUPLICATE KEY UPDATE last_primary_key_json=VALUES(last_primary_key_json),chunks=VALUES(chunks),rows_scanned=VALUES(rows_scanned),total_rows=VALUES(total_rows),inserts_applied=VALUES(inserts_applied),updates_applied=VALUES(updates_applied),extra_target_rows=VALUES(extra_target_rows),mode=VALUES(mode),status=VALUES(status),last_error=NULL",
        quote_identifier_path(progress_table),
        quote_sql_literal(&progress.table),
        nullable_sql_literal(&last_primary_key),
        progress.chunks,
        progress.rows_scanned,
        nullable_u64(progress.total_rows),
        progress.inserts,
        progress.updates,
        progress.extra_target_rows,
        quote_sql_literal(progress.mode.as_str()),
        quote_sql_literal(progress.status.as_str())
    )
}

fn build_progress_error_sql(progress_table: &str, table: &str, error: &TableSyncError) -> String {
    format!(
        "INSERT INTO {} (table_name,mode,status,last_error) VALUES ({},'unknown','error',{}) ON DUPLICATE KEY UPDATE status='error',last_error=VALUES(last_error)",
        quote_identifier_path(progress_table),
        quote_sql_literal(table),
        quote_sql_literal(&error.to_string())
    )
}

fn parse_progress_row(
    table: &str,
    output: &str,
) -> Result<Option<SyncTableProgress>, TableSyncError> {
    let Some(fields) = parse_progress_fields(output, 10)? else {
        return Ok(None);
    };
    Ok(Some(SyncTableProgress {
        run_id: None,
        run_spec_json: None,
        table: table.to_string(),
        last_primary_key: parse_primary_key_json(fields[0])?,
        chunks: parse_u64("chunks", fields[1])?,
        rows_scanned: parse_u64("rows_scanned", fields[2])?,
        total_rows: parse_optional_u64("total_rows", fields[3])?,
        inserts: parse_u64("inserts_applied", fields[4])?,
        updates: parse_u64("updates_applied", fields[5])?,
        extra_target_rows: parse_u64("extra_target_rows", fields[6])?,
        mode: SyncMode::parse(fields[7])?,
        status: SyncProgressStatus::parse(fields[8])?,
        last_error: non_empty(fields[9]),
    }))
}

fn parse_progress_fields(
    output: &str,
    expected: usize,
) -> Result<Option<Vec<&str>>, TableSyncError> {
    let Some(line) = output.lines().find(|line| !line.trim().is_empty()) else {
        return Ok(None);
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() != expected {
        return Err(TableSyncError::Progress(format!(
            "progress row has {} fields, expected {expected}",
            fields.len()
        )));
    }
    Ok(Some(fields))
}

pub(crate) fn build_create_sync_run_table_sql(table: &str) -> String {
    build_create_table_sql(
        table,
        &[
            CREATE_RUN_ID_COLUMN,
            CREATE_SYNC_RUN_TABLE_NAME_COLUMN,
            CREATE_RUN_SPEC_COLUMN,
            CREATE_PROGRESS_TABLE_COLUMNS,
        ],
    )
}

pub(crate) fn build_add_total_rows_column_sql(
    default_schema: &str,
    progress_table: &str,
) -> String {
    let (schema, table) = qualified_table_parts(default_schema, progress_table);
    let alter_table = format!(
        "ALTER TABLE {} ADD COLUMN total_rows BIGINT UNSIGNED NULL AFTER rows_scanned",
        quote_identifier_path(progress_table)
    );
    format!(
        "SET @cdc_total_rows_ddl = (SELECT IF(COUNT(*) = 0, {}, 'SELECT 1') FROM information_schema.columns WHERE table_schema = {} AND table_name = {} AND column_name = 'total_rows'); PREPARE cdc_total_rows_stmt FROM @cdc_total_rows_ddl; EXECUTE cdc_total_rows_stmt; DEALLOCATE PREPARE cdc_total_rows_stmt",
        quote_sql_literal(&alter_table),
        quote_sql_literal(&schema),
        quote_sql_literal(&table)
    )
}

pub(crate) fn build_create_progress_schema_sql(table: &str) -> Option<String> {
    let schema = table.split_once('.')?.0;
    Some(format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        quote_ident(schema)
    ))
}

fn build_sync_run_schema_query(default_schema: &str, progress_table: &str) -> String {
    let (schema, table) = qualified_table_parts(default_schema, progress_table);
    format!(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = {} AND table_name = {} AND column_name IN ('run_id','run_spec_json')",
        quote_sql_literal(&schema),
        quote_sql_literal(&table)
    )
}

fn require_sync_run_schema(table: &str, output: &str) -> Result<(), TableSyncError> {
    if output.trim() == "2" {
        return Ok(());
    }
    Err(TableSyncError::Progress(format!(
        "progress table `{table}` is not a run-scoped progress table; use a new table such as `cdc.table_sync_runs`"
    )))
}

fn build_acquire_selection_lock_sql(table: &str, run_spec_json: &str) -> String {
    format!(
        "SELECT GET_LOCK(SHA2(CONCAT('sync-run-claim:',{},':',{}),256),0)",
        quote_sql_literal(table),
        quote_sql_literal(run_spec_json)
    )
}

fn build_release_selection_lock_sql(table: &str, run_spec_json: &str) -> String {
    format!(
        "SELECT RELEASE_LOCK(SHA2(CONCAT('sync-run-claim:',{},':',{}),256))",
        quote_sql_literal(table),
        quote_sql_literal(run_spec_json)
    )
}

fn build_acquire_run_lock_sql(run_id: &str) -> String {
    format!("SELECT GET_LOCK(SHA2({},256),0)", quote_sql_literal(run_id))
}

fn build_release_run_lock_sql(run_id: &str) -> String {
    format!(
        "SELECT RELEASE_LOCK(SHA2({},256))",
        quote_sql_literal(run_id)
    )
}

fn require_selection_lock_result(
    table: &str,
    run_spec_json: &str,
    output: &str,
) -> Result<(), TableSyncError> {
    if output.trim() == "1" {
        return Ok(());
    }
    Err(TableSyncError::Progress(format!(
        "compatible failed-run selection is already active for table `{table}` and immutable specification `{run_spec_json}`"
    )))
}

fn require_selection_lock_release(
    table: &str,
    run_spec_json: &str,
    output: &str,
) -> Result<(), TableSyncError> {
    if output.trim() == "1" {
        return Ok(());
    }
    Err(TableSyncError::Progress(format!(
        "compatible failed-run selection lock was not owned for table `{table}` and immutable specification `{run_spec_json}`"
    )))
}

fn require_run_lock_result(run_id: &str, output: &str) -> Result<(), TableSyncError> {
    if output.trim() == "1" {
        return Ok(());
    }
    Err(TableSyncError::Progress(format!(
        "run id `{run_id}` is already active"
    )))
}

fn require_run_lock_release(run_id: &str, output: &str) -> Result<(), TableSyncError> {
    if output.trim() == "1" {
        return Ok(());
    }
    Err(TableSyncError::Progress(format!(
        "run id `{run_id}` lock was not owned by this connection"
    )))
}

fn build_sync_run_select_sql(progress_table: &str, run_id: &str) -> String {
    format!(
        "SELECT table_name, run_spec_json, COALESCE(last_primary_key_json, ''), chunks, rows_scanned, COALESCE(total_rows, ''), inserts_applied, updates_applied, extra_target_rows, mode, status, COALESCE(last_error, '') FROM {} WHERE run_id = {} LIMIT 1",
        quote_identifier_path(progress_table),
        quote_sql_literal(run_id)
    )
}

fn build_failed_run_candidates_sql(progress_table: &str, table: &str) -> String {
    format!(
        "SELECT run_id, table_name, run_spec_json, mode, status FROM {} WHERE table_name = {} AND status = 'error' ORDER BY run_id FOR UPDATE",
        quote_identifier_path(progress_table),
        quote_sql_literal(table)
    )
}

pub(crate) fn build_sync_run_upsert_sql(
    progress_table: &str,
    progress: &SyncTableProgress,
) -> String {
    let (run_id, run_spec_json, last_primary_key) = sync_run_upsert_identity(progress);
    format!(
        "INSERT INTO {} (run_id,table_name,run_spec_json,last_primary_key_json,chunks,rows_scanned,total_rows,inserts_applied,updates_applied,extra_target_rows,mode,status,last_error) VALUES ({},{},{},{},{},{},{},{},{},{},{},{},NULL) ON DUPLICATE KEY UPDATE last_primary_key_json=VALUES(last_primary_key_json),chunks=VALUES(chunks),rows_scanned=VALUES(rows_scanned),total_rows=VALUES(total_rows),inserts_applied=VALUES(inserts_applied),updates_applied=VALUES(updates_applied),extra_target_rows=VALUES(extra_target_rows),status=VALUES(status),last_error=NULL",
        quote_identifier_path(progress_table),
        quote_sql_literal(run_id),
        quote_sql_literal(&progress.table),
        quote_sql_literal(run_spec_json),
        nullable_sql_literal(&last_primary_key),
        progress.chunks,
        progress.rows_scanned,
        nullable_u64(progress.total_rows),
        progress.inserts,
        progress.updates,
        progress.extra_target_rows,
        quote_sql_literal(progress.mode.as_str()),
        quote_sql_literal(progress.status.as_str())
    )
}

fn sync_run_upsert_identity(progress: &SyncTableProgress) -> (&str, &str, String) {
    let run_id = progress
        .run_id
        .as_deref()
        .expect("sync run progress requires run id");
    let run_spec_json = progress
        .run_spec_json
        .as_deref()
        .expect("sync run progress requires run specification");
    let last_primary_key = progress
        .last_primary_key
        .as_ref()
        .map(|values| json_string(values))
        .unwrap_or_default();
    (run_id, run_spec_json, last_primary_key)
}

fn build_sync_run_error_sql(progress_table: &str, run_id: &str, error: &TableSyncError) -> String {
    format!(
        "UPDATE {} SET status='error',last_error={} WHERE run_id={}",
        quote_identifier_path(progress_table),
        quote_sql_literal(&error.to_string()),
        quote_sql_literal(run_id)
    )
}

fn parse_run_candidates(output: &str) -> Result<Vec<SyncRunCandidate>, TableSyncError> {
    output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            if fields.len() != 5 {
                return Err(TableSyncError::Progress(format!(
                    "run candidate row has {} fields, expected 5",
                    fields.len()
                )));
            }
            Ok(SyncRunCandidate {
                run_id: fields[0].to_string(),
                table: fields[1].to_string(),
                run_spec_json: fields[2].to_string(),
                mode: SyncMode::parse(fields[3])?,
                status: SyncProgressStatus::parse(fields[4])?,
            })
        })
        .collect()
}

fn parse_sync_run_row(
    run_id: &str,
    output: &str,
) -> Result<Option<SyncTableProgress>, TableSyncError> {
    let Some(fields) = parse_progress_fields(output, 12)? else {
        return Ok(None);
    };
    Ok(Some(SyncTableProgress {
        run_id: Some(run_id.to_string()),
        table: fields[0].to_string(),
        run_spec_json: Some(fields[1].to_string()),
        last_primary_key: parse_primary_key_json(fields[2])?,
        chunks: parse_u64("chunks", fields[3])?,
        rows_scanned: parse_u64("rows_scanned", fields[4])?,
        total_rows: parse_optional_u64("total_rows", fields[5])?,
        inserts: parse_u64("inserts_applied", fields[6])?,
        updates: parse_u64("updates_applied", fields[7])?,
        extra_target_rows: parse_u64("extra_target_rows", fields[8])?,
        mode: SyncMode::parse(fields[9])?,
        status: SyncProgressStatus::parse(fields[10])?,
        last_error: non_empty(fields[11]),
    }))
}

fn parse_primary_key_json(value: &str) -> Result<Option<Vec<String>>, TableSyncError> {
    if value.is_empty() {
        return Ok(None);
    }

    serde_json::from_str(value)
        .map(Some)
        .map_err(|error| TableSyncError::Progress(format!("invalid primary key json: {error}")))
}

fn parse_u64(field: &str, value: &str) -> Result<u64, TableSyncError> {
    value
        .parse()
        .map_err(|_| TableSyncError::Progress(format!("{field} must be an integer")))
}

fn parse_optional_u64(field: &str, value: &str) -> Result<Option<u64>, TableSyncError> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_u64(field, value).map(Some)
}

fn json_string(values: &[String]) -> String {
    serde_json::to_string(values).expect("primary key JSON serialization cannot fail")
}

fn non_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

impl SyncMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry-run",
            Self::Apply => "apply",
            Self::MissingPrimaryKeys => "missing-pks",
        }
    }

    fn parse(value: &str) -> Result<Self, TableSyncError> {
        match value {
            "dry-run" => Ok(Self::DryRun),
            "apply" => Ok(Self::Apply),
            "missing-pks" | "missing-primary-keys" => Ok(Self::MissingPrimaryKeys),
            other => Err(TableSyncError::Progress(format!(
                "unknown sync mode in progress: {other}"
            ))),
        }
    }
}

impl SyncProgressStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Error => "error",
        }
    }

    fn parse(value: &str) -> Result<Self, TableSyncError> {
        match value {
            "running" => Ok(Self::Running),
            "complete" => Ok(Self::Complete),
            "error" => Ok(Self::Error),
            other => Err(TableSyncError::Progress(format!(
                "unknown sync progress status: {other}"
            ))),
        }
    }
}

fn nullable_sql_literal(value: &str) -> String {
    if value.is_empty() {
        "NULL".to_string()
    } else {
        quote_sql_literal(value)
    }
}

fn nullable_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_sql_preserves_exact_contract() {
        assert_eq!(
            build_create_progress_table_sql("cdc.table_sync_progress"),
            "CREATE TABLE IF NOT EXISTS `cdc`.`table_sync_progress` (table_name VARCHAR(255) NOT NULL PRIMARY KEY,last_primary_key_json TEXT NULL,chunks BIGINT UNSIGNED NOT NULL DEFAULT 0,rows_scanned BIGINT UNSIGNED NOT NULL DEFAULT 0,total_rows BIGINT UNSIGNED NULL,inserts_applied BIGINT UNSIGNED NOT NULL DEFAULT 0,updates_applied BIGINT UNSIGNED NOT NULL DEFAULT 0,extra_target_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,mode VARCHAR(16) NOT NULL,status VARCHAR(16) NOT NULL,last_error TEXT NULL,created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
        );
        assert_eq!(
            build_create_sync_run_table_sql("cdc.table_sync_runs"),
            "CREATE TABLE IF NOT EXISTS `cdc`.`table_sync_runs` (run_id VARCHAR(128) NOT NULL PRIMARY KEY,table_name VARCHAR(255) NOT NULL,run_spec_json LONGTEXT NOT NULL,last_primary_key_json TEXT NULL,chunks BIGINT UNSIGNED NOT NULL DEFAULT 0,rows_scanned BIGINT UNSIGNED NOT NULL DEFAULT 0,total_rows BIGINT UNSIGNED NULL,inserts_applied BIGINT UNSIGNED NOT NULL DEFAULT 0,updates_applied BIGINT UNSIGNED NOT NULL DEFAULT 0,extra_target_rows BIGINT UNSIGNED NOT NULL DEFAULT 0,mode VARCHAR(16) NOT NULL,status VARCHAR(16) NOT NULL,last_error TEXT NULL,created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
        );
    }

    #[test]
    fn add_total_rows_column_sql_is_conditional_for_existing_tables() {
        let sql = build_add_total_rows_column_sql("globalcomix", "cdc.table_sync_progress");

        assert!(sql.contains("information_schema.columns"));
        assert!(sql.contains("column_name = 'total_rows'"));
        assert!(sql.contains("ALTER TABLE `cdc`.`table_sync_progress` ADD COLUMN total_rows"));
    }

    #[test]
    fn run_table_schema_query_requires_both_identity_columns() {
        let sql = build_sync_run_schema_query("globalcomix", "cdc.table_sync_progress");

        assert!(sql.contains("table_schema = 'cdc'"));
        assert!(sql.contains("table_name = 'table_sync_progress'"));
        assert!(sql.contains("column_name IN ('run_id','run_spec_json')"));
    }

    #[test]
    fn selection_lock_sql_scopes_table_and_immutable_spec_without_waiting() {
        assert_eq!(
            build_acquire_selection_lock_sql("guests", "{\"scope\":\"current\"}"),
            "SELECT GET_LOCK(SHA2(CONCAT('sync-run-claim:','guests',':','{\"scope\":\"current\"}'),256),0)"
        );
        assert_eq!(
            build_release_selection_lock_sql("guests", "{\"scope\":\"current\"}"),
            "SELECT RELEASE_LOCK(SHA2(CONCAT('sync-run-claim:','guests',':','{\"scope\":\"current\"}'),256))"
        );
        assert_ne!(
            build_acquire_selection_lock_sql("guests", "spec-a"),
            build_acquire_selection_lock_sql("guests", "spec-b")
        );
        assert_ne!(
            build_acquire_selection_lock_sql("guests", "spec-a"),
            build_acquire_selection_lock_sql("sessions", "spec-a")
        );
    }

    #[test]
    fn failed_run_candidate_query_locks_current_rows_for_revalidation() {
        assert!(
            build_failed_run_candidates_sql("cdc.table_sync_runs", "guests")
                .ends_with("ORDER BY run_id FOR UPDATE")
        );
    }

    #[test]
    fn run_lock_sql_uses_hashed_run_id_and_never_waits() {
        assert_eq!(
            build_acquire_run_lock_sql("repair-01"),
            "SELECT GET_LOCK(SHA2('repair-01',256),0)"
        );
        assert_eq!(
            build_release_run_lock_sql("repair-01"),
            "SELECT RELEASE_LOCK(SHA2('repair-01',256))"
        );
    }

    #[test]
    fn create_progress_schema_sql_uses_dotted_prefix() {
        let sql =
            build_create_progress_schema_sql("cdc.table_sync_progress").expect("schema create sql");

        assert_eq!(sql, "CREATE DATABASE IF NOT EXISTS `cdc`");
        assert_eq!(
            build_create_progress_schema_sql("table_sync_progress"),
            None
        );
    }

    #[test]
    fn upsert_progress_sql_stores_last_primary_key_and_counts() {
        let progress = SyncTableProgress {
            run_id: Some("repair-20260710-01".to_string()),
            run_spec_json: Some("{\"table\":\"releases\"}".to_string()),
            table: "releases".to_string(),
            last_primary_key: Some(vec!["42".to_string()]),
            chunks: 2,
            rows_scanned: 2000,
            total_rows: Some(5000),
            inserts: 3,
            updates: 4,
            extra_target_rows: 5,
            mode: SyncMode::Apply,
            status: SyncProgressStatus::Running,
            last_error: None,
        };

        let sql = build_sync_run_upsert_sql("cdc.table_sync_runs", &progress);

        assert!(sql.contains("'repair-20260710-01'"));
        assert!(sql.contains("'{\"table\":\"releases\"}'"));
        assert!(sql.contains("'releases'"));
        assert!(sql.contains("'[\"42\"]'"));
        assert!(sql.contains("2,2000,5000,3,4,5"));
        assert!(sql.contains("'apply','running'"));
    }

    #[test]
    fn missing_primary_keys_progress_token_fits_existing_schema() {
        let token = SyncMode::MissingPrimaryKeys.as_str();

        assert_eq!(token, "missing-pks");
        assert!(token.len() <= 16);
        assert_eq!(SyncMode::parse(token), Ok(SyncMode::MissingPrimaryKeys));
    }

    #[test]
    fn parse_progress_row_restores_resume_state() {
        let row = "releases\t{\"table\":\"releases\"}\t[\"42\"]\t2\t2000\t5000\t3\t4\t5\tapply\trunning\t";

        let progress = parse_sync_run_row("repair-20260710-01", row)
            .expect("parse progress")
            .expect("progress row");

        assert_eq!(progress.run_id.as_deref(), Some("repair-20260710-01"));
        assert_eq!(
            progress.run_spec_json.as_deref(),
            Some("{\"table\":\"releases\"}")
        );
        assert_eq!(progress.table, "releases");
        assert_eq!(progress.last_primary_key, Some(vec!["42".to_string()]));
        assert_eq!(progress.chunks, 2);
        assert_eq!(progress.rows_scanned, 2000);
        assert_eq!(progress.total_rows, Some(5000));
        assert_eq!(progress.inserts, 3);
        assert_eq!(progress.updates, 4);
        assert_eq!(progress.extra_target_rows, 5);
        assert_eq!(progress.mode, SyncMode::Apply);
        assert_eq!(progress.status, SyncProgressStatus::Running);
    }
}
