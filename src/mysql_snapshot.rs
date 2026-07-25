use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, InventoryError, MariaDbInventoryReader, build_inventory,
};
use crate::live::TargetMySqlConfig;
use crate::mysql_client::{PersistentMySqlSource, PersistentProgressWriter};
use crate::snapshot::{
    ChunkRequest, FileSnapshotProgressStore, SnapshotError, SnapshotProgress,
    SnapshotProgressStore, SnapshotTable, snapshot_table_with_observer,
};
use crate::table_sync::{SyncMode, SyncProgressStatus, SyncTableProgress, TableSyncError};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod parallel;
mod progress_log;
mod target_schema;
#[cfg(test)]
use parallel::parallel_worker_count;
use parallel::{CatchupTableMode, catchup_table_mode, copy_catchup_table_parallel};
use progress_log::{
    CatchupSnapshotLogger, format_catchup_table_complete, format_catchup_table_start,
};
use target_schema::snapshot_target_for_table;
#[cfg(test)]
use target_schema::validate_target_table_columns;

const MYSQL_PROGRESS_SAVE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct MySqlConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl Default for MySqlConnectionConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            database: String::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CatchupSnapshotConfig {
    pub source: MySqlConnectionConfig,
    pub target: TargetMySqlConfig,
    pub progress_file: PathBuf,
    pub progress_table: String,
    pub chunk_size: usize,
    pub throttle: Duration,
    pub parallel_workers: usize,
    pub table: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatchupSnapshotReport {
    pub tables: Vec<CatchupSnapshotTableReport>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatchupSnapshotTableReport {
    pub table: String,
    pub rows_copied: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CatchupTableLogContext {
    table_number: usize,
    total_tables: usize,
    completed_tables: usize,
}

#[derive(Debug)]
pub enum CatchupSnapshotError {
    Config(String),
    Inventory(InventoryError),
    Snapshot(SnapshotError),
}

impl fmt::Display for CatchupSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => formatter.write_str(message),
            Self::Inventory(source) => write!(formatter, "inventory failed: {source}"),
            Self::Snapshot(source) => write!(formatter, "snapshot failed: {source}"),
        }
    }
}

impl std::error::Error for CatchupSnapshotError {}

impl From<InventoryError> for CatchupSnapshotError {
    fn from(source: InventoryError) -> Self {
        Self::Inventory(source)
    }
}

impl From<SnapshotError> for CatchupSnapshotError {
    fn from(source: SnapshotError) -> Self {
        Self::Snapshot(source)
    }
}

pub fn run_catchup_snapshot(
    config: &CatchupSnapshotConfig,
) -> Result<CatchupSnapshotReport, CatchupSnapshotError> {
    validate_config(config)?;
    let tables = read_snapshot_tables(config)?;
    let progress_store = catchup_progress_store(config)?;
    let source = PersistentMySqlSource::new(&config.source)?;
    let total_tables = tables.len();
    let mut reports = Vec::new();

    log_catchup_snapshot_start(total_tables, config);

    for table in tables {
        let report = run_catchup_table(
            config,
            &source,
            &progress_store,
            &table,
            &reports,
            total_tables,
        )?;
        reports.push(report);
    }

    println!("catchup_snapshot_complete tables={}", reports.len());
    Ok(CatchupSnapshotReport { tables: reports })
}

fn log_catchup_snapshot_start(total_tables: usize, config: &CatchupSnapshotConfig) {
    println!(
        "catchup_snapshot_start tables={} chunk_size={} parallel_workers={} progress_file={}",
        total_tables,
        config.chunk_size,
        config.parallel_workers,
        config.progress_file.display()
    );
}

fn run_catchup_table(
    config: &CatchupSnapshotConfig,
    source: &PersistentMySqlSource,
    progress_store: &CatchupProgressStore,
    table: &SnapshotTable,
    reports: &[CatchupSnapshotTableReport],
    total_tables: usize,
) -> Result<CatchupSnapshotTableReport, CatchupSnapshotError> {
    let result =
        run_catchup_table_inner(config, source, progress_store, table, reports, total_tables);
    if let Err(error) = &result {
        progress_store.record_table_error(&table.name, error);
    }
    result
}

fn run_catchup_table_inner(
    config: &CatchupSnapshotConfig,
    source: &PersistentMySqlSource,
    progress_store: &CatchupProgressStore,
    table: &SnapshotTable,
    reports: &[CatchupSnapshotTableReport],
    total_tables: usize,
) -> Result<CatchupSnapshotTableReport, CatchupSnapshotError> {
    prepare_snapshot_table_progress(source, progress_store, &table.name)?;
    let table_number = reports.len() + 1;
    log_catchup_table_start(progress_store, table, table_number, total_tables);
    let log_context = CatchupTableLogContext {
        table_number,
        total_tables,
        completed_tables: reports.len(),
    };
    let result = copy_catchup_table(config, progress_store, source, table, log_context)?;
    Ok(CatchupSnapshotTableReport {
        table: result.table,
        rows_copied: result.rows_copied,
    })
}

fn log_catchup_table_start(
    progress_store: &CatchupProgressStore,
    table: &SnapshotTable,
    table_number: usize,
    total_tables: usize,
) {
    let total_rows = progress_store.total_rows_for_table(&table.name);
    println!(
        "{}",
        format_catchup_table_start(&table.name, table_number, total_tables, total_rows)
    );
}

fn copy_catchup_table(
    config: &CatchupSnapshotConfig,
    progress_store: &CatchupProgressStore,
    source: &PersistentMySqlSource,
    table: &SnapshotTable,
    log_context: CatchupTableLogContext,
) -> Result<crate::snapshot::SnapshotResult, CatchupSnapshotError> {
    let total_rows = progress_store
        .total_rows_for_table(&table.name)
        .unwrap_or(0);
    match catchup_table_mode(total_rows, config.parallel_workers) {
        CatchupTableMode::Sequential => {
            copy_catchup_table_sequential(config, progress_store, source, table, log_context)
        }
        CatchupTableMode::Parallel { workers } => {
            copy_catchup_table_parallel(config, source, table, workers, log_context, total_rows)
        }
    }
}

fn copy_catchup_table_sequential(
    config: &CatchupSnapshotConfig,
    progress_store: &CatchupProgressStore,
    source: &PersistentMySqlSource,
    table: &SnapshotTable,
    log_context: CatchupTableLogContext,
) -> Result<crate::snapshot::SnapshotResult, CatchupSnapshotError> {
    let mut target = snapshot_target_for_table(config, source, table)?;
    let observer = CatchupSnapshotLogger::new(
        log_context.table_number,
        log_context.total_tables,
        log_context.completed_tables,
        config.throttle,
    );
    let result = snapshot_table_with_observer(
        table,
        config.chunk_size,
        progress_store,
        source,
        &mut target,
        &observer,
    )?;
    println!(
        "{}",
        format_catchup_table_complete(
            &result.table,
            log_context.table_number,
            log_context.total_tables,
            log_context.completed_tables + 1,
            result.rows_copied,
            observer.elapsed_seconds()
        )
    );
    Ok(result)
}

fn prepare_snapshot_table_progress(
    source: &PersistentMySqlSource,
    progress_store: &CatchupProgressStore,
    table: &str,
) -> Result<(), CatchupSnapshotError> {
    let total_rows = source.count_rows(table)?;
    progress_store.record_total_rows(table, total_rows);
    Ok(())
}

fn catchup_progress_store(
    config: &CatchupSnapshotConfig,
) -> Result<CatchupProgressStore, CatchupSnapshotError> {
    let mysql_store = PersistentProgressWriter::new(&config.target, config.progress_table.clone())
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))?;
    Ok(CatchupProgressStore {
        file_store: FileSnapshotProgressStore::new(&config.progress_file),
        mysql_store,
        progress_table: config.progress_table.clone(),
        total_rows: RefCell::new(BTreeMap::new()),
        mysql_save_state: RefCell::new(BTreeMap::new()),
    })
}

trait CatchupMysqlProgressStore {
    fn ensure(&self) -> Result<(), TableSyncError>;
    fn load_snapshot_progress(&self) -> Result<SnapshotProgress, TableSyncError>;
    fn save(&self, progress: &SyncTableProgress) -> Result<(), TableSyncError>;
    fn save_error_message(&self, table: &str, error: &str) -> Result<(), TableSyncError>;
}

impl CatchupMysqlProgressStore for PersistentProgressWriter {
    fn ensure(&self) -> Result<(), TableSyncError> {
        self.ensure()
    }

    fn load_snapshot_progress(&self) -> Result<SnapshotProgress, TableSyncError> {
        self.load_snapshot_progress()
    }

    fn save(&self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.save(progress)
    }

    fn save_error_message(&self, table: &str, error: &str) -> Result<(), TableSyncError> {
        self.save_error_message(table, error)
    }
}

struct CatchupProgressStore<M = PersistentProgressWriter> {
    file_store: FileSnapshotProgressStore,
    mysql_store: M,
    progress_table: String,
    total_rows: RefCell<BTreeMap<String, u64>>,
    mysql_save_state: RefCell<BTreeMap<String, MysqlProgressSaveState>>,
}

impl<M> SnapshotProgressStore for CatchupProgressStore<M>
where
    M: CatchupMysqlProgressStore,
{
    fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
        let file_progress = self.file_store.load()?;
        if !file_progress.tables.is_empty() {
            return Ok(file_progress);
        }

        self.load_mysql_progress()
    }

    fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
        self.file_store.save(progress)?;
        self.save_mysql_progress(progress)
            .map_err(|error| SnapshotError::InvalidTable(error.to_string()))
    }
}

impl<M> CatchupProgressStore<M>
where
    M: CatchupMysqlProgressStore,
{
    fn load_mysql_progress(&self) -> Result<SnapshotProgress, SnapshotError> {
        self.mysql_store
            .ensure()
            .map_err(|error| SnapshotError::ProgressSchemaEnsure {
                progress_table: self.progress_table.clone(),
                source: Box::new(SnapshotError::InvalidTable(error.to_string())),
            })?;
        self.mysql_store
            .load_snapshot_progress()
            .map_err(|error| SnapshotError::ProgressRowRead {
                progress_table: self.progress_table.clone(),
                source: Box::new(SnapshotError::InvalidTable(error.to_string())),
            })
    }

    fn record_total_rows(&self, table: &str, total_rows: u64) {
        self.total_rows
            .borrow_mut()
            .insert(table.to_string(), total_rows);
    }

    fn total_rows_for_table(&self, table: &str) -> Option<u64> {
        self.total_rows.borrow().get(table).copied()
    }

    fn record_table_error(&self, table: &str, error: &CatchupSnapshotError) {
        if let Err(save_error) = self.save_table_error(table, error) {
            eprintln!(
                "catchup_error_persist_failed table={} error={} persist_error={}",
                table, error, save_error
            );
        }
    }

    fn save_table_error(
        &self,
        table: &str,
        error: &CatchupSnapshotError,
    ) -> Result<(), TableSyncError> {
        self.mysql_store.ensure()?;
        self.mysql_store
            .save_error_message(table, &error.to_string())
    }

    fn save_mysql_progress(&self, progress: &SnapshotProgress) -> Result<(), TableSyncError> {
        save_mysql_snapshot_progress(
            &self.mysql_store,
            &self.total_rows.borrow(),
            &self.mysql_save_state,
            progress,
        )
    }
}

fn save_mysql_snapshot_progress(
    mysql_store: &impl CatchupMysqlProgressStore,
    total_rows: &BTreeMap<String, u64>,
    mysql_save_state: &RefCell<BTreeMap<String, MysqlProgressSaveState>>,
    progress: &SnapshotProgress,
) -> Result<(), TableSyncError> {
    mysql_store.ensure()?;
    for (table, table_progress) in &progress.tables {
        let status = snapshot_progress_status(table_progress.complete);
        if !should_save_mysql_progress(mysql_save_state, table, table_progress.rows_copied, status)
        {
            continue;
        }
        mysql_store.save(&SyncTableProgress {
            run_id: None,
            run_spec_json: None,
            table: table.clone(),
            last_primary_key: table_progress.last_primary_key.clone(),
            chunks: 0,
            rows_scanned: table_progress.rows_copied,
            total_rows: total_rows.get(table).copied(),
            inserts: table_progress.rows_copied,
            updates: 0,
            extra_target_rows: 0,
            delete_preflight_complete: false,
            mode: SyncMode::Apply,
            status,
            last_error: None,
        })?;
        record_mysql_progress_save(mysql_save_state, table, table_progress.rows_copied, status);
    }
    Ok(())
}

fn should_save_mysql_progress(
    mysql_save_state: &RefCell<BTreeMap<String, MysqlProgressSaveState>>,
    table: &str,
    rows_copied: u64,
    status: SyncProgressStatus,
) -> bool {
    mysql_save_state
        .borrow_mut()
        .entry(table.to_string())
        .or_default()
        .should_save(rows_copied, status, Instant::now())
}

fn record_mysql_progress_save(
    mysql_save_state: &RefCell<BTreeMap<String, MysqlProgressSaveState>>,
    table: &str,
    rows_copied: u64,
    status: SyncProgressStatus,
) {
    mysql_save_state
        .borrow_mut()
        .entry(table.to_string())
        .or_default()
        .record_save(rows_copied, status, Instant::now());
}

#[derive(Default)]
struct MysqlProgressSaveState {
    rows_copied: u64,
    status: Option<SyncProgressStatus>,
    saved_at: Option<Instant>,
}

impl MysqlProgressSaveState {
    fn should_save(&self, rows_copied: u64, status: SyncProgressStatus, now: Instant) -> bool {
        if self.status != Some(status) {
            return true;
        }
        if self.rows_copied == rows_copied {
            return false;
        }
        self.saved_at
            .is_none_or(|saved_at| now.duration_since(saved_at) >= MYSQL_PROGRESS_SAVE_INTERVAL)
    }

    fn record_save(&mut self, rows_copied: u64, status: SyncProgressStatus, now: Instant) {
        self.rows_copied = rows_copied;
        self.status = Some(status);
        self.saved_at = Some(now);
    }
}
fn snapshot_progress_status(complete: bool) -> SyncProgressStatus {
    if complete {
        SyncProgressStatus::Complete
    } else {
        SyncProgressStatus::Running
    }
}

pub fn build_select_chunk_sql(request: &ChunkRequest) -> String {
    let columns = quote_ident_list(&request.selected_columns);
    let order_by = quote_ident_list(&request.primary_key);
    let bounds = snapshot_chunk_bounds(request);

    format!(
        "SELECT {columns} FROM {}{bounds} ORDER BY {order_by} LIMIT {}",
        quote_ident(&request.table),
        request.limit
    )
}

fn snapshot_chunk_bounds(request: &ChunkRequest) -> String {
    let predicates = snapshot_bound_predicates(request);
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
}

fn snapshot_bound_predicates(request: &ChunkRequest) -> Vec<String> {
    let mut predicates = Vec::new();
    if let Some(start_after) = &request.start_after {
        predicates.push(primary_key_after_predicate(
            &request.primary_key,
            start_after,
        ));
    }
    if let Some(end_at) = &request.end_at {
        predicates.push(primary_key_at_or_before_predicate(
            &request.primary_key,
            end_at,
        ));
    }
    predicates
}

fn validate_config(config: &CatchupSnapshotConfig) -> Result<(), CatchupSnapshotError> {
    validate_connection("source", &config.source)?;
    validate_target(&config.target)?;
    if config.progress_file.as_os_str().is_empty() {
        return Err(CatchupSnapshotError::Config(
            "progress file is required".to_string(),
        ));
    }
    if config.chunk_size == 0 {
        return Err(CatchupSnapshotError::Config(
            "chunk size must be greater than zero".to_string(),
        ));
    }
    if config.parallel_workers == 0 {
        return Err(CatchupSnapshotError::Config(
            "parallel workers must be greater than zero".to_string(),
        ));
    }

    Ok(())
}

fn validate_connection(
    role: &str,
    config: &MySqlConnectionConfig,
) -> Result<(), CatchupSnapshotError> {
    if config.host.is_empty() {
        return Err(CatchupSnapshotError::Config(format!(
            "{role} host is required"
        )));
    }
    if config.user.is_empty() {
        return Err(CatchupSnapshotError::Config(format!(
            "{role} user is required"
        )));
    }
    if config.password.is_empty() {
        return Err(CatchupSnapshotError::Config(format!(
            "{role} password is required"
        )));
    }
    if config.database.is_empty() {
        return Err(CatchupSnapshotError::Config(format!(
            "{role} database is required"
        )));
    }
    Ok(())
}

fn validate_target(target: &TargetMySqlConfig) -> Result<(), CatchupSnapshotError> {
    if target.host.is_empty() {
        return Err(CatchupSnapshotError::Config(
            "target host is required".to_string(),
        ));
    }
    if target.user.is_empty() {
        return Err(CatchupSnapshotError::Config(
            "target user is required".to_string(),
        ));
    }
    if target.password.is_empty() {
        return Err(CatchupSnapshotError::Config(
            "target password is required".to_string(),
        ));
    }
    if target.database.is_empty() {
        return Err(CatchupSnapshotError::Config(
            "target database is required".to_string(),
        ));
    }
    if target.tls_ca_file.is_empty() {
        return Err(CatchupSnapshotError::Config(
            "target TLS CA file is required".to_string(),
        ));
    }
    Ok(())
}

fn read_snapshot_tables(
    config: &CatchupSnapshotConfig,
) -> Result<Vec<SnapshotTable>, CatchupSnapshotError> {
    let inventory_config = InventoryConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        endpoint_role: InventoryEndpointRole::Source,
        use_tls: false,
        tls_ca_file: None,
        ..InventoryConfig::default()
    };
    let reader = MariaDbInventoryReader::new(inventory_config);
    let inventory = build_inventory(&config.source.database, &reader)?;
    let tables = inventory
        .tables
        .iter()
        .filter(|table| config.table.as_ref().is_none_or(|name| &table.name == name))
        .map(SnapshotTable::from)
        .collect();

    Ok(tables)
}

#[cfg(test)]
fn parse_snapshot_rows(
    columns: &[String],
    primary_key: &[String],
    output: &str,
) -> Result<Vec<crate::snapshot::SnapshotRow>, SnapshotError> {
    output
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| parse_snapshot_row(columns, primary_key, line))
        .collect()
}

#[cfg(test)]
fn parse_snapshot_row(
    columns: &[String],
    primary_key: &[String],
    line: &str,
) -> Result<crate::snapshot::SnapshotRow, SnapshotError> {
    let fields = line.split('\t').map(str::to_string).collect::<Vec<_>>();
    if fields.len() != columns.len() {
        return Err(SnapshotError::InvalidTable(format!(
            "snapshot row has {} fields for {} columns",
            fields.len(),
            columns.len()
        )));
    }

    let values = columns
        .iter()
        .cloned()
        .zip(fields.into_iter().map(Some))
        .collect::<BTreeMap<_, _>>();
    let primary_key = primary_key_values(primary_key, &values)?;
    Ok(crate::snapshot::SnapshotRow {
        primary_key,
        values,
    })
}

#[cfg(test)]
fn primary_key_values(
    primary_key: &[String],
    values: &BTreeMap<String, Option<String>>,
) -> Result<Vec<String>, SnapshotError> {
    primary_key
        .iter()
        .map(|column| {
            let value = values.get(column).cloned().ok_or_else(|| {
                SnapshotError::InvalidTable(format!(
                    "primary-key column `{column}` was not selected"
                ))
            })?;
            value.ok_or_else(|| {
                SnapshotError::InvalidTable(format!("primary-key column `{column}` was NULL"))
            })
        })
        .collect()
}

fn primary_key_after_predicate(columns: &[String], values: &[String]) -> String {
    let mut predicates = Vec::new();
    for index in 0..columns.len() {
        predicates.push(primary_key_after_branch(columns, values, index));
    }
    if predicates.len() < 2 {
        return predicates.join(" OR ");
    }
    // `AND` binds tighter than `OR`, so an ungrouped multi-column bound leaves the window
    // unbounded once a second bound is combined with `AND`.
    format!("({})", predicates.join(" OR "))
}

fn primary_key_at_or_before_predicate(columns: &[String], values: &[String]) -> String {
    format!("NOT ({})", primary_key_after_predicate(columns, values))
}

fn primary_key_after_branch(columns: &[String], values: &[String], index: usize) -> String {
    let mut parts = Vec::new();
    for equal_index in 0..index {
        parts.push(format!(
            "{} = {}",
            quote_ident(&columns[equal_index]),
            quote_sql_literal(&values[equal_index])
        ));
    }
    parts.push(format!(
        "{} > {}",
        quote_ident(&columns[index]),
        quote_sql_literal(&values[index])
    ));
    format!("({})", parts.join(" AND "))
}

fn quote_ident_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests;
