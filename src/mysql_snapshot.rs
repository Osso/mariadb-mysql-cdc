use crate::inventory::{InventoryConfig, InventoryError, MariaDbInventoryReader, build_inventory};
use crate::live::TargetMySqlConfig;
use crate::mysql_client::{
    PersistentMySqlSource, PersistentProgressWriter, PersistentTargetExecutor,
};
use crate::snapshot::{
    ChunkRequest, FileSnapshotProgressStore, SnapshotChunkProgress, SnapshotError,
    SnapshotObserver, SnapshotProgress, SnapshotProgressStore, SnapshotTable,
    snapshot_table_with_observer,
};
use crate::table_sync::{SyncMode, SyncProgressStatus, SyncTableProgress, TableSyncError};
use crate::target::{SnapshotInsertMode, TargetMySqlWriter};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const MYSQL_PROGRESS_SAVE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct MySqlConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub mariadb: String,
}

impl Default for MySqlConnectionConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            database: String::new(),
            mariadb: "mariadb".to_string(),
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
        "catchup_snapshot_start tables={} chunk_size={} progress_file={}",
        total_tables,
        config.chunk_size,
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
    prepare_snapshot_table_progress(source, progress_store, &table.name)?;
    let table_number = reports.len() + 1;
    log_catchup_table_start(progress_store, table, table_number, total_tables);
    let result = copy_catchup_table(
        config,
        progress_store,
        source,
        table,
        table_number,
        total_tables,
        reports.len(),
    )?;
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
    table_number: usize,
    total_tables: usize,
    completed_tables: usize,
) -> Result<crate::snapshot::SnapshotResult, CatchupSnapshotError> {
    let mut target = snapshot_target_for_table(config, table)?;
    let observer = CatchupSnapshotLogger::new(table_number, total_tables, completed_tables);
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
            table_number,
            total_tables,
            completed_tables + 1,
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
        total_rows: RefCell::new(BTreeMap::new()),
        mysql_save_state: RefCell::new(BTreeMap::new()),
    })
}

trait CatchupMysqlProgressStore {
    fn ensure(&self) -> Result<(), TableSyncError>;
    fn load_snapshot_progress(&self) -> Result<SnapshotProgress, TableSyncError>;
    fn save(&self, progress: &SyncTableProgress) -> Result<(), TableSyncError>;
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
}

struct CatchupProgressStore<M = PersistentProgressWriter> {
    file_store: FileSnapshotProgressStore,
    mysql_store: M,
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
            .and_then(|_| self.mysql_store.load_snapshot_progress())
            .map_err(|error| SnapshotError::InvalidTable(error.to_string()))
    }

    fn record_total_rows(&self, table: &str, total_rows: u64) {
        self.total_rows
            .borrow_mut()
            .insert(table.to_string(), total_rows);
    }

    fn total_rows_for_table(&self, table: &str) -> Option<u64> {
        self.total_rows.borrow().get(table).copied()
    }

    fn save_mysql_progress(&self, progress: &SnapshotProgress) -> Result<(), TableSyncError> {
        self.mysql_store.ensure()?;
        for (table, table_progress) in &progress.tables {
            let status = snapshot_progress_status(table_progress.complete);
            if !self.should_save_mysql_progress(table, table_progress.rows_copied, status) {
                continue;
            }
            self.mysql_store.save(&SyncTableProgress {
                table: table.clone(),
                last_primary_key: table_progress.last_primary_key.clone(),
                chunks: 0,
                rows_scanned: table_progress.rows_copied,
                total_rows: self.total_rows.borrow().get(table).copied(),
                inserts: table_progress.rows_copied,
                updates: 0,
                extra_target_rows: 0,
                mode: SyncMode::Apply,
                status,
                last_error: None,
            })?;
            self.record_mysql_progress_save(table, table_progress.rows_copied, status);
        }
        Ok(())
    }

    fn should_save_mysql_progress(
        &self,
        table: &str,
        rows_copied: u64,
        status: SyncProgressStatus,
    ) -> bool {
        self.mysql_save_state
            .borrow_mut()
            .entry(table.to_string())
            .or_default()
            .should_save(rows_copied, status, Instant::now())
    }

    fn record_mysql_progress_save(
        &self,
        table: &str,
        rows_copied: u64,
        status: SyncProgressStatus,
    ) {
        self.mysql_save_state
            .borrow_mut()
            .entry(table.to_string())
            .or_default()
            .record_save(rows_copied, status, Instant::now());
    }
}

struct CatchupSnapshotLogger {
    table_number: usize,
    total_tables: usize,
    completed_tables: usize,
    started_at: Instant,
}

impl CatchupSnapshotLogger {
    fn new(table_number: usize, total_tables: usize, completed_tables: usize) -> Self {
        Self {
            table_number,
            total_tables,
            completed_tables,
            started_at: Instant::now(),
        }
    }

    fn elapsed_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }
}

impl SnapshotObserver for CatchupSnapshotLogger {
    fn chunk_copied(&self, progress: &SnapshotChunkProgress) {
        println!(
            "{}",
            format_catchup_chunk_progress(
                progress,
                self.table_number,
                self.total_tables,
                self.completed_tables,
                self.elapsed_seconds(),
            )
        );
    }
}

fn format_catchup_table_start(
    table: &str,
    table_number: usize,
    total_tables: usize,
    total_rows: Option<u64>,
) -> String {
    format!(
        "catchup_table_start table={} table_number={} total_tables={} completed_tables={} total_rows={}",
        table,
        table_number,
        total_tables,
        table_number - 1,
        display_optional_u64(total_rows)
    )
}

fn format_catchup_chunk_progress(
    progress: &SnapshotChunkProgress,
    table_number: usize,
    total_tables: usize,
    completed_tables: usize,
    elapsed_seconds: u64,
) -> String {
    format!(
        "catchup_table_progress table={} table_number={} total_tables={} completed_tables={} chunk_start={} chunk_end={} chunk_rows={} imported_rows={} skipped_rows={} elapsed_seconds={}",
        progress.table,
        table_number,
        total_tables,
        completed_tables,
        display_primary_key(&progress.chunk_start),
        display_primary_key(&Some(progress.chunk_end.clone())),
        progress.chunk_rows,
        progress.rows_copied,
        0,
        elapsed_seconds
    )
}

fn format_catchup_table_complete(
    table: &str,
    table_number: usize,
    total_tables: usize,
    completed_tables: usize,
    rows_copied: u64,
    elapsed_seconds: u64,
) -> String {
    format!(
        "catchup_table_complete table={} table_number={} total_tables={} completed_tables={} rows_copied={} elapsed_seconds={}",
        table, table_number, total_tables, completed_tables, rows_copied, elapsed_seconds
    )
}

fn display_primary_key(value: &Option<Vec<String>>) -> String {
    value
        .as_ref()
        .map(|values| values.join(","))
        .unwrap_or_else(|| "-".to_string())
}

fn display_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
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
    let start_after = request
        .start_after
        .as_ref()
        .map(|values| {
            format!(
                " WHERE {}",
                primary_key_after_predicate(&request.primary_key, values)
            )
        })
        .unwrap_or_default();

    format!(
        "SELECT {columns} FROM {}{start_after} ORDER BY {order_by} LIMIT {}",
        quote_ident(&request.table),
        request.limit
    )
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
        mariadb: config.source.mariadb.clone(),
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

fn snapshot_target_for_table(
    config: &CatchupSnapshotConfig,
    table: &SnapshotTable,
) -> Result<TargetMySqlWriter<PersistentTargetExecutor>, SnapshotError> {
    let executor = PersistentTargetExecutor::new(&config.target)
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))?;
    Ok(TargetMySqlWriter::from_snapshot_table(
        table,
        executor,
        SnapshotInsertMode::IgnoreDuplicate,
    ))
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
        .zip(fields)
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
    values: &BTreeMap<String, String>,
) -> Result<Vec<String>, SnapshotError> {
    primary_key
        .iter()
        .map(|column| {
            values.get(column).cloned().ok_or_else(|| {
                SnapshotError::InvalidTable(format!(
                    "primary-key column `{column}` was not selected"
                ))
            })
        })
        .collect()
}

fn primary_key_after_predicate(columns: &[String], values: &[String]) -> String {
    let mut predicates = Vec::new();
    for index in 0..columns.len() {
        predicates.push(primary_key_after_branch(columns, values, index));
    }
    predicates.join(" OR ")
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
