use crate::inventory::{InventoryConfig, InventoryError, MariaDbInventoryReader, build_inventory};
use crate::live::{MysqlCliExecutor, TargetMySqlConfig};
use crate::snapshot::{
    ChunkRequest, FileSnapshotProgressStore, SnapshotError, SnapshotProgress,
    SnapshotProgressStore, SnapshotRow, SnapshotSource, SnapshotTable, snapshot_table,
};
use crate::table_sync::{
    MySqlSyncProgressStore, SyncMode, SyncProgressStatus, SyncProgressStore, SyncTableProgress,
    TableSyncError,
};
use crate::target::{SnapshotInsertMode, TargetMySqlWriter};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::process::Command;
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

pub struct MySqlSnapshotSource {
    config: MySqlConnectionConfig,
}

impl MySqlSnapshotSource {
    pub fn new(config: MySqlConnectionConfig) -> Self {
        Self { config }
    }
}

impl SnapshotSource for MySqlSnapshotSource {
    fn read_chunk(&self, request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError> {
        let sql = build_select_chunk_sql(request);
        let output = run_mysql_query(&self.config, &sql).map_err(SnapshotError::InvalidTable)?;

        parse_snapshot_rows(&request.selected_columns, &request.primary_key, &output)
    }
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
    let progress_store = catchup_progress_store(config);
    let source = MySqlSnapshotSource::new(config.source.clone());
    let mut reports = Vec::new();

    println!(
        "catchup_snapshot_start tables={} chunk_size={} progress_file={}",
        tables.len(),
        config.chunk_size,
        config.progress_file.display()
    );

    for table in tables {
        prepare_snapshot_table_progress(config, &progress_store, &table.name)?;
        println!("catchup_table_start table={}", table.name);
        let mut target = snapshot_target_for_table(config, &table);
        let result = snapshot_table(
            &table,
            config.chunk_size,
            &progress_store,
            &source,
            &mut target,
        )?;
        println!(
            "catchup_table_complete table={} rows_copied={}",
            result.table, result.rows_copied
        );
        reports.push(CatchupSnapshotTableReport {
            table: result.table,
            rows_copied: result.rows_copied,
        });
    }

    println!("catchup_snapshot_complete tables={}", reports.len());
    Ok(CatchupSnapshotReport { tables: reports })
}

fn prepare_snapshot_table_progress(
    config: &CatchupSnapshotConfig,
    progress_store: &CatchupProgressStore,
    table: &str,
) -> Result<(), CatchupSnapshotError> {
    let total_rows = count_snapshot_rows(config, table)?;
    progress_store.record_total_rows(table, total_rows);
    Ok(())
}

fn catchup_progress_store(config: &CatchupSnapshotConfig) -> CatchupProgressStore {
    let mysql_store = MySqlSyncProgressStore::new(
        config.source.mariadb.clone(),
        config.target.clone(),
        config.progress_table.clone(),
    );
    CatchupProgressStore {
        file_store: FileSnapshotProgressStore::new(&config.progress_file),
        mysql_store,
        total_rows: RefCell::new(BTreeMap::new()),
        mysql_save_state: RefCell::new(BTreeMap::new()),
    }
}

struct CatchupProgressStore {
    file_store: FileSnapshotProgressStore,
    mysql_store: MySqlSyncProgressStore,
    total_rows: RefCell<BTreeMap<String, u64>>,
    mysql_save_state: RefCell<BTreeMap<String, MysqlProgressSaveState>>,
}

impl SnapshotProgressStore for CatchupProgressStore {
    fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
        self.file_store.load()
    }

    fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
        self.file_store.save(progress)?;
        self.save_mysql_progress(progress)
            .map_err(|error| SnapshotError::InvalidTable(error.to_string()))
    }
}

impl CatchupProgressStore {
    fn record_total_rows(&self, table: &str, total_rows: u64) {
        self.total_rows
            .borrow_mut()
            .insert(table.to_string(), total_rows);
    }

    fn save_mysql_progress(&self, progress: &SnapshotProgress) -> Result<(), TableSyncError> {
        let mut store = self.mysql_store.clone();
        store.ensure()?;
        for (table, table_progress) in &progress.tables {
            let status = snapshot_progress_status(table_progress.complete);
            if !self.should_save_mysql_progress(table, table_progress.rows_copied, status) {
                continue;
            }
            store.save(&SyncTableProgress {
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

fn count_snapshot_rows(config: &CatchupSnapshotConfig, table: &str) -> Result<u64, SnapshotError> {
    let sql = build_count_rows_sql(table);
    let output = run_mysql_query(&config.source, &sql).map_err(SnapshotError::InvalidTable)?;
    output
        .trim()
        .parse()
        .map_err(|_| SnapshotError::InvalidTable(format!("{table} row count was not an integer")))
}

fn build_count_rows_sql(table: &str) -> String {
    format!("SELECT COUNT(*) FROM {}", quote_ident(table))
}

fn snapshot_target_for_table(
    config: &CatchupSnapshotConfig,
    table: &SnapshotTable,
) -> TargetMySqlWriter<MysqlCliExecutor> {
    let executor = MysqlCliExecutor::new(config.source.mariadb.clone(), config.target.clone());
    TargetMySqlWriter::from_snapshot_table(table, executor, SnapshotInsertMode::IgnoreDuplicate)
}

fn run_mysql_query(config: &MySqlConnectionConfig, sql: &str) -> Result<String, String> {
    let output = Command::new(&config.mariadb)
        .args([
            "--batch",
            "--raw",
            "--skip-column-names",
            "--default-character-set=utf8mb4",
            "--host",
            &config.host,
            "--port",
            &config.port.to_string(),
            "--user",
            &config.user,
            &config.database,
            "-e",
            sql,
        ])
        .env("MYSQL_PWD", &config.password)
        .output()
        .map_err(|error| format!("failed to run mariadb: {error}"))?;

    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(format!(
        "mariadb exited with {}: {}",
        output.status,
        stderr.trim()
    ))
}

fn parse_snapshot_rows(
    columns: &[String],
    primary_key: &[String],
    output: &str,
) -> Result<Vec<SnapshotRow>, SnapshotError> {
    output
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| parse_snapshot_row(columns, primary_key, line))
        .collect()
}

fn parse_snapshot_row(
    columns: &[String],
    primary_key: &[String],
    line: &str,
) -> Result<SnapshotRow, SnapshotError> {
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
    Ok(SnapshotRow {
        primary_key,
        values,
    })
}

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
mod tests {
    use super::*;

    #[test]
    fn builds_first_chunk_select() {
        let sql = build_select_chunk_sql(&ChunkRequest {
            table: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            selected_columns: vec!["id".to_string(), "name".to_string()],
            start_after: None,
            limit: 100,
        });

        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `accounts` ORDER BY `id` LIMIT 100"
        );
    }

    #[test]
    fn builds_resume_select_for_composite_primary_key() {
        let sql = build_select_chunk_sql(&ChunkRequest {
            table: "edges".to_string(),
            primary_key: vec!["left_id".to_string(), "right_id".to_string()],
            selected_columns: vec!["left_id".to_string(), "right_id".to_string()],
            start_after: Some(vec!["10".to_string(), "20".to_string()]),
            limit: 50,
        });

        assert_eq!(
            sql,
            "SELECT `left_id`, `right_id` FROM `edges` WHERE (`left_id` > '10') OR (`left_id` = '10' AND `right_id` > '20') ORDER BY `left_id`, `right_id` LIMIT 50"
        );
    }

    #[test]
    fn parses_snapshot_rows_with_primary_key_values() {
        let rows = parse_snapshot_rows(
            &["id".to_string(), "name".to_string()],
            &["id".to_string()],
            "1\talpha\n2\tbeta\n",
        )
        .expect("rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].primary_key, vec!["1"]);
        assert_eq!(rows[1].values["name"], "beta");
    }

    #[test]
    fn mysql_progress_save_is_throttled_until_interval_or_completion() {
        let now = std::time::Instant::now();
        let mut state = MysqlProgressSaveState::default();
        let running = SyncProgressStatus::Running;
        let complete = SyncProgressStatus::Complete;

        assert!(state.should_save(1_000, running, now));
        state.record_save(1_000, running, now);

        assert!(!state.should_save(2_000, running, now + MYSQL_PROGRESS_SAVE_INTERVAL / 2));
        assert!(state.should_save(3_000, running, now + MYSQL_PROGRESS_SAVE_INTERVAL));
        assert!(state.should_save(3_000, complete, now + MYSQL_PROGRESS_SAVE_INTERVAL / 2));
    }
}
