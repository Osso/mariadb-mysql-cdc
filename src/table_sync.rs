use crate::snapshot::SnapshotRow;
use crate::target::PrimaryKey;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::process::Command;

#[derive(Clone, Debug)]
pub struct SyncTableConfig {
    pub source: crate::mysql_snapshot::MySqlConnectionConfig,
    pub target: crate::live::TargetMySqlConfig,
    pub mariadb: String,
    pub table: SyncTable,
    pub chunk_size: usize,
    pub mode: SyncMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncTable {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncChunkRequest {
    pub table: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
    pub limit: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncMode {
    DryRun,
    Apply,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncTableReport {
    pub table: String,
    pub chunks: u64,
    pub rows_scanned: u64,
    pub inserts: u64,
    pub updates: u64,
    pub extra_target_rows: u64,
}

pub trait SyncTableReader {
    fn read_rows(&self, request: &SyncChunkRequest) -> Result<Vec<SnapshotRow>, TableSyncError>;
}

pub trait SyncRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum TableSyncError {
    InvalidTable(String),
    Read(String),
    Repair(String),
}

impl fmt::Display for TableSyncError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTable(message) => write!(formatter, "invalid sync table: {message}"),
            Self::Read(message) => write!(formatter, "sync read failed: {message}"),
            Self::Repair(message) => write!(formatter, "sync repair failed: {message}"),
        }
    }
}

impl std::error::Error for TableSyncError {}

pub fn sync_table(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<SyncTableReport, TableSyncError> {
    validate_sync_table(table, chunk_size)?;
    let mut report = SyncTableReport {
        table: table.name.clone(),
        ..SyncTableReport::default()
    };
    let mut start_after = None;

    loop {
        let source_request = sync_chunk_request(table, start_after.clone(), None, chunk_size);
        let source_rows = source.read_rows(&source_request)?;
        if source_rows.is_empty() {
            return Ok(report);
        }

        let end_at = last_primary_key(&source_rows)?;
        let target_rows = read_target_window(
            table,
            start_after.clone(),
            Some(end_at.clone()),
            chunk_size,
            target,
        )?;

        repair_chunk(&source_rows, &target_rows, mode, repair_target, &mut report)?;
        report.chunks += 1;
        report.rows_scanned += source_rows.len() as u64;

        if source_rows.len() < chunk_size {
            return Ok(report);
        }
        start_after = Some(end_at);
    }
}

fn read_target_window(
    table: &SyncTable,
    mut start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    chunk_size: usize,
    target: &impl SyncTableReader,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    let mut rows = Vec::new();

    loop {
        let page = target.read_rows(&sync_chunk_request(
            table,
            start_after.clone(),
            end_at.clone(),
            chunk_size,
        ))?;
        if page.is_empty() {
            return Ok(rows);
        }

        let page_is_complete = page.len() < chunk_size;
        start_after = Some(last_primary_key(&page)?);
        rows.extend(page);

        if page_is_complete {
            return Ok(rows);
        }
    }
}

pub fn run_sync_table(config: &SyncTableConfig) -> Result<SyncTableReport, TableSyncError> {
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new(target_connection_config(config));
    let executor =
        crate::live::MysqlCliExecutor::new(config.mariadb.clone(), config.target.clone());
    let mut repair_target = crate::target::TargetMySqlWriter::from_snapshot_table(
        &snapshot_table(&config.table),
        executor,
        crate::target::SnapshotInsertMode::IgnoreDuplicate,
    );
    sync_table(
        &config.table,
        config.chunk_size,
        config.mode,
        &source,
        &target,
        &mut repair_target,
    )
}

fn target_connection_config(
    config: &SyncTableConfig,
) -> crate::mysql_snapshot::MySqlConnectionConfig {
    crate::mysql_snapshot::MySqlConnectionConfig {
        host: config.target.host.clone(),
        port: config.target.port,
        user: config.target.user.clone(),
        password: config.target.password.clone(),
        database: config.target.database.clone(),
        mariadb: config.mariadb.clone(),
    }
}

fn snapshot_table(table: &SyncTable) -> crate::snapshot::SnapshotTable {
    crate::snapshot::SnapshotTable {
        name: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: table.columns.clone(),
    }
}

fn validate_sync_table(table: &SyncTable, chunk_size: usize) -> Result<(), TableSyncError> {
    if table.name.is_empty() {
        return Err(TableSyncError::InvalidTable(
            "table name is required".to_string(),
        ));
    }
    if table.primary_key.is_empty() {
        return Err(TableSyncError::InvalidTable(
            "primary key is required".to_string(),
        ));
    }
    if table.columns.is_empty() {
        return Err(TableSyncError::InvalidTable(
            "columns are required".to_string(),
        ));
    }
    if chunk_size == 0 {
        return Err(TableSyncError::InvalidTable(
            "chunk size must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn sync_chunk_request(
    table: &SyncTable,
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    limit: usize,
) -> SyncChunkRequest {
    SyncChunkRequest {
        table: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: table.columns.clone(),
        start_after,
        end_at,
        limit,
    }
}

fn last_primary_key(rows: &[SnapshotRow]) -> Result<Vec<String>, TableSyncError> {
    rows.last()
        .map(|row| row.primary_key.clone())
        .ok_or_else(|| TableSyncError::Read("source chunk unexpectedly empty".to_string()))
}

fn repair_chunk(
    source_rows: &[SnapshotRow],
    target_rows: &[SnapshotRow],
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
    report: &mut SyncTableReport,
) -> Result<(), TableSyncError> {
    let source_by_key = rows_by_key(source_rows);
    let target_by_key = rows_by_key(target_rows);

    for primary_key in row_keys(&source_by_key, &target_by_key) {
        match (
            source_by_key.get(&primary_key),
            target_by_key.get(&primary_key),
        ) {
            (Some(source), Some(target)) if source.values != target.values => {
                apply_update(source, mode, repair_target)?;
                report.updates += 1;
            }
            (Some(source), None) => {
                apply_insert(source, mode, repair_target)?;
                report.inserts += 1;
            }
            (None, Some(_target)) => report.extra_target_rows += 1,
            _ => {}
        }
    }

    Ok(())
}

fn rows_by_key(rows: &[SnapshotRow]) -> BTreeMap<Vec<String>, &SnapshotRow> {
    rows.iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect()
}

fn row_keys(
    source: &BTreeMap<Vec<String>, &SnapshotRow>,
    target: &BTreeMap<Vec<String>, &SnapshotRow>,
) -> BTreeSet<Vec<String>> {
    source.keys().chain(target.keys()).cloned().collect()
}

fn apply_insert(
    row: &SnapshotRow,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    if mode == SyncMode::Apply {
        repair_target.insert_row(row)?;
    }
    Ok(())
}

fn apply_update(
    row: &SnapshotRow,
    mode: SyncMode,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    if mode == SyncMode::Apply {
        repair_target.update_row(row)?;
    }
    Ok(())
}

impl<E> SyncRepairTarget for crate::target::TargetMySqlWriter<E>
where
    E: crate::target::TargetExecutor,
{
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        self.insert_rows(std::slice::from_ref(row))
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        crate::target::TargetMySqlWriter::update_row(self, row)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }
}

pub fn primary_key(values: Vec<String>) -> PrimaryKey {
    PrimaryKey::new(values)
}

pub struct MySqlSyncReader {
    config: crate::mysql_snapshot::MySqlConnectionConfig,
}

impl MySqlSyncReader {
    pub fn new(config: crate::mysql_snapshot::MySqlConnectionConfig) -> Self {
        Self { config }
    }
}

impl SyncTableReader for MySqlSyncReader {
    fn read_rows(&self, request: &SyncChunkRequest) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let sql = build_sync_select_sql(request);
        let output = run_mysql_query(&self.config, &sql)?;
        parse_sync_rows(&request.columns, &request.primary_key, &output)
    }
}

pub fn build_sync_select_sql(request: &SyncChunkRequest) -> String {
    let columns = quote_ident_list(&request.columns);
    let order_by = quote_ident_list(&request.primary_key);
    let bounds = sync_bounds(request);
    format!(
        "SELECT {columns} FROM {}{bounds} ORDER BY {order_by} LIMIT {}",
        quote_ident(&request.table),
        request.limit
    )
}

fn sync_bounds(request: &SyncChunkRequest) -> String {
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
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
}

fn run_mysql_query(
    config: &crate::mysql_snapshot::MySqlConnectionConfig,
    sql: &str,
) -> Result<String, TableSyncError> {
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
        .map_err(|error| TableSyncError::Read(format!("failed to run mariadb: {error}")))?;

    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(TableSyncError::Read(format!(
        "mariadb exited with {}: {}",
        output.status,
        stderr.trim()
    )))
}

fn parse_sync_rows(
    columns: &[String],
    primary_key: &[String],
    output: &str,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    output
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| parse_sync_row(columns, primary_key, line))
        .collect()
}

fn parse_sync_row(
    columns: &[String],
    primary_key: &[String],
    line: &str,
) -> Result<SnapshotRow, TableSyncError> {
    let fields = line.split('\t').map(str::to_string).collect::<Vec<_>>();
    if fields.len() != columns.len() {
        return Err(TableSyncError::Read(format!(
            "sync row has {} fields for {} columns",
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
) -> Result<Vec<String>, TableSyncError> {
    primary_key
        .iter()
        .map(|column| {
            values.get(column).cloned().ok_or_else(|| {
                TableSyncError::Read(format!("primary key column `{column}` missing from row"))
            })
        })
        .collect()
}

fn primary_key_after_predicate(columns: &[String], values: &[String]) -> String {
    primary_key_bound_predicate(columns, values, ">")
}

fn primary_key_at_or_before_predicate(columns: &[String], values: &[String]) -> String {
    format!(
        "NOT ({})",
        primary_key_bound_predicate(columns, values, ">")
    )
}

fn primary_key_bound_predicate(columns: &[String], values: &[String], operator: &str) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, _column)| primary_key_bound_branch(columns, values, index, operator))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn primary_key_bound_branch(
    columns: &[String],
    values: &[String],
    index: usize,
    operator: &str,
) -> String {
    let mut parts = Vec::new();
    for equal_index in 0..index {
        parts.push(format!(
            "{} = {}",
            quote_ident(&columns[equal_index]),
            quote_sql_literal(&values[equal_index])
        ));
    }
    parts.push(format!(
        "{} {operator} {}",
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
    use std::cell::RefCell;

    #[test]
    fn dry_run_reports_repairs_without_applying_them() {
        let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
        let target = FakeReader::new(vec![row("0", "extra"), row("1", "old")]);
        let mut repair_target = RecordingRepairTarget::default();

        let report = sync_table(
            &account_table(),
            10,
            SyncMode::DryRun,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        assert_eq!(report.inserts, 1);
        assert_eq!(report.updates, 1);
        assert_eq!(report.extra_target_rows, 1);
        assert!(repair_target.inserts.borrow().is_empty());
        assert!(repair_target.updates.borrow().is_empty());
    }

    #[test]
    fn apply_repairs_missing_and_different_target_rows() {
        let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
        let target = FakeReader::new(vec![row("1", "old")]);
        let mut repair_target = RecordingRepairTarget::default();

        let report = sync_table(
            &account_table(),
            10,
            SyncMode::Apply,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        assert_eq!(report.inserts, 1);
        assert_eq!(report.updates, 1);
        assert_eq!(
            repair_target.inserts.borrow().as_slice(),
            &[row("2", "bravo")]
        );
        assert_eq!(
            repair_target.updates.borrow().as_slice(),
            &[row("1", "alpha")]
        );
    }

    #[test]
    fn target_read_is_bounded_by_source_chunk_end() {
        let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo"), row("3", "coda")]);
        let target = FakeReader::new(vec![]);
        let mut repair_target = RecordingRepairTarget::default();

        sync_table(
            &account_table(),
            2,
            SyncMode::DryRun,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        let target_requests = target.requests.borrow();
        assert_eq!(target_requests[0].end_at, Some(vec!["2".to_string()]));
        assert_eq!(target_requests[1].start_after, Some(vec!["2".to_string()]));
        assert_eq!(target_requests[1].end_at, Some(vec!["3".to_string()]));
    }

    #[test]
    fn target_read_allows_extra_rows_inside_source_window() {
        let source = FakeReader::new(vec![row("4", "delta")]);
        let target = FakeReader::new(vec![
            row("1", "extra"),
            row("2", "extra"),
            row("3", "extra"),
            row("4", "delta"),
        ]);
        let mut repair_target = RecordingRepairTarget::default();

        let report = sync_table(
            &account_table(),
            1,
            SyncMode::DryRun,
            &source,
            &target,
            &mut repair_target,
        )
        .expect("sync report");

        assert_eq!(report.extra_target_rows, 3);
        assert!(target.requests.borrow().len() > 1);
    }

    #[test]
    fn builds_sync_select_with_start_and_end_bounds() {
        let sql = build_sync_select_sql(&SyncChunkRequest {
            table: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
            start_after: Some(vec!["10".to_string()]),
            end_at: Some(vec!["20".to_string()]),
            limit: 100,
        });

        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `accounts` WHERE (`id` > '10') AND NOT ((`id` > '20')) ORDER BY `id` LIMIT 100"
        );
    }

    fn account_table() -> SyncTable {
        SyncTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        }
    }

    fn row(id: &str, name: &str) -> SnapshotRow {
        SnapshotRow {
            primary_key: vec![id.to_string()],
            values: BTreeMap::from([
                ("id".to_string(), id.to_string()),
                ("name".to_string(), name.to_string()),
            ]),
        }
    }

    struct FakeReader {
        rows: Vec<SnapshotRow>,
        requests: RefCell<Vec<SyncChunkRequest>>,
    }

    impl FakeReader {
        fn new(rows: Vec<SnapshotRow>) -> Self {
            Self {
                rows,
                requests: RefCell::new(Vec::new()),
            }
        }
    }

    impl SyncTableReader for FakeReader {
        fn read_rows(
            &self,
            request: &SyncChunkRequest,
        ) -> Result<Vec<SnapshotRow>, TableSyncError> {
            self.requests.borrow_mut().push(request.clone());
            Ok(self
                .rows
                .iter()
                .filter(|row| row_in_window(row, request))
                .take(request.limit)
                .cloned()
                .collect())
        }
    }

    fn row_in_window(row: &SnapshotRow, request: &SyncChunkRequest) -> bool {
        let after_start = request
            .start_after
            .as_ref()
            .is_none_or(|start| row.primary_key > *start);
        let before_end = request
            .end_at
            .as_ref()
            .is_none_or(|end| row.primary_key <= *end);
        after_start && before_end
    }

    #[derive(Default)]
    struct RecordingRepairTarget {
        inserts: RefCell<Vec<SnapshotRow>>,
        updates: RefCell<Vec<SnapshotRow>>,
    }

    impl SyncRepairTarget for RecordingRepairTarget {
        fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
            self.inserts.borrow_mut().push(row.clone());
            Ok(())
        }

        fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
            self.updates.borrow_mut().push(row.clone());
            Ok(())
        }
    }
}
