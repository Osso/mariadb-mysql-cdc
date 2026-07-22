use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, MariaDbInventoryReader, SchemaInventory,
    TableInventory, build_inventory,
};
use crate::table_sync::{MySqlSyncRunProgressStore, SyncProgressStore};
use crate::{live, mysql_snapshot, table_sync};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const MAX_CATALOG_CONCURRENCY: usize = 4;
const DEFAULT_CHUNK_SIZE: usize = 10_000;
const DB_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct CatalogConnectionConfig {
    pub source: mysql_snapshot::MySqlConnectionConfig,
    pub target: live::TargetMySqlConfig,
}

#[derive(Clone, Debug)]
pub struct TableCatalogConfig {
    pub connections: CatalogConnectionConfig,
    pub syncable_output: PathBuf,
    pub non_syncable_output: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SyncCatalogConfig {
    pub connections: CatalogConnectionConfig,
    pub catalog: PathBuf,
    pub progress_table: String,
    pub run_id_prefix: String,
    pub chunk_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncableCatalog {
    pub tables: Vec<SyncableTableEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NonSyncableCatalog {
    pub tables: Vec<NonSyncableTableEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncableTableEntry {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
    pub estimated_source_rows: u64,
    pub parent_dependencies: Vec<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NonSyncableReason {
    MissingPrimaryKey,
    MissingTargetTable,
    IncompatibleSchema,
    UnsupportedGeneratedColumns,
    DependencyOnNonSyncable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NonSyncableTableEntry {
    pub name: String,
    pub estimated_source_rows: u64,
    pub reasons: Vec<NonSyncableReason>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Catalogs {
    pub syncable: Vec<SyncableTableEntry>,
    pub non_syncable: Vec<NonSyncableTableEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogTableFailure {
    pub table: String,
    pub reason: String,
}

#[derive(Clone, Debug)]
pub struct CatalogRunState {
    catalog: SyncableCatalog,
    max_concurrency: usize,
    external_active: BTreeSet<String>,
    external_active_count: usize,
    running: BTreeSet<String>,
    completed: BTreeSet<String>,
    failures: BTreeMap<String, String>,
}

impl CatalogRunState {
    pub fn new(
        catalog: SyncableCatalog,
        max_concurrency: usize,
        external_active: BTreeSet<String>,
        external_active_count: usize,
    ) -> Self {
        Self {
            catalog,
            max_concurrency,
            external_active,
            external_active_count,
            running: BTreeSet::new(),
            completed: BTreeSet::new(),
            failures: BTreeMap::new(),
        }
    }

    pub fn available_slots(&self) -> usize {
        self.max_concurrency
            .saturating_sub(self.external_active_count + self.running.len())
    }

    pub fn ready_tables(&self) -> Vec<&str> {
        self.catalog
            .tables
            .iter()
            .filter(|entry| !self.is_accounted_for(&entry.name))
            .filter(|entry| !self.external_active.contains(&entry.name))
            .filter(|entry| {
                entry
                    .parent_dependencies
                    .iter()
                    .all(|parent| self.completed.contains(parent))
            })
            .map(|entry| entry.name.as_str())
            .collect()
    }

    pub fn mark_running(&mut self, table: &str) {
        self.running.insert(table.to_string());
    }

    pub fn mark_completed(&mut self, table: &str) {
        self.running.remove(table);
        self.completed.insert(table.to_string());
    }

    pub fn mark_failed(&mut self, table: &str, reason: &str) {
        self.running.remove(table);
        self.failures.insert(table.to_string(), reason.to_string());
    }

    pub fn all_failures(&self) -> Vec<CatalogTableFailure> {
        let mut failures = self
            .failures
            .iter()
            .map(|(table, reason)| CatalogTableFailure {
                table: table.clone(),
                reason: reason.clone(),
            })
            .collect::<Vec<_>>();
        failures.extend(self.blocked_failures());
        failures
    }

    pub fn blocked_failures(&self) -> Vec<CatalogTableFailure> {
        self.catalog
            .tables
            .iter()
            .filter(|entry| !self.is_accounted_for(&entry.name))
            .filter_map(|entry| {
                entry.parent_dependencies.iter().find_map(|parent| {
                    self.failures.get(parent).map(|reason| CatalogTableFailure {
                        table: entry.name.clone(),
                        reason: format!("parent `{parent}` failed: {reason}"),
                    })
                })
            })
            .collect()
    }

    fn is_accounted_for(&self, table: &str) -> bool {
        self.running.contains(table)
            || self.completed.contains(table)
            || self.failures.contains_key(table)
    }
}

pub fn build_catalogs(
    source: &SchemaInventory,
    target: &SchemaInventory,
    estimated_rows: &BTreeMap<String, u64>,
) -> Catalogs {
    let (mut candidates, mut excluded) = classify_source_tables(source, target, estimated_rows);
    propagate_non_syncable_dependencies(&mut candidates, &mut excluded);
    Catalogs {
        syncable: sorted_syncable_entries(candidates),
        non_syncable: sorted_non_syncable_entries(excluded, estimated_rows),
    }
}

fn classify_source_tables(
    source: &SchemaInventory,
    target: &SchemaInventory,
    estimated_rows: &BTreeMap<String, u64>,
) -> (
    BTreeMap<String, SyncableTableEntry>,
    BTreeMap<String, BTreeSet<NonSyncableReason>>,
) {
    let target_tables = target
        .tables
        .iter()
        .map(|table| (table.name.as_str(), table))
        .collect::<BTreeMap<_, _>>();
    let mut candidates = BTreeMap::new();
    let mut excluded = BTreeMap::new();
    for source_table in source
        .tables
        .iter()
        .filter(|table| table.table_type == "BASE TABLE")
    {
        let reasons = exclusion_reasons(
            source_table,
            target_tables.get(source_table.name.as_str()).copied(),
        );
        if reasons.is_empty() {
            candidates.insert(
                source_table.name.clone(),
                syncable_entry(source, source_table, estimated_rows),
            );
        } else {
            excluded.insert(source_table.name.clone(), reasons);
        }
    }
    (candidates, excluded)
}

fn exclusion_reasons(
    source: &TableInventory,
    target: Option<&TableInventory>,
) -> BTreeSet<NonSyncableReason> {
    let mut reasons = BTreeSet::new();
    if source.primary_key.is_empty() {
        reasons.insert(NonSyncableReason::MissingPrimaryKey);
    }
    if source
        .columns
        .iter()
        .any(|column| column.generated.is_some())
    {
        reasons.insert(NonSyncableReason::UnsupportedGeneratedColumns);
    }
    match target {
        None => {
            reasons.insert(NonSyncableReason::MissingTargetTable);
        }
        Some(target) if !schemas_are_compatible(source, target) => {
            reasons.insert(NonSyncableReason::IncompatibleSchema);
        }
        Some(_) => {}
    }
    reasons
}

fn syncable_entry(
    inventory: &SchemaInventory,
    table: &TableInventory,
    estimated_rows: &BTreeMap<String, u64>,
) -> SyncableTableEntry {
    SyncableTableEntry {
        name: table.name.clone(),
        primary_key: table.primary_key.clone(),
        columns: writable_columns(table),
        estimated_source_rows: estimated_rows.get(&table.name).copied().unwrap_or(0),
        parent_dependencies: inventory
            .foreign_keys
            .iter()
            .filter(|foreign_key| foreign_key.table == table.name)
            .map(|foreign_key| foreign_key.referenced_table.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

fn propagate_non_syncable_dependencies(
    candidates: &mut BTreeMap<String, SyncableTableEntry>,
    excluded: &mut BTreeMap<String, BTreeSet<NonSyncableReason>>,
) {
    loop {
        let dependent = candidates
            .iter()
            .filter(|(_, entry)| {
                entry
                    .parent_dependencies
                    .iter()
                    .any(|parent| excluded.contains_key(parent))
            })
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        if dependent.is_empty() {
            return;
        }
        for name in dependent {
            candidates.remove(&name);
            excluded
                .entry(name)
                .or_default()
                .insert(NonSyncableReason::DependencyOnNonSyncable);
        }
    }
}

fn sorted_syncable_entries(
    candidates: BTreeMap<String, SyncableTableEntry>,
) -> Vec<SyncableTableEntry> {
    let mut entries = candidates.into_values().collect::<Vec<_>>();
    entries.sort_by_key(|entry| (entry.estimated_source_rows, entry.name.clone()));
    entries
}

fn sorted_non_syncable_entries(
    excluded: BTreeMap<String, BTreeSet<NonSyncableReason>>,
    estimated_rows: &BTreeMap<String, u64>,
) -> Vec<NonSyncableTableEntry> {
    let mut entries = excluded
        .into_iter()
        .map(|(name, reasons)| NonSyncableTableEntry {
            estimated_source_rows: estimated_rows.get(&name).copied().unwrap_or(0),
            name,
            reasons: reasons.into_iter().collect(),
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| (entry.estimated_source_rows, entry.name.clone()));
    entries
}

pub fn run_table_catalog_command(args: Vec<String>, usage: &str) {
    let config = parse_table_catalog_config(args).unwrap_or_else(|error| cli_error(error, usage));
    if let Err(error) = write_table_catalogs(&config) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

pub fn run_sync_catalog_command(args: Vec<String>, usage: &str) {
    let config = parse_sync_catalog_config(args).unwrap_or_else(|error| cli_error(error, usage));
    if let Err(error) = run_sync_catalog(&config) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn write_table_catalogs(config: &TableCatalogConfig) -> Result<(), String> {
    let source = read_inventory_source(&config.connections.source)?;
    let target = read_inventory_target(&config.connections.target)?;
    let estimated_rows = read_estimated_rows(&config.connections.source)?;
    let catalogs = build_catalogs(&source, &target, &estimated_rows);
    write_pretty_json(
        &config.syncable_output,
        &SyncableCatalog {
            tables: catalogs.syncable,
        },
    )?;
    write_pretty_json(
        &config.non_syncable_output,
        &NonSyncableCatalog {
            tables: catalogs.non_syncable,
        },
    )
}

fn run_sync_catalog(config: &SyncCatalogConfig) -> Result<(), String> {
    let bytes = fs::read(&config.catalog)
        .map_err(|error| format!("failed to read {}: {error}", config.catalog.display()))?;
    let catalog: SyncableCatalog = serde_json::from_slice(&bytes)
        .map_err(|error| format!("failed to decode {}: {error}", config.catalog.display()))?;
    validate_catalog(&catalog)?;
    MySqlSyncRunProgressStore::new(
        config.connections.target.clone(),
        config.progress_table.clone(),
    )
    .ensure()
    .map_err(|error| error.to_string())?;
    let mut state =
        CatalogRunState::new(catalog.clone(), MAX_CATALOG_CONCURRENCY, BTreeSet::new(), 0);
    let (sender, receiver) = mpsc::channel::<(String, Result<(), String>)>();

    loop {
        let statuses = read_run_statuses(config, &catalog)?;
        let catalog_workers_visible_in_progress = state
            .running
            .intersection(&statuses.external_active)
            .count();
        state.external_active = statuses.external_active;
        state.external_active_count = statuses
            .external_active_count
            .saturating_sub(catalog_workers_visible_in_progress);
        for table in statuses.completed {
            state.mark_completed(&table);
        }
        while let Ok((table, result)) = receiver.try_recv() {
            match result {
                Ok(()) => state.mark_completed(&table),
                Err(error) => state.mark_failed(&table, &error),
            }
        }

        let failures = state.all_failures();
        if !failures.is_empty() && state.running.is_empty() {
            return Err(format_catalog_failures(&failures));
        }
        if state.completed.len() == catalog.tables.len() {
            return Ok(());
        }

        let slots = state.available_slots();
        let ready = if failures.is_empty() {
            state.ready_tables()
        } else {
            Vec::new()
        };
        for table_name in ready
            .into_iter()
            .take(slots)
            .map(str::to_string)
            .collect::<Vec<_>>()
        {
            let entry = catalog
                .tables
                .iter()
                .find(|entry| entry.name == table_name)
                .cloned()
                .expect("ready catalog entry");
            state.mark_running(&table_name);
            let worker_config = config.clone();
            let worker_sender = sender.clone();
            thread::spawn(move || {
                let result = run_catalog_table(&worker_config, &entry);
                let _ = worker_sender.send((entry.name, result));
            });
        }

        if state.running.is_empty()
            && state.available_slots() > 0
            && state.ready_tables().is_empty()
        {
            return Err("catalog has unresolved or cyclic dependencies".to_string());
        }
        thread::sleep(Duration::from_secs(2));
    }
}

#[derive(Default)]
struct RunStatuses {
    external_active: BTreeSet<String>,
    completed: Vec<String>,
    external_active_count: usize,
}

fn read_run_statuses(
    config: &SyncCatalogConfig,
    catalog: &SyncableCatalog,
) -> Result<RunStatuses, String> {
    let mut connection = target_connection(&config.connections.target)?;
    let table_path = quote_identifier_path(&config.progress_table);
    let sql = format!(
        "SELECT run_id, table_name, status, COALESCE(last_error, ''), IS_USED_LOCK(SHA2(run_id, 256)) FROM {table_path} WHERE status IN ('running','completed','error')"
    );
    let rows = connection
        .query::<(String, String, String, String, Option<u64>), _>(sql)
        .map_err(|error| format!("failed to read catalog run status: {error}"))?;
    Ok(classify_run_statuses(catalog, &config.run_id_prefix, rows))
}

fn classify_run_statuses(
    catalog: &SyncableCatalog,
    run_id_prefix: &str,
    rows: Vec<(String, String, String, String, Option<u64>)>,
) -> RunStatuses {
    let catalog_names = catalog
        .tables
        .iter()
        .map(|entry| entry.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut statuses = RunStatuses::default();
    for (run_id, table, status, _error, lock_owner) in rows {
        if status == "running" && lock_owner.is_some() {
            statuses.external_active_count += 1;
            statuses.external_active.insert(table.clone());
        }
        let expected = deterministic_run_id(run_id_prefix, &table);
        if run_id == expected && catalog_names.contains(table.as_str()) && status == "completed" {
            statuses.completed.push(table);
        }
    }
    statuses
}

fn run_catalog_table(config: &SyncCatalogConfig, entry: &SyncableTableEntry) -> Result<(), String> {
    let sync_config = table_sync::SyncTableConfig {
        source: config.connections.source.clone(),
        target: config.connections.target.clone(),
        table: table_sync::SyncTable {
            name: entry.name.clone(),
            primary_key: entry.primary_key.clone(),
            columns: entry.columns.clone(),
        },
        chunk_size: config.chunk_size,
        mode: table_sync::SyncMode::Apply,
        progress_table: config.progress_table.clone(),
        run_id: deterministic_run_id(&config.run_id_prefix, &entry.name),
        start_after: None,
        end_at: None,
        max_deletes: Some(0),
        updated_since: None,
        plan_hash: None,
    };
    table_sync::run_sync_table(&sync_config)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn deterministic_run_id(prefix: &str, table: &str) -> String {
    format!("{prefix}-{table}")
}

fn validate_catalog(catalog: &SyncableCatalog) -> Result<(), String> {
    let names = catalog
        .tables
        .iter()
        .map(|entry| entry.name.as_str())
        .collect::<BTreeSet<_>>();
    if names.len() != catalog.tables.len() {
        return Err("catalog contains duplicate table names".to_string());
    }
    for entry in &catalog.tables {
        for parent in &entry.parent_dependencies {
            if !names.contains(parent.as_str()) {
                return Err(format!(
                    "catalog table `{}` depends on missing parent `{parent}`",
                    entry.name
                ));
            }
        }
    }
    Ok(())
}

fn schemas_are_compatible(source: &TableInventory, target: &TableInventory) -> bool {
    source.primary_key == target.primary_key && writable_columns(source) == writable_columns(target)
}

fn writable_columns(table: &TableInventory) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| column.generated.is_none())
        .map(|column| column.name.clone())
        .collect()
}

fn write_pretty_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("failed to encode {}: {error}", path.display()))?;
    bytes.push(b'\n');
    fs::write(path, bytes).map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn read_inventory_source(
    source: &mysql_snapshot::MySqlConnectionConfig,
) -> Result<SchemaInventory, String> {
    let reader = MariaDbInventoryReader::new(InventoryConfig {
        host: source.host.clone(),
        port: source.port,
        user: source.user.clone(),
        password: source.password.clone(),
        endpoint_role: InventoryEndpointRole::Source,
        ..InventoryConfig::default()
    });
    build_inventory(&source.database, &reader).map_err(|error| error.to_string())
}

fn read_inventory_target(target: &live::TargetMySqlConfig) -> Result<SchemaInventory, String> {
    let reader = MariaDbInventoryReader::new(InventoryConfig {
        host: target.host.clone(),
        port: target.port,
        user: target.user.clone(),
        password: target.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(target.tls_ca_file.clone()),
        ..InventoryConfig::default()
    });
    build_inventory(&target.database, &reader).map_err(|error| error.to_string())
}

fn read_estimated_rows(
    source: &mysql_snapshot::MySqlConnectionConfig,
) -> Result<BTreeMap<String, u64>, String> {
    let mut connection = source_connection(source)?;
    let sql = "SELECT TABLE_NAME, COALESCE(TABLE_ROWS, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME";
    connection
        .exec::<(String, u64), _, _>(sql, (&source.database,))
        .map(|rows| rows.into_iter().collect())
        .map_err(|error| format!("failed to read estimated source row counts: {error}"))
}

fn source_connection(config: &mysql_snapshot::MySqlConnectionConfig) -> Result<Conn, String> {
    let opts = Opts::from(
        OptsBuilder::default()
            .ip_or_hostname(Some(&config.host))
            .tcp_port(config.port)
            .user(Some(&config.user))
            .pass(Some(&config.password))
            .db_name(Some(&config.database))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(DB_TIMEOUT))
            .read_timeout(Some(DB_TIMEOUT))
            .write_timeout(Some(DB_TIMEOUT)),
    );
    Conn::new(opts).map_err(|error| format!("source connection failed: {error}"))
}

fn target_connection(config: &live::TargetMySqlConfig) -> Result<Conn, String> {
    let endpoint = format!("target `{}`:{}", config.host, config.port);
    let opts = Opts::from(
        OptsBuilder::default()
            .ip_or_hostname(Some(&config.host))
            .tcp_port(config.port)
            .user(Some(&config.user))
            .pass(Some(&config.password))
            .db_name(Some(&config.database))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(DB_TIMEOUT))
            .read_timeout(Some(DB_TIMEOUT))
            .write_timeout(Some(DB_TIMEOUT))
            .ssl_opts(crate::mysql_support::ssl_opts_from_ca(
                &endpoint,
                &config.host,
                &config.tls_ca_file,
            )?),
    );
    Conn::new(opts).map_err(|error| format!("target connection failed: {error}"))
}

fn parse_table_catalog_config(args: Vec<String>) -> Result<TableCatalogConfig, String> {
    let (connections, values) =
        parse_common_options(args, &["--syncable-output", "--non-syncable-output"])?;
    Ok(TableCatalogConfig {
        connections,
        syncable_output: required_path(&values, "--syncable-output")?,
        non_syncable_output: required_path(&values, "--non-syncable-output")?,
    })
}

fn parse_sync_catalog_config(args: Vec<String>) -> Result<SyncCatalogConfig, String> {
    let (connections, values) = parse_common_options(
        args,
        &[
            "--catalog",
            "--progress-table",
            "--run-id-prefix",
            "--chunk-size",
        ],
    )?;
    let run_id_prefix = required_value(&values, "--run-id-prefix")?;
    if run_id_prefix.is_empty() {
        return Err("--run-id-prefix must not be empty".to_string());
    }
    let chunk_size = values
        .get("--chunk-size")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| "--chunk-size must be an integer".to_string())?
        .unwrap_or(DEFAULT_CHUNK_SIZE);
    if chunk_size == 0 {
        return Err("--chunk-size must be greater than zero".to_string());
    }
    Ok(SyncCatalogConfig {
        connections,
        catalog: required_path(&values, "--catalog")?,
        progress_table: values
            .get("--progress-table")
            .cloned()
            .unwrap_or_else(|| "cdc.table_sync_runs".to_string()),
        run_id_prefix,
        chunk_size,
    })
}

fn parse_common_options(
    args: Vec<String>,
    extra_flags: &[&str],
) -> Result<(CatalogConnectionConfig, BTreeMap<String, String>), String> {
    let mut values = BTreeMap::new();
    let mut index = 0;
    while index < args.len() {
        let flag = args[index].as_str();
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;
        if !common_flags().contains(&flag) && !extra_flags.contains(&flag) {
            return Err(format!("unknown option: {flag}"));
        }
        values.insert(flag.to_string(), value.clone());
        index += 2;
    }
    let source_password_env = required_value(&values, "--source-password-env")?;
    let target_password_env = required_value(&values, "--target-password-env")?;
    let source = mysql_snapshot::MySqlConnectionConfig {
        host: required_value(&values, "--source-host")?,
        port: parse_port(&values, "--source-port", 3306)?,
        user: required_value(&values, "--source-user")?,
        password: crate::read_env_password(&source_password_env)?,
        database: required_value(&values, "--source-database")?,
    };
    let target = live::TargetMySqlConfig {
        host: required_value(&values, "--target-host")?,
        port: parse_port(&values, "--target-port", 3306)?,
        user: required_value(&values, "--target-user")?,
        password: crate::read_env_password(&target_password_env)?,
        database: required_value(&values, "--target-database")?,
        tls_ca_file: required_value(&values, "--target-tls-ca-file")?,
        insert_conflict_policy: live::InsertConflictPolicy::Error,
    };
    Ok((CatalogConnectionConfig { source, target }, values))
}

fn common_flags() -> &'static [&'static str] {
    &[
        "--source-host",
        "--source-port",
        "--source-user",
        "--source-password-env",
        "--source-database",
        "--target-host",
        "--target-port",
        "--target-user",
        "--target-password-env",
        "--target-database",
        "--target-tls-ca-file",
    ]
}

fn required_value(values: &BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .get(flag)
        .cloned()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{flag} is required"))
}

fn required_path(values: &BTreeMap<String, String>, flag: &str) -> Result<PathBuf, String> {
    required_value(values, flag).map(PathBuf::from)
}

fn parse_port(values: &BTreeMap<String, String>, flag: &str, default: u16) -> Result<u16, String> {
    values
        .get(flag)
        .map(|value| value.parse::<u16>())
        .transpose()
        .map_err(|_| format!("{flag} must be an integer"))
        .map(|value| value.unwrap_or(default))
}

fn quote_identifier_path(identifier: &str) -> String {
    identifier
        .split('.')
        .map(crate::mysql_support::quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

fn format_catalog_failures(failures: &[CatalogTableFailure]) -> String {
    failures
        .iter()
        .map(|failure| format!("{}: {}", failure.table, failure.reason))
        .collect::<Vec<_>>()
        .join("; ")
}

fn cli_error(error: String, usage: &str) -> ! {
    eprintln!("{error}\n\n{usage}");
    std::process::exit(2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{ColumnInventory, ForeignKeyInventory, SchemaInventory, TableInventory};

    #[test]
    fn catalogs_pk_tables_by_estimated_rows_and_propagates_exclusions() {
        let source = inventory(
            vec![
                table("child", vec![column("id"), column("parent_id")], vec!["id"]),
                table("missing_pk", vec![column("value")], vec![]),
                table("parent", vec![column("id")], vec!["id"]),
            ],
            vec![foreign_key("child", "parent")],
        );
        let target = inventory(
            vec![
                table("child", vec![column("id"), column("parent_id")], vec!["id"]),
                table("missing_pk", vec![column("value")], vec![]),
            ],
            vec![],
        );
        let rows = BTreeMap::from([
            ("child".to_string(), 2),
            ("missing_pk".to_string(), 1),
            ("parent".to_string(), 3),
        ]);

        let catalogs = build_catalogs(&source, &target, &rows);

        assert!(catalogs.syncable.is_empty());
        assert_eq!(
            catalogs
                .non_syncable
                .iter()
                .map(|entry| (&entry.name, &entry.reasons))
                .collect::<Vec<_>>(),
            vec![
                (
                    &"missing_pk".to_string(),
                    &vec![NonSyncableReason::MissingPrimaryKey]
                ),
                (
                    &"child".to_string(),
                    &vec![NonSyncableReason::DependencyOnNonSyncable]
                ),
                (
                    &"parent".to_string(),
                    &vec![NonSyncableReason::MissingTargetTable]
                ),
            ]
        );
    }

    #[test]
    fn compatible_writable_columns_allow_target_type_widening() {
        let source_table = table(
            "items",
            vec![
                typed_column("id", "int", false),
                typed_column("name", "varchar(20)", true),
            ],
            vec!["id"],
        );
        let target_table = table(
            "items",
            vec![
                typed_column("id", "bigint", false),
                typed_column("name", "varchar(80)", false),
            ],
            vec!["id"],
        );
        let catalogs = build_catalogs(
            &inventory(vec![source_table], vec![]),
            &inventory(vec![target_table], vec![]),
            &BTreeMap::from([("items".to_string(), 5)]),
        );
        assert_eq!(
            catalogs
                .syncable
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["items"]
        );
    }

    #[test]
    fn scheduler_reserves_external_slots_and_orders_ready_tables_by_size() {
        let catalog = SyncableCatalog {
            tables: vec![
                entry("tiny", 1, &[]),
                entry("child", 2, &["parent"]),
                entry("parent", 3, &[]),
                entry("large", 9, &[]),
            ],
        };
        let state = CatalogRunState::new(catalog, 4, BTreeSet::from(["guests".to_string()]), 1);
        assert_eq!(state.ready_tables(), vec!["tiny", "parent", "large"]);
        assert_eq!(state.available_slots(), 3);
    }

    #[test]
    fn external_run_count_reserves_slots_even_when_tables_repeat() {
        let catalog = SyncableCatalog {
            tables: vec![entry("tiny", 1, &[])],
        };
        let state = CatalogRunState::new(catalog, 4, BTreeSet::from(["guests".to_string()]), 3);
        assert_eq!(state.available_slots(), 1);
    }

    #[test]
    fn direct_failure_is_reported_with_blocked_dependents() {
        let catalog = SyncableCatalog {
            tables: vec![entry("parent", 1, &[]), entry("child", 2, &["parent"])],
        };
        let mut state = CatalogRunState::new(catalog, 4, BTreeSet::new(), 0);
        state.mark_failed("parent", "boom");
        assert_eq!(
            state.all_failures(),
            vec![
                CatalogTableFailure {
                    table: "parent".into(),
                    reason: "boom".into()
                },
                CatalogTableFailure {
                    table: "child".into(),
                    reason: "parent `parent` failed: boom".into()
                },
            ]
        );
    }

    #[test]
    fn failed_parent_blocks_child_explicitly() {
        let catalog = SyncableCatalog {
            tables: vec![entry("parent", 1, &[]), entry("child", 2, &["parent"])],
        };
        let mut state = CatalogRunState::new(catalog, 4, BTreeSet::new(), 0);
        state.mark_failed("parent", "boom");
        assert_eq!(
            state.blocked_failures(),
            vec![CatalogTableFailure {
                table: "child".into(),
                reason: "parent `parent` failed: boom".into()
            }]
        );
    }

    #[test]
    fn stale_running_rows_without_advisory_locks_do_not_consume_slots() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let statuses = classify_run_statuses(
            &catalog,
            "batch",
            vec![(
                "other-a".into(),
                "a".into(),
                "running".into(),
                String::new(),
                None,
            )],
        );
        assert_eq!(statuses.external_active_count, 0);
        assert!(statuses.external_active.is_empty());
    }

    #[test]
    fn deterministic_json_and_run_ids_are_stable() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let first = serde_json::to_string_pretty(&catalog).expect("json");
        let second = serde_json::to_string_pretty(&catalog).expect("json");
        assert_eq!(first, second);
        assert_eq!(
            deterministic_run_id("full-20260722", "a"),
            "full-20260722-a"
        );
    }

    fn inventory(
        tables: Vec<TableInventory>,
        foreign_keys: Vec<ForeignKeyInventory>,
    ) -> SchemaInventory {
        SchemaInventory {
            schema: "db".into(),
            tables,
            indexes: vec![],
            foreign_keys,
            views: vec![],
            triggers: vec![],
            routines: vec![],
            events: vec![],
        }
    }
    fn table(name: &str, columns: Vec<ColumnInventory>, pk: Vec<&str>) -> TableInventory {
        TableInventory {
            name: name.into(),
            table_type: "BASE TABLE".into(),
            engine: None,
            collation: None,
            primary_key: pk.into_iter().map(str::to_string).collect(),
            columns,
        }
    }
    fn column(name: &str) -> ColumnInventory {
        typed_column(name, "bigint", false)
    }
    fn typed_column(name: &str, column_type: &str, is_nullable: bool) -> ColumnInventory {
        ColumnInventory {
            name: name.into(),
            ordinal_position: 1,
            column_type: column_type.into(),
            data_type: column_type.split('(').next().unwrap_or(column_type).into(),
            is_nullable,
            default_value: None,
            extra: String::new(),
            comment: String::new(),
            generated: None,
        }
    }
    fn foreign_key(table: &str, parent: &str) -> ForeignKeyInventory {
        ForeignKeyInventory {
            table: table.into(),
            name: format!("fk_{table}_{parent}"),
            columns: vec!["parent_id".into()],
            referenced_table: parent.into(),
            referenced_columns: vec!["id".into()],
        }
    }
    fn entry(name: &str, rows: u64, parents: &[&str]) -> SyncableTableEntry {
        SyncableTableEntry {
            name: name.into(),
            primary_key: vec!["id".into()],
            columns: vec!["id".into()],
            estimated_source_rows: rows,
            parent_dependencies: parents.iter().map(|v| (*v).into()).collect(),
        }
    }
}
