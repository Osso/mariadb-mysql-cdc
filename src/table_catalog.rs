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
        self.failures.remove(table);
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
        let mut blocked = BTreeMap::<String, String>::new();
        loop {
            let mut changed = false;
            for entry in &self.catalog.tables {
                if self.is_accounted_for(&entry.name) || blocked.contains_key(&entry.name) {
                    continue;
                }
                if let Some((parent, reason)) =
                    entry.parent_dependencies.iter().find_map(|parent| {
                        self.failures
                            .get(parent)
                            .or_else(|| blocked.get(parent))
                            .map(|reason| (parent, reason))
                    })
                {
                    blocked.insert(
                        entry.name.clone(),
                        format!("parent `{parent}` failed: {reason}"),
                    );
                    changed = true;
                }
            }
            if !changed {
                return blocked
                    .into_iter()
                    .map(|(table, reason)| CatalogTableFailure { table, reason })
                    .collect();
            }
        }
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
            .filter(|foreign_key| {
                foreign_key.table == table.name && foreign_key.referenced_table != table.name
            })
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
    let catalog = read_syncable_catalog(&config.catalog)?;
    validate_catalog(&catalog)?;
    validate_catalog_run_ids(&catalog, &config.run_id_prefix)?;
    ensure_progress_table(config)?;
    let mut state =
        CatalogRunState::new(catalog.clone(), MAX_CATALOG_CONCURRENCY, BTreeSet::new(), 0);
    let (sender, receiver) = mpsc::channel::<(String, Result<(), String>)>();

    loop {
        refresh_run_state(config, &catalog, &receiver, &mut state)?;
        let failures = state.all_failures();
        if !failures.is_empty() && owned_catalog_work_has_settled(&state) {
            return Err(format_catalog_failures(&failures));
        }
        if state.completed.len() == catalog.tables.len() {
            return Ok(());
        }
        spawn_ready_tables(config, &catalog, &sender, &mut state)?;
        if catalog_dependencies_are_stuck(&state) {
            return Err("catalog has unresolved or cyclic dependencies".to_string());
        }
        thread::sleep(Duration::from_secs(2));
    }
}

fn read_syncable_catalog(path: &Path) -> Result<SyncableCatalog, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("failed to decode {}: {error}", path.display()))
}

fn ensure_progress_table(config: &SyncCatalogConfig) -> Result<(), String> {
    MySqlSyncRunProgressStore::new(
        config.connections.target.clone(),
        config.progress_table.clone(),
    )
    .ensure()
    .map_err(|error| error.to_string())
}

fn refresh_run_state(
    config: &SyncCatalogConfig,
    catalog: &SyncableCatalog,
    receiver: &mpsc::Receiver<(String, Result<(), String>)>,
    state: &mut CatalogRunState,
) -> Result<(), String> {
    let statuses = read_run_statuses(config, catalog)?;
    let visible_catalog_workers = state
        .running
        .intersection(&statuses.external_active)
        .count();
    state.external_active = statuses.external_active;
    state.external_active_count = statuses
        .external_active_count
        .saturating_sub(visible_catalog_workers);
    for table in statuses.completed {
        state.mark_completed(&table);
    }
    while let Ok((table, result)) = receiver.try_recv() {
        match result {
            Ok(()) => state.mark_completed(&table),
            Err(error) => state.mark_failed(&table, &error),
        }
    }
    Ok(())
}

fn spawn_ready_tables(
    config: &SyncCatalogConfig,
    catalog: &SyncableCatalog,
    sender: &mpsc::Sender<(String, Result<(), String>)>,
    state: &mut CatalogRunState,
) -> Result<(), String> {
    let table_names = state
        .ready_tables()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    for table_name in table_names {
        if state.available_slots() == 0 {
            return Ok(());
        }
        let active_count = state.external_active_count + state.running.len();
        let Some(reservation) = reserve_catalog_worker(config, &table_name, active_count)? else {
            continue;
        };
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
            let _reservation = reservation;
            let result = run_catalog_table_reserved(&worker_config, &entry);
            let _ = worker_sender.send((entry.name, result));
        });
    }
    Ok(())
}

pub(crate) struct SyncWorkerReservation {
    _connection: Conn,
}

#[cfg(test)]
trait NamedLockSet {
    type Reservation;

    fn try_reserve(&mut self, names: &[String]) -> Result<Option<Self::Reservation>, String>;
}

#[cfg(test)]
fn acquire_sync_reservation<L: NamedLockSet>(
    locks: &mut L,
    server_namespace: &str,
    database: &str,
    table: &str,
) -> Result<Option<L::Reservation>, String> {
    for slot in 0..MAX_CATALOG_CONCURRENCY {
        let names = vec![
            sync_table_lock_name(server_namespace, database, table),
            catalog_slot_lock_name(server_namespace, slot),
        ];
        if let Some(reservation) = locks.try_reserve(&names)? {
            return Ok(Some(reservation));
        }
    }
    Ok(None)
}

pub(crate) fn reserve_sync_worker(
    target: &live::TargetMySqlConfig,
    progress_table: &str,
    table: &str,
) -> Result<Option<SyncWorkerReservation>, String> {
    reserve_sync_worker_with_active_count(target, progress_table, table, 0)
}

fn reserve_sync_worker_with_active_count(
    target: &live::TargetMySqlConfig,
    progress_table: &str,
    table: &str,
    active_count: usize,
) -> Result<Option<SyncWorkerReservation>, String> {
    if active_count >= MAX_CATALOG_CONCURRENCY {
        return Ok(None);
    }
    let mut connection = target_connection(target)?;
    let server_namespace = sync_server_lock_namespace(&target.host, target.port);
    let admission_lock = catalog_admission_lock_name(&server_namespace);
    if !acquire_named_lock(&mut connection, &admission_lock)? {
        return Ok(None);
    }
    let result = reserve_under_admission_lock(
        &mut connection,
        &server_namespace,
        &target.database,
        progress_table,
        table,
        active_count,
    );
    release_named_lock(&mut connection, &admission_lock)?;
    result.map(|reserved| {
        reserved.then_some(SyncWorkerReservation {
            _connection: connection,
        })
    })
}

fn reserve_under_admission_lock(
    connection: &mut Conn,
    server_namespace: &str,
    database: &str,
    progress_table: &str,
    table: &str,
    active_count: usize,
) -> Result<bool, String> {
    let occupied_slots = count_occupied_slots(connection, server_namespace)?;
    let legacy_runs =
        count_legacy_active_runs(connection, server_namespace, database, progress_table)?;
    if !admission_has_capacity(legacy_runs, occupied_slots, active_count) {
        return Ok(false);
    }
    let table_lock = sync_table_lock_name(server_namespace, database, table);
    if !acquire_named_lock(connection, &table_lock)? {
        return Ok(false);
    }
    for slot in 0..MAX_CATALOG_CONCURRENCY {
        let slot_lock = catalog_slot_lock_name(server_namespace, slot);
        if acquire_named_lock(connection, &slot_lock)? {
            return Ok(true);
        }
    }
    release_named_lock(connection, &table_lock)?;
    Ok(false)
}

fn count_occupied_slots(connection: &mut Conn, server_namespace: &str) -> Result<usize, String> {
    let mut occupied = 0;
    for slot in 0..MAX_CATALOG_CONCURRENCY {
        let name = catalog_slot_lock_name(server_namespace, slot);
        if named_lock_owner(connection, &name)?.is_some() {
            occupied += 1;
        }
    }
    Ok(occupied)
}

fn count_legacy_active_runs(
    connection: &mut Conn,
    server_namespace: &str,
    database: &str,
    progress_table: &str,
) -> Result<usize, String> {
    let table_path = quote_identifier_path(progress_table);
    let sql = format!(
        "SELECT run_id, table_name, IS_USED_LOCK(SHA2(run_id,256)) FROM {table_path} WHERE status IN ('running','complete','error')"
    );
    let rows = connection
        .query::<(String, String, Option<u64>), _>(sql)
        .map_err(|error| format!("failed to count active table sync runs: {error}"))?;
    let mut count = 0;
    for (_run_id, table, owner) in rows {
        if owner.is_none() {
            continue;
        }
        let table_lock = sync_table_lock_name(server_namespace, database, &table);
        if named_lock_owner(connection, &table_lock)?.is_none() {
            count += 1;
        }
    }
    Ok(count)
}

fn admission_has_capacity(
    legacy_active_runs: usize,
    occupied_slots: usize,
    observed_active_count: usize,
) -> bool {
    legacy_active_runs + occupied_slots < MAX_CATALOG_CONCURRENCY
        && observed_active_count < MAX_CATALOG_CONCURRENCY
}

fn reserve_catalog_worker(
    config: &SyncCatalogConfig,
    table: &str,
    active_count: usize,
) -> Result<Option<SyncWorkerReservation>, String> {
    reserve_sync_worker_with_active_count(
        &config.connections.target,
        &config.progress_table,
        table,
        active_count,
    )
}

fn sync_server_lock_namespace(host: &str, port: u16) -> String {
    format!("{}:{port}", host.to_ascii_lowercase())
}

fn sync_table_lock_name(server_namespace: &str, database: &str, table: &str) -> String {
    format!("sync-table:{server_namespace}:{database}:{table}")
}

fn catalog_slot_lock_name(server_namespace: &str, slot: usize) -> String {
    format!("sync-table-slot:{server_namespace}:{slot}")
}

fn catalog_admission_lock_name(server_namespace: &str) -> String {
    format!("sync-table-admission:{server_namespace}")
}

fn acquire_named_lock(connection: &mut Conn, name: &str) -> Result<bool, String> {
    connection
        .exec_first::<u8, _, _>("SELECT GET_LOCK(SHA2(?,256),0)", (name,))
        .map(|value| value == Some(1))
        .map_err(|error| format!("failed to reserve catalog lock `{name}`: {error}"))
}

fn release_named_lock(connection: &mut Conn, name: &str) -> Result<(), String> {
    connection
        .exec_drop("SELECT RELEASE_LOCK(SHA2(?,256))", (name,))
        .map_err(|error| format!("failed to release catalog lock `{name}`: {error}"))
}

fn named_lock_owner(connection: &mut Conn, name: &str) -> Result<Option<u64>, String> {
    connection
        .exec_first("SELECT IS_USED_LOCK(SHA2(?,256))", (name,))
        .map_err(|error| format!("failed to inspect catalog lock `{name}`: {error}"))
}

fn owned_catalog_work_has_settled(state: &CatalogRunState) -> bool {
    state.running.is_empty() && state.ready_tables().is_empty()
}

fn catalog_dependencies_are_stuck(state: &CatalogRunState) -> bool {
    let waiting_for_catalog_table = state.catalog.tables.iter().any(|entry| {
        !state.is_accounted_for(&entry.name) && state.external_active.contains(&entry.name)
    });
    state.running.is_empty()
        && !waiting_for_catalog_table
        && state.available_slots() > 0
        && state.ready_tables().is_empty()
        && state.all_failures().is_empty()
}

#[derive(Debug, Default)]
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
        "SELECT run_id, table_name, run_spec_json, status, COALESCE(last_error, ''), IS_USED_LOCK(SHA2(run_id, 256)) FROM {table_path} WHERE status IN ('running','complete','error')"
    );
    let rows = connection
        .query::<(String, String, String, String, String, Option<u64>), _>(sql)
        .map_err(|error| format!("failed to read catalog run status: {error}"))?;
    let expected_specs = expected_catalog_run_specs(config, catalog)?;
    classify_run_statuses(catalog, &config.run_id_prefix, &expected_specs, rows)
}

fn expected_catalog_run_specs(
    config: &SyncCatalogConfig,
    catalog: &SyncableCatalog,
) -> Result<BTreeMap<String, String>, String> {
    catalog
        .tables
        .iter()
        .map(|entry| {
            let sync_config = catalog_table_sync_config(config, entry);
            table_sync::expected_sync_run_spec_json(&sync_config)
                .map(|spec| (entry.name.clone(), spec))
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn classify_run_statuses(
    catalog: &SyncableCatalog,
    run_id_prefix: &str,
    expected_specs: &BTreeMap<String, String>,
    rows: Vec<(String, String, String, String, String, Option<u64>)>,
) -> Result<RunStatuses, String> {
    let catalog_names = catalog
        .tables
        .iter()
        .map(|entry| entry.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut statuses = RunStatuses::default();
    for (run_id, table, run_spec_json, status, _error, lock_owner) in rows {
        if lock_owner.is_some() {
            statuses.external_active_count += 1;
            statuses.external_active.insert(table.clone());
        }
        let expected = deterministic_run_id(run_id_prefix, &table);
        if run_id == expected && catalog_names.contains(table.as_str()) && status == "complete" {
            if expected_specs.get(&table) != Some(&run_spec_json) {
                return Err(format!(
                    "run id `{run_id}` already exists with a different immutable specification"
                ));
            }
            statuses.completed.push(table);
        }
    }
    Ok(statuses)
}

fn run_catalog_table_reserved(
    config: &SyncCatalogConfig,
    entry: &SyncableTableEntry,
) -> Result<(), String> {
    let sync_config = catalog_table_sync_config(config, entry);
    table_sync::run_sync_table_reserved(&sync_config)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn catalog_table_sync_config(
    config: &SyncCatalogConfig,
    entry: &SyncableTableEntry,
) -> table_sync::SyncTableConfig {
    table_sync::SyncTableConfig {
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
    }
}

fn deterministic_run_id(prefix: &str, table: &str) -> String {
    format!("{prefix}-{table}")
}

fn validate_catalog_run_ids(catalog: &SyncableCatalog, prefix: &str) -> Result<(), String> {
    for entry in &catalog.tables {
        let run_id = deterministic_run_id(prefix, &entry.name);
        if run_id.len() > 128 {
            return Err(format!(
                "generated run id for table `{}` is {} bytes; cdc.table_sync_runs.run_id allows at most 128",
                entry.name,
                run_id.len()
            ));
        }
    }
    Ok(())
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
    source.primary_key == target.primary_key
        && compatible_character_set(source.collation.as_deref(), target.collation.as_deref())
        && writable_columns(source) == writable_columns(target)
        && source
            .columns
            .iter()
            .filter(|column| column.generated.is_none())
            .zip(
                target
                    .columns
                    .iter()
                    .filter(|column| column.generated.is_none()),
            )
            .all(|(source, target)| column_type_is_compatible(source, target))
}

fn compatible_character_set(source: Option<&str>, target: Option<&str>) -> bool {
    source.map(collation_character_set) == target.map(collation_character_set)
}

fn collation_character_set(collation: &str) -> &str {
    collation.split('_').next().unwrap_or(collation)
}

fn column_type_is_compatible(
    source: &crate::inventory::ColumnInventory,
    target: &crate::inventory::ColumnInventory,
) -> bool {
    if source.is_nullable && !target.is_nullable {
        return false;
    }
    if source.character_set != target.character_set || source.collation != target.collation {
        return false;
    }
    if source.data_type == target.data_type {
        if integer_rank(&source.data_type) > 0 {
            return source.column_type.contains("unsigned")
                == target.column_type.contains("unsigned");
        }
        return if matches!(
            source.data_type.as_str(),
            "char" | "varchar" | "binary" | "varbinary"
        ) {
            variable_length_capacity(&target.column_type)
                >= variable_length_capacity(&source.column_type)
        } else {
            source.column_type == target.column_type
        };
    }
    integer_rank(&target.data_type) >= integer_rank(&source.data_type)
        && integer_rank(&source.data_type) > 0
        && source.column_type.contains("unsigned") == target.column_type.contains("unsigned")
}

fn variable_length_capacity(column_type: &str) -> usize {
    column_type
        .split_once('(')
        .and_then(|(_, suffix)| suffix.split(')').next())
        .and_then(|value| value.split(',').next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(usize::MAX)
}

fn integer_rank(data_type: &str) -> usize {
    match data_type {
        "tinyint" => 1,
        "smallint" => 2,
        "mediumint" => 3,
        "int" | "integer" => 4,
        "bigint" => 5,
        _ => 0,
    }
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
    fn completed_status_clears_prior_worker_failure() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let mut state = CatalogRunState::new(catalog, 4, BTreeSet::new(), 0);
        state.mark_failed("a", "temporary");
        state.mark_completed("a");
        assert!(state.all_failures().is_empty());
    }

    #[test]
    fn unrelated_tables_remain_ready_after_failure() {
        let catalog = SyncableCatalog {
            tables: vec![entry("failed", 1, &[]), entry("independent", 2, &[])],
        };
        let mut state = CatalogRunState::new(catalog, 4, BTreeSet::new(), 0);
        state.mark_failed("failed", "boom");
        assert_eq!(state.ready_tables(), vec!["independent"]);
    }

    #[test]
    fn external_activity_is_waiting_not_dependency_deadlock() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let state = CatalogRunState::new(catalog, 4, BTreeSet::from(["a".into()]), 1);
        assert!(!catalog_dependencies_are_stuck(&state));
    }

    #[test]
    fn unrelated_external_sync_does_not_delay_owned_failure_return() {
        let catalog = SyncableCatalog {
            tables: vec![entry("parent", 1, &[]), entry("child", 2, &["parent"])],
        };
        let mut state = CatalogRunState::new(catalog, 4, BTreeSet::from(["unrelated".into()]), 1);
        state.mark_failed("parent", "boom");

        assert!(owned_catalog_work_has_settled(&state));
    }

    #[test]
    fn unrelated_external_sync_does_not_hide_dependency_cycle() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &["b"]), entry("b", 2, &["a"])],
        };
        let state = CatalogRunState::new(catalog, 4, BTreeSet::from(["unrelated".into()]), 1);

        assert!(catalog_dependencies_are_stuck(&state));
    }

    #[test]
    fn nullable_source_requires_nullable_target() {
        let source_table = table(
            "items",
            vec![typed_column("id", "bigint", true)],
            vec!["id"],
        );
        let target_table = table(
            "items",
            vec![typed_column("id", "bigint", false)],
            vec!["id"],
        );
        let catalogs = build_catalogs(
            &inventory(vec![source_table], vec![]),
            &inventory(vec![target_table], vec![]),
            &BTreeMap::new(),
        );
        assert_eq!(
            catalogs.non_syncable[0].reasons,
            vec![NonSyncableReason::IncompatibleSchema]
        );
    }

    #[test]
    fn incompatible_column_character_sets_are_excluded_when_table_defaults_match() {
        let mut source_name = typed_column("name", "varchar(20)", true);
        source_name.character_set = Some("utf8mb4".into());
        source_name.collation = Some("utf8mb4_unicode_ci".into());
        let mut target_name = source_name.clone();
        target_name.character_set = Some("latin1".into());
        target_name.collation = Some("latin1_swedish_ci".into());
        let mut source_table = table("items", vec![column("id"), source_name], vec!["id"]);
        source_table.collation = Some("utf8mb4_unicode_ci".into());
        let mut target_table = table("items", vec![column("id"), target_name], vec!["id"]);
        target_table.collation = source_table.collation.clone();

        let catalogs = build_catalogs(
            &inventory(vec![source_table], vec![]),
            &inventory(vec![target_table], vec![]),
            &BTreeMap::new(),
        );

        assert_eq!(
            catalogs.non_syncable[0].reasons,
            vec![NonSyncableReason::IncompatibleSchema]
        );
    }

    #[test]
    fn incompatible_character_sets_are_excluded() {
        let mut source_table = table(
            "items",
            vec![typed_column("id", "bigint", false)],
            vec!["id"],
        );
        source_table.collation = Some("utf8mb4_unicode_ci".into());
        let mut target_table = source_table.clone();
        target_table.collation = Some("latin1_swedish_ci".into());
        let catalogs = build_catalogs(
            &inventory(vec![source_table], vec![]),
            &inventory(vec![target_table], vec![]),
            &BTreeMap::new(),
        );
        assert_eq!(
            catalogs.non_syncable[0].reasons,
            vec![NonSyncableReason::IncompatibleSchema]
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
                typed_column("name", "varchar(80)", true),
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
    fn integer_display_width_differences_are_compatible() {
        let source_table = table(
            "items",
            vec![typed_column("id", "int(18) unsigned", false)],
            vec!["id"],
        );
        let target_table = table(
            "items",
            vec![typed_column("id", "int unsigned", false)],
            vec!["id"],
        );
        let catalogs = build_catalogs(
            &inventory(vec![source_table], vec![]),
            &inventory(vec![target_table], vec![]),
            &BTreeMap::new(),
        );
        assert_eq!(catalogs.syncable.len(), 1);
    }

    #[test]
    fn incompatible_numeric_narrowing_is_excluded() {
        let source_table = table(
            "items",
            vec![typed_column("id", "bigint", false)],
            vec!["id"],
        );
        let target_table = table("items", vec![typed_column("id", "int", false)], vec!["id"]);
        let catalogs = build_catalogs(
            &inventory(vec![source_table], vec![]),
            &inventory(vec![target_table], vec![]),
            &BTreeMap::new(),
        );
        assert_eq!(
            catalogs.non_syncable[0].reasons,
            vec![NonSyncableReason::IncompatibleSchema]
        );
    }

    #[test]
    fn self_referencing_fk_does_not_block_table_start() {
        let source = inventory(
            vec![table(
                "nodes",
                vec![column("id"), column("parent_id")],
                vec!["id"],
            )],
            vec![foreign_key("nodes", "nodes")],
        );
        let target = inventory(
            vec![table(
                "nodes",
                vec![column("id"), column("parent_id")],
                vec!["id"],
            )],
            vec![],
        );
        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());
        assert!(catalogs.syncable[0].parent_dependencies.is_empty());
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
    fn blocked_failure_reporting_is_transitive() {
        let catalog = SyncableCatalog {
            tables: vec![
                entry("a", 1, &[]),
                entry("b", 2, &["a"]),
                entry("c", 3, &["b"]),
            ],
        };
        let mut state = CatalogRunState::new(catalog, 4, BTreeSet::new(), 0);
        state.mark_failed("a", "boom");
        assert_eq!(
            state
                .all_failures()
                .iter()
                .map(|failure| failure.table.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
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
    fn lock_active_error_rows_consume_slots() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let statuses = classify_run_statuses(
            &catalog,
            "batch",
            &BTreeMap::new(),
            vec![(
                "batch-a".into(),
                "a".into(),
                "spec".into(),
                "error".into(),
                "temporary".into(),
                Some(7),
            )],
        )
        .expect("active error run");
        assert_eq!(statuses.external_active_count, 1);
        assert!(statuses.external_active.contains("a"));
    }

    #[test]
    fn complete_catalog_children_remain_terminal_after_restart() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let expected_specs = BTreeMap::from([("a".to_string(), "expected".to_string())]);
        let statuses = classify_run_statuses(
            &catalog,
            "batch",
            &expected_specs,
            vec![(
                "batch-a".into(),
                "a".into(),
                "expected".into(),
                "complete".into(),
                String::new(),
                None,
            )],
        )
        .expect("matching complete run");
        assert_eq!(statuses.completed, vec!["a"]);
    }

    #[test]
    fn complete_catalog_child_with_changed_spec_fails_closed() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let expected_specs = BTreeMap::from([("a".to_string(), "expected".to_string())]);
        let error = classify_run_statuses(
            &catalog,
            "batch",
            &expected_specs,
            vec![(
                "batch-a".into(),
                "a".into(),
                "stale".into(),
                "complete".into(),
                String::new(),
                None,
            )],
        )
        .expect_err("mismatched completed spec");

        assert!(error.contains("different immutable specification"));
    }

    #[test]
    fn stale_running_rows_without_advisory_locks_do_not_consume_slots() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let statuses = classify_run_statuses(
            &catalog,
            "batch",
            &BTreeMap::new(),
            vec![(
                "other-a".into(),
                "a".into(),
                "spec".into(),
                "running".into(),
                String::new(),
                None,
            )],
        )
        .expect("stale run");
        assert_eq!(statuses.external_active_count, 0);
        assert!(statuses.external_active.is_empty());
    }

    #[test]
    fn slot_lock_namespace_is_shared_across_databases_on_one_server() {
        let first = sync_server_lock_namespace("mysql.example.com", 25060);
        let second = sync_server_lock_namespace("mysql.example.com", 25060);

        assert_eq!(
            catalog_slot_lock_name(&first, 2),
            catalog_slot_lock_name(&second, 2)
        );
        assert_eq!(
            catalog_admission_lock_name(&first),
            catalog_admission_lock_name(&second)
        );
    }

    #[test]
    fn admission_counts_legacy_run_locks_and_new_slot_reservations() {
        assert!(!admission_has_capacity(1, 3, 0));
        assert!(!admission_has_capacity(0, 3, 4));
        assert!(admission_has_capacity(1, 2, 3));
    }

    #[test]
    fn direct_and_catalog_reservations_cannot_overlap_the_same_table() {
        let mut locks = TestNamedLocks::default();
        let direct = acquire_sync_reservation(&mut locks, "server:25060", "db", "items")
            .expect("direct reservation")
            .expect("direct acquired");
        assert!(
            acquire_sync_reservation(&mut locks, "server:25060", "db", "items")
                .expect("catalog reservation")
                .is_none()
        );
        locks.release(direct);
        assert!(
            acquire_sync_reservation(&mut locks, "server:25060", "db", "items")
                .expect("catalog retry")
                .is_some()
        );
    }

    #[test]
    fn rejects_generated_run_ids_longer_than_progress_column() {
        let prefix = "x".repeat(124);
        let catalog = SyncableCatalog {
            tables: vec![entry("items", 1, &[])],
        };
        let error = validate_catalog_run_ids(&catalog, &prefix).expect_err("oversized run id");
        assert!(error.contains("128"));
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

    #[derive(Default)]
    struct TestNamedLocks {
        held: BTreeSet<String>,
    }

    impl NamedLockSet for TestNamedLocks {
        type Reservation = Vec<String>;

        fn try_reserve(&mut self, names: &[String]) -> Result<Option<Self::Reservation>, String> {
            if names.iter().any(|name| self.held.contains(name)) {
                return Ok(None);
            }
            self.held.extend(names.iter().cloned());
            Ok(Some(names.to_vec()))
        }
    }

    impl TestNamedLocks {
        fn release(&mut self, names: Vec<String>) {
            for name in names {
                self.held.remove(&name);
            }
        }
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
            data_type: column_type
                .split(['(', ' '])
                .next()
                .unwrap_or(column_type)
                .into(),
            is_nullable,
            character_set: None,
            collation: None,
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
