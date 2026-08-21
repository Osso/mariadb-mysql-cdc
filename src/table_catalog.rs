use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, MariaDbInventoryReader, SchemaInventory,
    TableInventory, build_inventory,
};
use crate::primary_key_ordering::{PrimaryKeyOrdering, primary_key_ordering_from_inventory};
use crate::{live, sync};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Maximum table workers in one unified catalog sync run.
const MAX_CATALOG_CONCURRENCY: usize = 16;
const DEFAULT_CHUNK_SIZE: usize = 10_000;
const DB_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct CatalogConnectionConfig {
    pub source: crate::mysql_config::MySqlConnectionConfig,
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
    #[serde(default)]
    pub primary_key_ordering: Vec<PrimaryKeyOrdering>,
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
    CrossSchemaDependency,
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

pub fn build_catalogs(
    source: &SchemaInventory,
    target: &SchemaInventory,
    estimated_rows: &BTreeMap<String, u64>,
) -> Catalogs {
    let (dependency_inventory, cross_schema_tables) = dependency_inventory(source, target);
    let (mut candidates, mut excluded) = classify_source_tables(
        &dependency_inventory,
        target,
        estimated_rows,
        &cross_schema_tables,
    );
    propagate_non_syncable_dependencies(&dependency_inventory, &mut candidates, &mut excluded);
    Catalogs {
        syncable: sorted_syncable_entries(candidates),
        non_syncable: sorted_non_syncable_entries(excluded, estimated_rows),
    }
}

fn dependency_inventory(
    source: &SchemaInventory,
    target: &SchemaInventory,
) -> (SchemaInventory, BTreeSet<String>) {
    let mut local_dependencies = BTreeMap::new();
    let mut cross_schema_tables = BTreeSet::new();
    for inventory in [source, target] {
        for foreign_key in &inventory.foreign_keys {
            if foreign_key.referenced_schema != inventory.schema {
                cross_schema_tables.insert(foreign_key.table.clone());
                continue;
            }
            let mut dependency = foreign_key.clone();
            dependency.referenced_schema = source.schema.clone();
            let key = (
                dependency.table.clone(),
                dependency.referenced_table.clone(),
            );
            local_dependencies.entry(key).or_insert(dependency);
        }
    }
    let mut inventory = source.clone();
    inventory.foreign_keys = local_dependencies.into_values().collect();
    (inventory, cross_schema_tables)
}

fn classify_source_tables(
    source: &SchemaInventory,
    target: &SchemaInventory,
    estimated_rows: &BTreeMap<String, u64>,
    cross_schema_tables: &BTreeSet<String>,
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
        let mut reasons = exclusion_reasons(
            source_table,
            target_tables.get(source_table.name.as_str()).copied(),
        );
        if cross_schema_tables.contains(&source_table.name) {
            reasons.insert(NonSyncableReason::CrossSchemaDependency);
        }
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
        primary_key_ordering: primary_key_ordering_from_inventory(table)
            .expect("catalog primary-key columns exist in source inventory"),
        columns: writable_columns(table),
        estimated_source_rows: estimated_rows.get(&table.name).copied().unwrap_or(0),
        parent_dependencies: inventory
            .foreign_keys
            .iter()
            .filter(|foreign_key| {
                foreign_key.table == table.name
                    && foreign_key.referenced_schema == inventory.schema
                    && foreign_key.referenced_table != table.name
            })
            .map(|foreign_key| foreign_key.referenced_table.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

fn propagate_non_syncable_dependencies(
    inventory: &SchemaInventory,
    candidates: &mut BTreeMap<String, SyncableTableEntry>,
    excluded: &mut BTreeMap<String, BTreeSet<NonSyncableReason>>,
) {
    let source_tables = inventory
        .tables
        .iter()
        .filter(|table| table.table_type == "BASE TABLE")
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    let dependencies = inventory
        .foreign_keys
        .iter()
        .filter(|foreign_key| {
            source_tables.contains(foreign_key.table.as_str())
                && foreign_key.referenced_schema == inventory.schema
                && foreign_key.table != foreign_key.referenced_table
        })
        .fold(
            BTreeMap::<String, BTreeSet<String>>::new(),
            |mut result, foreign_key| {
                result
                    .entry(foreign_key.table.clone())
                    .or_default()
                    .insert(foreign_key.referenced_table.clone());
                result
            },
        );
    loop {
        let dependent = dependencies
            .iter()
            .filter(|(_, parents)| {
                parents.iter().any(|parent| {
                    !source_tables.contains(parent.as_str()) || excluded.contains_key(parent)
                })
            })
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let mut changed = false;
        for name in dependent {
            candidates.remove(&name);
            changed |= excluded
                .entry(name)
                .or_default()
                .insert(NonSyncableReason::DependencyOnNonSyncable);
        }
        if !changed {
            return;
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
    validate_distinct_catalog_output_paths(&config.syncable_output, &config.non_syncable_output)?;
    let source = read_inventory_source(&config.connections.source)?;
    let target = read_inventory_target(&config.connections.target)?;
    let estimated_rows = read_estimated_rows(&config.connections.source)?;
    let catalogs = build_catalogs(&source, &target, &estimated_rows);
    let syncable_bytes = encode_pretty_json(
        &config.syncable_output,
        &SyncableCatalog {
            tables: catalogs.syncable,
        },
    )?;
    let non_syncable_bytes = encode_pretty_json(
        &config.non_syncable_output,
        &NonSyncableCatalog {
            tables: catalogs.non_syncable,
        },
    )?;
    write_catalog_bytes_with_hook(
        &config.syncable_output,
        &config.non_syncable_output,
        &syncable_bytes,
        &non_syncable_bytes,
        || {},
    )
}

fn run_sync_catalog(config: &SyncCatalogConfig) -> Result<(), String> {
    let catalog = read_syncable_catalog(&config.catalog)?;
    validate_catalog(&catalog)?;
    sync::run_mysql_sync(sync_config_from_catalog(config, &catalog)).map(|_| ())
}

pub(crate) fn sync_config_from_catalog(
    config: &SyncCatalogConfig,
    catalog: &SyncableCatalog,
) -> sync::SyncConfig {
    sync::SyncConfig {
        source: config.connections.source.clone(),
        target: config.connections.target.clone(),
        tables: catalog
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect(),
        chunk_size: config.chunk_size,
        parallelism: MAX_CATALOG_CONCURRENCY,
        progress_table: config.progress_table.clone(),
        run_id: None,
        run_id_prefix: Some(config.run_id_prefix.clone()),
    }
}

fn read_syncable_catalog(path: &Path) -> Result<SyncableCatalog, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let catalog: SyncableCatalog = serde_json::from_slice(&bytes)
        .map_err(|error| format!("failed to decode {}: {error}", path.display()))?;
    for table in &catalog.tables {
        if table.primary_key_ordering.len() != table.primary_key.len() {
            return Err(format!(
                "catalog table `{}` has {} primary_key_ordering entries for {} primary-key columns; regenerate the catalog",
                table.name,
                table.primary_key_ordering.len(),
                table.primary_key.len()
            ));
        }
    }
    Ok(catalog)
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
    validate_catalog_dependencies_exist(catalog, &names)?;
    validate_catalog_dependencies_are_acyclic(catalog, names)
}

fn validate_catalog_dependencies_exist(
    catalog: &SyncableCatalog,
    names: &BTreeSet<&str>,
) -> Result<(), String> {
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

fn validate_catalog_dependencies_are_acyclic<'a>(
    catalog: &'a SyncableCatalog,
    mut remaining: BTreeSet<&'a str>,
) -> Result<(), String> {
    let dependencies = catalog
        .tables
        .iter()
        .map(|entry| {
            (
                entry.name.as_str(),
                entry
                    .parent_dependencies
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    while !remaining.is_empty() {
        let ready = remaining
            .iter()
            .filter(|name| dependencies[*name].is_disjoint(&remaining))
            .copied()
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err("catalog has unresolved or cyclic dependencies".to_string());
        }
        remaining.retain(|name| !ready.contains(name));
    }
    Ok(())
}

pub(crate) fn schemas_are_compatible(source: &TableInventory, target: &TableInventory) -> bool {
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
    if source.character_set != target.character_set
        || !collations_are_equivalent(source.collation.as_deref(), target.collation.as_deref())
    {
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

/// Whether two column collations name the same converged collation.
///
/// MariaDB 11.8 defaults new tables to the UCA-1400 collations, which MySQL 8 does not have, and
/// the prerequisite schema stage converges each to its MySQL equivalent rather than rewriting the
/// column. Comparing the raw names here classified every recently created table as
/// `incompatible_schema` even though
/// its schema had already converged, which silently excluded 40 tables from every catalog sync.
fn collations_are_equivalent(source: Option<&str>, target: Option<&str>) -> bool {
    match (source, target) {
        (Some(source), Some(target)) => {
            crate::sync_schema::canonical_collation(source)
                == crate::sync_schema::canonical_collation(target)
        }
        (source, target) => source == target,
    }
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

fn validate_distinct_catalog_output_paths(
    syncable_output: &Path,
    non_syncable_output: &Path,
) -> Result<(), String> {
    let syncable = catalog_output_destination(syncable_output)?;
    let non_syncable = catalog_output_destination(non_syncable_output)?;
    if syncable.path == non_syncable.path || same_existing_file(&syncable, &non_syncable) {
        return Err(
            "--syncable-output and --non-syncable-output must not resolve to the same filesystem destination"
                .to_string(),
        );
    }
    Ok(())
}

struct CatalogOutputDestination {
    path: PathBuf,
    #[cfg(unix)]
    file_identity: Option<(u64, u64)>,
}

fn catalog_output_destination(path: &Path) -> Result<CatalogOutputDestination, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("failed to resolve current directory: {error}"))?
            .join(path)
    };
    resolve_catalog_output_destination(&absolute, &mut BTreeSet::new())
}

fn resolve_catalog_output_destination(
    path: &Path,
    visited_links: &mut BTreeSet<PathBuf>,
) -> Result<CatalogOutputDestination, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            resolve_catalog_output_symlink(path, visited_links)
        }
        Ok(metadata) => existing_catalog_output_destination(path, &metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            unresolved_catalog_output_destination(path)
        }
        Err(error) => Err(format!(
            "failed to resolve catalog output {}: {error}",
            path.display()
        )),
    }
}

fn resolve_catalog_output_symlink(
    path: &Path,
    visited_links: &mut BTreeSet<PathBuf>,
) -> Result<CatalogOutputDestination, String> {
    if !visited_links.insert(path.to_path_buf()) {
        return Err(format!(
            "failed to resolve catalog output {}: symbolic link cycle",
            path.display()
        ));
    }
    let target = fs::read_link(path).map_err(|error| {
        format!(
            "failed to resolve catalog output {}: {error}",
            path.display()
        )
    })?;
    let target = if target.is_absolute() {
        target
    } else {
        path.parent().unwrap_or_else(|| Path::new("/")).join(target)
    };
    resolve_catalog_output_destination(&target, visited_links)
}

fn existing_catalog_output_destination(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<CatalogOutputDestination, String> {
    Ok(CatalogOutputDestination {
        path: fs::canonicalize(path).map_err(|error| {
            format!(
                "failed to resolve catalog output {}: {error}",
                path.display()
            )
        })?,
        #[cfg(unix)]
        file_identity: Some(file_identity(metadata)),
    })
}

fn unresolved_catalog_output_destination(path: &Path) -> Result<CatalogOutputDestination, String> {
    let components = path
        .components()
        .map(|component| component.as_os_str().to_os_string())
        .collect::<Vec<_>>();
    let (mut resolved, suffix_start) = canonicalize_existing_path_prefix(path, &components)?;
    for component in &components[suffix_start..] {
        resolved.push(component);
    }
    Ok(CatalogOutputDestination {
        path: normalize_lexical_path(&resolved),
        #[cfg(unix)]
        file_identity: None,
    })
}

fn canonicalize_existing_path_prefix(
    path: &Path,
    components: &[std::ffi::OsString],
) -> Result<(PathBuf, usize), String> {
    for split in (0..components.len()).rev() {
        let candidate = components[..split].iter().collect::<PathBuf>();
        match fs::canonicalize(&candidate) {
            Ok(canonical) => return Ok((canonical, split)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to resolve catalog output ancestor {}: {error}",
                    candidate.display()
                ));
            }
        }
    }
    Err(format!(
        "catalog output {} has no existing ancestor",
        path.display()
    ))
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

#[cfg(unix)]
fn file_identity(metadata: &fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (metadata.dev(), metadata.ino())
}

fn same_existing_file(first: &CatalogOutputDestination, second: &CatalogOutputDestination) -> bool {
    #[cfg(unix)]
    {
        first.file_identity.is_some() && first.file_identity == second.file_identity
    }
    #[cfg(not(unix))]
    {
        let _ = (first, second);
        false
    }
}

fn encode_pretty_json(path: &Path, value: &impl Serialize) -> Result<Vec<u8>, String> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("failed to encode {}: {error}", path.display()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn write_catalog_bytes_with_hook(
    syncable_output: &Path,
    non_syncable_output: &Path,
    syncable_bytes: &[u8],
    non_syncable_bytes: &[u8],
    before_open: impl FnOnce(),
) -> Result<(), String> {
    validate_distinct_catalog_output_paths(syncable_output, non_syncable_output)?;
    before_open();
    let mut syncable_file = open_catalog_output(syncable_output)?;
    let mut non_syncable_file = open_catalog_output(non_syncable_output)?;
    if same_opened_file(&syncable_file, &non_syncable_file)? {
        return Err(
            "--syncable-output and --non-syncable-output resolve to the same opened filesystem file"
                .to_string(),
        );
    }
    truncate_and_write_catalog(&mut syncable_file, syncable_output, syncable_bytes)?;
    truncate_and_write_catalog(
        &mut non_syncable_file,
        non_syncable_output,
        non_syncable_bytes,
    )
}

fn open_catalog_output(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))
}

fn same_opened_file(first: &File, second: &File) -> Result<bool, String> {
    #[cfg(unix)]
    {
        let first = first
            .metadata()
            .map_err(|error| format!("failed to inspect opened catalog output: {error}"))?;
        let second = second
            .metadata()
            .map_err(|error| format!("failed to inspect opened catalog output: {error}"))?;
        Ok(file_identity(&first) == file_identity(&second))
    }
    #[cfg(not(unix))]
    {
        let _ = (first, second);
        Ok(false)
    }
}

fn truncate_and_write_catalog(file: &mut File, path: &Path, bytes: &[u8]) -> Result<(), String> {
    file.set_len(0)
        .map_err(|error| format!("failed to truncate {}: {error}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("failed to seek {}: {error}", path.display()))?;
    file.write_all(bytes)
        .map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn read_inventory_source(
    source: &crate::mysql_config::MySqlConnectionConfig,
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
    source: &crate::mysql_config::MySqlConnectionConfig,
) -> Result<BTreeMap<String, u64>, String> {
    let mut connection = source_connection(source)?;
    let sql = "SELECT TABLE_NAME, COALESCE(TABLE_ROWS, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME";
    connection
        .exec::<(String, u64), _, _>(sql, (&source.database,))
        .map(|rows| rows.into_iter().collect())
        .map_err(|error| format!("failed to read estimated source row counts: {error}"))
}

fn source_connection(config: &crate::mysql_config::MySqlConnectionConfig) -> Result<Conn, String> {
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
            .unwrap_or_else(|| sync::DEFAULT_SYNC_PROGRESS_TABLE.to_string()),
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
    let source = crate::mysql_config::MySqlConnectionConfig {
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

fn cli_error(error: String, usage: &str) -> ! {
    eprintln!("{error}\n\n{usage}");
    std::process::exit(2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{ColumnInventory, ForeignKeyInventory, SchemaInventory, TableInventory};

    #[test]
    fn sync_progress_defaults_catalog_to_unified_table() {
        unsafe {
            std::env::set_var("CDC_CATALOG_SOURCE_PASSWORD", "source-password");
            std::env::set_var("CDC_CATALOG_TARGET_PASSWORD", "target-password");
        }
        let args = [
            "--source-host",
            "source",
            "--source-user",
            "reader",
            "--source-password-env",
            "CDC_CATALOG_SOURCE_PASSWORD",
            "--source-database",
            "source-db",
            "--target-host",
            "target",
            "--target-user",
            "writer",
            "--target-password-env",
            "CDC_CATALOG_TARGET_PASSWORD",
            "--target-database",
            "target-db",
            "--target-tls-ca-file",
            "/tmp/ca.pem",
            "--catalog",
            "catalog.json",
            "--run-id-prefix",
            "nightly",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();

        let config = parse_sync_catalog_config(args).expect("sync catalog config");

        assert_eq!(config.progress_table, "cdc.sync_runs");
    }

    #[test]
    fn excluded_child_keeps_own_reason_and_dependency_reason() {
        let source = inventory(
            vec![
                table("child", vec![column("parent_id")], vec![]),
                table("parent", vec![column("id")], vec!["id"]),
            ],
            vec![foreign_key("child", "parent")],
        );
        let target = inventory(
            vec![table("child", vec![column("parent_id")], vec![])],
            vec![],
        );

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());
        let child = catalogs
            .non_syncable
            .iter()
            .find(|entry| entry.name == "child")
            .expect("excluded child");

        assert_eq!(
            child.reasons,
            vec![
                NonSyncableReason::MissingPrimaryKey,
                NonSyncableReason::DependencyOnNonSyncable,
            ]
        );
    }

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

    /// MariaDB 11.8 defaults new tables to UCA-1400, which the prerequisite schema stage converges
    /// to the MySQL spelling. Such a table is fully synced and must stay syncable.
    #[test]
    fn converged_uca1400_column_collations_stay_syncable() {
        let mut source_name = typed_column("name", "varchar(32)", false);
        source_name.character_set = Some("utf8mb4".into());
        source_name.collation = Some("utf8mb4_uca1400_ai_ci".into());
        let mut target_name = source_name.clone();
        target_name.collation = Some("utf8mb4_0900_ai_ci".into());
        let mut source_table = table("items", vec![column("id"), source_name], vec!["id"]);
        source_table.collation = Some("utf8mb4_uca1400_ai_ci".into());
        let mut target_table = table("items", vec![column("id"), target_name], vec!["id"]);
        target_table.collation = Some("utf8mb4_0900_ai_ci".into());

        let catalogs = build_catalogs(
            &inventory(vec![source_table], vec![]),
            &inventory(vec![target_table], vec![]),
            &BTreeMap::new(),
        );

        assert!(
            catalogs.non_syncable.is_empty(),
            "converged collations must not exclude the table: {:?}",
            catalogs.non_syncable
        );
        assert_eq!(catalogs.syncable[0].name, "items");
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
    fn cross_schema_parent_excludes_child_when_local_parent_is_absent() {
        let source = inventory(
            vec![table(
                "child",
                vec![column("id"), column("parent_id")],
                vec!["id"],
            )],
            vec![foreign_key_in_schema("child", "parent", "shared")],
        );
        let target = inventory(
            vec![table(
                "child",
                vec![column("id"), column("parent_id")],
                vec!["id"],
            )],
            vec![],
        );

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());

        assert_eq!(
            catalogs.non_syncable[0].reasons,
            vec![NonSyncableReason::CrossSchemaDependency]
        );
    }

    #[test]
    fn same_named_local_parent_does_not_satisfy_cross_schema_dependency() {
        let source = inventory(
            vec![
                table("child", vec![column("id"), column("parent_id")], vec!["id"]),
                table("parent", vec![column("id")], vec!["id"]),
            ],
            vec![foreign_key_in_schema("child", "parent", "shared")],
        );
        let target = source.clone();

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());
        let child = catalogs
            .non_syncable
            .iter()
            .find(|entry| entry.name == "child")
            .expect("cross-schema child");

        assert_eq!(
            child.reasons,
            vec![NonSyncableReason::CrossSchemaDependency]
        );
        assert!(catalogs.syncable.iter().any(|entry| entry.name == "parent"));
    }

    #[test]
    fn cross_schema_exclusion_propagates_to_local_descendants() {
        let source = inventory(
            vec![
                table("child", vec![column("id"), column("parent_id")], vec!["id"]),
                table(
                    "grandchild",
                    vec![column("id"), column("child_id")],
                    vec!["id"],
                ),
            ],
            vec![
                foreign_key_in_schema("child", "parent", "shared"),
                foreign_key("grandchild", "child"),
            ],
        );
        let target = inventory(source.tables.clone(), vec![]);

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());
        let grandchild = catalogs
            .non_syncable
            .iter()
            .find(|entry| entry.name == "grandchild")
            .expect("dependent grandchild");

        assert_eq!(
            grandchild.reasons,
            vec![NonSyncableReason::DependencyOnNonSyncable]
        );
    }

    #[test]
    fn target_only_local_fk_gates_child_on_parent() {
        let tables = vec![
            table("child", vec![column("id"), column("parent_id")], vec!["id"]),
            table("parent", vec![column("id")], vec!["id"]),
        ];
        let source = inventory(tables.clone(), vec![]);
        let target = inventory(tables, vec![foreign_key("child", "parent")]);

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());
        let child = catalogs
            .syncable
            .iter()
            .find(|entry| entry.name == "child")
            .expect("target-constrained child");

        assert_eq!(child.parent_dependencies, vec!["parent"]);
    }

    #[test]
    fn target_only_fk_to_parent_missing_from_source_excludes_child() {
        let child = table("child", vec![column("id"), column("parent_id")], vec!["id"]);
        let source = inventory(vec![child.clone()], vec![]);
        let target = inventory(
            vec![child, table("parent", vec![column("id")], vec!["id"])],
            vec![foreign_key("child", "parent")],
        );

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());
        let child = catalogs
            .non_syncable
            .iter()
            .find(|entry| entry.name == "child")
            .expect("child with target-only parent");

        assert_eq!(
            child.reasons,
            vec![NonSyncableReason::DependencyOnNonSyncable]
        );
        assert!(catalogs.syncable.is_empty());
    }

    #[test]
    fn target_only_fk_does_not_create_catalog_entry_without_source_child() {
        let source = inventory(vec![], vec![]);
        let target = inventory(
            vec![
                table("child", vec![column("id"), column("parent_id")], vec!["id"]),
                table("parent", vec![column("id")], vec!["id"]),
            ],
            vec![foreign_key("child", "parent")],
        );

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());

        assert!(catalogs.syncable.is_empty());
        assert!(catalogs.non_syncable.is_empty());
    }

    #[test]
    fn target_external_schema_matching_source_schema_is_still_cross_schema() {
        let tables = vec![
            table("child", vec![column("id"), column("parent_id")], vec!["id"]),
            table("parent", vec![column("id")], vec!["id"]),
        ];
        let mut source = inventory(
            tables.clone(),
            vec![foreign_key_in_schema("child", "parent", "app")],
        );
        source.schema = "app".into();
        let mut target = inventory(
            tables,
            vec![foreign_key_in_schema("child", "parent", "app")],
        );
        target.schema = "app_new".into();

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());
        let child = catalogs
            .non_syncable
            .iter()
            .find(|entry| entry.name == "child")
            .expect("target cross-schema child");

        assert_eq!(
            child.reasons,
            vec![NonSyncableReason::CrossSchemaDependency]
        );
    }

    #[test]
    fn target_only_cross_schema_fk_excludes_child_and_descendants() {
        let tables = vec![
            table("child", vec![column("id"), column("parent_id")], vec!["id"]),
            table(
                "grandchild",
                vec![column("id"), column("child_id")],
                vec!["id"],
            ),
        ];
        let source = inventory(tables.clone(), vec![foreign_key("grandchild", "child")]);
        let target = inventory(
            tables,
            vec![foreign_key_in_schema("child", "parent", "shared")],
        );

        let catalogs = build_catalogs(&source, &target, &BTreeMap::new());
        let child = catalogs
            .non_syncable
            .iter()
            .find(|entry| entry.name == "child")
            .expect("target cross-schema child");
        let grandchild = catalogs
            .non_syncable
            .iter()
            .find(|entry| entry.name == "grandchild")
            .expect("dependent grandchild");

        assert_eq!(
            child.reasons,
            vec![NonSyncableReason::CrossSchemaDependency]
        );
        assert_eq!(
            grandchild.reasons,
            vec![NonSyncableReason::DependencyOnNonSyncable]
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
    fn dependency_cycle_is_rejected_before_external_scheduling() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &["b"]), entry("b", 2, &["a"])],
        };

        let error = validate_catalog(&catalog).expect_err("cycle must fail prevalidation");

        assert!(error.contains("cyclic dependencies"), "{error}");
    }

    #[test]
    fn legacy_catalog_without_primary_key_ordering_is_rejected() {
        let directory = unique_catalog_test_directory("legacy-ordering");
        fs::create_dir_all(&directory).expect("create catalog test directory");
        let path = directory.join("syncable.json");
        fs::write(
            &path,
            r#"{"tables":[{"name":"accounts","primary_key":["id"],"columns":["id"],"estimated_source_rows":1,"parent_dependencies":[]}]}"#,
        )
        .expect("write legacy catalog");

        let error = read_syncable_catalog(&path)
            .expect_err("catalog without primary-key ordering must be regenerated");

        assert!(error.contains("primary_key_ordering"));
        fs::remove_dir_all(directory).expect("remove catalog test directory");
    }

    #[test]
    fn deterministic_catalog_json_is_stable() {
        let catalog = SyncableCatalog {
            tables: vec![entry("a", 1, &[])],
        };
        let first = serde_json::to_string_pretty(&catalog).expect("json");
        let second = serde_json::to_string_pretty(&catalog).expect("json");
        assert_eq!(first, second);
    }

    #[test]
    fn identical_catalog_output_paths_fail_before_writing() {
        let path = std::env::temp_dir().join(format!(
            "cdc-identical-catalog-output-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        let config = TableCatalogConfig {
            connections: CatalogConnectionConfig {
                source: crate::mysql_config::MySqlConnectionConfig {
                    host: "unreachable-source".to_string(),
                    port: 3306,
                    user: "reader".to_string(),
                    password: "secret".to_string(),
                    database: "source".to_string(),
                },
                target: live::TargetMySqlConfig {
                    host: "unreachable-target".to_string(),
                    port: 25060,
                    user: "writer".to_string(),
                    password: "secret".to_string(),
                    database: "target".to_string(),
                    tls_ca_file: "/missing/ca.pem".to_string(),
                    insert_conflict_policy: live::InsertConflictPolicy::Error,
                },
            },
            syncable_output: path.clone(),
            non_syncable_output: path.clone(),
        };

        let error = write_table_catalogs(&config).expect_err("identical output paths");

        assert!(error.contains("same filesystem destination"), "{error}");
        assert!(!path.exists(), "identical output path was created");
    }

    #[test]
    fn lexical_alias_catalog_outputs_fail_before_writing() {
        let directory = unique_catalog_test_directory("lexical-alias");
        fs::create_dir_all(&directory).expect("create test directory");
        let first = directory.join("catalog.json");
        let second = directory.join("subdir").join("..").join("catalog.json");
        fs::create_dir(directory.join("subdir")).expect("create alias directory");

        let error = validate_distinct_catalog_output_paths(&first, &second)
            .expect_err("lexical aliases must conflict");

        assert!(error.contains("same filesystem destination"), "{error}");
        assert!(!first.exists());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn nonexistent_parent_components_are_resolved_after_physical_ancestor() {
        let directory = unique_catalog_test_directory("nonexistent-parent-components");
        fs::create_dir_all(&directory).expect("create test directory");
        let physical = directory.join("catalog.json");
        let alias = directory
            .join("missing-parent")
            .join("..")
            .join("catalog.json");

        let error = validate_distinct_catalog_output_paths(&physical, &alias)
            .expect_err("nonexistent parent alias must conflict");

        assert!(error.contains("same filesystem destination"), "{error}");
        assert!(!physical.exists(), "catalog output was created");
        let _ = fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn intermediate_symlink_parent_alias_conflicts_before_writing() {
        use std::os::unix::fs::symlink;

        let directory = unique_catalog_test_directory("intermediate-symlink-parent");
        let first_parent = directory.join("a");
        let physical_parent = directory.join("b");
        fs::create_dir_all(physical_parent.join("sub")).expect("create physical parent");
        fs::create_dir_all(&first_parent).expect("create alias parent");
        symlink("../b/sub", first_parent.join("link")).expect("create intermediate symlink");
        let physical = physical_parent.join("catalog.json");
        let alias = first_parent.join("link").join("..").join("catalog.json");

        let error = validate_distinct_catalog_output_paths(&physical, &alias)
            .expect_err("physical aliases must conflict");

        assert!(error.contains("same filesystem destination"), "{error}");
        assert!(!physical.exists(), "catalog output was created");
        let _ = fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn dangling_relative_symlink_catalog_output_conflicts_with_its_target() {
        use std::os::unix::fs::symlink;

        let directory = unique_catalog_test_directory("dangling-relative-symlink");
        fs::create_dir_all(&directory).expect("create test directory");
        let target = directory.join("catalog.json");
        let alias = directory.join("catalog-link.json");
        symlink("catalog.json", &alias).expect("create dangling relative symlink");

        let error = validate_distinct_catalog_output_paths(&target, &alias)
            .expect_err("dangling symlink target must conflict");

        assert!(error.contains("same filesystem destination"), "{error}");
        assert!(!target.exists(), "target was created");
        assert!(fs::symlink_metadata(&alias).is_ok(), "symlink was removed");
        let _ = fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_output_symlink_cycle_fails_closed() {
        use std::os::unix::fs::symlink;

        let directory = unique_catalog_test_directory("symlink-cycle");
        fs::create_dir_all(&directory).expect("create test directory");
        let first = directory.join("first.json");
        let second = directory.join("second.json");
        symlink("second.json", &first).expect("create first cycle link");
        symlink("first.json", &second).expect("create second cycle link");

        let error = validate_distinct_catalog_output_paths(&first, &directory.join("other.json"))
            .expect_err("symlink cycle must fail closed");

        assert!(
            error.contains("failed to resolve catalog output"),
            "{error}"
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_catalog_outputs_fail_without_overwriting_target() {
        use std::os::unix::fs::symlink;

        let directory = unique_catalog_test_directory("symlink");
        fs::create_dir_all(&directory).expect("create test directory");
        let target = directory.join("catalog.json");
        let alias = directory.join("catalog-link.json");
        fs::write(&target, b"preserve").expect("write target");
        symlink(&target, &alias).expect("create symlink");

        let error = validate_distinct_catalog_output_paths(&target, &alias)
            .expect_err("symlink aliases must conflict");

        assert!(error.contains("same filesystem destination"), "{error}");
        assert_eq!(fs::read(&target).expect("read target"), b"preserve");
        let _ = fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn output_alias_mutation_before_final_open_preserves_existing_content() {
        use std::os::unix::fs::symlink;

        let directory = unique_catalog_test_directory("final-open-alias");
        fs::create_dir_all(&directory).expect("create test directory");
        let syncable = directory.join("syncable.json");
        let non_syncable = directory.join("non-syncable.json");
        fs::write(&syncable, b"preserve").expect("write existing syncable output");
        fs::write(&non_syncable, b"other").expect("write distinct non-syncable output");

        let error = write_catalog_bytes_with_hook(
            &syncable,
            &non_syncable,
            b"new syncable\n",
            b"new non-syncable\n",
            || {
                fs::remove_file(&non_syncable).expect("remove distinct output");
                symlink(&syncable, &non_syncable).expect("redirect output alias");
            },
        )
        .expect_err("final opened files must be distinct");

        assert!(error.contains("same opened filesystem file"), "{error}");
        assert_eq!(
            fs::read(&syncable).expect("read preserved output"),
            b"preserve"
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn hardlink_catalog_outputs_fail_without_overwriting_target() {
        let directory = unique_catalog_test_directory("hardlink");
        fs::create_dir_all(&directory).expect("create test directory");
        let target = directory.join("catalog.json");
        let alias = directory.join("catalog-hardlink.json");
        fs::write(&target, b"preserve").expect("write target");
        fs::hard_link(&target, &alias).expect("create hardlink");

        let error = validate_distinct_catalog_output_paths(&target, &alias)
            .expect_err("hardlink aliases must conflict");

        assert!(error.contains("same filesystem destination"), "{error}");
        assert_eq!(fs::read(&target).expect("read target"), b"preserve");
        let _ = fs::remove_dir_all(directory);
    }

    fn unique_catalog_test_directory(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cdc-table-catalog-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
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
        foreign_key_in_schema(table, parent, "db")
    }

    fn foreign_key_in_schema(
        table: &str,
        parent: &str,
        referenced_schema: &str,
    ) -> ForeignKeyInventory {
        ForeignKeyInventory {
            table: table.into(),
            name: format!("fk_{table}_{parent}"),
            columns: vec!["parent_id".into()],
            referenced_schema: referenced_schema.into(),
            referenced_table: parent.into(),
            referenced_columns: vec!["id".into()],
        }
    }
    fn entry(name: &str, rows: u64, parents: &[&str]) -> SyncableTableEntry {
        SyncableTableEntry {
            name: name.into(),
            primary_key: vec!["id".into()],
            primary_key_ordering: vec![PrimaryKeyOrdering::Native],
            columns: vec!["id".into()],
            estimated_source_rows: rows,
            parent_dependencies: parents.iter().map(|v| (*v).into()).collect(),
        }
    }
}
