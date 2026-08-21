use super::model::{SyncPrimaryKeyOrdering, SyncTable};
use crate::inventory::{SchemaInventory, TableInventory};
use crate::live::TargetMySqlConfig;
use crate::mysql_config::MySqlConnectionConfig;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const SYNC_RUN_ID_DOMAIN: &[u8] = b"mariadb-mysql-cdc:sync-run-id:v1\0";
const MAX_SYNC_RUN_ID_BYTES: usize = 128;
pub(crate) const DEFAULT_SYNC_PROGRESS_TABLE: &str = "cdc.sync_runs";

#[derive(Clone, Debug)]
pub(crate) struct SyncConfig {
    pub(crate) source: MySqlConnectionConfig,
    pub(crate) target: TargetMySqlConfig,
    pub(crate) tables: Vec<String>,
    pub(crate) chunk_size: usize,
    pub(crate) parallelism: usize,
    pub(crate) progress_table: String,
    pub(crate) run_id: Option<String>,
    pub(crate) run_id_prefix: Option<String>,
    pub(crate) authorized_old_run_spec_sha256: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SyncEndpointSpec {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) database: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SyncRunSpec {
    pub(crate) source: SyncEndpointSpec,
    pub(crate) target: SyncEndpointSpec,
    pub(crate) tables: Vec<SyncTable>,
    pub(crate) chunk_size: usize,
    pub(crate) parallelism: usize,
    pub(crate) progress_table: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncRunIdentity {
    pub(crate) run_id: String,
    pub(crate) run_spec: SyncRunSpec,
    pub(crate) run_spec_json: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdditiveRunSpecMigrationPlan {
    pub(crate) changed_tables: Vec<AdditiveRunSpecTableChange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdditiveRunSpecTableChange {
    pub(crate) table: String,
    pub(crate) added_columns: Vec<String>,
}

pub(crate) fn plan_additive_run_spec_migration(
    persisted: &SyncRunSpec,
    current: &SyncRunSpec,
    source: &SchemaInventory,
    target: &SchemaInventory,
) -> Result<AdditiveRunSpecMigrationPlan, String> {
    validate_unchanged_migration_settings(persisted, current)?;
    validate_unchanged_table_scope(persisted, current)?;

    let source_tables = inventory_tables_by_name(source);
    let target_tables = inventory_tables_by_name(target);
    let changed_tables = persisted
        .tables
        .iter()
        .zip(&current.tables)
        .map(|(persisted_table, current_table)| {
            plan_additive_table_change(
                persisted_table,
                current_table,
                &source_tables,
                &target_tables,
            )
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    if changed_tables.is_empty() {
        return Err("additive run-spec migration has no added writable columns".to_string());
    }
    Ok(AdditiveRunSpecMigrationPlan { changed_tables })
}

fn validate_unchanged_migration_settings(
    persisted: &SyncRunSpec,
    current: &SyncRunSpec,
) -> Result<(), String> {
    for (unchanged, error) in [
        (
            persisted.source == current.source,
            "additive run-spec migration source endpoint changed",
        ),
        (
            persisted.target == current.target,
            "additive run-spec migration target endpoint changed",
        ),
        (
            persisted.chunk_size == current.chunk_size,
            "additive run-spec migration chunk size changed",
        ),
        (
            persisted.parallelism == current.parallelism,
            "additive run-spec migration parallelism changed",
        ),
        (
            persisted.progress_table == current.progress_table,
            "additive run-spec migration progress table changed",
        ),
    ] {
        if !unchanged {
            return Err(error.to_string());
        }
    }
    Ok(())
}

fn validate_unchanged_table_scope(
    persisted: &SyncRunSpec,
    current: &SyncRunSpec,
) -> Result<(), String> {
    let persisted_names = persisted
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<Vec<_>>();
    let current_names = current
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<Vec<_>>();
    if persisted_names != current_names {
        return Err("additive run-spec migration table scope or order changed".to_string());
    }
    Ok(())
}

fn inventory_tables_by_name(inventory: &SchemaInventory) -> BTreeMap<&str, &TableInventory> {
    inventory
        .tables
        .iter()
        .map(|table| (table.name.as_str(), table))
        .collect()
}

fn plan_additive_table_change(
    persisted: &SyncTable,
    current: &SyncTable,
    source_tables: &BTreeMap<&str, &TableInventory>,
    target_tables: &BTreeMap<&str, &TableInventory>,
) -> Result<Option<AdditiveRunSpecTableChange>, String> {
    validate_unchanged_primary_key(persisted, current)?;
    let added_columns = added_writable_columns(persisted, current)?;
    let source = required_inventory_table(source_tables, "source", &persisted.name)?;
    let target = required_inventory_table(target_tables, "target", &persisted.name)?;
    validate_current_table_inventory(current, source, target)?;

    Ok(
        (!added_columns.is_empty()).then(|| AdditiveRunSpecTableChange {
            table: persisted.name.clone(),
            added_columns,
        }),
    )
}

fn validate_unchanged_primary_key(
    persisted: &SyncTable,
    current: &SyncTable,
) -> Result<(), String> {
    if persisted.primary_key != current.primary_key {
        return Err(format!(
            "additive run-spec migration table `{}` primary key changed",
            persisted.name
        ));
    }
    if persisted.primary_key_ordering != current.primary_key_ordering {
        return Err(format!(
            "additive run-spec migration table `{}` primary-key ordering changed",
            persisted.name
        ));
    }
    Ok(())
}

fn required_inventory_table<'a>(
    tables: &'a BTreeMap<&str, &TableInventory>,
    endpoint: &str,
    table: &str,
) -> Result<&'a TableInventory, String> {
    tables
        .get(table)
        .copied()
        .ok_or_else(|| format!("additive run-spec migration {endpoint} table `{table}` is missing"))
}

fn validate_current_table_inventory(
    current: &SyncTable,
    source: &TableInventory,
    target: &TableInventory,
) -> Result<(), String> {
    let source_table = sync_table_from_inventory(source)?;
    if source_table != *current {
        return Err(format!(
            "additive run-spec migration table `{}` current specification does not match source inventory",
            current.name
        ));
    }
    if !crate::table_catalog::schemas_are_compatible(source, target) {
        return Err(format!(
            "additive run-spec migration table `{}` current source and target schemas are incompatible",
            current.name
        ));
    }
    Ok(())
}

fn added_writable_columns(
    persisted: &SyncTable,
    current: &SyncTable,
) -> Result<Vec<String>, String> {
    let persisted_names = persisted
        .columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let current_names = current
        .columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    validate_no_removed_columns(persisted, &persisted_names, &current_names)?;
    validate_existing_column_order(persisted, current, &persisted_names)?;

    Ok(current
        .columns
        .iter()
        .filter(|column| !persisted_names.contains(column.as_str()))
        .cloned()
        .collect())
}

fn validate_no_removed_columns(
    persisted: &SyncTable,
    persisted_names: &BTreeSet<&str>,
    current_names: &BTreeSet<&str>,
) -> Result<(), String> {
    let removed = persisted_names
        .difference(current_names)
        .copied()
        .collect::<Vec<_>>();
    if removed.is_empty() {
        return Ok(());
    }
    Err(format!(
        "additive run-spec migration table `{}` removed writable columns: {}",
        persisted.name,
        removed.join(", ")
    ))
}

fn validate_existing_column_order(
    persisted: &SyncTable,
    current: &SyncTable,
    persisted_names: &BTreeSet<&str>,
) -> Result<(), String> {
    let retained = current
        .columns
        .iter()
        .filter(|column| persisted_names.contains(column.as_str()))
        .collect::<Vec<_>>();
    if retained == persisted.columns.iter().collect::<Vec<_>>() {
        return Ok(());
    }
    Err(format!(
        "additive run-spec migration table `{}` reordered existing writable columns",
        persisted.name
    ))
}

pub(crate) fn validate_sync_config(config: &SyncConfig) -> Result<(), String> {
    validate_source_connection(&config.source)?;
    validate_target_connection(&config.target)?;
    validate_sync_scope(config)?;
    validate_progress_table(&config.progress_table)?;
    validate_run_identity(config)?;
    validate_run_spec_migration_authorization(config)
}

pub(crate) fn build_sync_run_identity(
    config: &SyncConfig,
    mut tables: Vec<SyncTable>,
) -> Result<SyncRunIdentity, String> {
    validate_sync_config(config)?;
    validate_concrete_tables(&tables)?;
    tables.sort_by(|left, right| left.name.cmp(&right.name));

    let run_spec = SyncRunSpec {
        source: endpoint_spec(
            &config.source.host,
            config.source.port,
            &config.source.database,
        ),
        target: endpoint_spec(
            &config.target.host,
            config.target.port,
            &config.target.database,
        ),
        tables,
        chunk_size: config.chunk_size,
        parallelism: config.parallelism,
        progress_table: config.progress_table.clone(),
    };
    let run_spec_json = serde_json::to_string(&run_spec)
        .map_err(|error| format!("encode sync run specification: {error}"))?;
    let run_id = resolved_run_id(config, &run_spec_json)?;

    Ok(SyncRunIdentity {
        run_id,
        run_spec,
        run_spec_json,
    })
}

pub(crate) fn sync_table_from_inventory(table: &TableInventory) -> Result<SyncTable, String> {
    validate_inventory_columns(table)?;
    validate_primary_key(table)?;

    let columns = table
        .columns
        .iter()
        .filter(|column| column.generated.is_none())
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let primary_key_ordering = table
        .primary_key
        .iter()
        .map(|name| primary_key_ordering(table, name))
        .collect::<Result<Vec<_>, _>>()?;
    if primary_key_ordering.len() != table.primary_key.len() {
        return Err(format!(
            "table `{}` has {} primary-key columns but {} ordering entries",
            table.name,
            table.primary_key.len(),
            primary_key_ordering.len()
        ));
    }

    Ok(SyncTable {
        name: table.name.clone(),
        primary_key: table.primary_key.clone(),
        primary_key_ordering,
        columns,
    })
}

fn validate_source_connection(source: &MySqlConnectionConfig) -> Result<(), String> {
    require_nonempty(&source.host, "source host is required")?;
    require_nonempty(&source.user, "source user is required")?;
    require_nonempty(&source.password, "source password is required")?;
    require_nonempty(&source.database, "source database is required")
}

fn validate_target_connection(target: &TargetMySqlConfig) -> Result<(), String> {
    require_nonempty(&target.host, "target host is required")?;
    require_nonempty(&target.user, "target user is required")?;
    require_nonempty(&target.password, "target password is required")?;
    require_nonempty(&target.database, "target database is required")?;
    require_nonempty(&target.tls_ca_file, "target TLS CA file is required")
}

fn validate_sync_scope(config: &SyncConfig) -> Result<(), String> {
    if config.tables.is_empty() {
        return Err("at least one table is required".to_string());
    }
    if let Some(table) = duplicate_name(config.tables.iter().map(String::as_str)) {
        return Err(format!("selected table `{table}` is duplicated"));
    }
    if config.chunk_size == 0 {
        return Err("chunk size must be greater than zero".to_string());
    }
    if config.parallelism == 0 {
        return Err("parallelism must be greater than zero".to_string());
    }
    Ok(())
}

fn validate_progress_table(progress_table: &str) -> Result<(), String> {
    let mut parts = progress_table.split('.');
    let schema = parts.next().unwrap_or_default();
    let table = parts.next().unwrap_or_default();
    let exact_pair = parts.next().is_none();
    if !exact_pair || schema.trim().is_empty() || table.trim().is_empty() {
        return Err(
            "progress table must be exactly schema-qualified with nonempty parts".to_string(),
        );
    }
    Ok(())
}

fn validate_run_identity(config: &SyncConfig) -> Result<(), String> {
    match (&config.run_id, &config.run_id_prefix) {
        (Some(run_id), None) => validate_exact_run_id(run_id),
        (None, Some(prefix)) => require_nonempty(prefix, "run id prefix is required"),
        _ => Err("exactly one of run_id or run_id_prefix is required".to_string()),
    }
}

fn validate_run_spec_migration_authorization(config: &SyncConfig) -> Result<(), String> {
    let Some(authorized_sha256) = &config.authorized_old_run_spec_sha256 else {
        return Ok(());
    };
    let valid_sha256 = authorized_sha256.len() == 64
        && authorized_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !valid_sha256 {
        return Err(
            "authorized old run-spec SHA-256 must be exactly 64 lowercase ASCII hex characters"
                .to_string(),
        );
    }
    if config.run_id_prefix.is_some() {
        return Err(
            "authorized old run-spec SHA-256 requires an exact run_id, not run_id_prefix"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_exact_run_id(run_id: &str) -> Result<(), String> {
    require_nonempty(run_id, "run id is required")?;
    if run_id.len() > MAX_SYNC_RUN_ID_BYTES {
        return Err(format!(
            "run id is {} bytes; cdc.sync_runs.run_id allows at most {MAX_SYNC_RUN_ID_BYTES}",
            run_id.len()
        ));
    }
    Ok(())
}

fn validate_concrete_tables(tables: &[SyncTable]) -> Result<(), String> {
    if let Some(table) = duplicate_name(tables.iter().map(|table| table.name.as_str())) {
        return Err(format!("concrete sync table `{table}` is duplicated"));
    }
    Ok(())
}

fn resolved_run_id(config: &SyncConfig, run_spec_json: &str) -> Result<String, String> {
    if let Some(run_id) = &config.run_id {
        return Ok(run_id.clone());
    }
    let prefix = config
        .run_id_prefix
        .as_deref()
        .expect("validated sync run id prefix");
    let run_id = derive_run_id(prefix, run_spec_json);
    if run_id.len() > MAX_SYNC_RUN_ID_BYTES {
        return Err(format!(
            "generated run id is {} bytes; cdc.sync_runs.run_id allows at most {MAX_SYNC_RUN_ID_BYTES}",
            run_id.len()
        ));
    }
    Ok(run_id)
}

fn derive_run_id(prefix: &str, run_spec_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(SYNC_RUN_ID_DOMAIN);
    update_framed_hash(&mut hasher, prefix.as_bytes());
    update_framed_hash(&mut hasher, run_spec_json.as_bytes());
    format!("sync-v1-{:x}", hasher.finalize())
}

fn update_framed_hash(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn endpoint_spec(host: &str, port: u16, database: &str) -> SyncEndpointSpec {
    SyncEndpointSpec {
        host: host.to_string(),
        port,
        database: database.to_string(),
    }
}

fn validate_inventory_columns(table: &TableInventory) -> Result<(), String> {
    if let Some(column) = duplicate_name(table.columns.iter().map(|column| column.name.as_str())) {
        return Err(format!(
            "column `{column}` is duplicated in `{}` inventory",
            table.name
        ));
    }
    Ok(())
}

fn validate_primary_key(table: &TableInventory) -> Result<(), String> {
    if table.primary_key.is_empty() {
        return Err(format!("table `{}` has no primary key", table.name));
    }
    if let Some(column) = duplicate_name(table.primary_key.iter().map(String::as_str)) {
        return Err(format!(
            "primary-key column `{column}` is duplicated in `{}` inventory",
            table.name
        ));
    }
    Ok(())
}

fn primary_key_ordering(
    table: &TableInventory,
    primary_key: &str,
) -> Result<SyncPrimaryKeyOrdering, String> {
    let column = table
        .columns
        .iter()
        .find(|column| column.name == primary_key)
        .ok_or_else(|| {
            format!(
                "primary-key column `{primary_key}` is absent from `{}` inventory",
                table.name
            )
        })?;
    if column.generated.is_some() {
        return Err(format!(
            "primary-key column `{primary_key}` is not writable in `{}`",
            table.name
        ));
    }
    Ok(
        match crate::sql_type::parse_enum_column_type(&column.column_type) {
            Some(labels) => SyncPrimaryKeyOrdering::Enum(labels),
            None => SyncPrimaryKeyOrdering::Native,
        },
    )
}

fn duplicate_name<'a>(names: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let mut seen = BTreeSet::new();
    names.into_iter().find(|name| !seen.insert(*name))
}

fn require_nonempty(value: &str, error: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(error.to_string());
    }
    Ok(())
}
