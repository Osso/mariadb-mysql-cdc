use super::model::{SyncPrimaryKeyOrdering, SyncTable};
use crate::inventory::TableInventory;
use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const SYNC_RUN_ID_DOMAIN: &[u8] = b"mariadb-mysql-cdc:sync-run-id:v1\0";
const MAX_SYNC_RUN_ID_BYTES: usize = 128;

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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SyncEndpointSpec {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) database: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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

pub(crate) fn validate_sync_config(config: &SyncConfig) -> Result<(), String> {
    validate_source_connection(&config.source)?;
    validate_target_connection(&config.target)?;
    validate_sync_scope(config)?;
    validate_progress_table(&config.progress_table)?;
    validate_run_identity(config)
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
