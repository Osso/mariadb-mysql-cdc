use super::mysql::MySqlSyncReader;
use super::range::sync_table_with_progress_range;
use super::recent::{RecentUpdateSyncContext, sync_recent_updates_with_progress};
use super::*;
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, MariaDbInventoryReader, SchemaInventory,
    build_inventory,
};
use std::rc::Rc;
use std::time::Duration;

const SYNC_CONNECTION_ATTEMPTS: usize = 5;
const SYNC_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(1);

pub fn sync_table(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<SyncTableReport, TableSyncError> {
    let mut progress_store = NoopSyncProgressStore;
    sync_table_with_progress(
        table,
        chunk_size,
        mode,
        source,
        target,
        repair_target,
        &mut progress_store,
    )
}

pub fn sync_table_with_progress(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableReport, TableSyncError> {
    sync_table_with_progress_range(
        table,
        SyncRunOptions {
            run_id: "ephemeral".to_string(),
            run_scope: "ephemeral".to_string(),
            chunk_size,
            mode,
            start_after: None,
            end_at: None,
        },
        source,
        target,
        repair_target,
        progress_store,
    )
}

pub fn run_sync_table(config: &SyncTableConfig) -> Result<SyncTableReport, TableSyncError> {
    progress::MySqlSyncRunProgressStore::new(config.target.clone(), config.progress_table.clone())
        .ensure()?;
    let _reservation = crate::table_catalog::reserve_sync_worker(
        &config.target,
        &config.progress_table,
        &config.table.name,
    )
    .map_err(TableSyncError::Progress)?
    .ok_or_else(|| {
        TableSyncError::Progress(format!(
            "table sync capacity or table reservation unavailable for `{}`",
            config.table.name
        ))
    })?;
    run_sync_table_reserved(config)
}

pub(crate) fn run_sync_table_reserved(
    config: &SyncTableConfig,
) -> Result<SyncTableReport, TableSyncError> {
    let result = retry_sync_table_operation(
        config.mode,
        SYNC_CONNECTION_ATTEMPTS,
        SYNC_CONNECTION_RETRY_DELAY,
        || run_sync_table_phase(config, SyncPhase::All),
    );
    record_terminal_sync_run_error(config, result)
}

fn record_terminal_sync_run_error(
    config: &SyncTableConfig,
    result: Result<SyncTableReport, TableSyncError>,
) -> Result<SyncTableReport, TableSyncError> {
    let Err(error) = result else {
        return result;
    };
    if !should_record_terminal_sync_run_error(&error) {
        return Err(error);
    }
    let mut progress_store = progress::MySqlSyncRunProgressStore::new(
        config.target.clone(),
        config.progress_table.clone(),
    );
    if let Err(save_error) = progress_store.save_error(&config.run_id, &error) {
        return Err(TableSyncError::Progress(format!(
            "{error}; also failed to persist run error: {save_error}"
        )));
    }
    Err(error)
}

pub(crate) fn retry_sync_table_operation<F>(
    mode: SyncMode,
    max_attempts: usize,
    retry_delay: Duration,
    mut operation: F,
) -> Result<SyncTableReport, TableSyncError>
where
    F: FnMut() -> Result<SyncTableReport, TableSyncError>,
{
    let attempts = if matches!(mode, SyncMode::Apply | SyncMode::MissingPrimaryKeys) {
        max_attempts.max(1)
    } else {
        1
    };
    for attempt in 1..=attempts {
        match operation() {
            Ok(report) => return Ok(report),
            Err(error) if attempt < attempts && is_retryable_sync_error(&error) => {
                if !retry_delay.is_zero() {
                    std::thread::sleep(retry_delay);
                }
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("sync retry loop has at least one attempt")
}

/// `Verification` is deliberately absent. The terminal parity pass is read-only: `repair_chunk`
/// returns after counting for `SyncPhase::Verify`. A retry resumes the chunk phase at the saved
/// tail primary key, so it cannot repair drift the pass found earlier in the table, then re-runs the
/// same read-only pass and reaches the same conclusion. Retrying only multiplies a full-table scan.
pub(crate) fn is_retryable_sync_error(error: &TableSyncError) -> bool {
    if matches!(
        error,
        TableSyncError::Read(_) | TableSyncError::Progress(_) | TableSyncError::Duplicate(_)
    ) {
        return true;
    }
    let TableSyncError::Repair(message) = error else {
        return false;
    };
    let message = message.to_ascii_lowercase();
    [
        "timed out",
        "timeout",
        "connection reset",
        "connection refused",
        "connection closed",
        "connection aborted",
        "broken pipe",
        "unexpected eof",
        "server has gone away",
        "lost connection",
        "network is unreachable",
        "could not connect",
        "not connected",
        "packet out of sync",
        "resource temporarily unavailable",
        "error 1205",
        "error 1213",
        "deadlock",
        "lock wait timeout",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

pub fn run_sync_table_phase(
    config: &SyncTableConfig,
    phase: SyncPhase,
) -> Result<SyncTableReport, TableSyncError> {
    run_sync_table_phase_with_run_spec(config, phase, None)
}

pub(crate) fn run_sync_table_phase_with_run_spec(
    config: &SyncTableConfig,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
) -> Result<SyncTableReport, TableSyncError> {
    validate_sync_table_config(config)?;
    let source_inventory = read_source_inventory(config)?;
    let target_inventory = read_target_inventory(config)?;
    let config = resolved_sync_table_config(config, &source_inventory)?;
    let source = MySqlSyncReader::new(config.source.clone());
    let target =
        MySqlSyncReader::new_with_target(target_connection_config(&config), &config.target)
            .map_err(TableSyncError::Read)?;
    let mut progress_store = progress::MySqlSyncRunProgressStore::new(
        config.target.clone(),
        config.progress_table.clone(),
    );
    let mut repair_target = mysql_repair_target(&config, source_inventory, target_inventory)?;
    run_sync_table_with_targets_phase(
        &config,
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        phase,
        run_spec_json,
    )
}

pub(crate) fn run_sync_table_phase_with_consistent_source(
    config: &SyncTableConfig,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<SyncTableReport, TableSyncError> {
    validate_sync_table_config(config)?;
    let config = resolved_sync_table_config(config, source_inventory)?;
    run_consistent_source_phase(
        &config,
        phase,
        run_spec_json,
        shared_source,
        source_inventory,
        target_inventory,
    )
}

fn run_consistent_source_phase(
    config: &SyncTableConfig,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<SyncTableReport, TableSyncError> {
    let source = consistent_source_reader(config, &shared_source);
    let target = consistent_target_reader(config)?;
    let mut progress_store = progress::MySqlSyncRunProgressStore::new(
        config.target.clone(),
        config.progress_table.clone(),
    );
    let mut repair_target = mysql_repair_target_with_consistent_source(
        config,
        shared_source,
        source_inventory.clone(),
        target_inventory.clone(),
    )?;
    run_sync_table_with_targets_phase(
        config,
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        phase,
        run_spec_json,
    )
}

fn consistent_source_reader(
    config: &SyncTableConfig,
    shared_source: &Rc<crate::mysql_client::PersistentMySqlSource>,
) -> MySqlSyncReader {
    MySqlSyncReader::new_with_shared_source(config.source.clone(), Rc::clone(shared_source))
}

fn consistent_target_reader(config: &SyncTableConfig) -> Result<MySqlSyncReader, TableSyncError> {
    MySqlSyncReader::new_with_target(target_connection_config(config), &config.target)
        .map_err(TableSyncError::Read)
}

pub(crate) fn should_record_sync_run_error(error: &TableSyncError) -> bool {
    matches!(error, TableSyncError::Read(_) | TableSyncError::Repair(_))
        && !is_retryable_sync_error(error)
}

/// Retries are exhausted by the time a run returns, so any surviving error ends the run. Recording
/// it keeps the durable row from staying `running` with no live worker, which otherwise reads as an
/// in-flight sync. Progress and table-validation errors are excluded because they must not replace
/// an already saved run status.
pub(crate) fn should_record_terminal_sync_run_error(error: &TableSyncError) -> bool {
    !matches!(
        error,
        TableSyncError::Progress(_) | TableSyncError::InvalidTable(_)
    )
}

fn mysql_repair_target(
    config: &SyncTableConfig,
    source_inventory: SchemaInventory,
    target_inventory: SchemaInventory,
) -> Result<MySqlSyncRepairTarget, TableSyncError> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new_for_sync(&config.target)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    let writer = crate::target::TargetMySqlWriter::from_snapshot_table(
        &snapshot_table(&config.table),
        executor,
        sync_insert_mode(config),
    );
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new_with_target(target_connection_config(config), &config.target)
        .map_err(TableSyncError::Read)?;
    Ok(MySqlSyncRepairTarget::new_with_fk_repair(
        writer,
        source,
        target,
        source_inventory,
        target_inventory,
    ))
}

fn mysql_repair_target_with_consistent_source(
    config: &SyncTableConfig,
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: SchemaInventory,
    target_inventory: SchemaInventory,
) -> Result<MySqlSyncRepairTarget, TableSyncError> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new_for_sync(&config.target)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    let writer = crate::target::TargetMySqlWriter::from_snapshot_table(
        &snapshot_table(&config.table),
        executor,
        sync_insert_mode(config),
    );
    let source = MySqlSyncReader::new_with_shared_source(config.source.clone(), shared_source);
    let target = MySqlSyncReader::new_with_target(target_connection_config(config), &config.target)
        .map_err(TableSyncError::Read)?;
    Ok(MySqlSyncRepairTarget::new_with_fk_repair(
        writer,
        source,
        target,
        source_inventory,
        target_inventory,
    ))
}

fn resolved_sync_table_config(
    config: &SyncTableConfig,
    source_inventory: &SchemaInventory,
) -> Result<SyncTableConfig, TableSyncError> {
    let source_table = source_inventory
        .tables
        .iter()
        .find(|table| table.name == config.table.name)
        .ok_or_else(|| {
            TableSyncError::InvalidTable(format!(
                "table `{}` is absent from source inventory",
                config.table.name
            ))
        })?;
    let ordering = super::primary_key_ordering_from_inventory(source_table)?;
    if !config.table.primary_key_ordering.is_empty()
        && config.table.primary_key_ordering != ordering
    {
        return Err(TableSyncError::InvalidTable(format!(
            "primary-key ordering for `{}` disagrees with source inventory",
            config.table.name
        )));
    }
    let mut resolved = config.clone();
    resolved.table.primary_key_ordering = ordering;
    Ok(resolved)
}

fn read_source_inventory(config: &SyncTableConfig) -> Result<SchemaInventory, TableSyncError> {
    let reader = MariaDbInventoryReader::new(InventoryConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        endpoint_role: InventoryEndpointRole::Source,
        use_tls: false,
        tls_ca_file: None,
        ..InventoryConfig::default()
    });
    build_inventory(&config.source.database, &reader)
        .map_err(|error| TableSyncError::Read(error.to_string()))
}

fn read_target_inventory(config: &SyncTableConfig) -> Result<SchemaInventory, TableSyncError> {
    let reader = MariaDbInventoryReader::new(InventoryConfig {
        host: config.target.host.clone(),
        port: config.target.port,
        user: config.target.user.clone(),
        password: config.target.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(config.target.tls_ca_file.clone()),
        ..InventoryConfig::default()
    });
    build_inventory(&config.target.database, &reader)
        .map_err(|error| TableSyncError::Read(error.to_string()))
}

pub(crate) fn expected_sync_run_spec_json(
    config: &SyncTableConfig,
) -> Result<String, TableSyncError> {
    super::range::build_run_spec_json(
        &build_sync_run_scope(config)?,
        &config.table,
        config.chunk_size,
        config.mode,
        &config.start_after,
        &config.end_at,
    )
}

pub(crate) fn build_sync_run_scope(config: &SyncTableConfig) -> Result<String, TableSyncError> {
    let insert_conflict_policy = match config.target.insert_conflict_policy {
        crate::live::InsertConflictPolicy::Error => "error",
        crate::live::InsertConflictPolicy::IgnoreDuplicate => "ignore-duplicate",
        crate::live::InsertConflictPolicy::ReplaceDivergentPk => "replace-divergent-pk",
    };
    serde_json::to_string(&SyncRunScope {
        source_host: &config.source.host,
        source_port: config.source.port,
        source_database: &config.source.database,
        target_host: &config.target.host,
        target_port: config.target.port,
        target_database: &config.target.database,
        insert_conflict_policy,
        plan_hash: config.plan_hash.as_deref(),
    })
    .map_err(|error| TableSyncError::Progress(format!("serialize run scope: {error}")))
}

fn run_sync_table_with_targets_phase(
    config: &SyncTableConfig,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
) -> Result<SyncTableReport, TableSyncError> {
    if phase.is_verification() && config.updated_since.is_some() {
        return Err(TableSyncError::InvalidTable(
            "verify phase cannot use updated_since".to_string(),
        ));
    }
    match &config.updated_since {
        Some(updated_since) => run_recent_update_sync(
            config,
            source,
            repair_target,
            progress_store,
            updated_since.clone(),
        ),
        None => run_range_sync(
            config,
            source,
            target,
            repair_target,
            progress_store,
            phase,
            run_spec_json,
        ),
    }
}

fn run_recent_update_sync(
    config: &SyncTableConfig,
    source: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
    updated_since: UpdatedSince,
) -> Result<SyncTableReport, TableSyncError> {
    sync_recent_updates_with_progress(
        &config.run_id,
        &build_sync_run_scope(config)?,
        RecentUpdateSyncContext {
            table: &config.table,
            chunk_size: config.chunk_size,
            mode: config.mode,
            source,
            repair_target,
            progress_store,
            updated_since,
        },
    )
}

fn run_range_sync(
    config: &SyncTableConfig,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
) -> Result<SyncTableReport, TableSyncError> {
    sync_table_with_progress_range_phase_with_run_spec(
        RangeSyncRequest {
            table: &config.table,
            options: SyncRunOptions {
                run_id: config.run_id.clone(),
                run_scope: build_sync_run_scope(config)?,
                chunk_size: config.chunk_size,
                mode: config.mode,
                start_after: config.start_after.clone(),
                end_at: config.end_at.clone(),
            },
            source,
            target,
            repair_target,
            progress_store,
            phase,
        },
        run_spec_json,
    )
}

pub(crate) fn find_compatible_failed_run(
    config: &SyncTableConfig,
    phase: SyncPhase,
    table: &str,
) -> Result<Option<SyncRunCandidate>, TableSyncError> {
    if config.mode != SyncMode::Apply || phase != SyncPhase::InsertMissing {
        return Ok(None);
    }
    let mut progress_store = progress::MySqlSyncRunProgressStore::new(
        config.target.clone(),
        config.progress_table.clone(),
    );
    progress_store.ensure()?;
    let mut resumed_config = config.clone();
    resumed_config.mode = SyncMode::MissingPrimaryKeys;
    resumed_config.plan_hash = None;
    let expected_run_spec_json = super::range::build_run_spec_json(
        &build_sync_run_scope(&resumed_config)?,
        &resumed_config.table,
        resumed_config.chunk_size,
        resumed_config.mode,
        &resumed_config.start_after,
        &resumed_config.end_at,
    )?;
    claim_compatible_failed_run(&mut progress_store, table, phase, &expected_run_spec_json)
}
