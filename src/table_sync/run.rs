use super::mysql::MySqlSyncReader;
use super::range::sync_table_with_progress_range;
use super::recent::{RecentUpdateSyncContext, sync_recent_updates_with_progress};
use super::*;

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
            max_deletes: Some(0),
        },
        source,
        target,
        repair_target,
        progress_store,
    )
}

pub fn run_sync_table(config: &SyncTableConfig) -> Result<SyncTableReport, TableSyncError> {
    run_sync_table_phase(config, SyncPhase::All)
}

pub fn run_sync_table_phase(
    config: &SyncTableConfig,
    phase: SyncPhase,
) -> Result<SyncTableReport, TableSyncError> {
    validate_sync_table_config(config)?;
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new(target_connection_config(config));
    let mut progress_store = progress::MySqlSyncRunProgressStore::new(
        config.target.clone(),
        config.progress_table.clone(),
    );
    let mut repair_target = mysql_repair_target(config)?;
    run_sync_table_with_targets_phase(
        config,
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        phase,
    )
}

pub(crate) fn should_record_sync_run_error(error: &TableSyncError) -> bool {
    matches!(error, TableSyncError::Read(_) | TableSyncError::Repair(_))
}

fn mysql_repair_target(
    config: &SyncTableConfig,
) -> Result<
    crate::target::TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
    TableSyncError,
> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new(&config.target)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    Ok(crate::target::TargetMySqlWriter::from_snapshot_table(
        &snapshot_table(&config.table),
        executor,
        sync_insert_mode(config),
    ))
}

pub(crate) fn build_sync_run_scope(config: &SyncTableConfig) -> Result<String, TableSyncError> {
    let insert_conflict_policy = match config.target.insert_conflict_policy {
        crate::live::InsertConflictPolicy::Error => "error",
        crate::live::InsertConflictPolicy::IgnoreDuplicate => "ignore-duplicate",
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
) -> Result<SyncTableReport, TableSyncError> {
    if phase == SyncPhase::Verify && config.updated_since.is_some() {
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
        None => run_range_sync(config, source, target, repair_target, progress_store, phase),
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
) -> Result<SyncTableReport, TableSyncError> {
    sync_table_with_progress_range_phase(
        &config.table,
        SyncRunOptions {
            run_id: config.run_id.clone(),
            run_scope: build_sync_run_scope(config)?,
            chunk_size: config.chunk_size,
            mode: config.mode,
            start_after: config.start_after.clone(),
            end_at: config.end_at.clone(),
            max_deletes: config.max_deletes,
        },
        source,
        target,
        repair_target,
        progress_store,
        phase,
    )
}
