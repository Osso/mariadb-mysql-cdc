use super::mysql::MySqlSyncReader;
use super::range::sync_table_with_progress_range;
use super::recent::{RecentUpdateSyncContext, sync_recent_updates_with_progress};
use super::*;
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
            max_deletes: Some(0),
        },
        source,
        target,
        repair_target,
        progress_store,
    )
}

pub fn run_sync_table(config: &SyncTableConfig) -> Result<SyncTableReport, TableSyncError> {
    retry_sync_table_operation(
        config.mode,
        SYNC_CONNECTION_ATTEMPTS,
        SYNC_CONNECTION_RETRY_DELAY,
        || run_sync_table_phase(config, SyncPhase::All),
    )
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
    let attempts = if mode == SyncMode::MissingPrimaryKeys {
        max_attempts.max(1)
    } else {
        1
    };
    for attempt in 1..=attempts {
        match operation() {
            Ok(report) => return Ok(report),
            Err(error) if attempt < attempts && is_retryable_connection_error(&error) => {
                if !retry_delay.is_zero() {
                    std::thread::sleep(retry_delay);
                }
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("sync retry loop has at least one attempt")
}

fn is_retryable_connection_error(error: &TableSyncError) -> bool {
    if matches!(error, TableSyncError::Read(_) | TableSyncError::Progress(_)) {
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
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new_with_target(target_connection_config(config), &config.target)
        .map_err(TableSyncError::Read)?;
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
        run_spec_json,
    )
}

pub(crate) fn should_record_sync_run_error(error: &TableSyncError) -> bool {
    matches!(error, TableSyncError::Read(_) | TableSyncError::Repair(_))
}

fn mysql_repair_target(config: &SyncTableConfig) -> Result<MySqlSyncRepairTarget, TableSyncError> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new(&config.target)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    Ok(MySqlSyncRepairTarget::new(
        crate::target::TargetMySqlWriter::from_snapshot_table(
            &snapshot_table(&config.table),
            executor,
            sync_insert_mode(config),
        ),
    ))
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
                max_deletes: config.max_deletes,
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
        resumed_config.max_deletes,
    )?;
    claim_compatible_failed_run(&mut progress_store, table, phase, &expected_run_spec_json)
}
