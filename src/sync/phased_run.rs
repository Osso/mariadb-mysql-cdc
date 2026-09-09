use super::chunk::sync_next_chunk_with_phase;
use super::config::{SyncConfig, SyncRunIdentity};
use super::dependency_order::{Direction, plan_dependency_batches};
use super::model::{
    SyncChunkConfig, SyncChunkProgress, SyncChunkProgressStore, SyncMutationPhase, SyncTable,
};
use super::mysql::{
    MySqlSyncProgressStore, MySqlSyncSource, MySqlSyncTargetSession, open_sync_connection,
};
use super::phase_progress::{
    MySqlPhaseProgressStore, PhaseChunkProgressStore, build_create_sync_phase_progress_table_sql,
};
use super::run::run_sync_tables_bounded;
use crate::inventory::SchemaInventory;
use crate::mysql_client::sync_target_opts;
use mysql::prelude::Queryable;
use std::collections::BTreeMap;

pub(crate) fn run_mysql_sync_phases(
    config: &SyncConfig,
    identity: &SyncRunIdentity,
    tables: Vec<SyncTable>,
    inventory: &SchemaInventory,
) -> Result<Vec<SyncChunkProgress>, String> {
    let mut legacy = MySqlSyncProgressStore::new(
        &config.target,
        config.progress_table.clone(),
        config.coordinator_session_wait_timeout_seconds,
    )?;
    let mut completed = Vec::new();
    let mut pending = BTreeMap::new();
    for table in tables {
        match legacy.load(&identity.run_id, &table.name)? {
            Some(progress) if progress.complete => completed.push(progress),
            _ => {
                pending.insert(table.name.clone(), table);
            }
        }
    }
    if pending.is_empty() {
        return Ok(completed);
    }
    let names = pending.keys().cloned().collect::<Vec<_>>();
    let inserts = plan_dependency_batches(
        &names,
        &inventory.foreign_keys,
        &inventory.schema,
        Direction::ParentFirst,
        config.parallelism,
    )
    .map_err(|error| format!("plan parent-first row phases: {error:?}"))?;
    let deletes = plan_dependency_batches(
        &names,
        &inventory.foreign_keys,
        &inventory.schema,
        Direction::ChildFirst,
        config.parallelism,
    )
    .map_err(|error| format!("plan child-first row phases: {error:?}"))?;
    let phase_table = format!("{}_phases", config.progress_table);
    let mut conn = open_sync_connection(sync_target_opts(&config.target)?)
        .map_err(|error| format!("connect phase progress: {error}"))?;
    conn.query_drop(build_create_sync_phase_progress_table_sql(&phase_table))
        .map_err(|error| format!("create phase progress table `{phase_table}`: {error}"))?;
    let mut totals: BTreeMap<String, SyncChunkProgress> = BTreeMap::new();
    for (phase, batches) in [
        (SyncMutationPhase::InsertMissing, &inserts),
        (SyncMutationPhase::UpdateDivergent, &inserts),
        (SyncMutationPhase::DeleteExtras, &deletes),
    ] {
        for batch in batches {
            let selected = batch.iter().map(|name| pending[name].clone()).collect();
            let reports =
                run_sync_tables_bounded(config, identity, selected, |config, identity, table| {
                    run_table_phase(config, identity, table, phase, &phase_table)
                })?;
            for report in reports {
                match totals.get_mut(&report.table) {
                    Some(total) => {
                        total.chunks += report.chunks;
                        total.rows_scanned += report.rows_scanned;
                        total.inserts += report.inserts;
                        total.updates += report.updates;
                        total.deletes += report.deletes;
                        total.last_primary_key = report.last_primary_key;
                    }
                    None => {
                        totals.insert(report.table.clone(), report);
                    }
                }
            }
        }
    }
    for report in totals.into_values() {
        legacy.save(&report)?;
        completed.push(report);
    }
    completed.sort_by(|left, right| left.table.cmp(&right.table));
    Ok(completed)
}

fn run_table_phase(
    config: &SyncConfig,
    identity: &SyncRunIdentity,
    table: SyncTable,
    phase: SyncMutationPhase,
    phase_table: &str,
) -> Result<SyncChunkProgress, String> {
    let chunk = SyncChunkConfig {
        run_id: identity.run_id.clone(),
        target_database: config.target.database.clone(),
        table: table.clone(),
        chunk_size: config.chunk_size,
    };
    let mut source = MySqlSyncSource::new(&config.source, table.clone())?;
    let mut target = MySqlSyncTargetSession::new(&config.target, table)?;
    let mut legacy = MySqlSyncProgressStore::new(
        &config.target,
        config.progress_table.clone(),
        config.coordinator_session_wait_timeout_seconds,
    )?;
    let mut conn = open_sync_connection(sync_target_opts(&config.target)?)
        .map_err(|error| format!("connect phase progress worker: {error}"))?;
    let mut phases = MySqlPhaseProgressStore::new(&mut conn, phase_table);
    let mut progress = PhaseChunkProgressStore::new(&mut phases, &mut legacy, phase)?;
    loop {
        let report =
            sync_next_chunk_with_phase(&chunk, &mut source, &mut target, &mut progress, phase)?;
        if report.complete {
            return Ok(report);
        }
    }
}
