use super::model::{
    SyncChunkConfig, SyncChunkProgress, SyncChunkProgressStore, SyncChunkReadRequest,
    SyncChunkSource, SyncChunkTargetSession, SyncInsertFailure, SyncTable, SyncUniqueOwnerAction,
    SyncUniqueOwnerConflict,
};
use crate::database_row::DatabaseRow;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn sync_next_chunk(
    config: &SyncChunkConfig,
    source: &mut impl SyncChunkSource,
    target: &mut impl SyncChunkTargetSession,
    progress_store: &mut impl SyncChunkProgressStore,
) -> Result<SyncChunkProgress, String> {
    let progress = load_progress(config, progress_store)?;
    if progress.complete {
        return Ok(progress);
    }

    prepare_target_chunk(config, target)?;
    let applied = apply_locked_chunk(config, progress, source, target);
    let progress = match applied {
        Ok(progress) => progress,
        Err(error) => return Err(rollback_and_unlock(target, error)),
    };
    save_progress_and_unlock(config, progress, target, progress_store)
}

fn prepare_target_chunk(
    config: &SyncChunkConfig,
    target: &mut impl SyncChunkTargetSession,
) -> Result<(), String> {
    target.set_autocommit(false).map_err(|error| {
        format!(
            "disable autocommit for target table `{}`.`{}`: {error}",
            config.target_database, config.table.name
        )
    })?;
    target
        .lock_table_write(&config.target_database, &config.table.name)
        .map_err(|error| {
            format!(
                "lock target table `{}`.`{}` for write: {error}",
                config.target_database, config.table.name
            )
        })
}

fn save_progress_and_unlock(
    config: &SyncChunkConfig,
    progress: SyncChunkProgress,
    target: &mut impl SyncChunkTargetSession,
    progress_store: &mut impl SyncChunkProgressStore,
) -> Result<SyncChunkProgress, String> {
    if let Err(error) = progress_store.save(&progress) {
        let primary = format!(
            "save sync progress for run `{}` table `{}`: {error}",
            config.run_id, config.table.name
        );
        return Err(unlock_after_error(target, primary));
    }
    target.unlock_tables().map_err(|error| {
        format!(
            "unlock target table `{}`.`{}` after durable progress: {error}",
            config.target_database, config.table.name
        )
    })?;
    Ok(progress)
}

fn load_progress(
    config: &SyncChunkConfig,
    progress_store: &mut impl SyncChunkProgressStore,
) -> Result<SyncChunkProgress, String> {
    let loaded = progress_store
        .load(&config.run_id, &config.table.name)
        .map_err(|error| {
            format!(
                "load sync progress for run `{}` table `{}`: {error}",
                config.run_id, config.table.name
            )
        })?;
    match loaded {
        Some(progress) => {
            validate_progress_identity(config, &progress)?;
            Ok(progress)
        }
        None => Ok(SyncChunkProgress {
            run_id: config.run_id.clone(),
            table: config.table.name.clone(),
            run_spec_json: config.run_spec_json.clone(),
            last_primary_key: None,
            complete: false,
            chunks: 0,
            rows_scanned: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
        }),
    }
}

fn validate_progress_identity(
    config: &SyncChunkConfig,
    progress: &SyncChunkProgress,
) -> Result<(), String> {
    if progress.run_id != config.run_id {
        return Err(format!(
            "loaded sync progress run ID mismatch for table `{}`: expected `{}`, found `{}`",
            config.table.name, config.run_id, progress.run_id
        ));
    }
    if progress.table != config.table.name {
        return Err(format!(
            "loaded sync progress table mismatch for run `{}`: expected `{}`, found `{}`",
            config.run_id, config.table.name, progress.table
        ));
    }
    if progress.run_spec_json != config.run_spec_json {
        return Err(format!(
            "loaded sync progress run specification mismatch for run `{}` table `{}`: expected `{}`, found `{}`",
            config.run_id, config.table.name, config.run_spec_json, progress.run_spec_json
        ));
    }
    Ok(())
}

fn apply_locked_chunk(
    config: &SyncChunkConfig,
    progress: SyncChunkProgress,
    source: &mut impl SyncChunkSource,
    target: &mut impl SyncChunkTargetSession,
) -> Result<SyncChunkProgress, String> {
    let start_after = progress.last_primary_key.clone();
    let source_rows = source
        .read_rows(&SyncChunkReadRequest {
            start_after: start_after.clone(),
            end_at: None,
            limit: config.chunk_size,
        })
        .map_err(|error| format!("read source chunk for `{}`: {error}", config.table.name))?;

    let next_progress = if source_rows.is_empty() {
        apply_target_tail(config, progress, start_after, target)?
    } else {
        apply_source_window(config, progress, start_after, source_rows, source, target)?
    };

    target
        .commit()
        .map_err(|error| format!("commit target chunk for `{}`: {error}", config.table.name))?;
    Ok(next_progress)
}

fn apply_source_window(
    config: &SyncChunkConfig,
    mut progress: SyncChunkProgress,
    start_after: Option<Vec<String>>,
    source_rows: Vec<DatabaseRow>,
    source: &mut impl SyncChunkSource,
    target: &mut impl SyncChunkTargetSession,
) -> Result<SyncChunkProgress, String> {
    let end_at = source_rows
        .last()
        .map(|row| row.primary_key.clone())
        .expect("non-empty source window");
    let target_rows = read_complete_target_window(config, start_after, &end_at, target)?;
    let changes = chunk_changes(&config.table, &source_rows, &target_rows);
    apply_changes(&config.table.name, source, target, &changes)?;

    progress.last_primary_key = Some(end_at);
    progress.complete = false;
    record_progress(&mut progress, source_rows.len(), &changes);
    Ok(progress)
}

fn read_complete_target_window(
    config: &SyncChunkConfig,
    mut start_after: Option<Vec<String>>,
    end_at: &[String],
    target: &mut impl SyncChunkTargetSession,
) -> Result<Vec<DatabaseRow>, String> {
    let mut target_rows = Vec::new();
    loop {
        let page = target
            .read_rows(&SyncChunkReadRequest {
                start_after,
                end_at: Some(end_at.to_vec()),
                limit: config.chunk_size,
            })
            .map_err(|error| format!("read target chunk for `{}`: {error}", config.table.name))?;
        let page_is_complete = page.len() < config.chunk_size;
        start_after = page.last().map(|row| row.primary_key.clone());
        target_rows.extend(page);
        if page_is_complete {
            return Ok(target_rows);
        }
    }
}

fn apply_target_tail(
    config: &SyncChunkConfig,
    mut progress: SyncChunkProgress,
    start_after: Option<Vec<String>>,
    target: &mut impl SyncChunkTargetSession,
) -> Result<SyncChunkProgress, String> {
    let target_rows = target
        .read_rows(&SyncChunkReadRequest {
            start_after,
            end_at: None,
            limit: config.chunk_size,
        })
        .map_err(|error| format!("read target tail for `{}`: {error}", config.table.name))?;
    let primary_keys = target_rows
        .iter()
        .map(|row| row.primary_key.clone())
        .collect::<Vec<_>>();
    if !primary_keys.is_empty() {
        target.delete_rows(&primary_keys).map_err(|error| {
            format!(
                "delete target-only rows from `{}`: {error}",
                config.table.name
            )
        })?;
    }

    progress.complete = target_rows.len() < config.chunk_size;
    progress.chunks += 1;
    progress.deletes += target_rows.len() as u64;
    Ok(progress)
}

struct ChunkChanges {
    deletes: Vec<Vec<String>>,
    updates: Vec<DatabaseRow>,
    inserts: Vec<DatabaseRow>,
}

fn chunk_changes(
    table: &SyncTable,
    source_rows: &[DatabaseRow],
    target_rows: &[DatabaseRow],
) -> ChunkChanges {
    let source_by_key = index_rows(source_rows);
    let target_by_key = index_rows(target_rows);
    ChunkChanges {
        deletes: target_only_primary_keys(target_rows, &source_by_key),
        updates: divergent_source_rows(table, source_rows, &target_by_key),
        inserts: missing_source_rows(source_rows, &target_by_key),
    }
}

fn index_rows(rows: &[DatabaseRow]) -> BTreeMap<Vec<String>, &DatabaseRow> {
    rows.iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect()
}

fn target_only_primary_keys(
    target_rows: &[DatabaseRow],
    source_by_key: &BTreeMap<Vec<String>, &DatabaseRow>,
) -> Vec<Vec<String>> {
    target_rows
        .iter()
        .filter(|row| !source_by_key.contains_key(&row.primary_key))
        .map(|row| row.primary_key.clone())
        .collect()
}

fn divergent_source_rows(
    table: &SyncTable,
    source_rows: &[DatabaseRow],
    target_by_key: &BTreeMap<Vec<String>, &DatabaseRow>,
) -> Vec<DatabaseRow> {
    source_rows
        .iter()
        .filter(|row| {
            target_by_key
                .get(&row.primary_key)
                .is_some_and(|target| rows_diverge(table, row, target))
        })
        .cloned()
        .collect()
}

fn missing_source_rows(
    source_rows: &[DatabaseRow],
    target_by_key: &BTreeMap<Vec<String>, &DatabaseRow>,
) -> Vec<DatabaseRow> {
    source_rows
        .iter()
        .filter(|row| !target_by_key.contains_key(&row.primary_key))
        .cloned()
        .collect()
}

fn rows_diverge(table: &SyncTable, source: &DatabaseRow, target: &DatabaseRow) -> bool {
    table
        .columns
        .iter()
        .filter(|column| !table.primary_key.contains(column))
        .any(|column| source.values.get(column) != target.values.get(column))
}

fn apply_changes(
    table: &str,
    source: &mut impl SyncChunkSource,
    target: &mut impl SyncChunkTargetSession,
    changes: &ChunkChanges,
) -> Result<(), String> {
    if !changes.deletes.is_empty() {
        target
            .delete_rows(&changes.deletes)
            .map_err(|error| format!("delete target-only rows from `{table}`: {error}"))?;
    }
    if !changes.updates.is_empty() {
        target
            .update_rows(&changes.updates)
            .map_err(|error| format!("update divergent rows in `{table}`: {error}"))?;
    }
    apply_strict_inserts(table, source, target, &changes.inserts)
}

fn apply_strict_inserts(
    table: &str,
    source: &mut impl SyncChunkSource,
    target: &mut impl SyncChunkTargetSession,
    rows: &[DatabaseRow],
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut pending_rows = rows.to_vec();
    let mut reconciled_conflicts = BTreeSet::new();
    let mut reconciled_intended_rows = BTreeMap::new();
    loop {
        let Some(failure) = try_strict_insert(table, target, &pending_rows)? else {
            break;
        };
        pending_rows = failure.retry_rows();
        inspect_and_reconcile_insert_failure(
            table,
            source,
            target,
            &failure,
            &mut reconciled_conflicts,
            &mut reconciled_intended_rows,
        )?;
    }
    if !reconciled_intended_rows.is_empty() {
        let rows = reconciled_intended_rows.into_values().collect::<Vec<_>>();
        target
            .verify_rows(&rows)
            .map_err(|error| format!("verify reconciled inserts in `{table}`: {error}"))?;
    }
    Ok(())
}

fn try_strict_insert(
    table: &str,
    target: &mut impl SyncChunkTargetSession,
    rows: &[DatabaseRow],
) -> Result<Option<SyncInsertFailure>, String> {
    match target.insert_rows(rows) {
        Ok(()) => Ok(None),
        Err(failure) if failure.mysql_code == Some(1062) => Ok(Some(failure)),
        Err(failure) => Err(format!("insert missing rows into `{table}`: {failure}")),
    }
}

fn inspect_and_reconcile_insert_failure(
    table: &str,
    source: &mut impl SyncChunkSource,
    target: &mut impl SyncChunkTargetSession,
    failure: &SyncInsertFailure,
    reconciled_conflicts: &mut BTreeSet<(String, Vec<String>, Vec<String>)>,
    reconciled_intended_rows: &mut BTreeMap<Vec<String>, DatabaseRow>,
) -> Result<(), String> {
    let conflicts = target
        .inspect_unique_owner_conflicts(failure)
        .map_err(|error| format!("inspect secondary unique conflict in `{table}`: {error}"))?;
    for conflict in conflicts {
        reconcile_unique_owner_conflict(
            table,
            source,
            target,
            conflict,
            reconciled_conflicts,
            reconciled_intended_rows,
        )?;
    }
    Ok(())
}

fn reconcile_unique_owner_conflict(
    table: &str,
    source: &mut impl SyncChunkSource,
    target: &mut impl SyncChunkTargetSession,
    conflict: SyncUniqueOwnerConflict,
    reconciled_conflicts: &mut BTreeSet<(String, Vec<String>, Vec<String>)>,
    reconciled_intended_rows: &mut BTreeMap<Vec<String>, DatabaseRow>,
) -> Result<(), String> {
    record_unique_owner_repair_key(table, &conflict, reconciled_conflicts)?;
    let source_owner = source
        .read_row_by_primary_key(&conflict.owner.primary_key)
        .map_err(|error| {
            format!(
                "read current source owner for `{table}` index `{}`: {error}",
                conflict.index.name
            )
        })?;
    let action = plan_unique_owner_action(table, &conflict, source_owner.as_ref())?;
    target
        .reconcile_unique_owner(&conflict, &action)
        .map_err(|error| {
            format!(
                "reconcile secondary unique owner in `{table}` index `{}`: {error}",
                conflict.index.name
            )
        })?;
    reconciled_intended_rows.insert(conflict.intended.primary_key.clone(), conflict.intended);
    Ok(())
}

fn record_unique_owner_repair_key(
    table: &str,
    conflict: &SyncUniqueOwnerConflict,
    reconciled_conflicts: &mut BTreeSet<(String, Vec<String>, Vec<String>)>,
) -> Result<(), String> {
    let repair_key = (
        conflict.index.name.clone(),
        conflict.intended.primary_key.clone(),
        conflict.owner.primary_key.clone(),
    );
    if reconciled_conflicts.insert(repair_key) {
        return Ok(());
    }
    Err(format!(
        "secondary unique conflict repeated for `{table}` index `{}` intended primary key {:?}",
        conflict.index.name, conflict.intended.primary_key
    ))
}

fn plan_unique_owner_action(
    table: &str,
    conflict: &SyncUniqueOwnerConflict,
    source_owner: Option<&DatabaseRow>,
) -> Result<SyncUniqueOwnerAction, String> {
    let intended_identity = validate_unique_owner_conflict(table, conflict)?;
    let Some(source_owner) = source_owner else {
        return Ok(SyncUniqueOwnerAction::Delete);
    };
    validate_current_source_owner(table, conflict, source_owner, &intended_identity)?;
    Ok(SyncUniqueOwnerAction::Update(source_owner.clone()))
}

fn validate_unique_owner_conflict(
    table: &str,
    conflict: &SyncUniqueOwnerConflict,
) -> Result<Vec<String>, String> {
    if conflict.owner.primary_key == conflict.intended.primary_key {
        return Err(format!(
            "secondary unique owner for `{table}` index `{}` has the intended primary key",
            conflict.index.name
        ));
    }
    let intended_identity = conflict.index.values(&conflict.intended, "intended")?;
    let target_identity = conflict.index.values(&conflict.owner, "target owner")?;
    if target_identity == intended_identity {
        return Ok(intended_identity);
    }
    Err(format!(
        "secondary unique owner for `{table}` index `{}` has ambiguous identity",
        conflict.index.name
    ))
}

fn validate_current_source_owner(
    table: &str,
    conflict: &SyncUniqueOwnerConflict,
    source_owner: &DatabaseRow,
    intended_identity: &[String],
) -> Result<(), String> {
    if source_owner.primary_key != conflict.owner.primary_key {
        return Err(format!(
            "current source owner primary key disagrees for `{table}` index `{}`",
            conflict.index.name
        ));
    }
    if source_owner
        .values
        .keys()
        .ne(conflict.intended.values.keys())
    {
        return Err(format!(
            "current source owner row is incomplete for `{table}` index `{}`",
            conflict.index.name
        ));
    }
    let source_identity = conflict
        .index
        .values(source_owner, "current source owner")?;
    if source_identity != intended_identity {
        return Ok(());
    }
    Err(format!(
        "current source owner legitimately owns `{}` identity for `{table}`",
        conflict.index.name
    ))
}

fn record_progress(progress: &mut SyncChunkProgress, source_rows: usize, changes: &ChunkChanges) {
    progress.chunks += 1;
    progress.rows_scanned += source_rows as u64;
    progress.inserts += changes.inserts.len() as u64;
    progress.updates += changes.updates.len() as u64;
    progress.deletes += changes.deletes.len() as u64;
}

fn rollback_and_unlock(target: &mut impl SyncChunkTargetSession, primary_error: String) -> String {
    let mut errors = vec![primary_error];
    if let Err(error) = target.rollback() {
        errors.push(format!("rollback failed: {error}"));
    }
    if let Err(error) = target.unlock_tables() {
        errors.push(format!("unlock tables failed: {error}"));
    }
    errors.join("; ")
}

fn unlock_after_error(target: &mut impl SyncChunkTargetSession, primary_error: String) -> String {
    match target.unlock_tables() {
        Ok(()) => primary_error,
        Err(error) => format!("{primary_error}; unlock tables failed: {error}"),
    }
}
