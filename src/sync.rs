use crate::snapshot::SnapshotRow;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncTable {
    pub(crate) name: String,
    pub(crate) primary_key: Vec<String>,
    pub(crate) columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncChunkConfig {
    pub(crate) run_id: String,
    pub(crate) target_database: String,
    pub(crate) table: SyncTable,
    pub(crate) chunk_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncChunkReadRequest {
    pub(crate) start_after: Option<Vec<String>>,
    pub(crate) end_at: Option<Vec<String>>,
    pub(crate) limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncChunkProgress {
    pub(crate) run_id: String,
    pub(crate) table: String,
    pub(crate) last_primary_key: Option<Vec<String>>,
    pub(crate) complete: bool,
    pub(crate) chunks: u64,
    pub(crate) rows_scanned: u64,
    pub(crate) inserts: u64,
    pub(crate) updates: u64,
    pub(crate) deletes: u64,
}

pub(crate) trait SyncChunkSource {
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<SnapshotRow>, String>;
}

pub(crate) trait SyncChunkTargetSession {
    fn set_autocommit(&mut self, enabled: bool) -> Result<(), String>;
    fn lock_table_write(&mut self, database: &str, table: &str) -> Result<(), String>;
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<SnapshotRow>, String>;
    fn delete_rows(&mut self, primary_keys: &[Vec<String>]) -> Result<(), String>;
    fn update_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), String>;
    fn insert_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), String>;
    fn commit(&mut self) -> Result<(), String>;
    fn rollback(&mut self) -> Result<(), String>;
    fn unlock_tables(&mut self) -> Result<(), String>;
}

pub(crate) trait SyncChunkProgressStore {
    fn load(&mut self, run_id: &str, table: &str) -> Result<Option<SyncChunkProgress>, String>;
    fn save(&mut self, progress: &SyncChunkProgress) -> Result<(), String>;
}

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
    Ok(loaded.unwrap_or_else(|| SyncChunkProgress {
        run_id: config.run_id.clone(),
        table: config.table.name.clone(),
        last_primary_key: None,
        complete: false,
        chunks: 0,
        rows_scanned: 0,
        inserts: 0,
        updates: 0,
        deletes: 0,
    }))
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
        apply_source_window(config, progress, start_after, source_rows, target)?
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
    source_rows: Vec<SnapshotRow>,
    target: &mut impl SyncChunkTargetSession,
) -> Result<SyncChunkProgress, String> {
    let end_at = source_rows
        .last()
        .map(|row| row.primary_key.clone())
        .expect("non-empty source window");
    let target_rows = target
        .read_rows(&SyncChunkReadRequest {
            start_after,
            end_at: Some(end_at.clone()),
            limit: config.chunk_size,
        })
        .map_err(|error| format!("read target chunk for `{}`: {error}", config.table.name))?;
    let changes = chunk_changes(&config.table, &source_rows, &target_rows);
    apply_changes(&config.table.name, target, &changes)?;

    progress.last_primary_key = Some(end_at);
    progress.complete = false;
    record_progress(&mut progress, source_rows.len(), &changes);
    Ok(progress)
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
    updates: Vec<SnapshotRow>,
    inserts: Vec<SnapshotRow>,
}

fn chunk_changes(
    table: &SyncTable,
    source_rows: &[SnapshotRow],
    target_rows: &[SnapshotRow],
) -> ChunkChanges {
    let source_by_key = index_rows(source_rows);
    let target_by_key = index_rows(target_rows);
    ChunkChanges {
        deletes: target_only_primary_keys(target_rows, &source_by_key),
        updates: divergent_source_rows(table, source_rows, &target_by_key),
        inserts: missing_source_rows(source_rows, &target_by_key),
    }
}

fn index_rows(rows: &[SnapshotRow]) -> BTreeMap<Vec<String>, &SnapshotRow> {
    rows.iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect()
}

fn target_only_primary_keys(
    target_rows: &[SnapshotRow],
    source_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
) -> Vec<Vec<String>> {
    target_rows
        .iter()
        .filter(|row| !source_by_key.contains_key(&row.primary_key))
        .map(|row| row.primary_key.clone())
        .collect()
}

fn divergent_source_rows(
    table: &SyncTable,
    source_rows: &[SnapshotRow],
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
) -> Vec<SnapshotRow> {
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
    source_rows: &[SnapshotRow],
    target_by_key: &BTreeMap<Vec<String>, &SnapshotRow>,
) -> Vec<SnapshotRow> {
    source_rows
        .iter()
        .filter(|row| !target_by_key.contains_key(&row.primary_key))
        .cloned()
        .collect()
}

fn rows_diverge(table: &SyncTable, source: &SnapshotRow, target: &SnapshotRow) -> bool {
    table
        .columns
        .iter()
        .filter(|column| !table.primary_key.contains(column))
        .any(|column| source.values.get(column) != target.values.get(column))
}

fn apply_changes(
    table: &str,
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
    if !changes.inserts.is_empty() {
        target
            .insert_rows(&changes.inserts)
            .map_err(|error| format!("insert missing rows into `{table}`: {error}"))?;
    }
    Ok(())
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
