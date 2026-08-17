use super::chunk::sync_next_chunk;
use super::config::{SyncConfig, SyncRunIdentity};
use super::model::{
    SyncChunkConfig, SyncChunkProgress, SyncChunkProgressStore, SyncChunkSource,
    SyncChunkTargetSession, SyncTable,
};
use super::mysql::{MySqlSyncProgressStore, MySqlSyncSource, MySqlSyncTargetSession};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

pub(crate) fn sync_table_to_completion(
    config: &SyncChunkConfig,
    source: &mut impl SyncChunkSource,
    target: &mut impl SyncChunkTargetSession,
    progress_store: &mut impl SyncChunkProgressStore,
) -> Result<SyncChunkProgress, String> {
    loop {
        let progress = sync_next_chunk(config, source, target, progress_store)?;
        if progress.complete {
            return Ok(progress);
        }
    }
}

pub(crate) fn run_mysql_sync_table(
    config: &SyncConfig,
    identity: &SyncRunIdentity,
    table: SyncTable,
) -> Result<SyncChunkProgress, String> {
    let table_name = table.name.clone();
    let chunk = SyncChunkConfig {
        run_id: identity.run_id.clone(),
        run_spec_json: identity.run_spec_json.clone(),
        target_database: config.target.database.clone(),
        table: table.clone(),
        chunk_size: config.chunk_size,
    };
    let mut source = MySqlSyncSource::new(&config.source, table.clone())
        .map_err(|error| format!("connect source for sync table `{table_name}`: {error}"))?;
    let mut target = MySqlSyncTargetSession::new(&config.target, table).map_err(|error| {
        format!("connect locked target session for sync table `{table_name}`: {error}")
    })?;
    let mut progress = MySqlSyncProgressStore::new(&config.target, config.progress_table.clone())
        .map_err(|error| {
        format!("connect progress store for sync table `{table_name}`: {error}")
    })?;
    sync_table_to_completion(&chunk, &mut source, &mut target, &mut progress)
}

pub(crate) fn run_mysql_sync_tables(
    config: &SyncConfig,
    identity: &SyncRunIdentity,
    tables: Vec<SyncTable>,
) -> Result<Vec<SyncChunkProgress>, String> {
    run_sync_tables_bounded(config, identity, tables, run_mysql_sync_table)
}

pub(crate) fn run_sync_tables_bounded<F>(
    config: &SyncConfig,
    identity: &SyncRunIdentity,
    mut tables: Vec<SyncTable>,
    run_table: F,
) -> Result<Vec<SyncChunkProgress>, String>
where
    F: Fn(&SyncConfig, &SyncRunIdentity, SyncTable) -> Result<SyncChunkProgress, String> + Sync,
{
    validate_row_stage_execution(config, &tables)?;
    tables.sort_by(|left, right| left.name.cmp(&right.name));

    let mut reports = Vec::with_capacity(tables.len());
    for batch in tables.chunks(config.parallelism) {
        let mut results = run_table_batch(config, identity, batch, &run_table);
        results.sort_by_key(|result| result.completion_order);
        for result in results {
            match result.progress {
                Ok(progress) => reports.push(progress),
                Err(error) => {
                    return Err(format!("sync table `{}` failed: {error}", result.table));
                }
            }
        }
    }
    reports.sort_by(|left, right| left.table.cmp(&right.table));
    Ok(reports)
}

struct TableWorkerResult {
    completion_order: usize,
    table: String,
    progress: Result<SyncChunkProgress, String>,
}

fn run_table_batch<F>(
    config: &SyncConfig,
    identity: &SyncRunIdentity,
    tables: &[SyncTable],
    run_table: &F,
) -> Vec<TableWorkerResult>
where
    F: Fn(&SyncConfig, &SyncRunIdentity, SyncTable) -> Result<SyncChunkProgress, String> + Sync,
{
    let completion_order = AtomicUsize::new(0);
    thread::scope(|scope| {
        let workers = tables
            .iter()
            .cloned()
            .map(|table| {
                let table_name = table.name.clone();
                let completion_order = &completion_order;
                let worker = scope.spawn(move || {
                    let progress = run_table(config, identity, table);
                    let order = completion_order.fetch_add(1, Ordering::SeqCst);
                    (order, progress)
                });
                (table_name, worker)
            })
            .collect::<Vec<_>>();

        workers
            .into_iter()
            .map(|(table, worker)| join_table_worker(table, worker))
            .collect()
    })
}

fn join_table_worker(
    table: String,
    worker: thread::ScopedJoinHandle<'_, (usize, Result<SyncChunkProgress, String>)>,
) -> TableWorkerResult {
    match worker.join() {
        Ok((completion_order, progress)) => TableWorkerResult {
            completion_order,
            table,
            progress,
        },
        Err(_) => TableWorkerResult {
            completion_order: usize::MAX,
            table,
            progress: Err("worker panicked".to_string()),
        },
    }
}

fn validate_row_stage_execution(config: &SyncConfig, tables: &[SyncTable]) -> Result<(), String> {
    if tables.is_empty() {
        return Err("sync row stage requires at least one table".to_string());
    }
    if config.parallelism == 0 {
        return Err("sync row stage parallelism must be greater than zero".to_string());
    }
    Ok(())
}
