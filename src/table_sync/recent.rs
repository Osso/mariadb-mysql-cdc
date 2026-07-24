use super::*;
use crate::snapshot::SnapshotRow;

pub fn sync_recent_updates(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    updated_since: UpdatedSince,
) -> Result<SyncTableReport, TableSyncError> {
    sync_recent_updates_impl(RecentUpdateRun {
        table,
        chunk_size,
        mode,
        source,
        repair_target,
        updated_since,
    })
}

struct RecentUpdateRun<'a, S, R> {
    table: &'a SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &'a S,
    repair_target: &'a mut R,
    updated_since: UpdatedSince,
}

fn sync_recent_updates_impl<S, R>(
    context: RecentUpdateRun<'_, S, R>,
) -> Result<SyncTableReport, TableSyncError>
where
    S: SyncTableReader,
    R: SyncRepairTarget,
{
    validate_sync_table(context.table, context.chunk_size)?;
    let report = SyncTableReport {
        table: context.table.name.clone(),
        ..SyncTableReport::default()
    };
    run_recent_update_loop(context, report)
}

fn run_recent_update_loop<S, R>(
    context: RecentUpdateRun<'_, S, R>,
    mut report: SyncTableReport,
) -> Result<SyncTableReport, TableSyncError>
where
    S: SyncTableReader,
    R: SyncRepairTarget,
{
    let mut start_after = None;
    loop {
        let Some((end_at, is_complete)) = apply_recent_update_page(RecentUpdatePageInput {
            table: context.table,
            chunk_size: context.chunk_size,
            mode: context.mode,
            source: context.source,
            repair_target: context.repair_target,
            updated_since: &context.updated_since,
            start_after: start_after.clone(),
            report: &mut report,
        })?
        else {
            return Ok(report);
        };
        start_after = Some(end_at);
        if is_complete {
            return Ok(report);
        }
    }
}

fn read_recent_update_chunk(
    table: &SyncTable,
    chunk_size: usize,
    source: &dyn SyncTableReader,
    start_after: Option<Vec<String>>,
    updated_since: UpdatedSince,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    source.read_rows(&sync_chunk_request_with_updated_since(
        table,
        start_after,
        chunk_size,
        updated_since,
    ))
}

struct RecentUpdatePageInput<'a> {
    table: &'a SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &'a dyn SyncTableReader,
    repair_target: &'a mut dyn SyncRepairTarget,
    updated_since: &'a UpdatedSince,
    start_after: Option<Vec<String>>,
    report: &'a mut SyncTableReport,
}

fn apply_recent_update_page(
    input: RecentUpdatePageInput<'_>,
) -> Result<Option<(Vec<String>, bool)>, TableSyncError> {
    let source_rows = read_recent_update_chunk(
        input.table,
        input.chunk_size,
        input.source,
        input.start_after,
        input.updated_since.clone(),
    )?;
    if source_rows.is_empty() {
        return Ok(None);
    }
    apply_recent_update_chunk(&source_rows, input.mode, input.repair_target, input.report)?;
    let end_at = last_primary_key(&source_rows)?;
    Ok(Some((end_at, source_rows.len() < input.chunk_size)))
}

pub(crate) fn sync_recent_updates_with_progress<S, R, P>(
    run_id: &str,
    run_scope: &str,
    mut context: RecentUpdateSyncContext<'_, S, R, P>,
) -> Result<SyncTableReport, TableSyncError>
where
    S: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let progress = load_recent_update_progress(
        run_id,
        run_scope,
        context.table,
        context.chunk_size,
        context.mode,
        &context.updated_since,
        context.progress_store,
    )?;
    let result = sync_recent_update_chunks(&mut context, progress);
    let result = persist_sync_run_error(run_id, result, context.progress_store);
    finish_sync_run(run_id, result, context.progress_store)
}

fn load_recent_update_progress(
    run_id: &str,
    run_scope: &str,
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    updated_since: &UpdatedSince,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableProgress, TableSyncError> {
    validate_sync_table(table, chunk_size)?;
    let run_spec_json =
        recent_update_run_spec_json(run_scope, table, chunk_size, mode, updated_since)?;
    restart_recent_update_progress(run_id, &run_spec_json, table, mode, progress_store)
}

fn restart_recent_update_progress(
    run_id: &str,
    run_spec_json: &str,
    table: &SyncTable,
    mode: SyncMode,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableProgress, TableSyncError> {
    progress_store.ensure()?;
    progress_store.acquire_run(run_id)?;
    let result = (|| {
        if let Some(progress) = progress_store.load(run_id)? {
            validate_resumable_progress(progress, run_id, run_spec_json)?;
        }
        let progress = SyncTableProgress::started(
            run_id.to_string(),
            run_spec_json.to_string(),
            table.name.clone(),
            mode,
        );
        progress_store.save(&progress)?;
        Ok(progress)
    })();
    release_on_load_error(run_id, result, progress_store)
}

fn recent_update_run_spec_json(
    run_scope: &str,
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    updated_since: &UpdatedSince,
) -> Result<String, TableSyncError> {
    serde_json::to_string(&SyncRunSpec {
        scope: run_scope,
        table,
        chunk_size,
        mode,
        start_after: &None,
        end_at: &None,
        updated_since: Some(updated_since),
    })
    .map_err(|error| TableSyncError::Progress(format!("serialize run specification: {error}")))
}

pub(crate) struct RecentUpdateSyncContext<'a, S, R, P>
where
    S: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    pub(crate) table: &'a SyncTable,
    pub(crate) chunk_size: usize,
    pub(crate) mode: SyncMode,
    pub(crate) source: &'a S,
    pub(crate) repair_target: &'a mut R,
    pub(crate) progress_store: &'a mut P,
    pub(crate) updated_since: UpdatedSince,
}

struct RecentChunkRun<'a> {
    table: &'a SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &'a dyn SyncTableReader,
    repair_target: &'a mut dyn SyncRepairTarget,
    progress_store: &'a mut dyn SyncProgressStore,
    updated_since: UpdatedSince,
    progress: SyncTableProgress,
    report: SyncTableReport,
    start_after: Option<Vec<String>>,
}

fn sync_recent_update_chunks<S, R, P>(
    context: &mut RecentUpdateSyncContext<'_, S, R, P>,
    progress: SyncTableProgress,
) -> Result<SyncTableReport, TableSyncError>
where
    S: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let report = progress.report();
    let start_after = progress.last_primary_key.clone();
    run_recent_chunks(RecentChunkRun {
        table: context.table,
        chunk_size: context.chunk_size,
        mode: context.mode,
        source: context.source,
        repair_target: context.repair_target,
        progress_store: context.progress_store,
        updated_since: context.updated_since.clone(),
        progress,
        report,
        start_after,
    })
}

fn run_recent_chunks(mut run: RecentChunkRun<'_>) -> Result<SyncTableReport, TableSyncError> {
    loop {
        let Some((end_at, is_complete)) = apply_recent_update_page(RecentUpdatePageInput {
            table: run.table,
            chunk_size: run.chunk_size,
            mode: run.mode,
            source: run.source,
            repair_target: run.repair_target,
            updated_since: &run.updated_since,
            start_after: run.start_after,
            report: &mut run.report,
        })?
        else {
            return complete_recent_update(&mut run.progress, run.progress_store, run.report);
        };
        save_recent_update_progress(
            &mut run.progress,
            &run.report,
            end_at.clone(),
            run.progress_store,
        )?;
        if is_complete {
            return complete_recent_update(&mut run.progress, run.progress_store, run.report);
        }
        run.start_after = Some(end_at);
    }
}

fn save_recent_update_progress(
    progress: &mut SyncTableProgress,
    report: &SyncTableReport,
    end_at: Vec<String>,
    progress_store: &mut dyn SyncProgressStore,
) -> Result<(), TableSyncError> {
    progress.record_chunk(report, end_at);
    progress_store.save(progress)
}

fn complete_recent_update(
    progress: &mut SyncTableProgress,
    progress_store: &mut dyn SyncProgressStore,
    report: SyncTableReport,
) -> Result<SyncTableReport, TableSyncError> {
    complete_sync_progress(progress, progress_store)?;
    Ok(report)
}
