use super::mysql::MySqlSyncReader;
use super::*;
use crate::snapshot::SnapshotRow;

pub fn sync_table_with_progress_range(
    table: &SyncTable,
    options: SyncRunOptions,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableReport, TableSyncError> {
    sync_table_with_progress_range_phase(
        table,
        options,
        source,
        target,
        repair_target,
        progress_store,
        SyncPhase::All,
    )
}

pub fn sync_table_with_progress_range_phase(
    table: &SyncTable,
    options: SyncRunOptions,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
    phase: SyncPhase,
) -> Result<SyncTableReport, TableSyncError> {
    sync_table_with_progress_range_phase_with_run_spec(
        RangeSyncRequest {
            table,
            options,
            source,
            target,
            repair_target,
            progress_store,
            phase,
        },
        None,
    )
}

pub(crate) struct RangeSyncRequest<'a, S, T, R, P> {
    pub(crate) table: &'a SyncTable,
    pub(crate) options: SyncRunOptions,
    pub(crate) source: &'a S,
    pub(crate) target: &'a T,
    pub(crate) repair_target: &'a mut R,
    pub(crate) progress_store: &'a mut P,
    pub(crate) phase: SyncPhase,
}

pub(crate) fn sync_table_with_progress_range_phase_with_run_spec<S, T, R, P>(
    request: RangeSyncRequest<'_, S, T, R, P>,
    run_spec_json: Option<&str>,
) -> Result<SyncTableReport, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let RangeSyncRequest {
        table,
        options,
        source,
        target,
        repair_target,
        progress_store,
        phase,
    } = request;
    let run_id = options.run_id.clone();
    let (progress, report, start_after) =
        prepare_range_sync(table, &options, progress_store, run_spec_json)?;
    let result = execute_range_sync(RangeExecution {
        table,
        options: &options,
        phase,
        source,
        target,
        repair_target,
        progress_store,
        progress,
        report,
        start_after,
    });
    let result = persist_sync_run_error(&run_id, result, progress_store);
    finish_sync_run(&run_id, result, progress_store)
}

fn prepare_range_sync(
    table: &SyncTable,
    options: &SyncRunOptions,
    progress_store: &mut impl SyncProgressStore,
    run_spec_json: Option<&str>,
) -> Result<(SyncTableProgress, SyncTableReport, Option<Vec<String>>), TableSyncError> {
    validate_sync_table(table, options.chunk_size)?;
    validate_sync_range(table, options.start_after.as_ref(), options.end_at.as_ref())?;
    let progress = load_range_sync_progress(
        &options.run_id,
        table,
        options,
        progress_store,
        run_spec_json,
    )?;
    let report = progress.report();
    let start_after = progress
        .last_primary_key
        .clone()
        .or(options.start_after.clone());
    Ok((progress, report, start_after))
}

struct RangeExecution<'a, S, T, R, P>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    table: &'a SyncTable,
    options: &'a SyncRunOptions,
    phase: SyncPhase,
    source: &'a S,
    target: &'a T,
    repair_target: &'a mut R,
    progress_store: &'a mut P,
    progress: SyncTableProgress,
    report: SyncTableReport,
    start_after: Option<Vec<String>>,
}

fn execute_range_sync<S, T, R, P>(
    mut context: RangeExecution<'_, S, T, R, P>,
) -> Result<SyncTableReport, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    run_range_chunks(&mut context)
}

fn run_range_chunks<S, T, R, P>(
    context: &mut RangeExecution<'_, S, T, R, P>,
) -> Result<SyncTableReport, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    loop {
        let next_start_after = sync_next_range_chunk(context)?;
        let Some(next_start_after) = next_start_after else {
            if context.phase.is_verification() {
                return verification_result(context);
            }
            if context.options.mode == SyncMode::Apply
                && context.phase == SyncPhase::All
                && context.repair_target.requires_terminal_verification()
            {
                verify_terminal_zero_drift(context)?;
            }
            complete_sync_progress(&mut context.progress, context.progress_store)?;
            return Ok(context.report.clone());
        };
        context.start_after = Some(next_start_after);
    }
}

fn sync_next_range_chunk<S, T, R, P>(
    context: &mut RangeExecution<'_, S, T, R, P>,
) -> Result<Option<Vec<String>>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    sync_next_chunk(SyncChunkContext {
        table: context.table,
        chunk_size: context.options.chunk_size,
        mode: context.options.mode,
        start_after: context.start_after.clone(),
        source: context.source,
        target: context.target,
        repair_target: context.repair_target,
        progress_store: context.progress_store,
        progress: &mut context.progress,
        report: &mut context.report,
        range_end_at: context.options.end_at.clone(),
        max_deletes: context.options.max_deletes,
        phase: context.phase,
    })
}

fn load_range_sync_progress(
    run_id: &str,
    table: &SyncTable,
    options: &SyncRunOptions,
    progress_store: &mut impl SyncProgressStore,
    run_spec_override: Option<&str>,
) -> Result<SyncTableProgress, TableSyncError> {
    let run_spec_json = match run_spec_override {
        Some(run_spec_json) => run_spec_json.to_string(),
        None => build_run_spec_json(
            &options.run_scope,
            table,
            options.chunk_size,
            options.mode,
            &options.start_after,
            &options.end_at,
            options.max_deletes,
        )?,
    };
    load_sync_progress(run_id, &run_spec_json, table, options.mode, progress_store)
}

struct SyncChunkContext<'a, S, T, R, P>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    table: &'a SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    start_after: Option<Vec<String>>,
    source: &'a S,
    target: &'a T,
    repair_target: &'a mut R,
    progress_store: &'a mut P,
    progress: &'a mut SyncTableProgress,
    report: &'a mut SyncTableReport,
    range_end_at: Option<Vec<String>>,
    max_deletes: Option<u64>,
    phase: SyncPhase,
}

fn sync_next_chunk<S, T, R, P>(
    mut context: SyncChunkContext<'_, S, T, R, P>,
) -> Result<Option<Vec<String>>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let source_rows = read_source_chunk(&context)?;
    if source_rows.is_empty() {
        let tail_start_after = context.start_after.clone();
        repair_target_tail(&mut context, tail_start_after)?;
        return Ok(None);
    }

    let end_at = repair_source_chunk(&mut context, &source_rows)?;

    if source_rows.len() < context.chunk_size {
        repair_target_tail(&mut context, Some(end_at))?;
        Ok(None)
    } else {
        Ok(Some(end_at))
    }
}

struct ExtraRowCount<'a, S, T>
where
    S: SyncTableReader,
    T: SyncTableReader,
{
    table: &'a SyncTable,
    chunk_size: usize,
    range_end_at: Option<Vec<String>>,
    source: &'a S,
    target: &'a T,
}

pub(crate) fn read_table_extra_row_count(config: &SyncTableConfig) -> Result<u64, TableSyncError> {
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new_with_target(target_connection_config(config), &config.target)
        .map_err(TableSyncError::Read)?;
    count_total_extra_rows(
        ExtraRowCount {
            table: &config.table,
            chunk_size: config.chunk_size,
            range_end_at: config.end_at.clone(),
            source: &source,
            target: &target,
        },
        config.start_after.clone(),
    )
}

fn count_total_extra_rows<S, T>(
    context: ExtraRowCount<'_, S, T>,
    mut start_after: Option<Vec<String>>,
) -> Result<u64, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
{
    let mut extra_target_rows = 0;
    loop {
        let page = count_source_page_extra_rows(&context, start_after.clone())?;
        let Some((page_extra, end_at, is_complete)) = page else {
            return Ok(extra_target_rows + count_extra_tail(&context, start_after)?);
        };
        extra_target_rows += page_extra;
        if is_complete {
            return Ok(extra_target_rows + count_extra_tail(&context, Some(end_at))?);
        }
        start_after = Some(end_at);
    }
}

fn count_extra_tail<S, T>(
    context: &ExtraRowCount<'_, S, T>,
    start_after: Option<Vec<String>>,
) -> Result<u64, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
{
    count_target_tail_extra_rows(
        context.table,
        start_after,
        context.range_end_at.clone(),
        context.chunk_size,
        context.target,
    )
}

fn count_source_page_extra_rows<S, T>(
    context: &ExtraRowCount<'_, S, T>,
    start_after: Option<Vec<String>>,
) -> Result<Option<(u64, Vec<String>, bool)>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
{
    let source_rows = context.source.read_rows(&sync_chunk_request(
        context.table,
        start_after.clone(),
        context.range_end_at.clone(),
        context.chunk_size,
    ))?;
    let Some(end_at) = last_primary_key(&source_rows).ok() else {
        return Ok(None);
    };
    let extra_target_rows = count_source_window_extra_rows(
        context.table,
        start_after,
        end_at.clone(),
        context.chunk_size,
        &source_rows,
        context.target,
    )?;
    let is_complete = source_rows.len() < context.chunk_size;
    Ok(Some((extra_target_rows, end_at, is_complete)))
}

fn count_source_window_extra_rows(
    table: &SyncTable,
    start_after: Option<Vec<String>>,
    end_at: Vec<String>,
    chunk_size: usize,
    source_rows: &[SnapshotRow],
    target: &dyn SyncTableReader,
) -> Result<u64, TableSyncError> {
    let target_rows = read_target_window(table, start_after, Some(end_at), chunk_size, target)?;
    Ok(count_extra_target_rows(source_rows, &target_rows))
}

fn count_target_tail_extra_rows(
    table: &SyncTable,
    start_after: Option<Vec<String>>,
    range_end_at: Option<Vec<String>>,
    chunk_size: usize,
    target: &dyn SyncTableReader,
) -> Result<u64, TableSyncError> {
    let target_rows = read_target_window(table, start_after, range_end_at, chunk_size, target)?;
    Ok(count_extra_target_rows(&[], &target_rows))
}

fn repair_source_chunk<S, T, R, P>(
    context: &mut SyncChunkContext<'_, S, T, R, P>,
    source_rows: &[SnapshotRow],
) -> Result<Vec<String>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let end_at = last_primary_key(source_rows)?;
    let target_rows = read_source_bounded_target_window(context, &end_at)?;
    if repair_two_parent_collision(context, source_rows, &target_rows, &end_at)? {
        return Ok(end_at);
    }
    repair_chunk(
        source_rows,
        &target_rows,
        context.mode,
        context.repair_target,
        context.report,
        context.max_deletes,
        context.phase,
    )?;
    record_repaired_source_chunk(context, source_rows.len(), end_at.clone())?;
    Ok(end_at)
}

fn repair_two_parent_collision<S, T, R, P>(
    context: &mut SyncChunkContext<'_, S, T, R, P>,
    source_rows: &[SnapshotRow],
    target_rows: &[SnapshotRow],
    end_at: &[String],
) -> Result<bool, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    if context.mode != SyncMode::MissingPrimaryKeys
        || context.phase.is_verification()
        || !context.target.requires_full_rows_for_missing_primary_keys()
    {
        return Ok(false);
    }
    let source_by_key = source_rows
        .iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect::<std::collections::BTreeMap<_, _>>();
    let target_by_key = target_rows
        .iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect::<std::collections::BTreeMap<_, _>>();
    let missing = source_by_key
        .iter()
        .filter(|(primary_key, _)| !target_by_key.contains_key(*primary_key))
        .map(|(_, row)| *row)
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(false);
    }
    if missing.len() != 1 {
        return Err(TableSyncError::Repair(
            "replace-divergent-pk requires exactly one missing source owner per chunk".to_string(),
        ));
    }
    let missing_source = missing[0];
    let displaced_targets = target_rows
        .iter()
        .filter(|target| {
            target.primary_key != missing_source.primary_key
                && non_primary_values_equal(context.table, target, missing_source)
        })
        .collect::<Vec<_>>();
    if displaced_targets.is_empty() {
        return Ok(false);
    }
    if displaced_targets.len() != 1 {
        return Err(TableSyncError::Repair(
            "replace-divergent-pk found an ambiguous displaced target owner".to_string(),
        ));
    }
    let displaced_target = displaced_targets[0];
    let Some(displaced_source) = source_by_key.get(&displaced_target.primary_key).copied() else {
        return Err(TableSyncError::Repair(
            "two-parent collision source owner is outside the stable chunk".to_string(),
        ));
    };
    let mut next_report = context.report.clone();
    next_report.chunks += 1;
    next_report.rows_scanned += source_rows.len() as u64;
    next_report.inserts += 1;
    let mut next_progress = context.progress.clone();
    next_progress.record_chunk(&next_report, end_at.to_vec());
    let progress_sql = context
        .progress_store
        .transactional_save_sql(&next_progress)
        .ok_or_else(|| {
            TableSyncError::Progress(
                "two-parent collision requires transactional progress storage".to_string(),
            )
        })?;
    context.repair_target.restore_displaced_owner_and_insert(
        context.table,
        displaced_source,
        displaced_target,
        missing_source,
        &progress_sql,
    )?;
    *context.report = next_report;
    *context.progress = next_progress;
    Ok(true)
}

fn non_primary_values_equal(table: &SyncTable, left: &SnapshotRow, right: &SnapshotRow) -> bool {
    table
        .columns
        .iter()
        .filter(|column| !table.primary_key.contains(column))
        .all(|column| left.values.get(column) == right.values.get(column))
}

fn read_source_bounded_target_window<S, T, R, P>(
    context: &SyncChunkContext<'_, S, T, R, P>,
    end_at: &[String],
) -> Result<Vec<SnapshotRow>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let primary_key_only_table;
    let target_table = if context.mode == SyncMode::MissingPrimaryKeys
        && !context.target.requires_full_rows_for_missing_primary_keys()
    {
        primary_key_only_table = SyncTable {
            name: context.table.name.clone(),
            primary_key: context.table.primary_key.clone(),
            columns: context.table.primary_key.clone(),
        };
        &primary_key_only_table
    } else {
        context.table
    };
    read_target_window(
        target_table,
        context.start_after.clone(),
        Some(end_at.to_vec()),
        context.chunk_size,
        context.target,
    )
}

fn record_repaired_source_chunk<S, T, R, P>(
    context: &mut SyncChunkContext<'_, S, T, R, P>,
    row_count: usize,
    end_at: Vec<String>,
) -> Result<(), TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    record_sync_chunk(
        context.progress,
        context.report,
        row_count,
        end_at,
        context.progress_store,
    )
}

fn repair_target_tail<S, T, R, P>(
    context: &mut SyncChunkContext<'_, S, T, R, P>,
    start_after: Option<Vec<String>>,
) -> Result<(), TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    if context.mode == SyncMode::MissingPrimaryKeys {
        return Ok(());
    }
    let target_rows = read_target_window(
        context.table,
        start_after,
        context.range_end_at.clone(),
        context.chunk_size,
        context.target,
    )?;
    repair_chunk(
        &[],
        &target_rows,
        context.mode,
        context.repair_target,
        context.report,
        context.max_deletes,
        context.phase,
    )
}

fn read_source_chunk<S, T, R, P>(
    context: &SyncChunkContext<'_, S, T, R, P>,
) -> Result<Vec<SnapshotRow>, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let request = sync_chunk_request(
        context.table,
        context.start_after.clone(),
        context.range_end_at.clone(),
        context.chunk_size,
    );
    context.source.read_rows(&request)
}

fn record_sync_chunk(
    progress: &mut SyncTableProgress,
    report: &mut SyncTableReport,
    row_count: usize,
    end_at: Vec<String>,
    progress_store: &mut impl SyncProgressStore,
) -> Result<(), TableSyncError> {
    report.chunks += 1;
    report.rows_scanned += row_count as u64;
    progress.record_chunk(report, end_at);
    progress_store.save(progress)
}

pub(crate) fn build_run_spec_json(
    run_scope: &str,
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    start_after: &Option<Vec<String>>,
    end_at: &Option<Vec<String>>,
    max_deletes: Option<u64>,
) -> Result<String, TableSyncError> {
    serde_json::to_string(&SyncRunSpec {
        scope: run_scope,
        table,
        chunk_size,
        mode,
        start_after,
        end_at,
        max_deletes,
        updated_since: None,
    })
    .map_err(|error| TableSyncError::Progress(format!("serialize run specification: {error}")))
}

fn load_sync_progress(
    run_id: &str,
    run_spec_json: &str,
    table: &SyncTable,
    mode: SyncMode,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableProgress, TableSyncError> {
    progress_store.ensure()?;
    progress_store.acquire_run(run_id)?;
    let result = (|| {
        let mut progress = match progress_store.load(run_id)? {
            Some(progress) => validate_resumable_progress(progress, run_id, run_spec_json)?,
            None => SyncTableProgress::started(
                run_id.to_string(),
                run_spec_json.to_string(),
                table.name.clone(),
                mode,
            ),
        };
        progress.mark_running(mode);
        progress_store.save(&progress)?;
        Ok(progress)
    })();
    release_on_load_error(run_id, result, progress_store)
}

pub(crate) fn release_on_load_error(
    run_id: &str,
    result: Result<SyncTableProgress, TableSyncError>,
    progress_store: &impl SyncProgressStore,
) -> Result<SyncTableProgress, TableSyncError> {
    if result.is_err() {
        let _ = progress_store.release_run(run_id);
    }
    result
}

pub(crate) fn persist_sync_run_error<T>(
    run_id: &str,
    result: Result<T, TableSyncError>,
    progress_store: &mut impl SyncProgressStore,
) -> Result<T, TableSyncError> {
    match result {
        Err(error) if should_record_sync_run_error(&error) => {
            if let Err(save_error) = progress_store.save_error(run_id, &error) {
                return Err(TableSyncError::Progress(format!(
                    "{error}; also failed to persist run error: {save_error}"
                )));
            }
            Err(error)
        }
        other => other,
    }
}

pub(crate) fn finish_sync_run<T>(
    run_id: &str,
    result: Result<T, TableSyncError>,
    progress_store: &impl SyncProgressStore,
) -> Result<T, TableSyncError> {
    let release_result = progress_store.release_run(run_id);
    match (result, release_result) {
        (Ok(value), Ok(())) | (Ok(value), Err(_)) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(release_error)) => Err(TableSyncError::Progress(format!(
            "{error}; also failed to release run lock: {release_error}"
        ))),
    }
}

pub(crate) fn validate_resumable_progress(
    progress: SyncTableProgress,
    run_id: &str,
    run_spec_json: &str,
) -> Result<SyncTableProgress, TableSyncError> {
    if progress.run_spec_json.as_deref() != Some(run_spec_json) {
        return Err(TableSyncError::Progress(format!(
            "run id `{run_id}` already exists with a different immutable specification"
        )));
    }
    if progress.status == SyncProgressStatus::Complete {
        return Err(TableSyncError::Progress(format!(
            "run id `{run_id}` is already complete; use a new run id"
        )));
    }
    Ok(progress)
}

pub(crate) fn complete_sync_progress(
    progress: &mut SyncTableProgress,
    progress_store: &mut dyn SyncProgressStore,
) -> Result<(), TableSyncError> {
    progress.mark_complete();
    progress_store.save(progress)
}

fn verify_terminal_zero_drift<S, T, R, P>(
    context: &mut RangeExecution<'_, S, T, R, P>,
) -> Result<(), TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let mut report = SyncTableReport {
        table: context.table.name.clone(),
        ..SyncTableReport::default()
    };
    let mut start_after = context.options.start_after.clone();
    loop {
        let source_rows = context.source.read_rows(&sync_chunk_request(
            context.table,
            start_after.clone(),
            context.options.end_at.clone(),
            context.options.chunk_size,
        ))?;
        if source_rows.is_empty() {
            let target_rows = read_target_window(
                context.table,
                start_after,
                context.options.end_at.clone(),
                context.options.chunk_size,
                context.target,
            )?;
            repair_chunk(
                &[],
                &target_rows,
                SyncMode::Apply,
                context.repair_target,
                &mut report,
                context.options.max_deletes,
                SyncPhase::Verify,
            )?;
            break;
        }

        let end_at = last_primary_key(&source_rows)?;
        let target_rows = read_target_window(
            context.table,
            start_after.clone(),
            Some(end_at.clone()),
            context.options.chunk_size,
            context.target,
        )?;
        repair_chunk(
            &source_rows,
            &target_rows,
            SyncMode::Apply,
            context.repair_target,
            &mut report,
            context.options.max_deletes,
            SyncPhase::Verify,
        )?;
        if source_rows.len() < context.options.chunk_size {
            let target_tail = read_target_window(
                context.table,
                Some(end_at),
                context.options.end_at.clone(),
                context.options.chunk_size,
                context.target,
            )?;
            repair_chunk(
                &[],
                &target_tail,
                SyncMode::Apply,
                context.repair_target,
                &mut report,
                context.options.max_deletes,
                SyncPhase::Verify,
            )?;
            break;
        }
        start_after = Some(end_at);
    }

    if report.inserts > 0 || report.updates > 0 || report.extra_target_rows > 0 {
        return Err(TableSyncError::Verification(format!(
            "table={} scope={} missing_rows={} extra_rows={} divergent_rows={}",
            context.table.name,
            verification_scope(context.options, SyncPhase::Verify),
            report.inserts,
            report.extra_target_rows,
            report.updates,
        )));
    }
    Ok(())
}

fn verification_result<S, T, R, P>(
    context: &mut RangeExecution<'_, S, T, R, P>,
) -> Result<SyncTableReport, TableSyncError>
where
    S: SyncTableReader,
    T: SyncTableReader,
    R: SyncRepairTarget,
    P: SyncProgressStore,
{
    let has_unrepaired_rows = match context.phase {
        SyncPhase::Verify => {
            context.report.inserts > 0
                || context.report.updates > 0
                || context.report.extra_target_rows > 0
        }
        SyncPhase::VerifyNoTargetExtras => context.report.extra_target_rows > 0,
        _ => false,
    };
    if has_unrepaired_rows {
        return Err(TableSyncError::Repair(format!(
            "verification failed: table={} scope={} missing_rows={} extra_rows={} divergent_rows={}",
            context.table.name,
            verification_scope(context.options, context.phase),
            context.report.inserts,
            context.report.extra_target_rows,
            context.report.updates,
        )));
    }
    complete_sync_progress(&mut context.progress, context.progress_store)?;
    Ok(context.report.clone())
}

fn verification_scope(options: &SyncRunOptions, phase: SyncPhase) -> String {
    let scope = if options.start_after.is_none() && options.end_at.is_none() {
        "full-table".to_string()
    } else {
        format!(
            "primary-key-window start_after={} end_at={}",
            format_bound(options.start_after.as_ref()),
            format_bound(options.end_at.as_ref())
        )
    };
    if phase == SyncPhase::VerifyNoTargetExtras {
        return format!("no-target-extras {scope}");
    }
    scope
}

fn format_bound(values: Option<&Vec<String>>) -> String {
    values
        .map(|values| format!("{values:?}"))
        .unwrap_or_else(|| "<none>".to_string())
}

fn read_target_window(
    table: &SyncTable,
    mut start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    chunk_size: usize,
    target: &dyn SyncTableReader,
) -> Result<Vec<SnapshotRow>, TableSyncError> {
    let mut rows = Vec::new();

    loop {
        let page = target.read_rows(&sync_chunk_request(
            table,
            start_after.clone(),
            end_at.clone(),
            chunk_size,
        ))?;
        if page.is_empty() {
            return Ok(rows);
        }

        let page_is_complete = page.len() < chunk_size;
        start_after = Some(last_primary_key(&page)?);
        rows.extend(page);

        if page_is_complete {
            return Ok(rows);
        }
    }
}
