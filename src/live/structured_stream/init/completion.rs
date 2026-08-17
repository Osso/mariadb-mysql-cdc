use super::*;

pub(in crate::live::structured_stream) fn complete_bounded_stop<C>(
    runtime: &mut StreamRuntime,
    checkpoint_store: Option<&C>,
    transaction_checkpoint_table: Option<&str>,
    transaction_checkpoint_name: Option<&str>,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    if let Err(error) = bounded_stop_completion_error(runtime.source_row_transaction_open) {
        rollback_stream_transaction(runtime)?;
        wait_for_parallel_target_transactions(runtime)?;
        return Err(error);
    }
    flush_stream_grouped_transaction(
        runtime,
        checkpoint_store,
        transaction_checkpoint_table,
        transaction_checkpoint_name,
    )?;
    wait_for_parallel_target_transactions(runtime)
}

pub(in crate::live::structured_stream) fn finish_stream<C>(
    config: &ApplyBinlogConfig,
    runtime: &mut StreamRuntime,
    checkpoint_store: Option<&C>,
    transaction_checkpoint_table: Option<&str>,
    transaction_checkpoint_name: Option<&str>,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    if let Some(stop_position) = config.source.stop_position {
        rollback_stream_transaction(runtime)?;
        wait_for_parallel_target_transactions(runtime)?;
        return Err(bounded_stop_not_reached_error(stop_position));
    }
    flush_stream_grouped_transaction(
        runtime,
        checkpoint_store,
        transaction_checkpoint_table,
        transaction_checkpoint_name,
    )?;
    wait_for_parallel_target_transactions(runtime)?;
    Err(stream_ended_error())
}

pub(in crate::live::structured_stream) fn flush_stream_grouped_transaction<C>(
    runtime: &mut StreamRuntime,
    checkpoint_store: Option<&C>,
    transaction_checkpoint_table: Option<&str>,
    transaction_checkpoint_name: Option<&str>,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    let StreamRuntime {
        applier,
        schema_resolver,
        state,
        target_transaction,
        current_file,
        group_config,
        ..
    } = runtime;
    let mut context = StreamEventContext {
        schema_resolver,
        state,
        target_transaction,
        checkpoint_store,
        transaction_checkpoint_table,
        transaction_checkpoint_name,
        current_file,
        group_config: *group_config,
    };
    flush_grouped_transaction(applier.executor(), &mut context)
}

pub(in crate::live::structured_stream) fn rollback_stream_transaction(
    runtime: &mut StreamRuntime,
) -> Result<(), ApplyBinlogError> {
    runtime
        .target_transaction
        .rollback_if_open(runtime.applier.executor())
}

pub(in crate::live::structured_stream) fn wait_for_parallel_target_transactions(
    runtime: &mut StreamRuntime,
) -> Result<(), ApplyBinlogError> {
    runtime
        .applier
        .executor()
        .flush_pending_transactions()
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    reap_parallel_target_transactions(runtime)
}

pub(in crate::live::structured_stream) fn reap_parallel_target_transactions(
    runtime: &mut StreamRuntime,
) -> Result<(), ApplyBinlogError> {
    let checkpoints = runtime
        .applier
        .executor()
        .take_committed_checkpoints()
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    let Some(progress) = runtime.durable_progress.as_mut() else {
        debug_assert!(checkpoints.is_empty());
        return Ok(());
    };
    record_committed_target_progress(progress, checkpoints);
    Ok(())
}

pub(in crate::live::structured_stream) fn record_committed_target_progress(
    progress: &mut StreamProgress,
    checkpoints: Vec<crate::checkpoint::Checkpoint>,
) {
    for checkpoint in checkpoints {
        let coordinate = BinlogCoordinate {
            file: checkpoint.source_file,
            position: checkpoint.source_position,
        };
        if progress.record_applied(&coordinate) {
            println!("{}", format_stream_progress(progress));
        }
    }
}
