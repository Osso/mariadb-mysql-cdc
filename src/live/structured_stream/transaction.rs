use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TargetTransactionGroupConfig {
    pub(super) size: usize,
    pub(super) timeout: Duration,
}

impl TargetTransactionGroupConfig {
    pub(super) fn from_apply_config(config: &ApplyBinlogConfig) -> Self {
        Self {
            size: config.target_transaction_group_size.max(1),
            timeout: Duration::from_millis(config.target_transaction_group_timeout_ms),
        }
    }
}

impl Default for TargetTransactionGroupConfig {
    fn default() -> Self {
        Self {
            size: 1,
            timeout: Duration::ZERO,
        }
    }
}

#[derive(Default)]
pub(super) struct TargetTransaction {
    open: bool,
    source_transactions: usize,
    opened_at: Option<Instant>,
    pending_file_checkpoint: Option<crate::checkpoint::Checkpoint>,
    /// Latest source coordinate reached inside the open target transaction; written once,
    /// in the same transaction, just before it commits.
    pending_transaction_checkpoint: Option<crate::checkpoint::Checkpoint>,
}

impl TargetTransaction {
    pub(super) fn begin_if_needed<E>(&mut self, executor: &E) -> Result<(), ApplyBinlogError>
    where
        E: TransactionalTargetExecutor,
    {
        if self.open {
            return Ok(());
        }
        executor
            .begin_transaction()
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        self.open = true;
        self.opened_at = Some(Instant::now());
        Ok(())
    }

    pub(super) fn commit_if_open<E>(&mut self, executor: &E) -> Result<(), ApplyBinlogError>
    where
        E: TransactionalTargetExecutor,
    {
        self.finish_if_open(executor, |executor| executor.commit_transaction())
    }

    pub(super) fn rollback_if_open<E>(&mut self, executor: &E) -> Result<(), ApplyBinlogError>
    where
        E: TransactionalTargetExecutor,
    {
        self.finish_if_open(executor, |executor| executor.rollback_transaction())
    }

    fn finish_if_open<E, F>(&mut self, executor: &E, finish: F) -> Result<(), ApplyBinlogError>
    where
        E: TransactionalTargetExecutor,
        F: FnOnce(&E) -> Result<(), crate::target::TargetExecuteError>,
    {
        if !self.open {
            return Ok(());
        }
        finish(executor).map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        self.reset();
        Ok(())
    }

    pub(super) fn record_source_transaction(&mut self) {
        if self.open {
            self.source_transactions += 1;
        }
    }

    pub(super) fn remember_file_checkpoint(&mut self, checkpoint: crate::checkpoint::Checkpoint) {
        self.pending_file_checkpoint = Some(checkpoint);
    }

    pub(super) fn take_file_checkpoint(&mut self) -> Option<crate::checkpoint::Checkpoint> {
        self.pending_file_checkpoint.take()
    }

    fn remember_transaction_checkpoint(&mut self, checkpoint: crate::checkpoint::Checkpoint) {
        self.pending_transaction_checkpoint = Some(checkpoint);
    }

    pub(super) fn should_flush(&self, config: TargetTransactionGroupConfig, force: bool) -> bool {
        self.has_completed_source_transactions()
            && (force
                || config.size <= 1
                || self.source_transactions >= config.size
                || self.group_timed_out(config))
    }

    pub(super) fn has_completed_source_transactions(&self) -> bool {
        self.source_transactions > 0
    }

    pub(super) fn group_timed_out(&self, config: TargetTransactionGroupConfig) -> bool {
        config.timeout > Duration::ZERO
            && self
                .opened_at
                .is_some_and(|opened_at| opened_at.elapsed() >= config.timeout)
    }

    pub(super) fn reset(&mut self) {
        self.open = false;
        self.source_transactions = 0;
        self.opened_at = None;
        self.pending_file_checkpoint = None;
        self.pending_transaction_checkpoint = None;
    }

    pub(super) fn is_open(&self) -> bool {
        self.open
    }
}

pub(super) fn apply_stream_event_transactionally<E, R, C>(
    applier: &mut RowApplier<E>,
    context: &mut StreamEventContext<'_, R, C>,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Result<StructuredEventOutcome, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
{
    if context
        .target_transaction
        .should_flush(context.group_config, false)
        || matches!(event, BinlogEvent::RotateEvent(_))
    {
        flush_grouped_transaction(applier.executor(), context)?;
    }

    if event_can_write_target(event, context.state) {
        context
            .target_transaction
            .begin_if_needed(applier.executor())?;
    }

    let outcome = match handle_structured_event(
        applier,
        context.schema_resolver,
        context.state,
        context.current_file,
        header,
        event,
    ) {
        Ok(outcome) => outcome,
        Err(error) => {
            context
                .target_transaction
                .rollback_if_open(applier.executor())?;
            return Err(error);
        }
    };

    if outcome.policy == EventPolicy::CommitTransaction {
        let force_flush = matches!(event, BinlogEvent::QueryEvent(_));
        finish_source_transaction(applier.executor(), context, event, &outcome, force_flush)?;
        return Ok(outcome);
    }

    save_outcome_checkpoint(context, event, &outcome)?;
    Ok(outcome)
}

pub(super) fn finish_source_transaction<E, R, C>(
    executor: &E,
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    outcome: &StructuredEventOutcome,
    force_flush: bool,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    C: StreamCheckpointStore,
{
    context.target_transaction.record_source_transaction();

    if context.transaction_checkpoint_table.is_some() {
        save_outcome_checkpoint(context, event, outcome)?;
        if context
            .target_transaction
            .should_flush(context.group_config, force_flush)
        {
            commit_target_group(executor, context)?;
        }
        return Ok(());
    }

    remember_file_checkpoint(context, event, outcome);
    if context
        .target_transaction
        .should_flush(context.group_config, force_flush)
    {
        flush_grouped_transaction(executor, context)?;
    }
    Ok(())
}

pub(super) fn flush_grouped_transaction<E, R, C>(
    executor: &E,
    context: &mut StreamEventContext<'_, R, C>,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    C: StreamCheckpointStore,
{
    if !context
        .target_transaction
        .has_completed_source_transactions()
    {
        return Ok(());
    }
    let checkpoint = context.target_transaction.take_file_checkpoint();
    if let Err(error) = commit_target_group(executor, context) {
        if let Some(checkpoint) = checkpoint {
            context
                .target_transaction
                .remember_file_checkpoint(checkpoint);
        }
        return Err(error);
    }
    if let Some(checkpoint) = checkpoint
        && let Some(store) = context.checkpoint_store
    {
        store.save_checkpoint(&checkpoint)?;
    }
    Ok(())
}

/// Commits the open target transaction after writing its pending transactional checkpoint,
/// so the rows and the checkpoint of every grouped source transaction commit atomically.
pub(super) fn commit_target_group<E, R, C>(
    executor: &E,
    context: &mut StreamEventContext<'_, R, C>,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
{
    let pending = context
        .target_transaction
        .pending_transaction_checkpoint
        .take();
    if let (Some(checkpoint), Some(checkpoint_table), Some(checkpoint_name)) = (
        pending,
        context.transaction_checkpoint_table,
        context.transaction_checkpoint_name,
    ) && let Err(error) =
        lock_validate_and_save_checkpoint(executor, checkpoint_table, checkpoint_name, &checkpoint)
    {
        context
            .target_transaction
            .remember_transaction_checkpoint(checkpoint);
        return Err(error);
    }
    context.target_transaction.commit_if_open(executor)
}

pub(super) fn remember_file_checkpoint<R, C>(
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    outcome: &StructuredEventOutcome,
) {
    let Some(coordinate) = &outcome.resume_coordinate else {
        return;
    };
    let checkpoint = crate::live::reconnect::coordinate_checkpoint(coordinate, event_name(event));
    context
        .target_transaction
        .remember_file_checkpoint(checkpoint);
    *context.current_file = coordinate.file.clone();
}

pub(super) fn event_can_write_target(event: &BinlogEvent, state: &StructuredEventState) -> bool {
    match event {
        BinlogEvent::WriteRowsEvent(rows) => !state.is_ignored_table_id(rows.table_id),
        BinlogEvent::UpdateRowsEvent(rows) => !state.is_ignored_table_id(rows.table_id),
        BinlogEvent::DeleteRowsEvent(rows) => !state.is_ignored_table_id(rows.table_id),
        BinlogEvent::QueryEvent(query) => {
            state.should_apply_schema(&query.database_name)
                && !crate::statement::is_data_changing_statement(&query.sql_statement)
        }
        _ => false,
    }
}

pub(super) fn save_outcome_checkpoint<R, C>(
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    outcome: &StructuredEventOutcome,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    let Some(coordinate) = &outcome.resume_coordinate else {
        return Ok(());
    };

    if context.target_transaction.is_open()
        && context.transaction_checkpoint_table.is_some()
        && context.transaction_checkpoint_name.is_some()
    {
        let checkpoint =
            crate::live::reconnect::coordinate_checkpoint(coordinate, event_name(event));
        context
            .target_transaction
            .remember_transaction_checkpoint(checkpoint);
        *context.current_file = coordinate.file.clone();
        return Ok(());
    }

    crate::live::reconnect::save_coordinate_checkpoint(
        context.checkpoint_store,
        coordinate,
        event_name(event),
    )?;
    *context.current_file = coordinate.file.clone();
    Ok(())
}
