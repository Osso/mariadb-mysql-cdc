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

pub(super) trait SupersededInsertVerifier {
    fn verify(
        &mut self,
        candidate: &crate::row::DeferredSupersededInsertCandidate,
        xid_end_position: u64,
    ) -> Result<super::superseded_insert::SupersededInsertProof, String>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct VerifiedSupersededInsert {
    pub(super) resolution_evidence: String,
    pub(super) checkpoint: crate::checkpoint::Checkpoint,
}

#[derive(Default)]
pub(super) struct TargetTransaction {
    open: bool,
    source_transactions: usize,
    opened_at: Option<Instant>,
    pending_file_checkpoint: Option<crate::checkpoint::Checkpoint>,
    pending_conflict_resolutions: Vec<crate::conflict_repair::ConflictResolution>,
    pending_conflict_observations: Vec<crate::conflict_repair::ConflictObservation>,
    deferred_superseded_inserts: Vec<crate::row::DeferredSupersededInsertCandidate>,
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

    pub(super) fn commit_if_open<E>(
        &mut self,
        executor: &E,
    ) -> Result<Vec<crate::conflict_repair::ConflictResolution>, ApplyBinlogError>
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
            .map(|_| ())
    }

    fn finish_if_open<E, F>(
        &mut self,
        executor: &E,
        finish: F,
    ) -> Result<Vec<crate::conflict_repair::ConflictResolution>, ApplyBinlogError>
    where
        E: TransactionalTargetExecutor,
        F: FnOnce(&E) -> Result<(), crate::target::TargetExecuteError>,
    {
        if !self.open {
            return Ok(Vec::new());
        }
        finish(executor).map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        let resolutions = std::mem::take(&mut self.pending_conflict_resolutions);
        self.reset();
        Ok(resolutions)
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

    #[cfg(test)]
    pub(super) fn pending_conflict_resolutions_mut(
        &mut self,
    ) -> &mut Vec<crate::conflict_repair::ConflictResolution> {
        &mut self.pending_conflict_resolutions
    }

    pub(super) fn pending_conflicts_mut(
        &mut self,
    ) -> (
        &mut Vec<crate::conflict_repair::ConflictResolution>,
        &mut Vec<crate::conflict_repair::ConflictObservation>,
        &mut Vec<crate::row::DeferredSupersededInsertCandidate>,
    ) {
        (
            &mut self.pending_conflict_resolutions,
            &mut self.pending_conflict_observations,
            &mut self.deferred_superseded_inserts,
        )
    }

    #[cfg(test)]
    pub(super) fn defer_superseded_insert(
        &mut self,
        candidate: crate::row::DeferredSupersededInsertCandidate,
    ) {
        self.deferred_superseded_inserts.push(candidate);
    }

    pub(super) fn has_deferred_superseded_inserts(&self) -> bool {
        !self.deferred_superseded_inserts.is_empty()
    }

    pub(super) fn finalize_deferred_superseded_inserts_at(&mut self, end_position: u64) {
        for candidate in &mut self.deferred_superseded_inserts {
            candidate.observation.coordinate.end_position = end_position;
        }
    }

    #[cfg(test)]
    pub(super) fn verify_deferred_superseded_inserts_at_xid<E, V>(
        &mut self,
        executor: &E,
        verifier: &mut V,
        xid_end_position: u64,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<VerifiedSupersededInsert, ApplyBinlogError>
    where
        E: TransactionalTargetExecutor,
        V: SupersededInsertVerifier,
    {
        let result = self.verify_and_commit_deferred_superseded_inserts(
            executor,
            verifier,
            xid_end_position,
            checkpoint_table,
            checkpoint_name,
        );
        if result.is_err() {
            self.rollback_if_open(executor)?;
        }
        result
    }

    pub(super) fn verify_deferred_superseded_inserts_at_xid_with_conflicts<E, V>(
        &mut self,
        executor: &E,
        verifier: &mut V,
        conflict_store: &mut dyn crate::conflict_repair::ConflictStore,
        xid_end_position: u64,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<VerifiedSupersededInsert, ApplyBinlogError>
    where
        E: TransactionalTargetExecutor,
        V: SupersededInsertVerifier,
    {
        self.finalize_deferred_superseded_inserts_at(xid_end_position);
        let observation = self
            .deferred_superseded_inserts
            .first()
            .map(|candidate| candidate.observation.clone())
            .ok_or_else(|| {
                ApplyBinlogError::Target("missing deferred superseded insert".to_string())
            })?;
        let result = self.verify_and_commit_deferred_superseded_inserts(
            executor,
            verifier,
            xid_end_position,
            checkpoint_table,
            checkpoint_name,
        );
        match result {
            Ok(verified) => {
                conflict_store
                    .observe(observation.clone())
                    .map_err(ApplyBinlogError::Target)?;
                conflict_store
                    .resolve_existing(conflict_resolution(
                        &observation,
                        &verified.resolution_evidence,
                    ))
                    .map_err(ApplyBinlogError::Target)?;
                Ok(verified)
            }
            Err(error) => {
                self.rollback_if_open(executor)?;
                conflict_store
                    .observe(observation)
                    .map_err(ApplyBinlogError::Target)?;
                Err(error)
            }
        }
    }

    fn verify_and_commit_deferred_superseded_inserts<E, V>(
        &mut self,
        executor: &E,
        verifier: &mut V,
        xid_end_position: u64,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<VerifiedSupersededInsert, ApplyBinlogError>
    where
        E: TransactionalTargetExecutor,
        V: SupersededInsertVerifier,
    {
        let candidate = self.deferred_superseded_inserts.first().ok_or_else(|| {
            ApplyBinlogError::Target("missing deferred superseded insert".to_string())
        })?;
        let proof = verifier
            .verify(candidate, xid_end_position)
            .map_err(ApplyBinlogError::Target)?;
        let evidence = proof.resolution_evidence();
        let checkpoint = crate::checkpoint::Checkpoint {
            source_file: candidate.observation.coordinate.file.clone(),
            source_position: xid_end_position,
            gtid: None,
            event_timestamp: 0,
            last_event: crate::checkpoint::LastEvent {
                event_type: "XidEvent".to_string(),
                description: format!(
                    "verified superseded insert transaction at {}:{xid_end_position}",
                    candidate.observation.coordinate.file
                ),
            },
        };
        executor
            .load_transaction_checkpoint_for_update(checkpoint_table, checkpoint_name)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        executor
            .save_transaction_checkpoint(checkpoint_table, checkpoint_name, &checkpoint)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        let resolution = conflict_resolution(&candidate.observation, &evidence);
        executor
            .execute_transaction_sql(&crate::conflict_repair::build_conflict_observation_sql(
                "cdc.row_conflicts",
                &candidate.observation,
            ))
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        executor
            .execute_transaction_sql(
                &crate::conflict_repair::build_conflict_resolution_for_source_row_sql(
                    "cdc.row_conflicts",
                    &resolution,
                ),
            )
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        self.commit_if_open(executor)?;
        Ok(VerifiedSupersededInsert {
            resolution_evidence: evidence,
            checkpoint,
        })
    }

    pub(super) fn has_pending_conflict_resolutions(&self) -> bool {
        !self.pending_conflict_resolutions.is_empty()
    }

    fn pending_conflict_resolutions(&self) -> &[crate::conflict_repair::ConflictResolution] {
        &self.pending_conflict_resolutions
    }

    pub(super) fn has_pending_conflict_observations(&self) -> bool {
        !self.pending_conflict_observations.is_empty()
    }

    pub(super) fn take_finalized_conflict_observations(
        &mut self,
        end_position: u64,
    ) -> Vec<crate::conflict_repair::ConflictObservation> {
        let mut observations = std::mem::take(&mut self.pending_conflict_observations);
        for observation in &mut observations {
            observation.coordinate.end_position = end_position;
            if let Some(request) = &mut observation.parent_recovery {
                request.set_source_end_position(end_position);
            }
        }
        observations
    }

    pub(super) fn finalize_conflict_resolutions_at(&mut self, end_position: u64) {
        for resolution in &mut self.pending_conflict_resolutions {
            resolution.evidence = format!(
                "{}; source transaction end position {end_position}",
                resolution.evidence
            );
        }
    }

    pub(super) fn reset(&mut self) {
        self.open = false;
        self.source_transactions = 0;
        self.opened_at = None;
        self.pending_file_checkpoint = None;
        self.pending_conflict_resolutions.clear();
        self.pending_conflict_observations.clear();
        self.deferred_superseded_inserts.clear();
    }

    pub(super) fn is_open(&self) -> bool {
        self.open
    }
}

fn conflict_resolution(
    observation: &crate::conflict_repair::ConflictObservation,
    evidence: &str,
) -> crate::conflict_repair::ConflictResolution {
    crate::conflict_repair::ConflictResolution {
        source_identity: observation.source_identity.clone(),
        schema: observation.schema.clone(),
        table: observation.table.clone(),
        source_primary_key: observation.source_primary_key.clone(),
        repair_run_id: format!(
            "stream-superseded-{}-{}-{}",
            observation.coordinate.file.replace('/', "_"),
            observation.coordinate.start_position,
            observation.table,
        ),
        evidence: evidence.to_string(),
    }
}

#[cfg(test)]
pub(super) fn apply_stream_event_transactionally(
    applier: &mut RowApplier<impl TransactionalTargetExecutor>,
    context: &mut StreamEventContext<'_, impl TableSchemaResolver, impl StreamCheckpointStore>,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Result<StructuredEventOutcome, ApplyBinlogError> {
    let mut conflict_store = crate::conflict_repair::InMemoryConflictStore::default();
    apply_stream_event_transactionally_with_conflicts(
        applier,
        context,
        header,
        event,
        "test-source",
        &mut conflict_store,
    )
}

pub(super) fn apply_stream_event_transactionally_with_conflicts<E, R, C>(
    applier: &mut RowApplier<E>,
    context: &mut StreamEventContext<'_, R, C>,
    header: &EventHeader,
    event: &BinlogEvent,
    source_identity: &str,
    conflict_store: &mut dyn crate::conflict_repair::ConflictStore,
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

    if context
        .target_transaction
        .has_pending_conflict_observations()
        && matches!(
            event,
            BinlogEvent::WriteRowsEvent(_)
                | BinlogEvent::UpdateRowsEvent(_)
                | BinlogEvent::DeleteRowsEvent(_)
        )
    {
        return Ok(StructuredEventOutcome {
            policy: EventPolicy::ApplyRows,
            resume_coordinate: None,
        });
    }

    if event_can_write_target(event, context.state) {
        context
            .target_transaction
            .begin_if_needed(applier.executor())?;
    }

    let (pending_resolutions, pending_observations, deferred_superseded_inserts) =
        context.target_transaction.pending_conflicts_mut();
    let mut conflict_context = RowConflictContext {
        store: conflict_store,
        pending_resolutions,
        pending_observations,
        deferred_superseded_inserts,
        source_identity,
        source_server_id: u64::from(header.server_id),
        end_position: u64::from(header.next_event_position),
        child_event_timestamp: u64::from(header.timestamp),
        observed_at_ms: current_time_ms(),
    };
    let outcome = match handle_structured_event_with_conflicts(
        applier,
        context.schema_resolver,
        context.state,
        context.current_file,
        header,
        event,
        Some(&mut conflict_context),
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
        if matches!(event, BinlogEvent::XidEvent(_))
            && context
                .target_transaction
                .has_pending_conflict_observations()
        {
            let end_position = outcome
                .resume_coordinate
                .as_ref()
                .map_or(0, |coordinate| coordinate.position);
            let observations = context
                .target_transaction
                .take_finalized_conflict_observations(end_position);
            context
                .target_transaction
                .rollback_if_open(applier.executor())?;
            return persist_deferred_conflicts(conflict_store, observations);
        }
        let force_flush = matches!(event, BinlogEvent::QueryEvent(_) | BinlogEvent::XidEvent(_));
        finish_source_transaction(
            applier.executor(),
            context,
            event,
            &outcome,
            force_flush,
            conflict_store,
        )?;
        return Ok(outcome);
    }

    save_outcome_checkpoint(applier.executor(), context, event, &outcome)?;
    Ok(outcome)
}

pub(super) fn finish_source_transaction<E, R, C>(
    executor: &E,
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    outcome: &StructuredEventOutcome,
    force_flush: bool,
    conflict_store: &mut dyn crate::conflict_repair::ConflictStore,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    C: StreamCheckpointStore,
{
    context.target_transaction.record_source_transaction();

    if matches!(event, BinlogEvent::XidEvent(_))
        && let Some(coordinate) = &outcome.resume_coordinate
    {
        context
            .target_transaction
            .finalize_conflict_resolutions_at(coordinate.position);
    }

    if context.transaction_checkpoint_table.is_some() {
        save_outcome_checkpoint(executor, context, event, outcome)?;
        if context
            .target_transaction
            .should_flush(context.group_config, force_flush)
            || context
                .target_transaction
                .has_pending_conflict_resolutions()
        {
            execute_conflict_resolutions_in_target_transaction(executor, context, conflict_store)?;
            let resolutions = context.target_transaction.commit_if_open(executor)?;
            finalize_conflict_resolution_cache(conflict_store, resolutions);
        }
        return Ok(());
    }

    remember_file_checkpoint(context, event, outcome);
    if context
        .target_transaction
        .should_flush(context.group_config, force_flush)
        || context
            .target_transaction
            .has_pending_conflict_resolutions()
    {
        flush_grouped_transaction_with_conflicts(executor, context, Some(conflict_store))?;
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
    flush_grouped_transaction_with_conflicts(executor, context, None)
}

fn flush_grouped_transaction_with_conflicts<E, R, C>(
    executor: &E,
    context: &mut StreamEventContext<'_, R, C>,
    mut conflict_store: Option<&mut dyn crate::conflict_repair::ConflictStore>,
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
    if let Some(conflict_store) = conflict_store.as_deref_mut() {
        execute_conflict_resolutions_in_target_transaction(executor, context, conflict_store)?;
    }
    let resolutions = match context.target_transaction.commit_if_open(executor) {
        Ok(resolutions) => resolutions,
        Err(error) => {
            if let Some(checkpoint) = checkpoint {
                context
                    .target_transaction
                    .remember_file_checkpoint(checkpoint);
            }
            return Err(error);
        }
    };
    if let Some(checkpoint) = checkpoint
        && let Some(store) = context.checkpoint_store
    {
        store.save_checkpoint(&checkpoint)?;
    }
    if let Some(conflict_store) = conflict_store {
        finalize_conflict_resolution_cache(conflict_store, resolutions);
    }
    Ok(())
}

fn persist_deferred_conflicts(
    conflict_store: &mut dyn crate::conflict_repair::ConflictStore,
    observations: Vec<crate::conflict_repair::ConflictObservation>,
) -> Result<StructuredEventOutcome, ApplyBinlogError> {
    let error_text = observations
        .first()
        .map(|observation| observation.error_text.clone())
        .unwrap_or_else(|| "unknown row conflict".to_string());
    let parent_recovery = observations
        .iter()
        .find_map(|observation| observation.parent_recovery.clone())
        .map(Box::new);
    for observation in observations {
        conflict_store
            .observe(observation)
            .map_err(ApplyBinlogError::Target)?;
    }
    let unresolved_count = conflict_store
        .unresolved_count_result()
        .map_err(ApplyBinlogError::Target)?;
    println!("cdc_row_conflict_progress unresolved_count={unresolved_count}");
    Err(ApplyBinlogError::RowConflictPersisted {
        message: error_text,
        parent_recovery,
    })
}

fn execute_conflict_resolutions_in_target_transaction<E, R, C>(
    executor: &E,
    context: &mut StreamEventContext<'_, R, C>,
    conflict_store: &dyn crate::conflict_repair::ConflictStore,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    C: StreamCheckpointStore,
{
    for resolution in context.target_transaction.pending_conflict_resolutions() {
        let sql = conflict_store.resolution_sql(resolution);
        if let Err(error) = executor.execute_transaction_sql(&sql) {
            context.target_transaction.rollback_if_open(executor)?;
            return Err(ApplyBinlogError::Target(error.to_string()));
        }
    }
    Ok(())
}

fn finalize_conflict_resolution_cache(
    conflict_store: &mut dyn crate::conflict_repair::ConflictStore,
    resolutions: Vec<crate::conflict_repair::ConflictResolution>,
) {
    for resolution in resolutions {
        conflict_store.mark_resolution_committed(resolution);
    }
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

pub(super) fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(super) fn save_outcome_checkpoint<E, R, C>(
    executor: &E,
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    outcome: &StructuredEventOutcome,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    C: StreamCheckpointStore,
{
    let Some(coordinate) = &outcome.resume_coordinate else {
        return Ok(());
    };

    if context.target_transaction.is_open()
        && let (Some(checkpoint_table), Some(checkpoint_name)) = (
            context.transaction_checkpoint_table,
            context.transaction_checkpoint_name,
        )
    {
        let checkpoint =
            crate::live::reconnect::coordinate_checkpoint(coordinate, event_name(event));
        lock_validate_and_save_checkpoint(
            executor,
            checkpoint_table,
            checkpoint_name,
            &checkpoint,
        )?;
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
