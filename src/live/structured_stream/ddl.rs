use super::*;
use crate::live::ddl_semantics::{DdlTransformation, supports_rename_columns_if_exists};
use crate::target::SqlStatement;

pub(super) fn handle_automatic_ddl_event<E, R, C, J, S, D>(
    applier: &mut RowApplier<E>,
    dependencies: AutomaticDdlDependencies<'_, J, S, D>,
    input: AutomaticDdlInput<'_, '_, R, C>,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
    S: DdlSemanticInventory,
    D: DdlEventLedger,
{
    let AutomaticDdlDependencies {
        journal,
        semantic_inventory,
        ledger,
        source_identity,
    } = dependencies;
    let AutomaticDdlInput {
        context,
        header,
        event,
    } = input;

    let Some((query, ddl_event)) = automatically_handled_ddl_event(
        source_identity,
        context.current_file,
        header,
        event,
        context.state,
    ) else {
        return Ok(None);
    };

    flush_grouped_transaction(applier.executor(), context)?;
    let status = journal
        .read_status(&ddl_event)
        .map_err(ApplyBinlogError::Statement)?;
    let outcome = match status {
        Some(DdlReplayStatus::Prepared) => {
            reconcile_prepared_automatic_ddl(
                applier.executor(),
                journal,
                semantic_inventory,
                context,
                event,
                &ddl_event,
            )?;
            resolved_ddl_outcome(ddl_event)
        }
        _ => match replay_action(&ddl_event, status).map_err(ApplyBinlogError::Statement)? {
            DdlReplayAction::PrepareAndExecute => prepare_and_execute_automatic_ddl(
                applier,
                journal,
                semantic_inventory,
                ledger,
                AutomaticDdlInput {
                    context,
                    header,
                    event,
                },
                &ddl_event,
            )?,
            DdlReplayAction::CheckpointOnly => checkpoint_only_automatic_ddl(
                applier.executor(),
                journal,
                context,
                event,
                &ddl_event,
            )?,
            DdlReplayAction::AlreadyCheckpointed => resolved_ddl_outcome(ddl_event.clone()),
        },
    };
    context
        .schema_resolver
        .invalidate_schema(&query.database_name);
    Ok(Some(outcome))
}

pub(super) fn prepare_and_execute_automatic_ddl<E, R, C, J, S, D>(
    applier: &mut RowApplier<E>,
    journal: &J,
    semantic_inventory: &S,
    ledger: &D,
    input: AutomaticDdlInput<'_, '_, R, C>,
    ddl_event: &DdlEvent,
) -> Result<StructuredEventOutcome, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
    S: DdlSemanticInventory,
    D: DdlEventLedger,
{
    let AutomaticDdlInput {
        context,
        header: _,
        event,
    } = input;
    let transformation = semantic_inventory
        .transform_sql(&ddl_event.raw_sql)
        .map_err(ApplyBinlogError::Statement)?;
    let mut evidence = capture_automatic_ddl_evidence(semantic_inventory, ledger, ddl_event)?;
    evidence.transformation_version = transformation.version.to_string();
    evidence.generated_sql = transformation.target_sql.clone();
    journal
        .prepare(ddl_event, &evidence)
        .map_err(ApplyBinlogError::Statement)?;
    #[cfg(feature = "integration-failpoints")]
    trigger_integration_failpoint(
        super::super::IntegrationFailpoint::PrepareFailure,
        "after-journal-prepare",
    );

    let outcome = execute_transformed_ddl(applier.executor(), ddl_event, transformation)?;
    #[cfg(feature = "integration-failpoints")]
    super::super::wait_for_integration_barrier(
        super::super::IntegrationFailpoint::TargetConnectionLoss,
        "after-target-operation-before-journal-applied",
    );
    #[cfg(feature = "integration-failpoints")]
    trigger_integration_failpoint(
        super::super::IntegrationFailpoint::PostDdlPreApplied,
        "after-ddl-before-journal-applied",
    );

    verify_automatic_ddl_postcondition(semantic_inventory, journal, ddl_event, &evidence)?;
    journal
        .mark_applied(ddl_event)
        .map_err(ApplyBinlogError::Statement)?;
    #[cfg(feature = "integration-failpoints")]
    trigger_integration_failpoint(
        super::super::IntegrationFailpoint::AppliedPreCheckpoint,
        "after-journal-applied-before-checkpoint",
    );
    finalize_automatic_ddl_checkpoint(applier.executor(), journal, context, event, ddl_event)?;
    Ok(outcome)
}

fn execute_transformed_ddl(
    executor: &impl TransactionalTargetExecutor,
    ddl_event: &DdlEvent,
    transformation: DdlTransformation,
) -> Result<StructuredEventOutcome, ApplyBinlogError> {
    if let Some(target_sql) = transformation.target_sql {
        executor
            .execute(&SqlStatement {
                sql: target_sql.clone(),
                params: Vec::new(),
            })
            .map_err(|error| {
                ApplyBinlogError::Statement(format!(
                    "failed transformed DDL at {}:{} version={} target_sql={target_sql}: {error}",
                    ddl_event.binlog_file, ddl_event.event_start_position, transformation.version,
                ))
            })?;
    }
    Ok(resolved_ddl_outcome(ddl_event.clone()))
}

pub(super) fn capture_automatic_ddl_evidence<S, D>(
    semantic_inventory: &S,
    ledger: &D,
    ddl_event: &DdlEvent,
) -> Result<DdlSemanticEvidence, ApplyBinlogError>
where
    S: DdlSemanticInventory,
    D: DdlEventLedger,
{
    match semantic_inventory.capture_evidence(
        &ddl_event.raw_sql,
        &ddl_event.binlog_file,
        ddl_event.event_end_position,
    ) {
        Ok(evidence) => Ok(evidence),
        Err(_) => {
            ledger
                .record_pending(ddl_event)
                .map_err(ApplyBinlogError::Statement)?;
            Err(pending_ddl_error(ddl_event))
        }
    }
}

pub(super) fn verify_automatic_ddl_postcondition<S, J>(
    semantic_inventory: &S,
    journal: &J,
    ddl_event: &DdlEvent,
    evidence: &DdlSemanticEvidence,
) -> Result<(), ApplyBinlogError>
where
    S: DdlSemanticInventory,
    J: DdlReplayJournal,
{
    let observed = semantic_inventory
        .observe_target_state(&ddl_event.raw_sql)
        .map_err(ApplyBinlogError::Statement)?;
    if observed == evidence.expected_post_state {
        return Ok(());
    }
    journal
        .mark_blocked(ddl_event)
        .map_err(ApplyBinlogError::Statement)?;
    Err(ApplyBinlogError::Statement(format!(
        "automatic DDL postcondition mismatch at {}:{}",
        ddl_event.binlog_file, ddl_event.event_start_position
    )))
}

pub(super) fn checkpoint_only_automatic_ddl<E, R, C, J>(
    executor: &E,
    journal: &J,
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    ddl_event: &DdlEvent,
) -> Result<StructuredEventOutcome, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
{
    println!(
        "cdc_ddl_checkpoint_only file={} start_position={}",
        ddl_event.binlog_file, ddl_event.event_start_position
    );
    finalize_automatic_ddl_checkpoint(executor, journal, context, event, ddl_event)?;
    Ok(resolved_ddl_outcome(ddl_event.clone()))
}

pub(super) fn reconcile_prepared_automatic_ddl<E, R, C, J, S>(
    executor: &E,
    journal: &J,
    semantic_inventory: &S,
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    ddl_event: &DdlEvent,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
    S: DdlSemanticInventory,
{
    let evidence = journal
        .read_evidence(ddl_event)
        .map_err(ApplyBinlogError::Statement)?
        .ok_or_else(|| {
            ApplyBinlogError::Statement(format!(
                "prepared automatic DDL lacks immutable evidence at {}:{}",
                ddl_event.binlog_file, ddl_event.event_start_position
            ))
        })?;
    let observed = semantic_inventory
        .observe_target_state(&ddl_event.raw_sql)
        .map_err(ApplyBinlogError::Statement)?;
    match reconcile_prepared(&evidence, &observed) {
        PreparedReconciliation::ProvenApplied => {
            println!(
                "cdc_ddl_reconcile_prepared outcome=proven_applied file={} start_position={}",
                ddl_event.binlog_file, ddl_event.event_start_position
            );
            journal
                .mark_applied(ddl_event)
                .map_err(ApplyBinlogError::Statement)?
        }
        PreparedReconciliation::Blocked => {
            journal
                .mark_blocked(ddl_event)
                .map_err(ApplyBinlogError::Statement)?;
            return Err(ApplyBinlogError::Statement(format!(
                "automatic DDL semantic reconciliation blocked at {}:{}: {}",
                ddl_event.binlog_file,
                ddl_event.event_start_position,
                prepared_reconciliation_block_reason(&evidence, &observed),
            )));
        }
    }
    finalize_automatic_ddl_checkpoint(executor, journal, context, event, ddl_event)
}

pub(super) fn handle_manual_ddl_event<E, R, C, D>(
    executor: &E,
    ledger: &D,
    source_identity: &str,
    context: &mut StreamEventContext<'_, R, C>,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    D: DdlEventLedger,
{
    let Some((query, ddl_event)) = manual_ddl_event(
        source_identity,
        context.current_file,
        header,
        event,
        context.state,
    ) else {
        return Ok(None);
    };

    flush_grouped_transaction(executor, context)?;
    let status = ledger
        .read_status(&ddl_event)
        .map_err(ApplyBinlogError::Statement)?;
    handle_ddl_status(executor, ledger, context, event, query, ddl_event, status)
}

pub(super) fn automatically_handled_ddl_event<'a>(
    source_identity: &str,
    current_file: &str,
    header: &EventHeader,
    event: &'a BinlogEvent,
    state: &StructuredEventState,
) -> Option<(&'a mysql_cdc::events::query_event::QueryEvent, DdlEvent)> {
    let BinlogEvent::QueryEvent(query) = event else {
        return None;
    };
    let operation = parse_ddl_operation(&query.sql_statement).ok();
    let supports_transformation = supports_rename_columns_if_exists(&query.sql_statement);
    let supports_automatic_operation = operation.as_ref().is_some_and(|operation| {
        if operation.family == DdlFamily::Index {
            supports_automatic_index_ddl(&query.sql_statement)
        } else {
            supports_automatic_semantic_recovery(operation)
        }
    });
    let supported_by_runtime = supports_transformation
        || (crate::statement::is_automatically_handled_schema_change(&query.sql_statement)
            && supports_automatic_operation);
    let can_handle_automatically = state.should_apply_schema(&query.database_name)
        && !query_contains_qualified_identifier(&query.sql_statement)
        && supported_by_runtime;
    if !can_handle_automatically {
        return None;
    }
    Some((
        query,
        ddl_event(source_identity, current_file, header, query),
    ))
}

pub(super) fn manual_ddl_event<'a>(
    source_identity: &str,
    current_file: &str,
    header: &EventHeader,
    event: &'a BinlogEvent,
    state: &StructuredEventState,
) -> Option<(&'a mysql_cdc::events::query_event::QueryEvent, DdlEvent)> {
    let BinlogEvent::QueryEvent(query) = event else {
        return None;
    };
    if !crate::statement::is_schema_changing_statement(&query.sql_statement) {
        return None;
    }
    let may_target_source_schema = state.should_apply_schema(&query.database_name)
        || query.database_name.is_empty()
        || query_references_source_schema(state, &query.sql_statement);
    if !may_target_source_schema {
        return None;
    }
    if automatically_handled_ddl_event(source_identity, current_file, header, event, state)
        .is_some()
    {
        return None;
    }
    Some((
        query,
        ddl_event(source_identity, current_file, header, query),
    ))
}

pub(super) fn handle_ddl_status<E, R, C, D>(
    executor: &E,
    ledger: &D,
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    query: &mysql_cdc::events::query_event::QueryEvent,
    ddl_event: DdlEvent,
    status: Option<DdlEventStatus>,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    D: DdlEventLedger,
{
    match status {
        None => {
            ledger
                .record_pending(&ddl_event)
                .map_err(ApplyBinlogError::Statement)?;
            Err(pending_ddl_error(&ddl_event))
        }
        Some(DdlEventStatus::Pending { raw_sql }) => {
            require_matching_ddl(&ddl_event, &raw_sql)?;
            Err(pending_ddl_error(&ddl_event))
        }
        Some(DdlEventStatus::Resolved { raw_sql }) => {
            require_matching_ddl(&ddl_event, &raw_sql)?;
            checkpoint_resolved_ddl(executor, context, event, &ddl_event)?;
            context
                .schema_resolver
                .invalidate_schema(&query.database_name);
            context.state.clear_query_context();
            Ok(Some(resolved_ddl_outcome(ddl_event)))
        }
    }
}

pub(super) fn resolved_ddl_outcome(event: DdlEvent) -> StructuredEventOutcome {
    StructuredEventOutcome {
        policy: EventPolicy::CommitTransaction,
        resume_coordinate: Some(BinlogCoordinate {
            file: event.binlog_file,
            position: event.event_end_position,
        }),
    }
}

pub(super) fn ddl_event(
    source_identity: &str,
    current_file: &str,
    header: &EventHeader,
    query: &mysql_cdc::events::query_event::QueryEvent,
) -> DdlEvent {
    let event_end_position = u64::from(header.next_event_position);
    let event_start_position = event_end_position.saturating_sub(u64::from(header.event_length));
    DdlEvent {
        source_identity: format!("{source_identity}#server-id={}", header.server_id),
        source_server_id: header.server_id,
        binlog_file: current_file.to_string(),
        event_start_position,
        event_end_position,
        schema_name: query.database_name.clone(),
        raw_sql: query.sql_statement.clone(),
    }
}

pub(super) fn require_matching_ddl(
    event: &DdlEvent,
    saved_sql: &str,
) -> Result<(), ApplyBinlogError> {
    if saved_sql == event.raw_sql {
        return Ok(());
    }
    Err(ApplyBinlogError::Statement(format!(
        "DDL ledger SQL mismatch at {}:{} for source_server_id={}",
        event.binlog_file, event.event_start_position, event.source_server_id
    )))
}

pub(super) fn pending_ddl_error(event: &DdlEvent) -> ApplyBinlogError {
    ApplyBinlogError::Statement(format!(
        "manual DDL resolution required source_server_id={} file={} start_position={} end_position={} schema={} sql={}",
        event.source_server_id,
        event.binlog_file,
        event.event_start_position,
        event.event_end_position,
        event.schema_name,
        event.raw_sql.replace(char::is_whitespace, " ")
    ))
}

pub(super) fn finalize_automatic_ddl_checkpoint<E, R, C, J>(
    executor: &E,
    journal: &J,
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    ddl_event: &DdlEvent,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
{
    let coordinate = BinlogCoordinate {
        file: ddl_event.binlog_file.clone(),
        position: ddl_event.event_end_position,
    };
    ensure_resolved_ddl_checkpoint_advances(context.checkpoint_store, &coordinate)?;
    let (Some(checkpoint_table), Some(checkpoint_name)) = (
        context.transaction_checkpoint_table,
        context.transaction_checkpoint_name,
    ) else {
        return Err(ApplyBinlogError::Checkpoint(
            "automatic DDL requires target-transaction checkpoint storage".to_string(),
        ));
    };
    let checkpoint = crate::live::reconnect::coordinate_checkpoint(&coordinate, event_name(event));

    executor
        .begin_transaction()
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    let result = (|| {
        let current = executor
            .load_transaction_checkpoint_for_update(checkpoint_table, checkpoint_name)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        ensure_automatic_ddl_checkpoint_predecessor(current.as_ref(), ddl_event)?;
        let transition = journal
            .checkpoint_transition_statement(ddl_event)
            .map_err(ApplyBinlogError::Statement)?;
        executor
            .execute(&transition)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        #[cfg(feature = "integration-failpoints")]
        trigger_integration_failpoint(
            super::super::IntegrationFailpoint::CheckpointTransaction,
            "after-journal-checkpoint-cas-before-checkpoint-write",
        );
        executor
            .save_transaction_checkpoint(checkpoint_table, checkpoint_name, &checkpoint)
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
        executor
            .commit_transaction()
            .map_err(|error| ApplyBinlogError::Target(error.to_string()))
    })();
    if let Err(error) = result {
        executor
            .rollback_transaction()
            .map_err(|rollback| ApplyBinlogError::Target(rollback.to_string()))?;
        return Err(error);
    }
    *context.current_file = coordinate.file;
    Ok(())
}

pub(super) fn ensure_automatic_ddl_checkpoint_predecessor(
    current: Option<&crate::checkpoint::Checkpoint>,
    event: &DdlEvent,
) -> Result<(), ApplyBinlogError> {
    let Some(current) = current else {
        return Err(ApplyBinlogError::Checkpoint(format!(
            "automatic DDL checkpoint predecessor mismatch at {}:{}: checkpoint row is missing",
            event.binlog_file, event.event_start_position
        )));
    };
    let current_is_before_event = current.source_file < event.binlog_file
        || (current.source_file == event.binlog_file
            && current.source_position <= event.event_start_position);
    if current_is_before_event {
        return Ok(());
    }
    Err(ApplyBinlogError::Checkpoint(format!(
        "automatic DDL checkpoint predecessor mismatch: expected at or before {}:{} but locked {}:{}",
        event.binlog_file, event.event_start_position, current.source_file, current.source_position
    )))
}

pub(super) fn checkpoint_resolved_ddl<E, R, C>(
    executor: &E,
    context: &mut StreamEventContext<'_, R, C>,
    event: &BinlogEvent,
    ddl_event: &DdlEvent,
) -> Result<(), ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    C: StreamCheckpointStore,
{
    let coordinate = BinlogCoordinate {
        file: ddl_event.binlog_file.clone(),
        position: ddl_event.event_end_position,
    };
    ensure_resolved_ddl_checkpoint_advances(context.checkpoint_store, &coordinate)?;
    let checkpoint = crate::live::reconnect::coordinate_checkpoint(&coordinate, event_name(event));
    if let (Some(table), Some(name)) = (
        context.transaction_checkpoint_table,
        context.transaction_checkpoint_name,
    ) {
        save_resolved_ddl_transaction_checkpoint(executor, table, name, &checkpoint)?;
    } else if let Some(store) = context.checkpoint_store {
        store.save_checkpoint(&checkpoint)?;
    }
    *context.current_file = coordinate.file;
    Ok(())
}

pub(super) fn ensure_resolved_ddl_checkpoint_advances(
    checkpoint_store: Option<&impl StreamCheckpointStore>,
    next: &BinlogCoordinate,
) -> Result<(), ApplyBinlogError> {
    let Some(store) = checkpoint_store else {
        return Ok(());
    };
    let current = store.load_checkpoint()?;
    ensure_coordinate_advances(current.as_ref(), next)
}

pub(super) fn ensure_coordinate_advances(
    current: Option<&crate::checkpoint::Checkpoint>,
    next: &BinlogCoordinate,
) -> Result<(), ApplyBinlogError> {
    let Some(current) = current else {
        return Ok(());
    };
    let current_coordinate = BinlogCoordinate {
        file: current.source_file.clone(),
        position: current.source_position,
    };
    if binlog_coordinate_is_before(next, &current_coordinate) {
        return Err(ApplyBinlogError::Checkpoint(format!(
            "refusing checkpoint regression from {}:{} to {}:{}",
            current_coordinate.file, current_coordinate.position, next.file, next.position
        )));
    }
    Ok(())
}

pub(super) fn binlog_coordinate_is_before(
    left: &BinlogCoordinate,
    right: &BinlogCoordinate,
) -> bool {
    left.file < right.file || (left.file == right.file && left.position < right.position)
}

pub(super) fn save_resolved_ddl_transaction_checkpoint(
    executor: &impl TransactionalTargetExecutor,
    table: &str,
    checkpoint_name: &str,
    checkpoint: &crate::checkpoint::Checkpoint,
) -> Result<(), ApplyBinlogError> {
    executor
        .begin_transaction()
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    let save_result =
        lock_validate_and_save_checkpoint(executor, table, checkpoint_name, checkpoint);
    if let Err(error) = save_result {
        executor
            .rollback_transaction()
            .map_err(|rollback_error| ApplyBinlogError::Target(rollback_error.to_string()))?;
        return Err(error);
    }
    executor
        .commit_transaction()
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))
}

pub(super) fn lock_validate_and_save_checkpoint(
    executor: &impl TransactionalTargetExecutor,
    table: &str,
    checkpoint_name: &str,
    checkpoint: &crate::checkpoint::Checkpoint,
) -> Result<(), ApplyBinlogError> {
    let current = executor
        .load_transaction_checkpoint_for_update(table, checkpoint_name)
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?
        .ok_or_else(|| {
            ApplyBinlogError::Checkpoint(format!(
                "required source-scoped checkpoint `{checkpoint_name}` disappeared during target transaction"
            ))
        })?;
    let next = BinlogCoordinate {
        file: checkpoint.source_file.clone(),
        position: checkpoint.source_position,
    };
    ensure_coordinate_advances(Some(&current), &next)?;
    executor
        .save_transaction_checkpoint(table, checkpoint_name, checkpoint)
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))
}
