use super::*;
use crate::live::ddl_semantics::{
    DDL_TRANSFORMATION_VERSION, DdlTransformation, supports_drop_columns_if_exists,
    supports_drop_procedure, supports_fixture_create_table, supports_rename_columns_if_exists,
    supports_source_only_release_move_procedure_create,
};
use crate::target::SqlStatement;

pub(super) fn handle_ddl_event<E, R, C, J, S>(
    applier: &mut RowApplier<E>,
    journal: &J,
    semantic_inventory: &S,
    source_identity: &str,
    context: &mut StreamEventContext<'_, R, C>,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
    S: DdlSemanticInventory,
{
    if let Some(outcome) = handle_automatic_ddl_event(
        applier,
        AutomaticDdlDependencies {
            journal,
            semantic_inventory,
            source_identity,
        },
        AutomaticDdlInput {
            context,
            header,
            event,
        },
    )? {
        return Ok(Some(outcome));
    }
    handle_untranslated_ddl_event(
        applier.executor(),
        journal,
        source_identity,
        context,
        header,
        event,
    )
}

fn handle_untranslated_ddl_event<E, R, C, J>(
    executor: &E,
    journal: &J,
    source_identity: &str,
    context: &mut StreamEventContext<'_, R, C>,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
{
    let Some((_, ddl_event)) = manual_ddl_event(
        source_identity,
        context.current_file,
        header,
        event,
        context.state,
    ) else {
        return Ok(None);
    };
    flush_grouped_transaction(executor, context)?;
    ensure_translation_pending(journal, &ddl_event)?;
    Err(ApplyBinlogError::DdlBlocked(format!(
        "DDL translator unavailable at {}:{}; checkpoint remains blocked",
        ddl_event.binlog_file, ddl_event.event_start_position
    )))
}

fn ensure_translation_pending(
    journal: &impl DdlReplayJournal,
    event: &DdlEvent,
) -> Result<(), ApplyBinlogError> {
    match journal
        .read_status(event)
        .map_err(ApplyBinlogError::Statement)?
    {
        None => journal
            .record_translation_pending(event)
            .map_err(ApplyBinlogError::Statement),
        Some(DdlReplayStatus::TranslationPending) => Ok(()),
        Some(status) => Err(ApplyBinlogError::Statement(format!(
            "cannot replace automatic DDL journal status {} with translation_pending at {}:{}",
            status.as_str(),
            event.binlog_file,
            event.event_start_position
        ))),
    }
}

pub(super) fn handle_automatic_ddl_event<E, R, C, J, S>(
    applier: &mut RowApplier<E>,
    dependencies: AutomaticDdlDependencies<'_, J, S>,
    input: AutomaticDdlInput<'_, '_, R, C>,
) -> Result<Option<StructuredEventOutcome>, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
    S: DdlSemanticInventory,
{
    let AutomaticDdlDependencies {
        journal,
        semantic_inventory,
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
        Some(DdlReplayStatus::Blocked) => {
            recover_blocked_automatic_ddl(
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

pub(super) fn prepare_and_execute_automatic_ddl<E, R, C, J, S>(
    applier: &mut RowApplier<E>,
    journal: &J,
    semantic_inventory: &S,
    input: AutomaticDdlInput<'_, '_, R, C>,
    ddl_event: &DdlEvent,
) -> Result<StructuredEventOutcome, ApplyBinlogError>
where
    E: TransactionalTargetExecutor,
    R: TableSchemaResolver,
    C: StreamCheckpointStore,
    J: DdlReplayJournal,
    S: DdlSemanticInventory,
{
    let AutomaticDdlInput {
        context,
        header: _,
        event,
    } = input;
    let create_table_requires_evidence_sql = parse_ddl_operation(&ddl_event.raw_sql)
        .is_ok_and(|operation| operation.create_table_ast.is_some());
    let (transformation, mut evidence) = if create_table_requires_evidence_sql {
        let evidence = capture_automatic_ddl_evidence(semantic_inventory, journal, ddl_event)?;
        let target_sql = evidence.generated_sql.clone().ok_or_else(|| {
            ApplyBinlogError::Statement(
                "CREATE TABLE evidence is missing deterministic generated SQL".to_string(),
            )
        })?;
        (
            DdlTransformation {
                version: DDL_TRANSFORMATION_VERSION,
                target_sql: Some(target_sql),
            },
            evidence,
        )
    } else {
        let transformation = match semantic_inventory.transform_sql(&ddl_event.raw_sql) {
            Ok(transformation) => transformation,
            Err(error) => {
                ensure_translation_pending(journal, ddl_event)?;
                return Err(ApplyBinlogError::DdlBlocked(error));
            }
        };
        let evidence = capture_automatic_ddl_evidence(semantic_inventory, journal, ddl_event)?;
        (transformation, evidence)
    };
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

pub(super) fn capture_automatic_ddl_evidence<S, J>(
    semantic_inventory: &S,
    journal: &J,
    ddl_event: &DdlEvent,
) -> Result<DdlSemanticEvidence, ApplyBinlogError>
where
    S: DdlSemanticInventory,
    J: DdlReplayJournal,
{
    match semantic_inventory.capture_evidence(
        &ddl_event.raw_sql,
        &ddl_event.binlog_file,
        ddl_event.event_end_position,
    ) {
        Ok(evidence) => Ok(evidence),
        Err(error) => {
            ensure_translation_pending(journal, ddl_event)?;
            Err(ApplyBinlogError::DdlBlocked(format!(
                "DDL transformation evidence unavailable at {}:{}: {error}",
                ddl_event.binlog_file, ddl_event.event_start_position
            )))
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
    Err(ApplyBinlogError::DdlBlocked(format!(
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

pub(super) fn recover_blocked_automatic_ddl<E, R, C, J, S>(
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
    let evidence = verified_blocked_recovery_evidence(journal, semantic_inventory, ddl_event)?;
    journal
        .recover_blocked(ddl_event, &evidence)
        .map_err(ApplyBinlogError::Statement)?;
    finalize_automatic_ddl_checkpoint(executor, journal, context, event, ddl_event)
}

fn verified_blocked_recovery_evidence<J, S>(
    journal: &J,
    semantic_inventory: &S,
    ddl_event: &DdlEvent,
) -> Result<DdlSemanticEvidence, ApplyBinlogError>
where
    J: DdlReplayJournal,
    S: DdlSemanticInventory,
{
    let mut evidence = read_blocked_evidence(journal, ddl_event)?;
    let expected = semantic_inventory
        .expected_target_state(&ddl_event.raw_sql)
        .map_err(ApplyBinlogError::Statement)?;
    let observed = semantic_inventory
        .observe_target_state(&ddl_event.raw_sql)
        .map_err(ApplyBinlogError::Statement)?;
    if observed != expected {
        return Err(ApplyBinlogError::DdlBlocked(format!(
            "blocked automatic DDL remains divergent at {}:{}",
            ddl_event.binlog_file, ddl_event.event_start_position
        )));
    }
    evidence.expected_post_state = expected;
    Ok(evidence)
}

fn read_blocked_evidence(
    journal: &impl DdlReplayJournal,
    ddl_event: &DdlEvent,
) -> Result<DdlSemanticEvidence, ApplyBinlogError> {
    journal
        .read_evidence(ddl_event)
        .map_err(ApplyBinlogError::Statement)?
        .ok_or_else(|| {
            ApplyBinlogError::Statement(format!(
                "blocked automatic DDL lacks evidence at {}:{}",
                ddl_event.binlog_file, ddl_event.event_start_position
            ))
        })
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
            return Err(ApplyBinlogError::DdlBlocked(format!(
                "automatic DDL semantic reconciliation blocked at {}:{}: {}",
                ddl_event.binlog_file,
                ddl_event.event_start_position,
                prepared_reconciliation_block_reason(&evidence, &observed),
            )));
        }
    }
    finalize_automatic_ddl_checkpoint(executor, journal, context, event, ddl_event)
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
    let supports_source_only_procedure =
        supports_source_only_release_move_procedure_create(&query.sql_statement);
    automatically_handled_ddl_event_with_source_only_support(
        source_identity,
        current_file,
        header,
        event,
        state,
        supports_source_only_procedure,
    )
}

pub(super) fn automatically_handled_ddl_event_with_source_only_support<'a>(
    source_identity: &str,
    current_file: &str,
    header: &EventHeader,
    event: &'a BinlogEvent,
    state: &StructuredEventState,
    supports_source_only_procedure: bool,
) -> Option<(&'a mysql_cdc::events::query_event::QueryEvent, DdlEvent)> {
    let BinlogEvent::QueryEvent(query) = event else {
        return None;
    };
    let supports_transformation =
        supports_ddl_transformation(&query.sql_statement, supports_source_only_procedure);
    let supported_by_runtime = supports_transformation
        || (crate::statement::is_automatically_handled_schema_change(&query.sql_statement)
            && supports_automatic_ddl_operation(&query.sql_statement));
    let contains_disallowed_qualification = !supports_source_only_procedure
        && query_contains_qualified_identifier(&query.sql_statement);
    let can_handle_automatically = state.should_apply_schema(&query.database_name)
        && !contains_disallowed_qualification
        && supported_by_runtime;
    if !can_handle_automatically {
        return None;
    }
    Some((
        query,
        ddl_event(source_identity, current_file, header, query),
    ))
}

fn supports_ddl_transformation(source_sql: &str, supports_source_only_procedure: bool) -> bool {
    supports_fixture_create_table(source_sql)
        || supports_production_alter_table(source_sql)
        || supports_source_only_procedure
        || supports_drop_procedure(source_sql)
        || supports_drop_columns_if_exists(source_sql)
        || supports_rename_columns_if_exists(source_sql)
}

fn supports_automatic_ddl_operation(source_sql: &str) -> bool {
    parse_ddl_operation(source_sql)
        .ok()
        .is_some_and(|operation| {
            if operation.family == DdlFamily::Index {
                supports_automatic_index_ddl(source_sql)
            } else {
                supports_automatic_semantic_recovery(&operation)
            }
        })
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
