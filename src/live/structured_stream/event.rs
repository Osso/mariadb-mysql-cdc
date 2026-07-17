use super::*;

#[cfg(test)]
pub(super) fn classify_event(
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
) -> StructuredEventOutcome {
    StructuredEventOutcome {
        policy: event_policy(event),
        resume_coordinate: resume_coordinate(current_file, header, event),
    }
}

#[cfg(test)]
pub(super) fn event_policy(event: &BinlogEvent) -> EventPolicy {
    match event {
        BinlogEvent::QueryEvent(_) => EventPolicy::Ignore,
        BinlogEvent::RowsQueryEvent(_) => EventPolicy::IgnoreAnnotation,
        BinlogEvent::TableMapEvent(_) => EventPolicy::ApplyTableMap,
        BinlogEvent::WriteRowsEvent(_)
        | BinlogEvent::UpdateRowsEvent(_)
        | BinlogEvent::DeleteRowsEvent(_) => EventPolicy::ApplyRows,
        BinlogEvent::XidEvent(_) => EventPolicy::CommitTransaction,
        _ => EventPolicy::Ignore,
    }
}

pub(super) fn handle_structured_event<E, R>(
    applier: &mut RowApplier<E>,
    schema_resolver: &R,
    state: &mut StructuredEventState,
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Result<StructuredEventOutcome, ApplyBinlogError>
where
    E: TargetExecutor,
    R: TableSchemaResolver,
{
    handle_structured_event_with_conflicts(
        applier,
        schema_resolver,
        state,
        current_file,
        header,
        event,
        None,
    )
}

pub(super) fn handle_structured_event_with_conflicts<E, R>(
    applier: &mut RowApplier<E>,
    schema_resolver: &R,
    state: &mut StructuredEventState,
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
    conflict_context: Option<&mut RowConflictContext<'_>>,
) -> Result<StructuredEventOutcome, ApplyBinlogError>
where
    E: TargetExecutor,
    R: TableSchemaResolver,
{
    let coordinate = event_coordinate(current_file, header, event);
    let policy = apply_structured_event(
        applier,
        schema_resolver,
        state,
        &coordinate,
        event,
        conflict_context,
    )?;
    Ok(StructuredEventOutcome {
        policy,
        resume_coordinate: resume_coordinate(current_file, header, event),
    })
}

pub(super) fn apply_structured_event<E, R>(
    applier: &mut RowApplier<E>,
    schema_resolver: &R,
    state: &mut StructuredEventState,
    coordinate: &BinlogCoordinate,
    event: &BinlogEvent,
    conflict_context: Option<&mut RowConflictContext<'_>>,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
    R: TableSchemaResolver,
{
    match event {
        BinlogEvent::TableMapEvent(table_map) => {
            apply_table_map_event(applier, schema_resolver, state, coordinate, table_map)
        }
        BinlogEvent::WriteRowsEvent(rows) => {
            apply_write_rows_event(applier, state, coordinate, rows, conflict_context)
        }
        BinlogEvent::UpdateRowsEvent(rows) => {
            apply_update_rows_event(applier, state, coordinate, rows, conflict_context)
        }
        BinlogEvent::DeleteRowsEvent(rows) => {
            apply_delete_rows_event(applier, state, coordinate, rows, conflict_context)
        }
        BinlogEvent::XidEvent(_) => Ok(EventPolicy::CommitTransaction),
        BinlogEvent::IntVarEvent(event) => {
            state.record_intvar(event);
            Ok(EventPolicy::Ignore)
        }
        BinlogEvent::UserVarEvent(event) => {
            state.record_uservar(event);
            Ok(EventPolicy::Ignore)
        }
        BinlogEvent::QueryEvent(query) => {
            let policy = apply_query_event(applier, state, coordinate, query)?;
            if policy == EventPolicy::CommitTransaction
                && crate::statement::is_schema_changing_statement(&query.sql_statement)
            {
                schema_resolver.invalidate_schema(&query.database_name);
            }
            Ok(policy)
        }
        BinlogEvent::RowsQueryEvent(_) => Ok(EventPolicy::IgnoreAnnotation),
        _ => Ok(EventPolicy::Ignore),
    }
}

pub(super) fn apply_query_event<E>(
    applier: &RowApplier<E>,
    state: &mut StructuredEventState,
    coordinate: &BinlogCoordinate,
    query: &mysql_cdc::events::query_event::QueryEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    if let Some(policy) = query_event_precheck(state, query)? {
        return Ok(policy);
    }
    reject_ambiguous_query_database(&query.sql_statement)?;
    apply_query_context(applier.executor(), state)?;
    let result = apply_query_statement(applier, coordinate, query);
    state.clear_query_context();
    result
}

fn query_event_precheck(
    state: &mut StructuredEventState,
    query: &mysql_cdc::events::query_event::QueryEvent,
) -> Result<Option<EventPolicy>, ApplyBinlogError> {
    let statement_dml = crate::statement::is_data_changing_statement(&query.sql_statement);
    let may_target_source_schema = state.should_apply_schema(&query.database_name)
        || query.database_name.is_empty()
        || query_references_source_schema(state, &query.sql_statement);
    if statement_dml && may_target_source_schema {
        state.clear_query_context();
        return Err(mapping_error(format!(
            "ROW/FULL contract violation: source emitted statement DML QueryEvent: {}",
            query.sql_statement.chars().take(120).collect::<String>()
        )));
    }
    if !state.should_apply_schema(&query.database_name) {
        state.clear_query_context();
        return Ok(Some(EventPolicy::Ignore));
    }
    if is_transaction_control_query(&query.sql_statement) {
        state.clear_query_context();
        return Ok(Some(EventPolicy::Ignore));
    }
    Ok(None)
}

fn apply_query_statement<E>(
    applier: &RowApplier<E>,
    coordinate: &BinlogCoordinate,
    query: &mysql_cdc::events::query_event::QueryEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    let event = StatementEvent {
        coordinate: coordinate.clone(),
        resume_position: coordinate.position,
        default_database: Some(query.database_name.clone()),
        sql: query.sql_statement.clone(),
    };
    let statement_applier =
        StatementApplier::new(applier.executor(), RecordingQuarantine::default());
    match statement_applier.apply(&event) {
        Ok(StatementOutcome::Replayed | StatementOutcome::Skipped) => {
            Ok(EventPolicy::CommitTransaction)
        }
        Ok(StatementOutcome::Quarantined(_)) => Err(ApplyBinlogError::Quarantined(
            statement_applier
                .quarantine_recorder()
                .recorded_statements(),
        )),
        Err(error) => Err(ApplyBinlogError::Statement(error.to_string())),
    }
}

pub(super) fn apply_query_context<E>(
    executor: &E,
    state: &StructuredEventState,
) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
{
    if !state.pending_uservars.is_empty() {
        return Err(mapping_error(format!(
            "cannot replay QueryEvent with user variables: {}",
            state.pending_uservars.join(", ")
        )));
    }

    for intvar in &state.pending_intvars {
        apply_intvar(executor, intvar)?;
    }
    Ok(())
}

pub(super) fn apply_intvar<E>(executor: &E, intvar: &PendingIntVar) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
{
    const INSERT_ID: u8 = 2;
    if intvar.intvar_type != INSERT_ID {
        return Err(mapping_error(format!(
            "cannot replay unsupported IntVarEvent type {}",
            intvar.intvar_type
        )));
    }

    executor
        .execute(&crate::target::SqlStatement {
            sql: "SET INSERT_ID = ?".to_string(),
            params: vec![Value::UInt(intvar.value)],
        })
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))
}

pub(super) fn reject_ambiguous_query_database(sql: &str) -> Result<(), ApplyBinlogError> {
    if query_contains_qualified_identifier(sql) {
        return Err(mapping_error(format!(
            "cannot replay QueryEvent with qualified identifier: {}",
            sql.chars().take(120).collect::<String>()
        )));
    }
    Ok(())
}

pub(super) fn query_references_source_schema(state: &StructuredEventState, sql: &str) -> bool {
    state
        .source_database
        .as_deref()
        .is_some_and(|schema| query_references_schema(sql, schema))
}

pub(super) fn is_transaction_control_query(sql: &str) -> bool {
    matches!(
        sql.trim()
            .trim_end_matches(';')
            .to_ascii_uppercase()
            .as_str(),
        "BEGIN" | "COMMIT" | "ROLLBACK"
    )
}

pub(super) fn require_full_row_image(
    columns_present: &[bool],
    operation: &str,
) -> Result<(), ApplyBinlogError> {
    if columns_present.iter().all(|present| *present) {
        return Ok(());
    }

    Err(mapping_error(format!(
        "cannot apply {operation} row event without FULL binlog row image"
    )))
}

pub(super) fn event_coordinate(
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
) -> BinlogCoordinate {
    resume_coordinate(current_file, header, event).unwrap_or_else(|| BinlogCoordinate {
        file: current_file.to_string(),
        position: u64::from(header.next_event_position),
    })
}

pub(super) fn resume_coordinate(
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Option<BinlogCoordinate> {
    match event {
        BinlogEvent::RotateEvent(rotate) => Some(BinlogCoordinate {
            file: rotate.binlog_filename.clone(),
            position: rotate.binlog_position,
        }),
        BinlogEvent::XidEvent(_) | BinlogEvent::QueryEvent(_) if header.next_event_position > 0 => {
            Some(BinlogCoordinate {
                file: current_file.to_string(),
                position: u64::from(header.next_event_position),
            })
        }
        _ => None,
    }
}
