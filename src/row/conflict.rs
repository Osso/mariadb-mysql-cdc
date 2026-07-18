use super::model::{
    DuplicateConflictInput, RowApplyError, RowConflictContext, RowOperation, RowResult,
    RowTableMap, conflict_operation, row_error,
};
use crate::conflict_repair::{ConflictCoordinate, ConflictObservation, ConflictStore};
use crate::mysql_client::value_to_string;
use crate::probe::BinlogCoordinate;
use crate::target::{TargetExecuteError, TargetExecutionOutcome, TargetExecutor, TargetRowChange};
use mysql::Value;

pub(crate) fn build_duplicate_conflict_observation(
    input: DuplicateConflictInput<'_>,
) -> ConflictObservation {
    ConflictObservation {
        source_identity: input.source_identity.to_string(),
        source_server_id: input.source_server_id,
        coordinate: ConflictCoordinate {
            file: input.coordinate.file.clone(),
            start_position: input.coordinate.position,
            end_position: input.end_position,
        },
        schema: input.schema.to_string(),
        table: input.table.to_string(),
        operation: conflict_operation(input.operation),
        source_primary_key: input
            .primary_key
            .iter()
            .map(value_to_conflict_key)
            .collect(),
        duplicate_index: input.duplicate_index,
        duplicate_owner_primary_key: input.duplicate_owner_primary_key,
        error_code: input.error_code,
        error_text: input.error_text.to_string(),
        observed_at_ms: input.observed_at_ms,
    }
}

pub(crate) fn execute_row_statement<E>(
    executor: &E,
    change: TargetRowChange,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    context: Option<&mut RowConflictContext<'_>>,
) -> RowResult<()>
where
    E: TargetExecutor,
{
    let primary_key = &change.primary_key_values;
    let outcome = executor.execute_row_change(&change).map_err(|source| {
        row_target_error(coordinate, table, operation, RowError::Target(source))
    })?;
    match outcome {
        TargetExecutionOutcome::Applied => Ok(()),
        TargetExecutionOutcome::DuplicateIgnored(_) => {
            println!(
                "{}",
                format_row_conflict_skipped(operation, table, coordinate, primary_key)
            );
            stage_successful_conflict_resolution(
                context,
                coordinate,
                table,
                operation,
                primary_key,
                "equal target row already existed",
            )
        }
        TargetExecutionOutcome::PrimaryKeyReplaced(conflict) => {
            println!(
                "{}",
                format_row_conflict_replaced(operation, table, coordinate, primary_key)
            );
            record_replaced_conflict(context, coordinate, table, operation, primary_key, conflict)
        }
        TargetExecutionOutcome::ConstraintConflict(conflict) => {
            println!(
                "{}",
                format_row_conflict_skipped(operation, table, coordinate, primary_key)
            );
            record_skipped_conflict(context, coordinate, table, operation, primary_key, conflict)
        }
    }
}

fn record_replaced_conflict(
    context: Option<&mut RowConflictContext<'_>>,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    primary_key: &[Value],
    conflict: crate::target::DuplicateConflict,
) -> RowResult<()> {
    let Some(context) = context else {
        return Ok(());
    };
    let reason = format!(
        "target row replaced with source image; {}",
        conflict.error_text
    );
    stage_successful_conflict_resolution(
        Some(context),
        coordinate,
        table,
        operation,
        primary_key,
        &reason,
    )
}

fn stage_successful_conflict_resolution(
    context: Option<&mut RowConflictContext<'_>>,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    primary_key: &[Value],
    reason: &str,
) -> RowResult<()> {
    let Some(context) = context else {
        return Ok(());
    };
    let primary_key = primary_key
        .iter()
        .map(value_to_conflict_key)
        .collect::<Vec<_>>();
    let repair_run_id = format!(
        "stream-{}-{}-{}",
        coordinate.file.replace('/', "_"),
        coordinate.position,
        table.table
    );
    let evidence = format!(
        "{reason}; source coordinate {}:{}; table `{}` primary key {:?}",
        coordinate.file, coordinate.position, table.table, primary_key
    );
    context
        .pending_resolutions
        .push(crate::conflict_repair::ConflictResolution {
            source_identity: context.source_identity.to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            source_primary_key: primary_key,
            repair_run_id,
            evidence,
        });
    let _ = operation;
    Ok(())
}

fn record_skipped_conflict(
    context: Option<&mut RowConflictContext<'_>>,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    primary_key: &[Value],
    conflict: crate::target::DuplicateConflict,
) -> RowResult<()> {
    let Some(context) = context else {
        return Ok(());
    };
    let conflict_error = conflict.error_text.clone();
    let observation =
        skipped_conflict_observation(context, coordinate, table, operation, primary_key, conflict);
    context
        .store
        .observe(observation)
        .map_err(|error| conflict_store_error(coordinate, table, operation, "persist", error))?;
    let unresolved_count = context
        .store
        .unresolved_count_result()
        .map_err(|error| conflict_store_error(coordinate, table, operation, "read", error))?;
    println!("cdc_row_conflict_progress unresolved_count={unresolved_count}");
    Err(row_target_error(
        coordinate,
        table,
        operation,
        RowError::Target(TargetExecuteError::new(format!(
            "row conflict persisted for repair: {conflict_error}"
        ))),
    ))
}

fn skipped_conflict_observation(
    context: &RowConflictContext<'_>,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    primary_key: &[Value],
    conflict: crate::target::DuplicateConflict,
) -> ConflictObservation {
    ConflictObservation {
        source_identity: context.source_identity.to_string(),
        source_server_id: context.source_server_id,
        coordinate: ConflictCoordinate {
            file: coordinate.file.clone(),
            start_position: coordinate.position,
            end_position: context.end_position,
        },
        schema: table.schema.clone(),
        table: table.table.clone(),
        operation: conflict_operation(operation),
        source_primary_key: primary_key.iter().map(value_to_conflict_key).collect(),
        duplicate_index: conflict.duplicate_index,
        duplicate_owner_primary_key: None,
        error_code: conflict.error_code,
        error_text: conflict.error_text,
        observed_at_ms: context.observed_at_ms,
    }
}

fn conflict_store_error(
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    action: &'static str,
    error: String,
) -> Box<RowApplyError> {
    row_target_error(
        coordinate,
        table,
        operation,
        RowError::ConflictStore { action, error },
    )
}

enum RowError {
    Target(TargetExecuteError),
    ConflictStore { action: &'static str, error: String },
}

fn row_target_error(
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    error: RowError,
) -> Box<RowApplyError> {
    let source = match error {
        RowError::Target(source) => source,
        RowError::ConflictStore { action, error } => {
            TargetExecuteError::new(format!("failed to {action} duplicate conflict: {error}"))
        }
    };
    row_error(RowApplyError::Target {
        coordinate: coordinate.clone(),
        schema: table.schema.clone(),
        table: table.table.clone(),
        operation,
        source,
    })
}

pub(crate) fn format_row_conflict_replaced(
    operation: RowOperation,
    table: &RowTableMap,
    coordinate: &BinlogCoordinate,
    primary_key: &[Value],
) -> String {
    let primary_key = primary_key
        .iter()
        .cloned()
        .map(value_to_string)
        .collect::<Vec<_>>();
    let primary_key = serde_json::to_string(&primary_key).expect("primary key JSON encoding");
    format!(
        "cdc_row_conflict_replaced operation={operation} schema={} table={} source_file={} source_position={} primary_key={primary_key}",
        table.schema, table.table, coordinate.file, coordinate.position,
    )
}

pub(crate) fn format_row_conflict_skipped(
    operation: RowOperation,
    table: &RowTableMap,
    coordinate: &BinlogCoordinate,
    primary_key: &[Value],
) -> String {
    let primary_key = primary_key
        .iter()
        .cloned()
        .map(value_to_string)
        .collect::<Vec<_>>();
    let primary_key = serde_json::to_string(&primary_key).expect("primary key JSON encoding");
    format!(
        "cdc_row_conflict_skipped operation={operation} schema={} table={} source_file={} source_position={} primary_key={primary_key}",
        table.schema, table.table, coordinate.file, coordinate.position,
    )
}

pub(crate) fn value_to_conflict_key(value: &Value) -> String {
    match value {
        Value::NULL => "NULL".to_string(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}")
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            format!(
                "{}{}:{minutes:02}:{seconds:02}.{micros:06}",
                if *negative { "-" } else { "" },
                days * 24 + u32::from(*hours)
            )
        }
    }
}

pub(crate) fn record_duplicate_conflict<C: ConflictStore>(
    recorder: &mut C,
    input: DuplicateConflictInput<'_>,
) -> Result<(), String> {
    recorder.observe(build_duplicate_conflict_observation(input))
}
