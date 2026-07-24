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
        parent_recovery: None,
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
        TargetExecutionOutcome::Applied => stage_successful_conflict_resolution(
            context,
            coordinate,
            table,
            operation,
            primary_key,
            "source row applied successfully",
        ),
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
            record_skipped_conflict(context, coordinate, table, operation, &change, conflict)
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
    let resolution = crate::conflict_repair::ConflictResolution {
        source_identity: context.source_identity.to_string(),
        schema: table.schema.clone(),
        table: table.table.clone(),
        source_primary_key: primary_key,
        repair_run_id,
        evidence,
    };
    if context
        .store
        .has_unresolved(&resolution)
        .map_err(|error| conflict_store_error(coordinate, table, operation, "inspect", error))?
    {
        context.pending_resolutions.push(resolution);
    }
    Ok(())
}

fn record_skipped_conflict(
    context: Option<&mut RowConflictContext<'_>>,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    change: &TargetRowChange,
    conflict: crate::target::DuplicateConflict,
) -> RowResult<()> {
    let Some(context) = context else {
        return Ok(());
    };
    let observation =
        skipped_conflict_observation(context, coordinate, table, operation, change, conflict);
    if is_deferred_superseded_insert(table, operation, change, &observation) {
        context
            .deferred_superseded_inserts
            .push(crate::row::DeferredSupersededInsertCandidate {
                observation,
                historical_change: change.clone(),
            });
        return Ok(());
    }
    context.pending_observations.push(observation);
    Ok(())
}

fn is_deferred_superseded_insert(
    table: &RowTableMap,
    operation: RowOperation,
    change: &TargetRowChange,
    observation: &ConflictObservation,
) -> bool {
    if operation != RowOperation::Insert
        || change.kind != crate::target::TargetRowChangeKind::Insert
    {
        return false;
    }
    let users_name = table.schema == "globalcomix"
        && table.table == "users"
        && observation.duplicate_index.as_deref() == Some("users.name");
    let comics_slug = table.schema == "globalcomix"
        && table.table == "comics"
        && observation.duplicate_index.as_deref() == Some("comics.slug");
    let releases_superseded_parent = table.schema == "globalcomix"
        && table.table == "releases"
        && observation.error_code == 1452
        && (is_exact_releases_category_constraint_error(&observation.error_text)
            || is_exact_releases_visibility_constraint_error(&observation.error_text));
    users_name || comics_slug || releases_superseded_parent
}

fn is_exact_releases_category_constraint_error(error_text: &str) -> bool {
    error_text.contains("`globalcomix`.`releases`, CONSTRAINT `releases_ibfk_2`")
        && error_text.contains("FOREIGN KEY (`comic_id`, `comic_category_id`)")
        && error_text.contains("REFERENCES `comics` (`id`, `section_id`)")
}

fn is_exact_releases_visibility_constraint_error(error_text: &str) -> bool {
    error_text.contains("`globalcomix`.`releases`, CONSTRAINT `releases_ibfk_3`")
        && error_text.contains("FOREIGN KEY (`comic_id`, `comic_is_visible`)")
        && error_text.contains("REFERENCES `comics` (`id`, `is_visible`)")
}

fn skipped_conflict_observation(
    context: &RowConflictContext<'_>,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    change: &TargetRowChange,
    conflict: crate::target::DuplicateConflict,
) -> ConflictObservation {
    let parent_recovery =
        build_exact_parent_recovery(context, coordinate, table, change, &conflict);
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
        source_primary_key: change
            .primary_key_values
            .iter()
            .map(value_to_conflict_key)
            .collect(),
        duplicate_index: conflict.duplicate_index,
        duplicate_owner_primary_key: None,
        error_code: conflict.error_code,
        parent_recovery,
        error_text: conflict.error_text,
        observed_at_ms: context.observed_at_ms,
    }
}

fn build_exact_parent_recovery(
    context: &RowConflictContext<'_>,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    change: &TargetRowChange,
    conflict: &crate::target::DuplicateConflict,
) -> Option<crate::live::ExactParentRecovery> {
    let values = change
        .writable_columns
        .iter()
        .zip(&change.source_values)
        .map(|(column, value)| (column.as_str(), value_to_conflict_key(value)))
        .collect::<std::collections::BTreeMap<_, _>>();
    if is_exact_sessions_guest_conflict(table, conflict) {
        return Some(crate::live::ExactParentRecovery::SessionsGuest(
            crate::live::SessionsGuestRecovery {
                source_file: coordinate.file.clone(),
                source_start_position: coordinate.position,
                source_end_position: context.end_position,
                child_event_timestamp: context.child_event_timestamp,
                schema: table.schema.clone(),
                table: table.table.clone(),
                constraint: crate::live::SESSIONS_GUEST_CONSTRAINT.to_string(),
                session_id: values.get("session_id")?.clone(),
                guest_id: values.get("guest_id")?.clone(),
                guest_hash: values.get("guest_hash")?.clone(),
            },
        ));
    }
    if is_exact_home_feed_card_conflict(table, conflict) {
        return Some(crate::live::ExactParentRecovery::HomeFeedCard(
            crate::live::HomeFeedCardRecovery {
                source_file: coordinate.file.clone(),
                source_start_position: coordinate.position,
                source_end_position: context.end_position,
                child_event_timestamp: context.child_event_timestamp,
                schema: table.schema.clone(),
                table: table.table.clone(),
                constraint: crate::live::HOME_FEED_SLIDE_CONSTRAINT.to_string(),
                slide_id: values.get("id")?.clone(),
                card_id: values.get("card_id")?.clone(),
            },
        ));
    }
    None
}

fn is_exact_sessions_guest_conflict(
    table: &RowTableMap,
    conflict: &crate::target::DuplicateConflict,
) -> bool {
    let has_sessions_guest_scope = table.schema == crate::live::SESSIONS_GUEST_CHILD_SCHEMA
        && table.table == crate::live::SESSIONS_GUEST_CHILD_TABLE;
    let has_sessions_guest_foreign_key = conflict.error_code
        == crate::live::SESSIONS_GUEST_FK_ERROR_CODE
        && is_exact_sessions_guest_constraint_error(&conflict.error_text);

    has_sessions_guest_scope && has_sessions_guest_foreign_key
}

fn is_exact_sessions_guest_constraint_error(error_text: &str) -> bool {
    error_text.contains(crate::live::SESSIONS_GUEST_FK_SIGNATURE)
        && error_text.contains(crate::live::SESSIONS_GUEST_PARENT_REFERENCE)
}

fn is_exact_home_feed_card_conflict(
    table: &RowTableMap,
    conflict: &crate::target::DuplicateConflict,
) -> bool {
    let has_slide_scope = table.schema == crate::live::HOME_FEED_SLIDE_CHILD_SCHEMA
        && table.table == crate::live::HOME_FEED_SLIDE_CHILD_TABLE;
    let has_card_foreign_key = conflict.error_code == crate::live::HOME_FEED_SLIDE_FK_ERROR_CODE
        && conflict
            .error_text
            .contains(crate::live::HOME_FEED_SLIDE_FK_SIGNATURE)
        && conflict
            .error_text
            .contains(crate::live::HOME_FEED_SLIDE_PARENT_REFERENCE);

    has_slide_scope && has_card_foreign_key
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

#[cfg(test)]
mod tests {
    use super::{
        is_exact_releases_category_constraint_error, is_exact_releases_visibility_constraint_error,
        is_exact_sessions_guest_constraint_error,
    };

    #[test]
    fn accepts_only_exact_releases_category_constraint_identity() {
        let exact = "Cannot add or update a child row: a foreign key constraint fails (`globalcomix`.`releases`, CONSTRAINT `releases_ibfk_2` FOREIGN KEY (`comic_id`, `comic_category_id`) REFERENCES `comics` (`id`, `section_id`))";
        let wrong_constraint = exact.replace("releases_ibfk_2", "releases_ibfk_1");
        let wrong_child = exact.replace("`comic_category_id`", "`category_id`");
        let wrong_parent = exact.replace(
            "`comics` (`id`, `section_id`)",
            "`comics_archive` (`id`, `section_id`)",
        );

        assert!(is_exact_releases_category_constraint_error(exact));
        assert!(!is_exact_releases_category_constraint_error(
            &wrong_constraint
        ));
        assert!(!is_exact_releases_category_constraint_error(&wrong_child));
        assert!(!is_exact_releases_category_constraint_error(&wrong_parent));
    }

    #[test]
    fn accepts_only_exact_releases_visibility_constraint_identity() {
        let exact = "Cannot add or update a child row: a foreign key constraint fails (`globalcomix`.`releases`, CONSTRAINT `releases_ibfk_3` FOREIGN KEY (`comic_id`, `comic_is_visible`) REFERENCES `comics` (`id`, `is_visible`))";
        let wrong_constraint = exact.replace("releases_ibfk_3", "releases_ibfk_2");
        let wrong_child = exact.replace("`comic_is_visible`", "`is_visible`");
        let wrong_parent = exact.replace(
            "`comics` (`id`, `is_visible`)",
            "`comics` (`id`, `section_id`)",
        );

        assert!(is_exact_releases_visibility_constraint_error(exact));
        assert!(!is_exact_releases_visibility_constraint_error(
            &wrong_constraint
        ));
        assert!(!is_exact_releases_visibility_constraint_error(&wrong_child));
        assert!(!is_exact_releases_visibility_constraint_error(
            &wrong_parent
        ));
    }

    #[test]
    fn accepts_only_exact_sessions_guest_constraint_identity() {
        let exact = "Cannot add or update a child row: a foreign key constraint fails (`globalcomix`.`sessions`, CONSTRAINT `fk_sessions_guest` FOREIGN KEY (`guest_id`, `guest_hash`) REFERENCES `guests` (`guest_id`, `guest_hash`))";
        let suffix = "Cannot add or update a child row: a foreign key constraint fails (`globalcomix`.`sessions`, CONSTRAINT `archive_fk_sessions_guest` FOREIGN KEY (`guest_id`, `guest_hash`) REFERENCES `guests` (`guest_id`, `guest_hash`))";
        let archived_parent = "Cannot add or update a child row: a foreign key constraint fails (`globalcomix`.`sessions`, CONSTRAINT `fk_sessions_guest` FOREIGN KEY (`guest_id`, `guest_hash`) REFERENCES `guests_archive` (`guest_id`, `guest_hash`))";
        let alternate_columns = "Cannot add or update a child row: a foreign key constraint fails (`globalcomix`.`sessions`, CONSTRAINT `fk_sessions_guest` FOREIGN KEY (`guest_id`, `guest_hash`) REFERENCES `guests` (`archived_guest_id`, `guest_hash`))";

        assert!(is_exact_sessions_guest_constraint_error(exact));
        assert!(!is_exact_sessions_guest_constraint_error(suffix));
        assert!(!is_exact_sessions_guest_constraint_error(archived_parent));
        assert!(!is_exact_sessions_guest_constraint_error(alternate_columns));
    }
}
