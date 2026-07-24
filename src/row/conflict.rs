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
    let deferred = is_deferred_superseded_insert(table, operation, change, &conflict);
    let observation =
        skipped_conflict_observation(context, coordinate, table, operation, change, conflict);
    if deferred {
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
    conflict: &crate::target::DuplicateConflict,
) -> bool {
    if operation != RowOperation::Insert
        || change.kind != crate::target::TargetRowChangeKind::Insert
    {
        return false;
    }
    let users_name = table.schema == "globalcomix"
        && table.table == "users"
        && conflict.duplicate_index.as_deref() == Some("users.name");
    let comics_slug = table.schema == "globalcomix"
        && table.table == "comics"
        && conflict.duplicate_index.as_deref() == Some("comics.slug");
    let releases_category = table.schema == "globalcomix"
        && table.table == "releases"
        && conflict.error_code == MISSING_PARENT_FK_ERROR_CODE
        && is_exact_releases_category_constraint_error(&conflict.error_text);
    users_name || comics_slug || releases_category
}

fn is_exact_releases_category_constraint_error(error_text: &str) -> bool {
    error_text.contains("`globalcomix`.`releases`, CONSTRAINT `releases_ibfk_2`")
        && error_text.contains("FOREIGN KEY (`comic_id`, `comic_category_id`)")
        && error_text.contains("REFERENCES `comics` (`id`, `section_id`)")
}

fn skipped_conflict_observation(
    context: &RowConflictContext<'_>,
    coordinate: &BinlogCoordinate,
    table: &RowTableMap,
    operation: RowOperation,
    change: &TargetRowChange,
    conflict: crate::target::DuplicateConflict,
) -> ConflictObservation {
    let parent_recovery = build_exact_parent_recovery(
        ChildEventPosition {
            coordinate,
            end_position: context.end_position,
            timestamp: context.child_event_timestamp,
        },
        table,
        operation,
        change,
        &conflict,
    );
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

/// Where the conflicting child event sits in the source binlog.
#[derive(Clone, Copy)]
struct ChildEventPosition<'a> {
    coordinate: &'a BinlogCoordinate,
    end_position: u64,
    timestamp: u64,
}

/// MySQL's "cannot add or update a child row: a foreign key constraint fails".
const MISSING_PARENT_FK_ERROR_CODE: u16 = 1452;

fn build_exact_parent_recovery(
    position: ChildEventPosition<'_>,
    table: &RowTableMap,
    operation: RowOperation,
    change: &TargetRowChange,
    conflict: &crate::target::DuplicateConflict,
) -> Option<crate::live::ExactParentRecovery> {
    let coordinate = position.coordinate;
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
                source_end_position: position.end_position,
                child_event_timestamp: position.timestamp,
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
                source_end_position: position.end_position,
                child_event_timestamp: position.timestamp,
                schema: table.schema.clone(),
                table: table.table.clone(),
                constraint: crate::live::HOME_FEED_SLIDE_CONSTRAINT.to_string(),
                slide_id: values.get("id")?.clone(),
                card_id: values.get("card_id")?.clone(),
            },
        ));
    }
    // A deferred conflict belongs to superseded-parent verification, which resolves it inside the
    // transaction. Claiming it here as a missing parent would attach a recovery request to a failed
    // verification, and that retries without consuming the transport budget instead of stalling.
    if is_deferred_superseded_insert(table, operation, change, conflict) {
        return None;
    }
    build_missing_parent_recovery(position, table, change, conflict)
        .map(crate::live::ExactParentRecovery::MissingParent)
}

/// Builds a recovery request for any other `1452` from the identity MySQL names in the error.
///
/// Every unproven case yields `None`, which keeps the ordinary durable-abort path.
fn build_missing_parent_recovery(
    position: ChildEventPosition<'_>,
    table: &RowTableMap,
    change: &TargetRowChange,
    conflict: &crate::target::DuplicateConflict,
) -> Option<crate::live::MissingParentRecovery> {
    if conflict.error_code != MISSING_PARENT_FK_ERROR_CODE {
        return None;
    }
    let crate::live::ForeignKeyViolation {
        child_schema,
        child_table,
        constraint,
        child_columns,
        parent_schema,
        parent_table,
        parent_columns,
    } = crate::live::parse_foreign_key_violation(&conflict.error_text)?;
    // The error must describe the table being applied, or the values read below are another table's.
    if child_schema != table.schema || child_table != table.table {
        return None;
    }
    let child_foreign_key_values = child_foreign_key_values(change, &child_columns)?;
    Some(crate::live::MissingParentRecovery {
        source_file: position.coordinate.file.clone(),
        source_start_position: position.coordinate.position,
        source_end_position: position.end_position,
        child_event_timestamp: position.timestamp,
        schema: table.schema.clone(),
        table: table.table.clone(),
        constraint,
        child_primary_key: change
            .primary_key_values
            .iter()
            .map(value_to_conflict_key)
            .collect(),
        child_columns,
        child_foreign_key_values,
        parent_schema: parent_schema.unwrap_or(child_schema),
        parent_table,
        parent_columns,
    })
}

/// Reads the child's foreign-key values from the source image, keeping SQL NULL distinct from the
/// literal `"NULL"` that `value_to_conflict_key` produces. Yields `None` when the image does not
/// carry a referenced column, since the parent identity is then unknown.
fn child_foreign_key_values(
    change: &TargetRowChange,
    columns: &[String],
) -> Option<Vec<Option<String>>> {
    columns
        .iter()
        .map(|column| {
            let index = change
                .writable_columns
                .iter()
                .position(|candidate| candidate == column)?;
            match change.source_values.get(index)? {
                Value::NULL => Some(None),
                value => Some(Some(value_to_conflict_key(value))),
            }
        })
        .collect()
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
        ChildEventPosition, RowOperation, RowTableMap, build_exact_parent_recovery,
        build_missing_parent_recovery, is_exact_releases_category_constraint_error,
        is_exact_sessions_guest_constraint_error,
    };
    use crate::probe::BinlogCoordinate;
    use crate::target::{DuplicateConflict, SqlStatement, TargetRowChange, TargetRowChangeKind};
    use mysql::Value;
    use std::collections::BTreeMap;

    /// The constraint that stalled the production stream twice on 2026-07-24.
    const PAID_SUBSCRIPTIONS_SESSION_ERROR: &str = "target mysql query failed: MySqlError { ERROR 1452 (23000): Cannot add or update a child \
         row: a foreign key constraint fails (`globalcomix`.`paid_subscriptions_users_pages`, \
         CONSTRAINT `fk_paid_subscriptions_users_pages_session_id` FOREIGN KEY (`session_id`) \
         REFERENCES `sessions` (`session_id`)) }";

    const CHILD_EVENT_TIMESTAMP: u64 = 1_753_300_000;
    const CHILD_END_POSITION: u64 = 1_024_916_500;

    fn coordinate() -> BinlogCoordinate {
        BinlogCoordinate {
            file: "mysqld-bin.002710".to_string(),
            position: 1_024_916_259,
        }
    }

    fn child_event_position(coordinate: &BinlogCoordinate) -> ChildEventPosition<'_> {
        ChildEventPosition {
            coordinate,
            end_position: CHILD_END_POSITION,
            timestamp: CHILD_EVENT_TIMESTAMP,
        }
    }

    fn table_map(schema: &str, table: &str, columns: &[&str]) -> RowTableMap {
        RowTableMap {
            table_id: 11,
            schema: schema.to_string(),
            table: table.to_string(),
            columns: columns.iter().map(|column| column.to_string()).collect(),
            primary_key: vec!["id".to_string()],
            generated_columns: Vec::new(),
            signed_columns: Vec::new(),
            enum_columns: BTreeMap::new(),
            set_columns: BTreeMap::new(),
        }
    }

    fn insert_change(image: &[(&str, Value)]) -> TargetRowChange {
        TargetRowChange {
            statement: SqlStatement {
                sql: "INSERT INTO t VALUES (?)".to_string(),
                params: Vec::new(),
            },
            kind: TargetRowChangeKind::Insert,
            table: "t".to_string(),
            primary_key_columns: vec!["id".to_string()],
            primary_key_values: vec![Value::Int(7)],
            writable_columns: image
                .iter()
                .map(|(column, _)| (*column).to_string())
                .collect(),
            source_values: image.iter().map(|(_, value)| value.clone()).collect(),
            set_columns: vec![None; image.len()],
        }
    }

    fn foreign_key_conflict(error_text: &str) -> DuplicateConflict {
        DuplicateConflict {
            error_code: 1452,
            error_text: error_text.to_string(),
            duplicate_index: None,
        }
    }

    #[test]
    fn builds_generic_recovery_for_the_paid_subscriptions_session_constraint() {
        let coordinate = coordinate();
        let table = table_map(
            "globalcomix",
            "paid_subscriptions_users_pages",
            &["id", "session_id"],
        );
        let change = insert_change(&[
            ("id", Value::Int(7)),
            ("session_id", Value::Bytes(b"abc123".to_vec())),
        ]);

        let recovery = build_missing_parent_recovery(
            child_event_position(&coordinate),
            &table,
            &change,
            &foreign_key_conflict(PAID_SUBSCRIPTIONS_SESSION_ERROR),
        )
        .expect("generic missing parent recovery");

        assert_eq!(
            recovery,
            crate::live::MissingParentRecovery {
                source_file: "mysqld-bin.002710".to_string(),
                source_start_position: 1_024_916_259,
                source_end_position: CHILD_END_POSITION,
                child_event_timestamp: CHILD_EVENT_TIMESTAMP,
                schema: "globalcomix".to_string(),
                table: "paid_subscriptions_users_pages".to_string(),
                constraint: "fk_paid_subscriptions_users_pages_session_id".to_string(),
                child_primary_key: vec!["7".to_string()],
                child_columns: vec!["session_id".to_string()],
                child_foreign_key_values: vec![Some("abc123".to_string())],
                parent_schema: "globalcomix".to_string(),
                parent_table: "sessions".to_string(),
                parent_columns: vec!["session_id".to_string()],
            }
        );
    }

    /// The image order does not have to match the order MySQL named the columns, and the planner
    /// aligns values with `parent_columns` positionally, so the order must come from the error.
    #[test]
    fn orders_multi_column_values_by_the_error_not_the_image() {
        let coordinate = coordinate();
        let table = table_map("globalcomix", "comics", &["id", "artist_name", "artist_id"]);
        let change = insert_change(&[
            ("id", Value::Int(7)),
            ("artist_name", Value::Bytes(b"Ada".to_vec())),
            ("artist_id", Value::Int(42)),
        ]);
        let error = "Cannot add or update a child row: a foreign key constraint fails \
                     (`globalcomix`.`comics`, CONSTRAINT `comics_ibfk_5` FOREIGN KEY \
                     (`artist_id`, `artist_name`) REFERENCES `artists` (`id`, `name`) ON DELETE \
                     RESTRICT ON UPDATE CASCADE)";

        let recovery = build_missing_parent_recovery(
            child_event_position(&coordinate),
            &table,
            &change,
            &foreign_key_conflict(error),
        )
        .expect("generic missing parent recovery");

        assert_eq!(
            recovery.child_columns,
            vec!["artist_id".to_string(), "artist_name".to_string()]
        );
        assert_eq!(
            recovery.child_foreign_key_values,
            vec![Some("42".to_string()), Some("Ada".to_string())]
        );
        assert_eq!(
            recovery.parent_columns,
            vec!["id".to_string(), "name".to_string()]
        );
    }

    /// `value_to_conflict_key` renders NULL as the literal `"NULL"`, which would look like a real
    /// value to the planner. Detection must keep it distinguishable so the planner can reject it.
    #[test]
    fn keeps_a_null_foreign_key_value_distinct_from_the_literal_null() {
        let coordinate = coordinate();
        let table = table_map(
            "globalcomix",
            "paid_subscriptions_users_pages",
            &["id", "session_id"],
        );
        let change = insert_change(&[("id", Value::Int(7)), ("session_id", Value::NULL)]);

        let recovery = build_missing_parent_recovery(
            child_event_position(&coordinate),
            &table,
            &change,
            &foreign_key_conflict(PAID_SUBSCRIPTIONS_SESSION_ERROR),
        )
        .expect("generic missing parent recovery");

        assert_eq!(recovery.child_foreign_key_values, vec![None]);
    }

    #[test]
    fn resolves_a_qualified_parent_schema_from_the_error() {
        let coordinate = coordinate();
        let table = table_map("globalcomix", "orders", &["id", "user_id"]);
        let change = insert_change(&[("id", Value::Int(7)), ("user_id", Value::Int(3))]);
        let error = "Cannot add or update a child row: a foreign key constraint fails \
                     (`globalcomix`.`orders`, CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`user_id`) \
                     REFERENCES `other`.`users` (`id`))";

        let recovery = build_missing_parent_recovery(
            child_event_position(&coordinate),
            &table,
            &change,
            &foreign_key_conflict(error),
        )
        .expect("generic missing parent recovery");

        assert_eq!(recovery.parent_schema, "other");
        assert_eq!(recovery.parent_table, "users");
    }

    #[test]
    fn declines_when_the_error_names_another_table() {
        let coordinate = coordinate();
        let table = table_map("globalcomix", "sessions", &["id", "session_id"]);
        let change = insert_change(&[
            ("id", Value::Int(7)),
            ("session_id", Value::Bytes(b"abc123".to_vec())),
        ]);

        assert!(
            build_missing_parent_recovery(
                child_event_position(&coordinate),
                &table,
                &change,
                &foreign_key_conflict(PAID_SUBSCRIPTIONS_SESSION_ERROR),
            )
            .is_none()
        );
    }

    #[test]
    fn declines_when_the_image_omits_the_referenced_column() {
        let coordinate = coordinate();
        let table = table_map(
            "globalcomix",
            "paid_subscriptions_users_pages",
            &["id", "session_id"],
        );
        let change = insert_change(&[("id", Value::Int(7))]);

        assert!(
            build_missing_parent_recovery(
                child_event_position(&coordinate),
                &table,
                &change,
                &foreign_key_conflict(PAID_SUBSCRIPTIONS_SESSION_ERROR),
            )
            .is_none()
        );
    }

    #[test]
    fn declines_a_duplicate_key_error() {
        let coordinate = coordinate();
        let table = table_map("globalcomix", "guests", &["id", "guest_hash"]);
        let change = insert_change(&[
            ("id", Value::Int(7)),
            ("guest_hash", Value::Bytes(b"abc".to_vec())),
        ]);
        let conflict = DuplicateConflict {
            error_code: 1062,
            error_text: "Duplicate entry 'abc' for key 'guests.idx_guest_hash'".to_string(),
            duplicate_index: Some("guests.idx_guest_hash".to_string()),
        };

        assert!(
            build_missing_parent_recovery(
                child_event_position(&coordinate),
                &table,
                &change,
                &conflict,
            )
            .is_none()
        );
    }

    /// `releases_ibfk_2` is resolved by superseded-parent verification inside the transaction.
    /// Attaching a missing-parent request to it would retry recovery instead of letting that run.
    #[test]
    fn declines_the_deferred_releases_category_constraint() {
        let coordinate = coordinate();
        let table = table_map(
            "globalcomix",
            "releases",
            &["id", "comic_id", "comic_category_id"],
        );
        let change = insert_change(&[
            ("id", Value::Int(7)),
            ("comic_id", Value::Int(4)),
            ("comic_category_id", Value::Int(9)),
        ]);
        let error = "Cannot add or update a child row: a foreign key constraint fails \
                     (`globalcomix`.`releases`, CONSTRAINT `releases_ibfk_2` FOREIGN KEY \
                     (`comic_id`, `comic_category_id`) REFERENCES `comics` (`id`, `section_id`))";

        assert!(
            build_exact_parent_recovery(
                child_event_position(&coordinate),
                &table,
                RowOperation::Insert,
                &change,
                &foreign_key_conflict(error),
            )
            .is_none()
        );
    }

    /// The proven single-constraint path must keep winning over the generic one.
    #[test]
    fn prefers_the_hardcoded_sessions_guest_recovery() {
        let coordinate = coordinate();
        let table = table_map(
            "globalcomix",
            "sessions",
            &["session_id", "guest_id", "guest_hash"],
        );
        let change = insert_change(&[
            ("session_id", Value::Bytes(b"s1".to_vec())),
            ("guest_id", Value::Int(5)),
            ("guest_hash", Value::Bytes(b"h1".to_vec())),
        ]);
        let error = "Cannot add or update a child row: a foreign key constraint fails \
                     (`globalcomix`.`sessions`, CONSTRAINT `fk_sessions_guest` FOREIGN KEY \
                     (`guest_id`, `guest_hash`) REFERENCES `guests` (`guest_id`, `guest_hash`))";

        let recovery = build_exact_parent_recovery(
            child_event_position(&coordinate),
            &table,
            RowOperation::Insert,
            &change,
            &foreign_key_conflict(error),
        )
        .expect("sessions guest recovery");

        assert!(matches!(
            recovery,
            crate::live::ExactParentRecovery::SessionsGuest(_)
        ));
    }

    /// Any other constraint now reaches the generic path instead of stalling.
    #[test]
    fn routes_an_unenumerated_constraint_to_generic_recovery() {
        let coordinate = coordinate();
        let table = table_map(
            "globalcomix",
            "paid_subscriptions_users_pages",
            &["id", "session_id"],
        );
        let change = insert_change(&[
            ("id", Value::Int(7)),
            ("session_id", Value::Bytes(b"abc123".to_vec())),
        ]);

        let recovery = build_exact_parent_recovery(
            child_event_position(&coordinate),
            &table,
            RowOperation::Insert,
            &change,
            &foreign_key_conflict(PAID_SUBSCRIPTIONS_SESSION_ERROR),
        )
        .expect("generic missing parent recovery");

        assert!(matches!(
            recovery,
            crate::live::ExactParentRecovery::MissingParent(_)
        ));
    }

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
