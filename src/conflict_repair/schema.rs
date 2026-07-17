use super::model::*;
use crate::mysql_support::quote_identifier_path;
use mysql::Conn;
use mysql::prelude::Queryable;

pub(crate) type ConflictColumn = (String, String, String, String, String);
pub(crate) type ConflictKeyIndex = (String, u64, u64, String, Option<u64>);
pub(crate) type ConflictConstraint = (String, String);
pub(crate) type ConflictTriggerRow = (String, String, String, String, String, String, u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TriggerMetadata {
    pub(crate) name: String,
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) event: String,
    pub(crate) timing: String,
    pub(crate) body: String,
    pub(crate) action_order: u64,
}

pub(crate) fn trigger_metadata_from_sql_row(
    (name, schema, table, event, timing, body, action_order): ConflictTriggerRow,
) -> TriggerMetadata {
    TriggerMetadata {
        name,
        schema,
        table,
        event,
        timing,
        body,
        action_order,
    }
}

pub(crate) type ConflictIdentityDefinition = (String, String);
pub(crate) type ConflictIdentityRow = (
    String,
    String,
    u64,
    String,
    u64,
    String,
    String,
    String,
    String,
);

pub(crate) const CONFLICT_INSERT_GUARD_BODY: &str = "BEGIN IF NEW.status <> 'unresolved' OR NEW.attempt_count <> 1 OR NEW.repair_run_id IS NOT NULL OR NEW.resolution_evidence IS NOT NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'row conflicts may only be inserted unresolved'; END IF; END";
pub(crate) const CONFLICT_UPDATE_GUARD_BODY: &str = "BEGIN IF NOT (OLD.conflict_identity <=> NEW.conflict_identity) OR NOT (OLD.source_identity <=> NEW.source_identity) OR NOT (OLD.source_server_id <=> NEW.source_server_id) OR NOT (OLD.source_file <=> NEW.source_file) OR NOT (OLD.source_start_position <=> NEW.source_start_position) OR NOT (OLD.source_end_position <=> NEW.source_end_position) OR NOT (OLD.schema_name <=> NEW.schema_name) OR NOT (OLD.table_name <=> NEW.table_name) OR NOT (OLD.operation <=> NEW.operation) OR NOT (OLD.source_primary_key_json <=> NEW.source_primary_key_json) OR OLD.status = 'resolved' AND (NEW.status <> 'resolved' OR NOT (OLD.duplicate_index <=> NEW.duplicate_index) OR NOT (OLD.duplicate_owner_primary_key_json <=> NEW.duplicate_owner_primary_key_json) OR NOT (OLD.error_code <=> NEW.error_code) OR NOT (OLD.error_text <=> NEW.error_text) OR NOT (OLD.first_observed_at_ms <=> NEW.first_observed_at_ms) OR NOT (OLD.last_observed_at_ms <=> NEW.last_observed_at_ms) OR NOT (OLD.attempt_count <=> NEW.attempt_count) OR NOT (OLD.repair_run_id <=> NEW.repair_run_id) OR NOT (OLD.resolution_evidence <=> NEW.resolution_evidence)) OR OLD.status = 'unresolved' AND ((NEW.status = 'unresolved' AND (NEW.repair_run_id IS NOT NULL OR NEW.resolution_evidence IS NOT NULL OR NEW.attempt_count <> OLD.attempt_count + 1)) OR (NEW.status = 'resolved' AND (NEW.repair_run_id IS NULL OR NEW.repair_run_id = '' OR NEW.resolution_evidence IS NULL OR NEW.resolution_evidence = '' OR NOT (OLD.duplicate_index <=> NEW.duplicate_index) OR NOT (OLD.duplicate_owner_primary_key_json <=> NEW.duplicate_owner_primary_key_json) OR NOT (OLD.error_code <=> NEW.error_code) OR NOT (OLD.error_text <=> NEW.error_text) OR NOT (OLD.first_observed_at_ms <=> NEW.first_observed_at_ms) OR NOT (OLD.last_observed_at_ms <=> NEW.last_observed_at_ms) OR NOT (OLD.attempt_count <=> NEW.attempt_count)))) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'row conflict identity is immutable and status transition is not allowed'; END IF; END";

pub fn build_conflict_validation_sql(table: &str) -> String {
    format!(
        "SELECT conflict_identity,source_identity,source_server_id,source_file,source_start_position,source_end_position,schema_name,table_name,operation,source_primary_key_json,duplicate_index,duplicate_owner_primary_key_json,error_code,error_text,first_observed_at_ms,last_observed_at_ms,attempt_count,status,repair_run_id,resolution_evidence FROM {table} LIMIT 0"
    )
}

pub(crate) fn split_conflict_table(table: &str) -> Result<(&str, &str), String> {
    let (schema, table_name) = table
        .split_once('.')
        .ok_or_else(|| "conflict table must be schema-qualified".to_string())?;
    if schema.is_empty() || table_name.is_empty() || table_name.contains('.') {
        return Err(format!("invalid conflict table path: {table}"));
    }
    Ok((schema, table_name))
}

const EXPECTED_CONFLICT_COLUMNS: &[(&str, &str, &str, &str, &str)] = &[
    ("conflict_identity", "char(64)", "NO", "<null>", ""),
    ("source_identity", "varchar(255)", "NO", "<null>", ""),
    ("source_server_id", "bigint unsigned", "NO", "<null>", ""),
    ("source_file", "varchar(255)", "NO", "<null>", ""),
    (
        "source_start_position",
        "bigint unsigned",
        "NO",
        "<null>",
        "",
    ),
    ("source_end_position", "bigint unsigned", "NO", "<null>", ""),
    ("schema_name", "varchar(255)", "NO", "<null>", ""),
    ("table_name", "varchar(255)", "NO", "<null>", ""),
    ("operation", "varchar(16)", "NO", "<null>", ""),
    ("source_primary_key_json", "text", "NO", "<null>", ""),
    ("duplicate_index", "varchar(255)", "YES", "<null>", ""),
    (
        "duplicate_owner_primary_key_json",
        "text",
        "YES",
        "<null>",
        "",
    ),
    ("error_code", "int unsigned", "NO", "<null>", ""),
    ("error_text", "text", "NO", "<null>", ""),
    (
        "first_observed_at_ms",
        "bigint unsigned",
        "NO",
        "<null>",
        "",
    ),
    ("last_observed_at_ms", "bigint unsigned", "NO", "<null>", ""),
    ("attempt_count", "bigint unsigned", "NO", "1", ""),
    ("status", "varchar(16)", "NO", "<null>", ""),
    ("repair_run_id", "varchar(255)", "YES", "<null>", ""),
    ("resolution_evidence", "text", "YES", "<null>", ""),
];

pub(crate) fn expected_conflict_columns() -> Vec<ConflictColumn> {
    EXPECTED_CONFLICT_COLUMNS
        .iter()
        .map(|row| {
            (
                row.0.into(),
                row.1.into(),
                row.2.into(),
                row.3.into(),
                row.4.into(),
            )
        })
        .collect()
}

pub(crate) fn expected_conflict_keys() -> Vec<ConflictKeyIndex> {
    [("PRIMARY", 0, 1, "conflict_identity", None)]
        .into_iter()
        .map(|(name, non_unique, sequence, column, prefix)| {
            (name.into(), non_unique, sequence, column.into(), prefix)
        })
        .collect()
}

pub(crate) fn validate_conflict_identity_definition(
    definition: &ConflictIdentityDefinition,
) -> Result<(), String> {
    let expected = ("ascii".to_string(), "ascii_bin".to_string());
    if definition == &expected {
        Ok(())
    } else {
        Err(format!(
            "row conflict identity charset/collation mismatch: expected {expected:?}, found {definition:?}"
        ))
    }
}

pub(crate) fn validate_conflict_identity_row(row: &ConflictIdentityRow) -> Result<(), String> {
    let key = conflict_key_from_identity_row(row)?;
    validate_conflict_identity(&row.0, &key)
}

fn conflict_key_from_identity_row(row: &ConflictIdentityRow) -> Result<ConflictKey, String> {
    let operation = parse_conflict_operation(&row.7)?;
    let source_primary_key = serde_json::from_str(&row.8)
        .map_err(|error| format!("invalid stored source primary key JSON: {error}"))?;
    Ok(ConflictKey {
        source_identity: row.1.clone(),
        source_server_id: row.2,
        coordinate: ConflictCoordinate {
            file: row.3.clone(),
            start_position: row.4,
            end_position: 0,
        },
        schema: row.5.clone(),
        table: row.6.clone(),
        operation,
        source_primary_key,
    })
}

fn parse_conflict_operation(operation: &str) -> Result<ConflictOperation, String> {
    match operation {
        "insert" => Ok(ConflictOperation::Insert),
        "update" => Ok(ConflictOperation::Update),
        "delete" => Ok(ConflictOperation::Delete),
        _ => Err(format!(
            "unknown conflict operation in stored row: {operation}"
        )),
    }
}

pub(crate) fn validate_conflict_columns(columns: &[ConflictColumn]) -> Result<(), String> {
    let expected = expected_conflict_columns();
    if columns == expected {
        Ok(())
    } else {
        Err(format!(
            "row conflict column schema mismatch: expected {expected:?}, found {columns:?}"
        ))
    }
}

pub(crate) fn validate_conflict_keys(keys: &[ConflictKeyIndex]) -> Result<(), String> {
    let expected = expected_conflict_keys();
    if keys == expected {
        Ok(())
    } else {
        Err(format!(
            "row conflict primary key mismatch: expected {expected:?}, found {keys:?}"
        ))
    }
}

pub(crate) fn validate_conflict_constraints(
    constraints: &[ConflictConstraint],
) -> Result<(), String> {
    let expected = vec![
        ("CHECK".into(), "YES".into()),
        ("PRIMARY KEY".into(), "YES".into()),
    ];
    if constraints == expected {
        Ok(())
    } else {
        Err(format!(
            "row conflict constraint mismatch: expected {expected:?}, found {constraints:?}"
        ))
    }
}

pub(crate) fn normalize_conflict_sql(sql: &str) -> String {
    sql.replace('`', "")
        .replace("_utf8mb4", "")
        .replace("\\'", "'")
        .split_whitespace()
        .collect::<String>()
        .to_ascii_lowercase()
}

pub(crate) fn validate_conflict_status_checks(checks: &[String]) -> Result<(), String> {
    let expected = "statusin('unresolved','resolved')";
    if checks.iter().any(|check| {
        let normalized = normalize_conflict_sql(check);
        let normalized = normalized
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .unwrap_or(&normalized);
        normalized == expected
    }) && checks.len() == 1
    {
        Ok(())
    } else {
        Err(format!(
            "row conflict status check mismatch: found {checks:?}"
        ))
    }
}

const CONFLICT_TRIGGER_INVENTORY_PROCEDURE: &str = "cdc.row_conflicts_trigger_inventory";
#[cfg(test)]
const SHOW_CREATE_PROCEDURE_SQL_PREFIX: &str = "SHOW CREATE PROCEDURE";
#[cfg(test)]
const CONFLICT_TRIGGER_INVENTORY_BODY: &str = "BEGIN SELECT trigger_name,event_object_schema,event_object_table,event_manipulation,action_timing,action_statement,action_order FROM information_schema.triggers WHERE event_object_schema = 'cdc' AND event_object_table = 'row_conflicts' ORDER BY event_manipulation, action_order; END";

pub(crate) fn conflict_trigger_inventory_routine_path(table: &str) -> Result<String, String> {
    if table != "cdc.row_conflicts" {
        return Err(format!(
            "row conflict inventory requires the exact table/procedure contract {CONFLICT_TRIGGER_INVENTORY_PROCEDURE}, found {table}"
        ));
    }
    Ok(quote_identifier_path(CONFLICT_TRIGGER_INVENTORY_PROCEDURE))
}

pub(crate) fn query_conflict_trigger_inventory(
    conn: &mut Conn,
    table: &str,
) -> Result<Vec<TriggerMetadata>, String> {
    let routine = conflict_trigger_inventory_routine_path(table)?;
    let rows = conn
        .query_opt::<ConflictTriggerRow, _>(format!("CALL {routine}()"))
        .map_err(|error| format!("row conflict trigger inventory failed: {error}"))?;
    rows.into_iter()
        .map(|row| {
            row.map(trigger_metadata_from_sql_row).map_err(|error| {
                format!("row conflict trigger inventory returned malformed metadata: {error:?}")
            })
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn validate_conflict_trigger_inventory_routine_definition(
    expected_schema: &str,
    expected_table: &str,
    show_create: &str,
) -> Result<(), String> {
    let normalized = normalize_conflict_sql(show_create);
    let required_body = normalize_conflict_sql(CONFLICT_TRIGGER_INVENTORY_BODY);
    let expected_procedure = normalize_conflict_sql(&format!(
        "procedure {expected_schema}.{expected_table}_trigger_inventory()"
    ));
    if !normalized.contains("createdefiner=")
        || !normalized.contains("sqlsecuritydefiner")
        || !normalized.contains("readssqldata")
        || !normalized.contains(&expected_procedure)
        || !normalized.ends_with(&required_body)
        || normalized.contains("sqlsecurityinvoker")
    {
        return Err(format!(
            "{SHOW_CREATE_PROCEDURE_SQL_PREFIX}: row conflict inventory routine is not an exact definer-safe reader"
        ));
    }
    Ok(())
}

pub(crate) fn validate_conflict_triggers(
    schema: &str,
    table: &str,
    triggers: &[TriggerMetadata],
) -> Result<(), String> {
    let expected = expected_conflict_triggers(schema, table);
    let actual = normalize_trigger_metadata(triggers);
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "row conflict guard inventory mismatch: expected {expected:?}, found {actual:?}"
        ))
    }
}

fn expected_conflict_triggers(schema: &str, table: &str) -> Vec<TriggerMetadata> {
    vec![
        TriggerMetadata {
            name: "row_conflicts_insert_guard".into(),
            schema: schema.into(),
            table: table.into(),
            event: "INSERT".into(),
            timing: "BEFORE".into(),
            body: normalize_conflict_sql(CONFLICT_INSERT_GUARD_BODY),
            action_order: 1,
        },
        TriggerMetadata {
            name: "row_conflicts_update_guard".into(),
            schema: schema.into(),
            table: table.into(),
            event: "UPDATE".into(),
            timing: "BEFORE".into(),
            body: normalize_conflict_sql(CONFLICT_UPDATE_GUARD_BODY),
            action_order: 1,
        },
    ]
}

fn normalize_trigger_metadata(triggers: &[TriggerMetadata]) -> Vec<TriggerMetadata> {
    triggers
        .iter()
        .map(|trigger| TriggerMetadata {
            body: normalize_conflict_sql(&trigger.body),
            ..trigger.clone()
        })
        .collect()
}

pub(crate) fn conflict_mysql_error(error: mysql::Error) -> String {
    format!("conflict store mysql query failed: {error}")
}

pub(crate) fn conflict_validation_error(error: String) -> String {
    format!("conflict store validation failed: {error}")
}
