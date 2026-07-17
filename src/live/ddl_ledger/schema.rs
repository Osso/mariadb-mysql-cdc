use super::{
    MONOTONIC_RESOLUTION_TRIGGER_BODY, PENDING_ONLY_TRIGGER_BODY,
    build_ddl_trigger_inventory_call_sql, ddl_ledger_mysql_error,
    ddl_trigger_inventory_routine_path,
};
use crate::mysql_support::quote_sql_literal;
use mysql::Conn;
use mysql::prelude::{FromRow, Queryable};

pub(super) type DdlLedgerColumn = (String, String, String, String, String);
pub(super) type TriggerMetadata = (String, String, String, String, String, String, u64);
pub(super) type TriggerShape = (String, String, u64);

pub(super) fn query_rows<T: FromRow>(conn: &mut Conn, sql: String) -> Result<Vec<T>, String> {
    conn.query(sql).map_err(ddl_ledger_mysql_error)
}

pub(super) fn query_ddl_ledger_columns(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<DdlLedgerColumn>, String> {
    query_rows(
        conn,
        format!(
            "SELECT column_name,LOWER(column_type),is_nullable,LOWER(COALESCE(CAST(column_default AS CHAR),'<null>')),LOWER(extra) FROM information_schema.columns WHERE table_schema={} AND table_name={} ORDER BY ordinal_position",
            quote_sql_literal(schema),
            quote_sql_literal(table),
        ),
    )
}

pub(super) fn query_ddl_ledger_primary_key(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, String> {
    query_rows(
        conn,
        format!(
            "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema={} AND table_name={} AND constraint_name='PRIMARY' ORDER BY ordinal_position",
            quote_sql_literal(schema),
            quote_sql_literal(table),
        ),
    )
}

pub(super) fn query_ddl_ledger_constraints(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<(String, String)>, String> {
    query_rows(
        conn,
        format!(
            "SELECT constraint_type,enforced FROM information_schema.table_constraints WHERE table_schema={} AND table_name={} ORDER BY constraint_type,constraint_name",
            quote_sql_literal(schema),
            quote_sql_literal(table),
        ),
    )
}

pub(super) fn query_ddl_status_checks(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, String> {
    query_rows(
        conn,
        format!(
            "SELECT cc.check_clause FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name WHERE tc.table_schema={} AND tc.table_name={} AND tc.constraint_type='CHECK'",
            quote_sql_literal(schema),
            quote_sql_literal(table),
        ),
    )
}

pub(super) fn query_ddl_trigger_inventory(
    conn: &mut Conn,
    table: &str,
) -> Result<Vec<TriggerMetadata>, String> {
    let rows = conn
        .query_opt::<TriggerMetadata, _>(build_ddl_trigger_inventory_call_sql(table))
        .map_err(|error| {
            format!(
                "DDL ledger trigger inventory routine {} failed: {error}",
                ddl_trigger_inventory_routine_path(table),
            )
        })?;
    rows.into_iter()
        .map(|row| {
            row.map_err(|error| {
                format!(
                    "DDL ledger trigger inventory routine {} returned malformed metadata: {error:?}",
                    ddl_trigger_inventory_routine_path(table),
                )
            })
        })
        .collect()
}

const EXPECTED_DDL_LEDGER_COLUMNS: [(&str, &str, &str, &str, &str); 11] = [
    ("source_identity", "varchar(384)", "NO", "<null>", ""),
    ("source_server_id", "int unsigned", "NO", "<null>", ""),
    ("binlog_file", "varchar(255)", "NO", "<null>", ""),
    (
        "event_start_position",
        "bigint unsigned",
        "NO",
        "<null>",
        "",
    ),
    ("event_end_position", "bigint unsigned", "NO", "<null>", ""),
    ("schema_name", "varchar(255)", "NO", "<null>", ""),
    ("raw_sql", "longtext", "NO", "<null>", ""),
    ("status", "varchar(32)", "NO", "<null>", ""),
    ("resolution_note", "text", "YES", "<null>", ""),
    (
        "created_at",
        "timestamp",
        "NO",
        "current_timestamp",
        "default_generated",
    ),
    (
        "updated_at",
        "timestamp",
        "NO",
        "current_timestamp",
        "default_generated on update current_timestamp",
    ),
];

pub(super) fn expected_ddl_ledger_columns() -> Vec<DdlLedgerColumn> {
    EXPECTED_DDL_LEDGER_COLUMNS
        .into_iter()
        .map(|(name, column_type, nullable, default_value, extra)| {
            (
                name.to_string(),
                column_type.to_string(),
                nullable.to_string(),
                default_value.to_string(),
                extra.to_string(),
            )
        })
        .collect()
}

pub(super) fn validate_ddl_ledger_columns(columns: &[DdlLedgerColumn]) -> Result<(), String> {
    let expected = expected_ddl_ledger_columns();
    if columns == expected {
        return Ok(());
    }
    Err(format!(
        "DDL ledger column schema mismatch: expected {expected:?}, found {columns:?}"
    ))
}

pub(super) fn validate_ddl_ledger_primary_key(columns: &[String]) -> Result<(), String> {
    let expected = ["source_identity", "binlog_file", "event_start_position"];
    if columns.iter().map(String::as_str).eq(expected) {
        return Ok(());
    }
    Err(format!(
        "DDL ledger primary key mismatch: expected {expected:?}, found {columns:?}"
    ))
}

pub(super) fn pending_only_trigger_name(table_name: &str) -> String {
    format!("{table_name}_pending_insert_guard")
}

pub(super) fn monotonic_resolution_trigger_name(table_name: &str) -> String {
    format!("{table_name}_monotonic_resolution_guard")
}

pub(super) fn validate_trigger_inventory_metadata(
    expected_schema: &str,
    expected_table: &str,
    rows: &[TriggerMetadata],
) -> Result<(Vec<TriggerShape>, Vec<TriggerShape>), String> {
    let validated = rows
        .iter()
        .map(|row| {
            validate_trigger_metadata_row(expected_schema, expected_table, row)?;
            Ok((row.3.clone(), trigger_shape(row)))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let (insert_triggers, update_triggers) = validated
        .into_iter()
        .partition::<Vec<_>, _>(|(event_manipulation, _)| event_manipulation == "INSERT");
    let insert_triggers = insert_triggers
        .into_iter()
        .map(|(_, trigger)| trigger)
        .collect();
    let update_triggers = update_triggers
        .into_iter()
        .map(|(_, trigger)| trigger)
        .collect();
    Ok((insert_triggers, update_triggers))
}

fn trigger_shape(row: &TriggerMetadata) -> TriggerShape {
    let (trigger_name, _, _, _, _, action_statement, action_order) = row;
    (
        trigger_name.clone(),
        action_statement.clone(),
        *action_order,
    )
}

fn validate_trigger_metadata_row(
    expected_schema: &str,
    expected_table: &str,
    row: &TriggerMetadata,
) -> Result<(), String> {
    validate_trigger_target(expected_schema, expected_table, row)?;
    validate_trigger_definition(&row.0, &row.3, &row.4)
}

fn validate_trigger_target(
    expected_schema: &str,
    expected_table: &str,
    row: &TriggerMetadata,
) -> Result<(), String> {
    let (_, trigger_schema, trigger_table, _, _, _, _) = row;
    if trigger_schema == expected_schema && trigger_table == expected_table {
        return Ok(());
    }
    Err(format!(
        "DDL ledger trigger metadata target mismatch: expected {expected_schema}.{expected_table}, found {trigger_schema}.{trigger_table}"
    ))
}

fn validate_trigger_definition(
    trigger_name: &str,
    event_manipulation: &str,
    action_timing: &str,
) -> Result<(), String> {
    if action_timing != "BEFORE" {
        return Err(format!(
            "DDL ledger trigger timing mismatch for {trigger_name}: expected BEFORE, found {action_timing}"
        ));
    }
    if matches!(event_manipulation, "INSERT" | "UPDATE") {
        return Ok(());
    }
    Err(format!(
        "DDL ledger trigger event mismatch for {trigger_name}: unexpected {event_manipulation}"
    ))
}

pub(super) fn validate_ddl_constraints(constraints: &[(String, String)]) -> Result<(), String> {
    let expected = [
        ("CHECK".to_string(), "YES".to_string()),
        ("PRIMARY KEY".to_string(), "YES".to_string()),
    ];
    if constraints == expected {
        return Ok(());
    }
    Err(format!(
        "DDL ledger constraint inventory mismatch: expected {expected:?}, found {constraints:?}"
    ))
}

fn normalize_sql_guard(sql: &str) -> String {
    sql.replace('`', "")
        .replace("_utf8mb4", "")
        .replace("\\'", "'")
        .split_whitespace()
        .collect::<String>()
        .to_ascii_lowercase()
}

pub(super) fn validate_ddl_status_checks(checks: &[String]) -> Result<(), String> {
    let expected = "statusin('pending','resolved')";
    let matches = checks.iter().any(|check| {
        let normalized = normalize_sql_guard(check);
        let without_outer_group = normalized
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .unwrap_or(&normalized);
        without_outer_group == expected
    });
    if matches {
        return Ok(());
    }
    Err(format!(
        "DDL ledger status check mismatch: expected `{expected}`, found {checks:?}"
    ))
}

pub(super) fn validate_pending_only_trigger(statement: &str) -> Result<(), String> {
    if normalize_sql_guard(statement) == normalize_sql_guard(PENDING_ONLY_TRIGGER_BODY) {
        return Ok(());
    }
    Err("DDL ledger INSERT trigger does not exactly enforce pending-only rows".to_string())
}

pub(super) fn validate_pending_trigger_inventory(
    expected_name: &str,
    triggers: &[(String, String, u64)],
) -> Result<(), String> {
    let [(name, statement, action_order)] = triggers else {
        return Err(format!(
            "DDL ledger must have exactly one BEFORE INSERT trigger, found {}",
            triggers.len()
        ));
    };
    if name != expected_name || *action_order != 1 {
        return Err(format!(
            "DDL ledger trigger identity/order mismatch: expected {expected_name} at order 1, found {name} at order {action_order}"
        ));
    }
    validate_pending_only_trigger(statement)
}

pub(super) fn validate_resolution_trigger_inventory(
    expected_name: &str,
    triggers: &[(String, String, u64)],
) -> Result<(), String> {
    let [(name, statement, action_order)] = triggers else {
        return Err(format!(
            "DDL ledger must have exactly one BEFORE UPDATE trigger, found {}",
            triggers.len()
        ));
    };
    if name != expected_name || *action_order != 1 {
        return Err(format!(
            "DDL ledger resolution trigger identity/order mismatch: expected {expected_name} at order 1, found {name} at order {action_order}"
        ));
    }
    if normalize_sql_guard(statement) == normalize_sql_guard(MONOTONIC_RESOLUTION_TRIGGER_BODY) {
        return Ok(());
    }
    Err(
        "DDL ledger UPDATE trigger does not exactly enforce immutable one-way resolution"
            .to_string(),
    )
}
