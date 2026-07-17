use super::grants::validate_runtime_grants;
use super::mysql_error;
use crate::mysql_support::{quote_ident, quote_sql_literal};
use mysql::Conn;
use mysql::prelude::Queryable;

pub(crate) type JournalColumn = (String, String, String, String, String);
pub(crate) type JournalKey = (String, u8, u64, String);
pub(crate) type JournalConstraint = (String, String);
pub(crate) type JournalTriggerMetadata = (String, String, String, String, String, String, u64);

type ColumnSpec = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);

pub(crate) const INSERT_GUARD_BODY: &str = "BEGIN IF NOT ((NEW.status = 'translation_pending' AND NEW.transformation_version = 'translator-unavailable' AND NEW.generated_sql IS NULL AND NEW.canonical_ast = '' AND NEW.pre_state = '' AND NEW.expected_post_state = '') OR (NEW.status = 'prepared' AND NEW.transformation_version <> '' AND NEW.canonical_ast <> '' AND NEW.pre_state <> '' AND NEW.expected_post_state <> '')) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'automatic DDL journal rows must begin translation_pending or prepared with valid evidence'; END IF; END";
#[cfg(test)]
pub(crate) const JOURNAL_PENDING_INSERT_TRIGGER_BODY: &str = INSERT_GUARD_BODY;
#[cfg(test)]
pub(crate) const JOURNAL_MONOTONIC_UPDATE_TRIGGER_BODY: &str = UPDATE_GUARD_BODY;
const UPDATE_GUARD_BODY: &str = "BEGIN IF NOT (OLD.source_identity <=> NEW.source_identity) OR NOT (OLD.source_server_id <=> NEW.source_server_id) OR NOT (OLD.binlog_file <=> NEW.binlog_file) OR NOT (OLD.event_start_position <=> NEW.event_start_position) OR NOT (OLD.event_end_position <=> NEW.event_end_position) OR NOT (OLD.schema_name <=> NEW.schema_name) OR NOT (OLD.raw_sql <=> NEW.raw_sql) OR NOT ((OLD.status = 'translation_pending' AND NEW.status = 'prepared' AND OLD.transformation_version = 'translator-unavailable' AND OLD.generated_sql IS NULL AND OLD.canonical_ast = '' AND OLD.pre_state = '' AND OLD.expected_post_state = '' AND NEW.transformation_version <> '' AND NEW.canonical_ast <> '' AND NEW.pre_state <> '' AND NEW.expected_post_state <> '') OR ((OLD.transformation_version <=> NEW.transformation_version) AND (OLD.generated_sql <=> NEW.generated_sql) AND (OLD.canonical_ast <=> NEW.canonical_ast) AND (OLD.pre_state <=> NEW.pre_state) AND (OLD.expected_post_state <=> NEW.expected_post_state) AND ((OLD.status = 'prepared' AND NEW.status IN ('applied','blocked')) OR (OLD.status = 'applied' AND NEW.status = 'checkpointed')))) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'automatic DDL journal identity/evidence is immutable and status transition is not allowed'; END IF; END";

pub(crate) fn journal_schema_and_table<'a>(
    table: &'a str,
    default_schema: &'a str,
) -> (&'a str, &'a str) {
    table.split_once('.').unwrap_or((default_schema, table))
}

pub(crate) fn query_journal_columns(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<JournalColumn>, String> {
    conn.query(format!(
        "SELECT column_name,LOWER(column_type),is_nullable,LOWER(COALESCE(CAST(column_default AS CHAR),'<null>')),LOWER(extra) FROM information_schema.columns WHERE table_schema={} AND table_name={} ORDER BY ordinal_position",
        quote_sql_literal(schema),
        quote_sql_literal(table),
    ))
    .map_err(mysql_error)
}

pub(crate) fn query_journal_keys(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<JournalKey>, String> {
    conn.query(format!(
        "SELECT index_name,non_unique,seq_in_index,column_name FROM information_schema.statistics WHERE table_schema={} AND table_name={} ORDER BY index_name,seq_in_index",
        quote_sql_literal(schema),
        quote_sql_literal(table),
    ))
    .map_err(mysql_error)
}

pub(crate) fn query_journal_constraints(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<JournalConstraint>, String> {
    conn.query(format!(
        "SELECT constraint_type,enforced FROM information_schema.table_constraints WHERE table_schema={} AND table_name={} ORDER BY constraint_type,constraint_name",
        quote_sql_literal(schema),
        quote_sql_literal(table),
    ))
    .map_err(mysql_error)
}

pub(crate) fn query_journal_status_checks(
    conn: &mut Conn,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, String> {
    conn.query(format!(
        "SELECT cc.check_clause FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name WHERE tc.table_schema={} AND tc.table_name={} AND tc.constraint_type='CHECK'",
        quote_sql_literal(schema),
        quote_sql_literal(table),
    ))
    .map_err(mysql_error)
}

fn journal_trigger_inventory_routine_name(table_name: &str) -> String {
    format!("{table_name}_trigger_inventory")
}

pub(crate) fn journal_trigger_inventory_routine_path(table: &str) -> String {
    let (schema, table_name) = table
        .split_once('.')
        .expect("DDL replay journal table must be schema-qualified");
    format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(&journal_trigger_inventory_routine_name(table_name))
    )
}

pub(crate) fn query_journal_trigger_inventory(
    conn: &mut Conn,
    table: &str,
) -> Result<Vec<JournalTriggerMetadata>, String> {
    let rows = conn
        .query_opt::<JournalTriggerMetadata, _>(format!(
            "CALL {}()",
            journal_trigger_inventory_routine_path(table)
        ))
        .map_err(|error| format!("DDL replay journal trigger inventory failed: {error}"))?;
    rows.into_iter()
        .map(|row| {
            row.map_err(|error| {
                format!(
                    "DDL replay journal trigger inventory returned malformed metadata: {error:?}"
                )
            })
        })
        .collect()
}

const EXPECTED_COLUMNS: &[ColumnSpec] = &[
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
    ("transformation_version", "varchar(64)", "NO", "<null>", ""),
    ("generated_sql", "longtext", "YES", "<null>", ""),
    ("canonical_ast", "longtext", "NO", "<null>", ""),
    ("pre_state", "longtext", "NO", "<null>", ""),
    ("expected_post_state", "longtext", "NO", "<null>", ""),
    ("status", "varchar(32)", "NO", "<null>", ""),
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

fn column_from_spec(spec: ColumnSpec) -> JournalColumn {
    let (name, column_type, nullable, default_value, extra) = spec;
    (
        name.into(),
        column_type.into(),
        nullable.into(),
        default_value.into(),
        extra.into(),
    )
}

pub(crate) fn expected_ddl_replay_journal_columns() -> Vec<JournalColumn> {
    EXPECTED_COLUMNS
        .iter()
        .copied()
        .map(column_from_spec)
        .collect()
}

pub(crate) fn expected_ddl_replay_journal_keys() -> Vec<JournalKey> {
    [
        ("PRIMARY", 0, 1, "source_identity"),
        ("PRIMARY", 0, 2, "binlog_file"),
        ("PRIMARY", 0, 3, "event_start_position"),
    ]
    .into_iter()
    .map(|(name, non_unique, seq, column)| (name.into(), non_unique, seq, column.into()))
    .collect()
}

pub(crate) fn expected_ddl_replay_journal_constraints() -> Vec<JournalConstraint> {
    vec![
        ("CHECK".into(), "YES".into()),
        ("PRIMARY KEY".into(), "YES".into()),
    ]
}

pub(crate) fn validate_ddl_replay_journal_columns(columns: &[JournalColumn]) -> Result<(), String> {
    validate_exact(
        columns,
        &expected_ddl_replay_journal_columns(),
        "column schema",
    )
}

pub(crate) fn validate_ddl_replay_journal_keys(keys: &[JournalKey]) -> Result<(), String> {
    validate_exact(keys, &expected_ddl_replay_journal_keys(), "key inventory")
}

pub(crate) fn validate_ddl_replay_journal_constraints(
    constraints: &[JournalConstraint],
) -> Result<(), String> {
    validate_exact(
        constraints,
        &expected_ddl_replay_journal_constraints(),
        "constraint inventory",
    )
}

fn validate_exact<T: std::fmt::Debug + PartialEq>(
    actual: &[T],
    expected: &[T],
    label: &str,
) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "DDL replay journal {label} mismatch: expected {expected:?}, found {actual:?}"
        ))
    }
}

pub(crate) fn validate_ddl_replay_journal_status_checks(checks: &[String]) -> Result<(), String> {
    let expected = "statusin('translation_pending','prepared','applied','checkpointed','blocked')";
    if checks
        .iter()
        .any(|check| normalize_check(check) == expected)
    {
        Ok(())
    } else {
        Err(format!(
            "DDL replay journal status check mismatch: expected `{expected}`, found {checks:?}"
        ))
    }
}

fn normalize_check(check: &str) -> String {
    let normalized = normalize_sql(check);
    normalized
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(&normalized)
        .to_string()
}

fn normalize_sql(sql: &str) -> String {
    sql.replace('`', "")
        .replace("_utf8mb4", "")
        .replace("\\'", "'")
        .split_whitespace()
        .collect::<String>()
        .to_ascii_lowercase()
}

fn split_trigger_rows<'a>(
    rows: &'a [JournalTriggerMetadata],
    expected_schema: &str,
    expected_table: &str,
) -> Result<(Vec<TriggerRow<'a>>, Vec<TriggerRow<'a>>), String> {
    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    for row in rows {
        validate_trigger_target(row, expected_schema, expected_table)?;
        match row.3.as_str() {
            "INSERT" => inserts.push(trigger_row(row)),
            "UPDATE" => updates.push(trigger_row(row)),
            event => {
                return Err(format!(
                    "unexpected DDL replay journal trigger event {event}"
                ));
            }
        }
    }
    Ok((inserts, updates))
}

type TriggerRow<'a> = (&'a String, &'a String, &'a u64);

fn trigger_row(row: &JournalTriggerMetadata) -> TriggerRow<'_> {
    (&row.0, &row.5, &row.6)
}

fn validate_trigger_target(
    row: &JournalTriggerMetadata,
    expected_schema: &str,
    expected_table: &str,
) -> Result<(), String> {
    if row.1 == expected_schema && row.2 == expected_table && row.4 == "BEFORE" {
        Ok(())
    } else {
        Err(format!(
            "DDL replay journal trigger target/timing drift: {row:?}"
        ))
    }
}

fn validate_single_trigger(
    triggers: &[TriggerRow<'_>],
    event: &str,
    expected_name: &str,
    expected_body: &str,
) -> Result<(), String> {
    let [(name, statement, order)] = triggers else {
        return Err(format!(
            "DDL replay journal requires one BEFORE {event} trigger, found {}",
            triggers.len()
        ));
    };
    if *name == expected_name
        && **order == 1
        && normalize_sql(statement) == normalize_sql(expected_body)
    {
        Ok(())
    } else {
        Err(format!(
            "DDL replay journal {event} guard drift: expected name={expected_name} order=1 body={}; found name={} order={} body={}",
            normalize_sql(expected_body),
            name,
            order,
            normalize_sql(statement),
        ))
    }
}

pub(crate) fn validate_journal_trigger_inventory(
    expected_schema: &str,
    expected_table: &str,
    rows: &[JournalTriggerMetadata],
) -> Result<(), String> {
    let (inserts, updates) = split_trigger_rows(rows, expected_schema, expected_table)?;
    validate_single_trigger(
        &inserts,
        "INSERT",
        "ddl_replay_journal_insert_guard",
        INSERT_GUARD_BODY,
    )?;
    validate_single_trigger(
        &updates,
        "UPDATE",
        "ddl_replay_journal_update_guard",
        UPDATE_GUARD_BODY,
    )
}

fn validate_journal_primary_key(columns: &[String]) -> Result<(), String> {
    let expected = ["source_identity", "binlog_file", "event_start_position"];
    if columns.iter().map(String::as_str).eq(expected) {
        Ok(())
    } else {
        Err(format!(
            "DDL replay journal primary key mismatch: expected {expected:?}, found {columns:?}"
        ))
    }
}

fn journal_primary_key_columns(keys: &[JournalKey]) -> Vec<String> {
    keys.iter()
        .filter(|(index_name, _, _, _)| index_name == "PRIMARY")
        .map(|(_, _, _, column)| column.clone())
        .collect()
}

pub(crate) struct JournalRuntimeContract<'a> {
    pub(crate) expected_schema: &'a str,
    pub(crate) expected_table: &'a str,
    pub(crate) columns: &'a [JournalColumn],
    pub(crate) keys: &'a [JournalKey],
    pub(crate) constraints: &'a [JournalConstraint],
    pub(crate) checks: &'a [String],
    pub(crate) triggers: &'a [JournalTriggerMetadata],
    pub(crate) grants: &'a [String],
    pub(crate) application_schema: &'a str,
    pub(crate) checkpoint_table: &'a str,
    pub(crate) journal_table: &'a str,
    pub(crate) conflict_table: &'a str,
    pub(crate) inventory_procedure: &'a str,
}

pub(crate) fn validate_journal_runtime_contract(
    contract: JournalRuntimeContract<'_>,
) -> Result<(), String> {
    validate_journal_structure(&contract)?;
    validate_runtime_access(&contract)
}

fn validate_journal_structure(contract: &JournalRuntimeContract<'_>) -> Result<(), String> {
    validate_ddl_replay_journal_columns(contract.columns)?;
    validate_ddl_replay_journal_keys(contract.keys)?;
    validate_journal_primary_key(&journal_primary_key_columns(contract.keys))?;
    validate_ddl_replay_journal_constraints(contract.constraints)?;
    validate_ddl_replay_journal_status_checks(contract.checks)?;
    validate_journal_trigger_inventory(
        contract.expected_schema,
        contract.expected_table,
        contract.triggers,
    )
}

fn validate_runtime_access(contract: &JournalRuntimeContract<'_>) -> Result<(), String> {
    validate_runtime_grants(
        contract.grants,
        contract.application_schema,
        contract.checkpoint_table,
        contract.journal_table,
        contract.conflict_table,
        contract.inventory_procedure,
    )
}

#[cfg(test)]
pub(crate) fn validate_inventory_routine_definition(
    expected_schema: &str,
    expected_table: &str,
    show_create: &str,
) -> Result<(), String> {
    let normalized = normalize_sql(show_create);
    let expected_name = normalize_sql(&format!(
        "{}.{}",
        expected_schema,
        journal_trigger_inventory_routine_name(expected_table)
    ));
    let short_name = normalize_sql(&journal_trigger_inventory_routine_name(expected_table));
    if normalized.contains("definer=")
        && normalized.contains("sqlsecuritydefiner")
        && normalized.contains("readssqldata")
        && (normalized.contains(&expected_name) || normalized.contains(&short_name))
    {
        Ok(())
    } else {
        Err("DDL replay journal inventory routine is not an exact definer-safe reader".into())
    }
}
