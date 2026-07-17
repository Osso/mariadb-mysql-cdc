use super::model::*;

pub fn build_conflict_observation_sql(table: &str, observation: &ConflictObservation) -> String {
    let values_sql = build_observation_values(observation);
    format!(
        "INSERT INTO {table} (conflict_identity,source_identity,source_server_id,source_file,source_start_position,source_end_position,schema_name,table_name,operation,source_primary_key_json,duplicate_index,duplicate_owner_primary_key_json,error_code,error_text,first_observed_at_ms,last_observed_at_ms,attempt_count,status) VALUES ({values_sql},1,'unresolved') ON DUPLICATE KEY UPDATE conflict_identity=IF({},conflict_identity,NULL),duplicate_index=IF(status='resolved',duplicate_index,VALUES(duplicate_index)),duplicate_owner_primary_key_json=IF(status='resolved',duplicate_owner_primary_key_json,VALUES(duplicate_owner_primary_key_json)),error_code=IF(status='resolved',error_code,VALUES(error_code)),error_text=IF(status='resolved',error_text,VALUES(error_text)),last_observed_at_ms=IF(status='resolved',last_observed_at_ms,VALUES(last_observed_at_ms)),attempt_count=IF(status='resolved',attempt_count,attempt_count+1),status=IF(status='resolved',status,'unresolved')",
        conflict_identity_full_match_sql(),
    )
}

fn build_observation_values(observation: &ConflictObservation) -> String {
    let source_primary_key = json_string(&observation.source_primary_key);
    let owner_primary_key = observation
        .duplicate_owner_primary_key
        .as_deref()
        .map(json_string);
    [
        sql_literal(&observation.conflict_identity()),
        sql_literal(&observation.source_identity),
        observation.source_server_id.to_string(),
        sql_literal(&observation.coordinate.file),
        observation.coordinate.start_position.to_string(),
        observation.coordinate.end_position.to_string(),
        sql_literal(&observation.schema),
        sql_literal(&observation.table),
        sql_literal(&format!("{:?}", observation.operation).to_ascii_lowercase()),
        sql_literal(&source_primary_key),
        optional_sql_literal(observation.duplicate_index.as_deref()),
        owner_primary_key
            .as_deref()
            .map(sql_literal)
            .unwrap_or_else(|| "NULL".to_string()),
        observation.error_code.to_string(),
        sql_literal(&observation.error_text),
        observation.observed_at_ms.to_string(),
        observation.observed_at_ms.to_string(),
    ]
    .join(",")
}

fn json_string(value: &[String]) -> String {
    serde_json::to_string(value).expect("primary key serializable")
}

fn optional_sql_literal(value: Option<&str>) -> String {
    value.map(sql_literal).unwrap_or_else(|| "NULL".to_string())
}

fn conflict_identity_full_match_sql() -> &'static str {
    "source_identity <=> VALUES(source_identity) AND source_server_id <=> VALUES(source_server_id) AND source_file <=> VALUES(source_file) AND source_start_position <=> VALUES(source_start_position) AND schema_name <=> VALUES(schema_name) AND table_name <=> VALUES(table_name) AND operation <=> VALUES(operation) AND source_primary_key_json <=> VALUES(source_primary_key_json)"
}

enum ConflictResolutionScope<'a> {
    Table {
        source_identity: &'a str,
        row_table: &'a str,
    },
    TableRow {
        schema: Option<&'a str>,
        row_table: &'a str,
        primary_key_json: String,
    },
}

impl<'a> ConflictResolutionScope<'a> {
    fn unqualified_row(row_table: &'a str, primary_key: &[String]) -> Self {
        Self::TableRow {
            schema: None,
            row_table,
            primary_key_json: json_string(primary_key),
        }
    }

    fn qualified_row(schema: &'a str, row_table: &'a str, primary_key: &[String]) -> Self {
        Self::TableRow {
            schema: Some(schema),
            row_table,
            primary_key_json: json_string(primary_key),
        }
    }
}

fn build_conflict_resolution_update_sql(
    ledger_table: &str,
    repair_run_id: &str,
    evidence: &str,
    scope: ConflictResolutionScope<'_>,
) -> String {
    let scope_sql = build_conflict_scope_sql(scope);
    format!(
        "UPDATE {ledger_table} SET status='resolved',repair_run_id={},resolution_evidence={} WHERE {scope_sql} AND status='unresolved'",
        sql_literal(repair_run_id),
        sql_literal(evidence),
    )
}

fn build_conflict_scope_sql(scope: ConflictResolutionScope<'_>) -> String {
    match scope {
        ConflictResolutionScope::Table {
            source_identity,
            row_table,
        } => format!(
            "source_identity={} AND table_name={}",
            sql_literal(source_identity),
            sql_literal(row_table)
        ),
        ConflictResolutionScope::TableRow {
            schema,
            row_table,
            primary_key_json,
        } => {
            let schema_sql = schema
                .map(|value| format!("schema_name={} AND ", sql_literal(value)))
                .unwrap_or_default();
            format!(
                "{schema_sql}table_name={} AND source_primary_key_json={}",
                sql_literal(row_table),
                sql_literal(&primary_key_json)
            )
        }
    }
}

pub fn build_conflict_table_resolution_sql(
    ledger_table: &str,
    source_identity: &str,
    row_table: &str,
    repair_run_id: &str,
    evidence: &str,
) -> String {
    build_conflict_resolution_update_sql(
        ledger_table,
        repair_run_id,
        evidence,
        ConflictResolutionScope::Table {
            source_identity,
            row_table,
        },
    )
}

pub fn build_conflict_resolution_by_table_sql(
    ledger_table: &str,
    row_table: &str,
    primary_key: &[String],
    repair_run_id: &str,
    evidence: &str,
) -> String {
    build_conflict_resolution_update_sql(
        ledger_table,
        repair_run_id,
        evidence,
        ConflictResolutionScope::unqualified_row(row_table, primary_key),
    )
}

pub fn build_conflict_resolution_sql(
    ledger_table: &str,
    schema: &str,
    row_table: &str,
    primary_key: &[String],
    repair_run_id: &str,
    evidence: &str,
) -> String {
    let scope = ConflictResolutionScope::qualified_row(schema, row_table, primary_key);
    build_conflict_resolution_update_sql(ledger_table, repair_run_id, evidence, scope)
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
