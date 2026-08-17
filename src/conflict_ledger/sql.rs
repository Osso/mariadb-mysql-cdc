use super::model::ConflictResolution;

fn json_string(value: &[String]) -> String {
    serde_json::to_string(value).expect("primary key serializable")
}

enum ConflictResolutionScope<'a> {
    Table {
        source_identity: &'a str,
        row_table: &'a str,
    },
    SourceSchemaTableRow {
        source_row_identity: String,
        source_identity: &'a str,
        schema: &'a str,
        row_table: &'a str,
        primary_key_json: String,
    },
    TableRow {
        row_table: &'a str,
        primary_key_json: String,
    },
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
        ConflictResolutionScope::SourceSchemaTableRow {
            source_row_identity,
            source_identity,
            schema,
            row_table,
            primary_key_json,
        } => build_source_row_scope_sql(
            &source_row_identity,
            source_identity,
            schema,
            row_table,
            &primary_key_json,
        ),
        ConflictResolutionScope::Table {
            source_identity,
            row_table,
        } => build_table_scope_sql(source_identity, row_table),
        ConflictResolutionScope::TableRow {
            row_table,
            primary_key_json,
        } => build_table_row_scope_sql(row_table, &primary_key_json),
    }
}

fn build_source_row_scope_sql(
    source_row_identity: &str,
    source_identity: &str,
    schema: &str,
    row_table: &str,
    primary_key_json: &str,
) -> String {
    format!(
        "source_row_identity={} AND source_identity={} AND schema_name={} AND table_name={} AND source_primary_key_json={}",
        sql_literal(source_row_identity),
        sql_literal(source_identity),
        sql_literal(schema),
        sql_literal(row_table),
        sql_literal(primary_key_json),
    )
}

fn build_table_scope_sql(source_identity: &str, row_table: &str) -> String {
    format!(
        "source_identity={} AND table_name={}",
        sql_literal(source_identity),
        sql_literal(row_table)
    )
}

fn build_table_row_scope_sql(row_table: &str, primary_key_json: &str) -> String {
    format!(
        "table_name={} AND source_primary_key_json={}",
        sql_literal(row_table),
        sql_literal(primary_key_json)
    )
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

pub fn build_conflict_resolution_for_source_row_sql(
    ledger_table: &str,
    resolution: &ConflictResolution,
) -> String {
    build_conflict_resolution_update_sql(
        ledger_table,
        &resolution.repair_run_id,
        &resolution.evidence,
        ConflictResolutionScope::SourceSchemaTableRow {
            source_row_identity: resolution.source_row_identity(),
            source_identity: &resolution.source_identity,
            schema: &resolution.schema,
            row_table: &resolution.table,
            primary_key_json: json_string(&resolution.source_primary_key),
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
        ConflictResolutionScope::TableRow {
            row_table,
            primary_key_json: json_string(primary_key),
        },
    )
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}
