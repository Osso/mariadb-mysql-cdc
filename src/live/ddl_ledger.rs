use super::{TargetMySqlConfig, target_session_init_command};
use crate::mysql_support::{
    quote_ident, quote_identifier_path, quote_sql_literal, target_mysql_opts,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts};

mod schema;
#[cfg(test)]
mod tests;

#[cfg(test)]
use schema::{TriggerMetadata, expected_ddl_ledger_columns, validate_pending_only_trigger};
use schema::{
    monotonic_resolution_trigger_name, pending_only_trigger_name, query_ddl_ledger_columns,
    query_ddl_ledger_constraints, query_ddl_ledger_primary_key, query_ddl_status_checks,
    query_ddl_trigger_inventory, validate_ddl_constraints, validate_ddl_ledger_columns,
    validate_ddl_ledger_primary_key, validate_ddl_status_checks,
    validate_pending_trigger_inventory, validate_resolution_trigger_inventory,
    validate_trigger_inventory_metadata,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlEvent {
    pub source_identity: String,
    pub source_server_id: u32,
    pub binlog_file: String,
    pub event_start_position: u64,
    pub event_end_position: u64,
    pub schema_name: String,
    pub raw_sql: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DdlEventStatus {
    Pending { raw_sql: String },
    Resolved { raw_sql: String },
}

#[cfg(test)]
pub fn build_create_ddl_ledger_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\
source_identity VARCHAR(384) NOT NULL,\
source_server_id INT UNSIGNED NOT NULL,\
binlog_file VARCHAR(255) NOT NULL,\
event_start_position BIGINT UNSIGNED NOT NULL,\
event_end_position BIGINT UNSIGNED NOT NULL,\
schema_name VARCHAR(255) NOT NULL,\
raw_sql LONGTEXT NOT NULL,\
status VARCHAR(32) NOT NULL,\
resolution_note TEXT NULL,\
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
CHECK (status IN ('pending','resolved')),\
PRIMARY KEY (source_identity,binlog_file,event_start_position)\
)",
        quote_identifier_path(table)
    )
}

const PENDING_ONLY_TRIGGER_BODY: &str = "BEGIN IF NEW.status <> 'pending' OR NEW.resolution_note IS NOT NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DDL events may only be inserted pending'; END IF; END";
const MONOTONIC_RESOLUTION_TRIGGER_BODY: &str = "BEGIN IF NOT (OLD.source_identity <=> NEW.source_identity) OR NOT (OLD.source_server_id <=> NEW.source_server_id) OR NOT (OLD.binlog_file <=> NEW.binlog_file) OR NOT (OLD.event_start_position <=> NEW.event_start_position) OR NOT (OLD.event_end_position <=> NEW.event_end_position) OR NOT (OLD.schema_name <=> NEW.schema_name) OR NOT (OLD.raw_sql <=> NEW.raw_sql) OR OLD.status <> 'pending' OR NEW.status <> 'resolved' OR NEW.resolution_note IS NULL OR NEW.resolution_note = '' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DDL resolution must preserve coordinates and transition pending to resolved once'; END IF; END";

pub fn build_pending_only_ddl_trigger_sql(table: &str) -> String {
    let (schema, table_name) = table
        .split_once('.')
        .expect("DDL ledger table must be schema-qualified");
    let trigger_name = pending_only_trigger_name(table_name);
    format!(
        "CREATE TRIGGER {}.{} BEFORE INSERT ON {} FOR EACH ROW {}",
        quote_ident(schema),
        quote_ident(&trigger_name),
        quote_identifier_path(table),
        PENDING_ONLY_TRIGGER_BODY,
    )
}

pub fn build_monotonic_ddl_resolution_trigger_sql(table: &str) -> String {
    let (schema, table_name) = table
        .split_once('.')
        .expect("DDL ledger table must be schema-qualified");
    let trigger_name = monotonic_resolution_trigger_name(table_name);
    format!(
        "CREATE TRIGGER {}.{} BEFORE UPDATE ON {} FOR EACH ROW {}",
        quote_ident(schema),
        quote_ident(&trigger_name),
        quote_identifier_path(table),
        MONOTONIC_RESOLUTION_TRIGGER_BODY,
    )
}

fn ddl_trigger_inventory_routine_name(table_name: &str) -> String {
    format!("{table_name}_trigger_inventory")
}

fn ddl_trigger_inventory_routine_path(table: &str) -> String {
    let (schema, table_name) = table
        .split_once('.')
        .expect("DDL ledger table must be schema-qualified");
    format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(&ddl_trigger_inventory_routine_name(table_name))
    )
}

fn build_ddl_trigger_inventory_call_sql(table: &str) -> String {
    format!("CALL {}()", ddl_trigger_inventory_routine_path(table))
}

pub fn build_record_pending_ddl_sql(table: &str, event: &DdlEvent) -> String {
    format!(
        "INSERT INTO {} (source_identity,source_server_id,binlog_file,event_start_position,event_end_position,schema_name,raw_sql,status) VALUES ({},{},{},{},{},{},{},'pending')",
        quote_identifier_path(table),
        quote_sql_literal(&event.source_identity),
        event.source_server_id,
        quote_sql_literal(&event.binlog_file),
        event.event_start_position,
        event.event_end_position,
        quote_sql_literal(&event.schema_name),
        quote_sql_literal(&event.raw_sql),
    )
}

pub fn build_ddl_status_select_sql(table: &str, event: &DdlEvent) -> String {
    format!(
        "SELECT status, raw_sql FROM {} WHERE source_identity={} AND binlog_file={} AND event_start_position={} LIMIT 1",
        quote_identifier_path(table),
        quote_sql_literal(&event.source_identity),
        quote_sql_literal(&event.binlog_file),
        event.event_start_position,
    )
}

pub trait DdlEventLedger {
    fn ensure(&self) -> Result<(), String>;
    fn read_status(&self, event: &DdlEvent) -> Result<Option<DdlEventStatus>, String>;
    fn record_pending(&self, event: &DdlEvent) -> Result<(), String>;
}

pub struct MySqlDdlEventLedger {
    table: String,
    target: TargetMySqlConfig,
}

impl MySqlDdlEventLedger {
    pub fn new(target: &TargetMySqlConfig, table: String) -> Self {
        Self {
            table,
            target: target.clone(),
        }
    }

    fn connect(&self) -> Result<Conn, String> {
        let mut conn = Conn::new(target_opts(&self.target)?).map_err(ddl_ledger_mysql_error)?;
        conn.query_drop(target_session_init_command())
            .map_err(ddl_ledger_mysql_error)?;
        Ok(conn)
    }

    fn validate_schema(&self, conn: &mut Conn) -> Result<(), String> {
        let (schema, table) = ledger_schema_and_table(&self.table, &self.target.database);
        self.validate_structure(conn, schema, table)?;
        self.validate_triggers(conn, schema, table)
    }

    fn validate_structure(&self, conn: &mut Conn, schema: &str, table: &str) -> Result<(), String> {
        validate_ddl_ledger_columns(&query_ddl_ledger_columns(conn, schema, table)?)?;
        validate_ddl_ledger_primary_key(&query_ddl_ledger_primary_key(conn, schema, table)?)?;
        validate_ddl_constraints(&query_ddl_ledger_constraints(conn, schema, table)?)?;
        validate_ddl_status_checks(&query_ddl_status_checks(conn, schema, table)?)
    }

    fn validate_triggers(&self, conn: &mut Conn, schema: &str, table: &str) -> Result<(), String> {
        let inventory = query_ddl_trigger_inventory(conn, &self.table)?;
        let (insert_triggers, update_triggers) =
            validate_trigger_inventory_metadata(schema, table, &inventory)?;
        self.validate_pending_trigger(table, &insert_triggers)?;
        self.validate_resolution_trigger(table, &update_triggers)
    }

    fn validate_pending_trigger(
        &self,
        table: &str,
        triggers: &[(String, String, u64)],
    ) -> Result<(), String> {
        validate_pending_trigger_inventory(&pending_only_trigger_name(table), triggers).map_err(
            |error| {
                format!(
                    "{error}; provision the ledger guard with: {}",
                    build_pending_only_ddl_trigger_sql(&self.table)
                )
            },
        )
    }

    fn validate_resolution_trigger(
        &self,
        table: &str,
        triggers: &[(String, String, u64)],
    ) -> Result<(), String> {
        validate_resolution_trigger_inventory(&monotonic_resolution_trigger_name(table), triggers)
            .map_err(|error| {
                format!(
                    "{error}; provision the resolution guard with: {}",
                    build_monotonic_ddl_resolution_trigger_sql(&self.table),
                )
            })
    }
}

impl DdlEventLedger for MySqlDdlEventLedger {
    fn ensure(&self) -> Result<(), String> {
        let mut conn = self.connect()?;
        self.validate_schema(&mut conn)
    }

    fn read_status(&self, event: &DdlEvent) -> Result<Option<DdlEventStatus>, String> {
        let row = self
            .connect()?
            .query_first::<(String, String), _>(build_ddl_status_select_sql(&self.table, event))
            .map_err(ddl_ledger_mysql_error)?;
        row.map(|(status, raw_sql)| parse_ddl_status_fields(&status, raw_sql))
            .transpose()
    }

    fn record_pending(&self, event: &DdlEvent) -> Result<(), String> {
        self.connect()?
            .query_drop(build_record_pending_ddl_sql(&self.table, event))
            .map_err(ddl_ledger_mysql_error)
    }
}

fn target_opts(target: &TargetMySqlConfig) -> Result<Opts, String> {
    target_mysql_opts(target)
}

fn ddl_ledger_mysql_error(error: mysql::Error) -> String {
    format!("DDL ledger MySQL operation failed: {error}")
}

fn ledger_schema_and_table<'a>(table: &'a str, default_schema: &'a str) -> (&'a str, &'a str) {
    table.split_once('.').unwrap_or((default_schema, table))
}

#[cfg(test)]
pub fn parse_ddl_status(output: &str) -> Result<Option<DdlEventStatus>, String> {
    let Some(line) = output.lines().find(|line| !line.trim().is_empty()) else {
        return Ok(None);
    };
    let Some((status, raw_sql)) = line.split_once('\t') else {
        return Err("DDL ledger row must contain status and raw_sql".to_string());
    };
    parse_ddl_status_fields(status, raw_sql.to_string()).map(Some)
}

fn parse_ddl_status_fields(status: &str, raw_sql: String) -> Result<DdlEventStatus, String> {
    match status {
        "pending" => Ok(DdlEventStatus::Pending { raw_sql }),
        "resolved" => Ok(DdlEventStatus::Resolved { raw_sql }),
        other => Err(format!("unknown DDL ledger status `{other}`")),
    }
}
