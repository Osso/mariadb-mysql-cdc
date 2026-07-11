use super::{TargetMySqlConfig, target_session_init_command};
use crate::mysql_support::{
    quote_ident, quote_identifier_path, quote_sql_literal, target_ssl_opts,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder};

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
        let mut conn = Conn::new(target_opts(&self.target)).map_err(ddl_ledger_mysql_error)?;
        conn.query_drop(target_session_init_command())
            .map_err(ddl_ledger_mysql_error)?;
        Ok(conn)
    }

    fn validate_schema(&self, conn: &mut Conn) -> Result<(), String> {
        let (schema, table) = ledger_schema_and_table(&self.table, &self.target.database);
        let columns = conn
            .query::<(String, String, String, String, String), _>(format!(
                "SELECT column_name,LOWER(column_type),is_nullable,LOWER(COALESCE(CAST(column_default AS CHAR),'<null>')),LOWER(extra) FROM information_schema.columns WHERE table_schema={} AND table_name={} ORDER BY ordinal_position",
                quote_sql_literal(schema),
                quote_sql_literal(table),
            ))
            .map_err(ddl_ledger_mysql_error)?;
        validate_ddl_ledger_columns(&columns)?;

        let primary_key = conn
            .query::<String, _>(format!(
                "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema={} AND table_name={} AND constraint_name='PRIMARY' ORDER BY ordinal_position",
                quote_sql_literal(schema),
                quote_sql_literal(table),
            ))
            .map_err(ddl_ledger_mysql_error)?;
        validate_ddl_ledger_primary_key(&primary_key)?;

        let constraints = conn
            .query::<(String, String), _>(format!(
                "SELECT constraint_type,enforced FROM information_schema.table_constraints WHERE table_schema={} AND table_name={} ORDER BY constraint_type,constraint_name",
                quote_sql_literal(schema),
                quote_sql_literal(table),
            ))
            .map_err(ddl_ledger_mysql_error)?;
        validate_ddl_constraints(&constraints)?;

        let status_checks = conn
            .query::<String, _>(format!(
                "SELECT cc.check_clause FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name WHERE tc.table_schema={} AND tc.table_name={} AND tc.constraint_type='CHECK'",
                quote_sql_literal(schema),
                quote_sql_literal(table),
            ))
            .map_err(ddl_ledger_mysql_error)?;
        validate_ddl_status_checks(&status_checks)?;

        let insert_triggers = conn
            .query::<(String, String, u64), _>(format!(
                "SELECT trigger_name,action_statement,action_order FROM information_schema.triggers WHERE trigger_schema={} AND event_object_table={} AND event_manipulation='INSERT' AND action_timing='BEFORE' ORDER BY action_order",
                quote_sql_literal(schema),
                quote_sql_literal(table),
            ))
            .map_err(ddl_ledger_mysql_error)?;
        validate_pending_trigger_inventory(&pending_only_trigger_name(table), &insert_triggers)
            .map_err(|error| {
                format!(
                    "{error}; provision the ledger guard with: {}",
                    build_pending_only_ddl_trigger_sql(&self.table),
                )
            })?;

        let update_triggers = conn
            .query::<(String, String, u64), _>(format!(
                "SELECT trigger_name,action_statement,action_order FROM information_schema.triggers WHERE trigger_schema={} AND event_object_table={} AND event_manipulation='UPDATE' AND action_timing='BEFORE' ORDER BY action_order",
                quote_sql_literal(schema),
                quote_sql_literal(table),
            ))
            .map_err(ddl_ledger_mysql_error)?;
        validate_resolution_trigger_inventory(
            &monotonic_resolution_trigger_name(table),
            &update_triggers,
        )
        .map_err(|error| {
            format!(
                "{error}; provision the resolution guard with: {}",
                build_monotonic_ddl_resolution_trigger_sql(&self.table),
            )
        })
    }

    fn reject_resolution_capable_runtime_grants(&self, conn: &mut Conn) -> Result<(), String> {
        let grants = conn
            .query::<String, _>("SHOW GRANTS")
            .map_err(ddl_ledger_mysql_error)?;
        if grants
            .iter()
            .any(|grant| grant_can_mutate_ledger(grant, &self.table))
        {
            return Err(format!(
                "CDC runtime user can mutate DDL ledger `{}`; use separate runtime and resolver credentials",
                self.table
            ));
        }
        Ok(())
    }
}

impl DdlEventLedger for MySqlDdlEventLedger {
    fn ensure(&self) -> Result<(), String> {
        let mut conn = self.connect()?;
        self.validate_schema(&mut conn)?;
        self.reject_resolution_capable_runtime_grants(&mut conn)
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

fn target_opts(target: &TargetMySqlConfig) -> Opts {
    let ssl = target_ssl_opts();
    Opts::from(
        OptsBuilder::default()
            .ip_or_hostname(Some(&target.host))
            .tcp_port(target.port)
            .user(Some(&target.user))
            .pass(Some(&target.password))
            .db_name(Some(&target.database))
            .prefer_socket(false)
            .ssl_opts(ssl),
    )
}

fn ddl_ledger_mysql_error(error: mysql::Error) -> String {
    format!("DDL ledger MySQL operation failed: {error}")
}

fn ledger_schema_and_table<'a>(table: &'a str, default_schema: &'a str) -> (&'a str, &'a str) {
    table.split_once('.').unwrap_or((default_schema, table))
}

fn expected_ddl_ledger_columns() -> Vec<(String, String, String, String, String)> {
    [
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
    ]
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

fn validate_ddl_ledger_columns(
    columns: &[(String, String, String, String, String)],
) -> Result<(), String> {
    let expected = expected_ddl_ledger_columns();
    if columns == expected {
        return Ok(());
    }
    Err(format!(
        "DDL ledger column schema mismatch: expected {expected:?}, found {columns:?}"
    ))
}

fn validate_ddl_ledger_primary_key(columns: &[String]) -> Result<(), String> {
    let expected = ["source_identity", "binlog_file", "event_start_position"];
    if columns.iter().map(String::as_str).eq(expected) {
        return Ok(());
    }
    Err(format!(
        "DDL ledger primary key mismatch: expected {expected:?}, found {columns:?}"
    ))
}

fn pending_only_trigger_name(table_name: &str) -> String {
    format!("{table_name}_pending_insert_guard")
}

fn monotonic_resolution_trigger_name(table_name: &str) -> String {
    format!("{table_name}_monotonic_resolution_guard")
}

fn validate_ddl_constraints(constraints: &[(String, String)]) -> Result<(), String> {
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

fn validate_ddl_status_checks(checks: &[String]) -> Result<(), String> {
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

fn validate_pending_only_trigger(statement: &str) -> Result<(), String> {
    if normalize_sql_guard(statement) == normalize_sql_guard(PENDING_ONLY_TRIGGER_BODY) {
        return Ok(());
    }
    Err("DDL ledger INSERT trigger does not exactly enforce pending-only rows".to_string())
}

fn validate_pending_trigger_inventory(
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

fn validate_resolution_trigger_inventory(
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

fn grant_can_mutate_ledger(grant: &str, ledger_table: &str) -> bool {
    let normalized_grant = grant
        .replace('`', "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    let Some(grant_body) = normalized_grant.strip_prefix("GRANT ") else {
        return false;
    };
    let Some((privileges, target)) = grant_body.split_once(" ON ") else {
        return grant_body.contains(" TO ");
    };
    let Some(scope) = target.split_once(" TO ").map(|(scope, _)| scope.trim()) else {
        return false;
    };
    let privileges = privileges.split(',').map(str::trim).collect::<Vec<_>>();
    if privileges
        .iter()
        .any(|privilege| privilege.starts_with("PROXY") || privilege == &"ROLE_ADMIN")
    {
        return true;
    }
    if scope == "*.*" {
        return privileges != ["USAGE"];
    }

    let ledger_table = ledger_table.replace('`', "").to_ascii_uppercase();
    let Some((ledger_schema, _)) = ledger_table.split_once('.') else {
        return true;
    };
    let scope_covers_ledger = scope == ledger_table || scope == format!("{ledger_schema}.*");
    if !scope_covers_ledger {
        return false;
    }
    privileges.iter().any(|privilege| {
        *privilege == "ALL"
            || *privilege == "ALL PRIVILEGES"
            || ["UPDATE", "DELETE", "ALTER", "DROP", "TRIGGER"]
                .iter()
                .any(|dangerous| privilege.starts_with(dangerous))
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_manual_ddl_resolution_ledger() {
        let sql = build_create_ddl_ledger_table_sql("cdc.ddl_events");

        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `cdc`.`ddl_events`"));
        assert!(sql.contains("source_identity VARCHAR(384) NOT NULL"));
        assert!(sql.contains("source_server_id INT UNSIGNED NOT NULL"));
        assert!(sql.contains("binlog_file VARCHAR(255) NOT NULL"));
        assert!(sql.contains("event_start_position BIGINT UNSIGNED NOT NULL"));
        assert!(sql.contains("event_end_position BIGINT UNSIGNED NOT NULL"));
        assert!(sql.contains("status VARCHAR(32) NOT NULL"));
        assert!(sql.contains("raw_sql LONGTEXT NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (source_identity,binlog_file,event_start_position)"));
    }

    #[test]
    fn validates_existing_ledger_columns_and_primary_key() {
        let columns = expected_ddl_ledger_columns();
        assert!(validate_ddl_ledger_columns(&columns).is_ok());
        assert!(
            validate_ddl_ledger_primary_key(&[
                "source_identity".to_string(),
                "binlog_file".to_string(),
                "event_start_position".to_string(),
            ])
            .is_ok()
        );

        let mut wrong_columns = columns;
        wrong_columns[0].1 = "varchar(512)".to_string();
        assert!(validate_ddl_ledger_columns(&wrong_columns).is_err());
        assert!(
            validate_ddl_ledger_primary_key(&[
                "binlog_file".to_string(),
                "source_identity".to_string(),
                "event_start_position".to_string(),
            ])
            .is_err()
        );
    }

    #[test]
    fn requires_exact_status_check_and_pending_only_insert_trigger() {
        assert!(
            validate_ddl_status_checks(&[
                "(`status` in (_utf8mb4'pending',_utf8mb4'resolved'))".to_string()
            ])
            .is_ok()
        );
        assert!(validate_ddl_status_checks(&["status <> ''".to_string()]).is_err());

        let trigger_sql = build_pending_only_ddl_trigger_sql("cdc.ddl_events");
        assert!(trigger_sql.contains("BEFORE INSERT ON `cdc`.`ddl_events`"));
        assert!(trigger_sql.contains("NEW.status <> 'pending'"));
        assert!(trigger_sql.contains("NEW.resolution_note IS NOT NULL"));
        assert!(validate_pending_only_trigger(PENDING_ONLY_TRIGGER_BODY).is_ok());
        assert!(validate_pending_only_trigger("SET NEW.status = 'resolved'").is_err());
        assert!(
            validate_ddl_constraints(&[
                ("CHECK".to_string(), "YES".to_string()),
                ("PRIMARY KEY".to_string(), "YES".to_string()),
            ])
            .is_ok()
        );
        assert!(
            validate_ddl_constraints(&[
                ("CHECK".to_string(), "NO".to_string()),
                ("PRIMARY KEY".to_string(), "YES".to_string()),
            ])
            .is_err()
        );
        assert!(
            validate_pending_trigger_inventory(
                "ddl_events_pending_insert_guard",
                &[(
                    "ddl_events_pending_insert_guard".to_string(),
                    PENDING_ONLY_TRIGGER_BODY.to_string(),
                    1,
                )],
            )
            .is_ok()
        );
        assert!(
            validate_pending_trigger_inventory(
                "ddl_events_pending_insert_guard",
                &[
                    (
                        "ddl_events_pending_insert_guard".to_string(),
                        PENDING_ONLY_TRIGGER_BODY.to_string(),
                        1,
                    ),
                    (
                        "later_bypass".to_string(),
                        "SET NEW.status='resolved'".to_string(),
                        2
                    ),
                ],
            )
            .is_err()
        );

        let update_trigger_sql = build_monotonic_ddl_resolution_trigger_sql("cdc.ddl_events");
        assert!(update_trigger_sql.contains("BEFORE UPDATE ON `cdc`.`ddl_events`"));
        assert!(update_trigger_sql.contains("OLD.event_end_position <=> NEW.event_end_position"));
        assert!(update_trigger_sql.contains("OLD.status <> 'pending'"));
        assert!(update_trigger_sql.contains("NEW.status <> 'resolved'"));
        assert!(
            validate_resolution_trigger_inventory(
                "ddl_events_monotonic_resolution_guard",
                &[(
                    "ddl_events_monotonic_resolution_guard".to_string(),
                    MONOTONIC_RESOLUTION_TRIGGER_BODY.to_string(),
                    1,
                )],
            )
            .is_ok()
        );
    }

    #[test]
    fn accepts_escaped_status_literals_returned_by_information_schema() {
        assert!(
            validate_ddl_status_checks(&[
                "(`status` in (_utf8mb4\\'pending\\',_utf8mb4\\'resolved'))".to_string()
            ])
            .is_ok()
        );
    }

    #[test]
    fn records_pending_event_without_overwriting_existing_resolution() {
        let event = ddl_event();
        let sql = build_record_pending_ddl_sql("cdc.ddl_events", &event);

        assert!(sql.starts_with("INSERT INTO `cdc`.`ddl_events`"));
        assert!(sql.contains("'pending'"));
        assert!(sql.contains("ALTER TABLE accounts ADD COLUMN handle varchar(64)"));
        assert!(!sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    #[test]
    fn rejects_runtime_grants_that_can_resolve_ddl() {
        assert!(grant_can_mutate_ledger(
            "GRANT ALL PRIVILEGES ON *.* TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(grant_can_mutate_ledger(
            "GRANT SELECT, INSERT, UPDATE ON `cdc`.`ddl_events` TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(!grant_can_mutate_ledger(
            "GRANT SELECT, INSERT ON `cdc`.`ddl_events` TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(!grant_can_mutate_ledger(
            "GRANT SELECT, INSERT, UPDATE ON `globalcomix`.* TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(grant_can_mutate_ledger(
            "GRANT SELECT, INSERT, UPDATE (`status`) ON `cdc`.`ddl_events` TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(grant_can_mutate_ledger(
            "GRANT SELECT, INSERT, DELETE ON `cdc`.`ddl_events` TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(grant_can_mutate_ledger(
            "GRANT SELECT, INSERT, ALTER ON `cdc`.* TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(grant_can_mutate_ledger(
            "GRANT `ddl_admin`@`%` TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(grant_can_mutate_ledger(
            "GRANT ROLE_ADMIN ON *.* TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(grant_can_mutate_ledger(
            "GRANT PROXY ON `admin`@`%` TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
        assert!(!grant_can_mutate_ledger(
            "GRANT USAGE ON *.* TO `cdc`@`%`",
            "cdc.ddl_events"
        ));
    }

    #[test]
    fn selects_status_and_sql_by_immutable_event_coordinate() {
        let event = ddl_event();
        let sql = build_ddl_status_select_sql("cdc.ddl_events", &event);

        assert!(sql.contains("source_identity='production-source#server-id=3'"));
        assert!(sql.contains("binlog_file='mysqld-bin.000777'"));
        assert!(sql.contains("event_start_position=99"));
        assert_eq!(
            parse_ddl_status("resolved\tALTER TABLE accounts ADD COLUMN handle varchar(64)\n")
                .expect("status"),
            Some(DdlEventStatus::Resolved {
                raw_sql: "ALTER TABLE accounts ADD COLUMN handle varchar(64)".to_string(),
            })
        );
    }

    fn ddl_event() -> DdlEvent {
        DdlEvent {
            source_identity: "production-source#server-id=3".to_string(),
            source_server_id: 3,
            binlog_file: "mysqld-bin.000777".to_string(),
            event_start_position: 99,
            event_end_position: 180,
            schema_name: "fixture_cdc".to_string(),
            raw_sql: "ALTER TABLE accounts ADD COLUMN handle varchar(64)".to_string(),
        }
    }
}
