use super::schema::{
    JournalRuntimeContract, journal_schema_and_table, journal_trigger_inventory_routine_path,
    query_journal_columns, query_journal_constraints, query_journal_keys,
    query_journal_status_checks, query_journal_trigger_inventory,
    validate_journal_runtime_contract,
};
use super::{
    DdlEvent, DdlReplayStatus, DdlSemanticEvidence, JournalBarrier, SqlStatement,
    TargetMySqlConfig, mysql_error, target_opts, target_session_init_command,
};
use crate::mysql_support::{quote_identifier_path, quote_sql_literal};
use mysql::Conn;
use mysql::prelude::Queryable;

pub trait DdlReplayJournal {
    fn ensure(&self) -> Result<(), String>;
    fn earliest_barrier(&self, source_identity: &str) -> Result<Option<JournalBarrier>, String>;
    fn read_status(&self, event: &DdlEvent) -> Result<Option<DdlReplayStatus>, String>;
    fn read_evidence(&self, event: &DdlEvent) -> Result<Option<DdlSemanticEvidence>, String>;
    fn record_translation_pending(&self, event: &DdlEvent) -> Result<(), String>;
    fn prepare(&self, event: &DdlEvent, evidence: &DdlSemanticEvidence) -> Result<(), String>;
    fn mark_applied(&self, event: &DdlEvent) -> Result<(), String>;
    fn mark_blocked(&self, event: &DdlEvent) -> Result<(), String>;
    fn checkpoint_transition_statement(&self, event: &DdlEvent) -> Result<SqlStatement, String>;
}

pub struct MySqlDdlReplayJournal {
    target: TargetMySqlConfig,
    table: String,
}

impl MySqlDdlReplayJournal {
    pub fn new(target: &TargetMySqlConfig, table: String) -> Self {
        Self {
            target: target.clone(),
            table,
        }
    }

    fn connect(&self) -> Result<Conn, String> {
        let mut connection = Conn::new(target_opts(&self.target)?).map_err(mysql_error)?;
        connection
            .query_drop(target_session_init_command())
            .map_err(mysql_error)?;
        Ok(connection)
    }

    fn transition(
        &self,
        event: &DdlEvent,
        from: DdlReplayStatus,
        to: DdlReplayStatus,
    ) -> Result<(), String> {
        allowed_journal_transition(from, to)?;
        let mut connection = self.connect()?;
        connection
            .query_drop(build_transition_sql(&self.table, event, from, to))
            .map_err(mysql_error)?;
        ensure_one_transition(&connection, event, from, to)
    }
}

impl DdlReplayJournal for MySqlDdlReplayJournal {
    fn ensure(&self) -> Result<(), String> {
        let mut conn = self.connect()?;
        let (schema, table) = journal_schema_and_table(&self.table, &self.target.database);
        let columns = query_journal_columns(&mut conn, schema, table)?;
        let keys = query_journal_keys(&mut conn, schema, table)?;
        let constraints = query_journal_constraints(&mut conn, schema, table)?;
        let checks = query_journal_status_checks(&mut conn, schema, table)?;
        let triggers = query_journal_trigger_inventory(&mut conn, &self.table)?;
        let grants = conn
            .query::<String, _>("SHOW GRANTS")
            .map_err(mysql_error)?;
        let inventory_procedure =
            journal_trigger_inventory_routine_path(&self.table).replace('`', "");
        validate_journal_runtime_contract(JournalRuntimeContract {
            expected_schema: schema,
            expected_table: table,
            columns: &columns,
            keys: &keys,
            constraints: &constraints,
            checks: &checks,
            triggers: &triggers,
            grants: &grants,
            application_schema: &self.target.database,
            checkpoint_table: "cdc.stream_checkpoint",
            journal_table: &self.table,
            conflict_table: "cdc.row_conflicts",
            inventory_procedure: &inventory_procedure,
        })
    }

    fn earliest_barrier(&self, source_identity: &str) -> Result<Option<JournalBarrier>, String> {
        let row = self
            .connect()?
            .query_first::<(String, u64, String), _>(build_barrier_select_sql(
                &self.table,
                source_identity,
            ))
            .map_err(mysql_error)?;
        row.map(parse_barrier).transpose()
    }

    fn read_status(&self, event: &DdlEvent) -> Result<Option<DdlReplayStatus>, String> {
        let row = self
            .connect()?
            .query_first::<JournalRow, _>(build_status_select_sql(&self.table, event))
            .map_err(mysql_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        validate_journal_event_identity(event, &row, "status")?;
        parse_status(&row.1).map(Some)
    }

    fn read_evidence(&self, event: &DdlEvent) -> Result<Option<DdlSemanticEvidence>, String> {
        let row = self
            .connect()?
            .query_first::<JournalRow, _>(build_status_select_sql(&self.table, event))
            .map_err(mysql_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        validate_journal_event_identity(event, &row, "evidence")?;
        Ok(Some(DdlSemanticEvidence {
            transformation_version: row.5,
            generated_sql: row.6,
            canonical_ast: row.7,
            pre_state: row.8,
            expected_post_state: row.9,
        }))
    }

    fn record_translation_pending(&self, event: &DdlEvent) -> Result<(), String> {
        let mut connection = self.connect()?;
        connection
            .query_drop(build_translation_pending_sql(&self.table, event))
            .map_err(mysql_error)?;
        ensure_one_write(&connection, event, "translation-pending DDL journal insert")
    }

    fn prepare(&self, event: &DdlEvent, evidence: &DdlSemanticEvidence) -> Result<(), String> {
        let status = self.read_status(event)?;
        let sql = if status == Some(DdlReplayStatus::TranslationPending) {
            build_promote_translation_sql(&self.table, event, evidence)
        } else {
            build_prepare_sql(&self.table, event, evidence)
        };
        let mut connection = self.connect()?;
        connection.query_drop(sql).map_err(mysql_error)?;
        ensure_one_write(&connection, event, "automatic DDL journal prepare")
    }

    fn mark_applied(&self, event: &DdlEvent) -> Result<(), String> {
        self.transition(event, DdlReplayStatus::Prepared, DdlReplayStatus::Applied)
    }

    fn mark_blocked(&self, event: &DdlEvent) -> Result<(), String> {
        self.transition(event, DdlReplayStatus::Prepared, DdlReplayStatus::Blocked)
    }

    fn checkpoint_transition_statement(&self, event: &DdlEvent) -> Result<SqlStatement, String> {
        Ok(SqlStatement {
            sql: build_transition_sql(
                &self.table,
                event,
                DdlReplayStatus::Applied,
                DdlReplayStatus::Checkpointed,
            ),
            params: Vec::new(),
        })
    }
}

fn ensure_one_write(connection: &Conn, event: &DdlEvent, operation: &str) -> Result<(), String> {
    if connection.affected_rows() == 1 {
        Ok(())
    } else {
        Err(format!(
            "{operation} did not update exactly one row at {}:{}",
            event.binlog_file, event.event_start_position
        ))
    }
}

fn ensure_one_transition(
    connection: &Conn,
    event: &DdlEvent,
    from: DdlReplayStatus,
    to: DdlReplayStatus,
) -> Result<(), String> {
    if connection.affected_rows() == 1 {
        Ok(())
    } else {
        Err(format!(
            "automatic DDL journal transition {} -> {} did not update exactly one row at {}:{}",
            from.as_str(),
            to.as_str(),
            event.binlog_file,
            event.event_start_position
        ))
    }
}

fn allowed_journal_transition(from: DdlReplayStatus, to: DdlReplayStatus) -> Result<(), String> {
    match (from, to) {
        (DdlReplayStatus::Prepared, DdlReplayStatus::Applied)
        | (DdlReplayStatus::Prepared, DdlReplayStatus::Blocked)
        | (DdlReplayStatus::Applied, DdlReplayStatus::Checkpointed) => Ok(()),
        _ => Err(format!(
            "invalid DDL replay journal transition {}->{}",
            from.as_str(),
            to.as_str()
        )),
    }
}

type JournalRow = (
    u32,
    String,
    String,
    u64,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
);

fn validate_journal_event_identity(
    event: &DdlEvent,
    row: &JournalRow,
    evidence_kind: &str,
) -> Result<(), String> {
    let mismatches = identity_mismatches(event, row);
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "automatic DDL journal {evidence_kind} identity mismatch at {}:{}: {}",
            event.binlog_file,
            event.event_start_position,
            mismatches.join(", "),
        ))
    }
}

fn identity_mismatches(event: &DdlEvent, row: &JournalRow) -> Vec<&'static str> {
    let mut mismatches = Vec::new();
    if row.0 != event.source_server_id {
        mismatches.push("source_server_id");
    }
    if row.4 != event.schema_name {
        mismatches.push("schema_name");
    }
    if row.2 != event.raw_sql {
        mismatches.push("raw_sql");
    }
    if row.3 != event.event_end_position {
        mismatches.push("event_end_position");
    }
    mismatches
}

fn parse_barrier(row: (String, u64, String)) -> Result<JournalBarrier, String> {
    let (binlog_file, event_start_position, status) = row;
    Ok(JournalBarrier {
        binlog_file,
        event_start_position,
        status: parse_status(&status)?,
    })
}

fn parse_status(status: &str) -> Result<DdlReplayStatus, String> {
    match status {
        "translation_pending" => Ok(DdlReplayStatus::TranslationPending),
        "prepared" => Ok(DdlReplayStatus::Prepared),
        "applied" => Ok(DdlReplayStatus::Applied),
        "checkpointed" => Ok(DdlReplayStatus::Checkpointed),
        "blocked" => Ok(DdlReplayStatus::Blocked),
        other => Err(format!("unknown automatic DDL journal status `{other}`")),
    }
}

pub fn build_translation_pending_sql(table: &str, event: &DdlEvent) -> String {
    format!(
        "INSERT INTO {} (source_identity,source_server_id,binlog_file,event_start_position,event_end_position,schema_name,raw_sql,transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state,status) VALUES ({},{},{},{},{},{},{},'translator-unavailable',NULL,'','','','translation_pending')",
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

pub fn build_promote_translation_sql(
    table: &str,
    event: &DdlEvent,
    evidence: &DdlSemanticEvidence,
) -> String {
    format!(
        "UPDATE {} SET transformation_version={},generated_sql={},canonical_ast={},pre_state={},expected_post_state={},status='prepared' WHERE source_identity={} AND source_server_id={} AND binlog_file={} AND event_start_position={} AND event_end_position={} AND schema_name={} AND raw_sql={} AND status='translation_pending'",
        quote_identifier_path(table),
        quote_sql_literal(&evidence.transformation_version),
        evidence
            .generated_sql
            .as_deref()
            .map(quote_sql_literal)
            .unwrap_or_else(|| "NULL".to_string()),
        quote_sql_literal(&evidence.canonical_ast),
        quote_sql_literal(&evidence.pre_state),
        quote_sql_literal(&evidence.expected_post_state),
        quote_sql_literal(&event.source_identity),
        event.source_server_id,
        quote_sql_literal(&event.binlog_file),
        event.event_start_position,
        event.event_end_position,
        quote_sql_literal(&event.schema_name),
        quote_sql_literal(&event.raw_sql),
    )
}

pub fn build_prepare_sql(table: &str, event: &DdlEvent, evidence: &DdlSemanticEvidence) -> String {
    format!(
        "INSERT INTO {} (source_identity,source_server_id,binlog_file,event_start_position,event_end_position,schema_name,raw_sql,transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state,status) VALUES ({},{},{},{},{},{},{},{},{},{},{},{},'prepared')",
        quote_identifier_path(table),
        quote_sql_literal(&event.source_identity),
        event.source_server_id,
        quote_sql_literal(&event.binlog_file),
        event.event_start_position,
        event.event_end_position,
        quote_sql_literal(&event.schema_name),
        quote_sql_literal(&event.raw_sql),
        quote_sql_literal(&evidence.transformation_version),
        evidence
            .generated_sql
            .as_deref()
            .map(quote_sql_literal)
            .unwrap_or_else(|| "NULL".to_string()),
        quote_sql_literal(&evidence.canonical_ast),
        quote_sql_literal(&evidence.pre_state),
        quote_sql_literal(&evidence.expected_post_state),
    )
}

pub fn build_barrier_select_sql(table: &str, source_identity: &str) -> String {
    let escaped_identity = source_identity
        .replace('=', "==")
        .replace('%', "=%")
        .replace('_', "=_");
    let pattern = format!("{escaped_identity}#server-id=%");
    format!(
        "SELECT binlog_file,event_start_position,status FROM {} WHERE source_identity LIKE {} ESCAPE '=' AND status IN ('translation_pending','prepared','blocked') ORDER BY binlog_file,event_start_position LIMIT 1",
        quote_identifier_path(table),
        quote_sql_literal(&pattern),
    )
}

pub fn build_status_select_sql(table: &str, event: &DdlEvent) -> String {
    format!(
        "SELECT source_server_id,status,raw_sql,event_end_position,schema_name,transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state FROM {} WHERE source_identity={} AND binlog_file={} AND event_start_position={} LIMIT 1",
        quote_identifier_path(table),
        quote_sql_literal(&event.source_identity),
        quote_sql_literal(&event.binlog_file),
        event.event_start_position,
    )
}

pub fn build_transition_sql(
    table: &str,
    event: &DdlEvent,
    from: DdlReplayStatus,
    to: DdlReplayStatus,
) -> String {
    format!(
        "UPDATE {} SET status='{}' WHERE source_identity={} AND source_server_id={} AND binlog_file={} AND event_start_position={} AND event_end_position={} AND schema_name={} AND raw_sql={} AND status='{}'",
        quote_identifier_path(table),
        to.as_str(),
        quote_sql_literal(&event.source_identity),
        event.source_server_id,
        quote_sql_literal(&event.binlog_file),
        event.event_start_position,
        event.event_end_position,
        quote_sql_literal(&event.schema_name),
        quote_sql_literal(&event.raw_sql),
        from.as_str(),
    )
}

pub fn replay_action(
    event: &DdlEvent,
    status: Option<DdlReplayStatus>,
) -> Result<super::DdlReplayAction, String> {
    match status {
        None | Some(DdlReplayStatus::TranslationPending) => {
            Ok(super::DdlReplayAction::PrepareAndExecute)
        }
        Some(DdlReplayStatus::Prepared) => Err(format!(
            "ambiguous automatic DDL at {}:{}; inspect target state before resolving journal entry",
            event.binlog_file, event.event_start_position
        )),
        Some(DdlReplayStatus::Blocked) => Err(format!(
            "blocked automatic DDL at {}:{} requires operator resolution",
            event.binlog_file, event.event_start_position
        )),
        Some(DdlReplayStatus::Applied) => Ok(super::DdlReplayAction::CheckpointOnly),
        Some(DdlReplayStatus::Checkpointed) => Ok(super::DdlReplayAction::AlreadyCheckpointed),
    }
}
