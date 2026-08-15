use crate::checkpoint::Checkpoint;
use crate::live::TargetMySqlConfig;
use crate::lost_binlog_recovery::{
    LostBinlogBarrier, LostBinlogReconciliationProof, LostBinlogRecoveryRecord,
    LostBinlogRecoveryStatus, LostBinlogRecoveryStore,
};
use crate::mysql_client::PersistentTargetExecutor;
#[cfg(not(test))]
use crate::mysql_support::{quote_identifier_path, quote_sql_literal};
use crate::target::TransactionalTargetExecutor;

pub const DEFAULT_RECOVERY_TABLE: &str = "cdc.stream_recovery_records";

#[cfg(test)]
fn quote_identifier_path(identifier: &str) -> String {
    identifier
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[cfg(test)]
fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRecoveryRecord<'a> {
    pub recovery_id: &'a str,
    pub checkpoint_name: &'a str,
    pub source_identity: &'a str,
    pub scope_hash: &'a str,
    pub old_checkpoint_json: &'a str,
    pub new_checkpoint_json: &'a str,
    pub old_barrier_source_identity: &'a str,
    pub old_barrier_file: &'a str,
    pub old_barrier_start_position: u64,
    pub old_barrier_end_position: u64,
    pub old_barrier_raw_sql: &'a str,
    pub operator_identity: &'a str,
    pub reason: &'a str,
    pub prepared_evidence_json: &'a str,
}

pub fn build_insert_prepared_recovery_sql(
    table: &str,
    record: &PreparedRecoveryRecord<'_>,
) -> String {
    let raw_sql = quote_sql_literal(record.old_barrier_raw_sql);
    format!(
        "INSERT INTO {} (recovery_id,checkpoint_name,source_identity,scope_hash,old_checkpoint_json,new_checkpoint_json,old_barrier_source_identity,old_barrier_file,old_barrier_start_position,old_barrier_end_position,old_barrier_raw_sql,operator_identity,reason,prepared_evidence_json,status) VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{},'prepared')",
        quote_identifier_path(table),
        quote_sql_literal(record.recovery_id),
        quote_sql_literal(record.checkpoint_name),
        quote_sql_literal(record.source_identity),
        quote_sql_literal(record.scope_hash),
        quote_sql_literal(record.old_checkpoint_json),
        quote_sql_literal(record.new_checkpoint_json),
        quote_sql_literal(record.old_barrier_source_identity),
        quote_sql_literal(record.old_barrier_file),
        record.old_barrier_start_position,
        record.old_barrier_end_position,
        raw_sql,
        quote_sql_literal(record.operator_identity),
        quote_sql_literal(record.reason),
        quote_sql_literal(record.prepared_evidence_json),
    )
}

#[cfg(test)]
pub fn build_checkpoint_cas_select_sql(table: &str, checkpoint_name: &str) -> String {
    format!(
        "SELECT checkpoint_json FROM {} WHERE checkpoint_name = {} LIMIT 1 FOR UPDATE",
        quote_identifier_path(table),
        quote_sql_literal(checkpoint_name),
    )
}

pub fn build_barrier_cas_select_sql(
    table: &str,
    source_identity: &str,
    binlog_file: &str,
    event_start_position: u64,
    event_end_position: u64,
    raw_sql: &str,
) -> String {
    format!(
        "SELECT source_identity,binlog_file,event_start_position,event_end_position,raw_sql,status FROM {} WHERE source_identity = {} AND binlog_file = {} AND event_start_position = {} AND event_end_position = {} AND raw_sql = {} AND status IN ('translation_pending','blocked') LIMIT 1 FOR UPDATE",
        quote_identifier_path(table),
        quote_sql_literal(source_identity),
        quote_sql_literal(binlog_file),
        event_start_position,
        event_end_position,
        quote_sql_literal(raw_sql),
    )
}

pub fn build_recovery_cas_select_sql(table: &str, recovery_id: &str) -> String {
    format!(
        "SELECT recovery_id,checkpoint_name,source_identity,scope_hash,operator_identity,reason,prepared_evidence_json,old_checkpoint_json,new_checkpoint_json,old_barrier_source_identity,old_barrier_file,old_barrier_start_position,old_barrier_end_position,old_barrier_raw_sql,status,abandoned_evidence_json,CAST(abandoned_at AS CHAR) FROM {} WHERE recovery_id = {} LIMIT 1 FOR UPDATE",
        quote_identifier_path(table),
        quote_sql_literal(recovery_id),
    )
}

pub fn build_barrier_recovery_owner_select_sql(table: &str, barrier: &LostBinlogBarrier) -> String {
    format!(
        "SELECT recovery_id,checkpoint_name,source_identity,scope_hash,operator_identity,reason,prepared_evidence_json,old_checkpoint_json,new_checkpoint_json,old_barrier_source_identity,old_barrier_file,old_barrier_start_position,old_barrier_end_position,old_barrier_raw_sql,status,abandoned_evidence_json,CAST(abandoned_at AS CHAR) FROM {} WHERE old_barrier_source_identity = {} AND old_barrier_file = {} AND old_barrier_start_position = {} AND old_barrier_end_position = {} AND old_barrier_raw_sql = {} ORDER BY CASE status WHEN 'prepared' THEN 0 WHEN 'committed' THEN 1 WHEN 'verified' THEN 2 ELSE 3 END,recovery_id LIMIT 1 FOR UPDATE",
        quote_identifier_path(table),
        quote_sql_literal(&barrier.source_identity),
        quote_sql_literal(&barrier.binlog_file),
        barrier.event_start_position,
        barrier.event_end_position,
        quote_sql_literal(&barrier.raw_sql),
    )
}

pub fn build_abandon_recovery_sql(
    table: &str,
    recovery: &LostBinlogRecoveryRecord,
    _replacement_recovery_id: &str,
    evidence_json: &str,
) -> String {
    format!(
        "UPDATE {} SET status = 'abandoned', abandoned_evidence_json = {}, abandoned_at = UTC_TIMESTAMP(6) WHERE recovery_id = {} AND checkpoint_name = {} AND source_identity = {} AND scope_hash = {} AND old_barrier_source_identity = {} AND old_barrier_file = {} AND old_barrier_start_position = {} AND old_barrier_end_position = {} AND old_barrier_raw_sql = {} AND status = 'prepared'",
        quote_identifier_path(table),
        quote_sql_literal(evidence_json),
        quote_sql_literal(&recovery.recovery_id),
        quote_sql_literal(&recovery.checkpoint_name),
        quote_sql_literal(&recovery.source_identity),
        quote_sql_literal(&recovery.scope_hash),
        quote_sql_literal(&recovery.expected_barrier.source_identity),
        quote_sql_literal(&recovery.expected_barrier.binlog_file),
        recovery.expected_barrier.event_start_position,
        recovery.expected_barrier.event_end_position,
        quote_sql_literal(&recovery.expected_barrier.raw_sql),
    )
}

#[cfg(test)]
pub fn build_checkpoint_update_sql(
    table: &str,
    checkpoint_name: &str,
    expected_checkpoint_json: &str,
    new_checkpoint_json: &str,
) -> String {
    format!(
        "UPDATE {} SET checkpoint_json = {} WHERE checkpoint_name = {} AND checkpoint_json = {}",
        quote_identifier_path(table),
        quote_sql_literal(new_checkpoint_json),
        quote_sql_literal(checkpoint_name),
        quote_sql_literal(expected_checkpoint_json),
    )
}

pub fn build_commit_recovery_sql(
    table: &str,
    recovery_id: &str,
    source_identity: &str,
    scope_hash: &str,
    evidence_json: &str,
) -> String {
    format!(
        "UPDATE {} SET status = 'committed', committed_evidence_json = {}, committed_at = UTC_TIMESTAMP(6) WHERE recovery_id = {} AND source_identity = {} AND scope_hash = {} AND status = 'prepared'",
        quote_identifier_path(table),
        quote_sql_literal(evidence_json),
        quote_sql_literal(recovery_id),
        quote_sql_literal(source_identity),
        quote_sql_literal(scope_hash),
    )
}

#[cfg(test)]
pub fn build_active_barrier_select_sql(
    journal_table: &str,
    recovery_table: &str,
    source_identity: &str,
) -> String {
    format!(
        "SELECT journal.binlog_file,journal.event_start_position,journal.status FROM {} journal WHERE journal.source_identity = {} AND journal.status IN ('translation_pending','prepared','blocked') AND NOT EXISTS (SELECT 1 FROM {} recovery WHERE recovery.status IN ('committed','verified') AND recovery.old_barrier_source_identity = journal.source_identity AND recovery.old_barrier_file = journal.binlog_file AND recovery.old_barrier_start_position = journal.event_start_position AND recovery.old_barrier_end_position = journal.event_end_position AND recovery.old_barrier_raw_sql_sha256 = SHA2(journal.raw_sql, 256)) ORDER BY journal.binlog_file,journal.event_start_position LIMIT 1",
        quote_identifier_path(journal_table),
        quote_sql_literal(source_identity),
        quote_identifier_path(recovery_table),
    )
}

pub struct MySqlLostBinlogRecoveryStore {
    executor: PersistentTargetExecutor,
    checkpoint_table: String,
    journal_table: String,
    recovery_table: String,
}

impl MySqlLostBinlogRecoveryStore {
    pub fn new(
        target: &TargetMySqlConfig,
        checkpoint_table: String,
        journal_table: String,
        recovery_table: String,
    ) -> Result<Self, String> {
        let executor = PersistentTargetExecutor::new(target).map_err(|error| error.to_string())?;
        Ok(Self {
            executor,
            checkpoint_table,
            journal_table,
            recovery_table,
        })
    }

    pub fn ensure(&self) -> Result<(), String> {
        let sql = format!(
            "SELECT recovery_id FROM {} LIMIT 0",
            quote_identifier_path(&self.recovery_table)
        );
        self.executor
            .query_rows_as_strings(&sql)
            .map_err(|error| format!("stream recovery table is unavailable: {error}"))?;
        Ok(())
    }

    fn query_optional_row(&self, sql: String) -> Result<Option<Vec<Option<String>>>, String> {
        let mut rows = self
            .executor
            .query_rows_as_strings(&sql)
            .map_err(|error| error.to_string())?;
        if rows.len() > 1 {
            return Err("lost-binlog recovery CAS query returned multiple rows".to_string());
        }
        Ok(rows.pop())
    }

    fn execute_and_require_one(&self, sql: String, operation: &str) -> Result<(), String> {
        self.executor
            .execute_raw_sql(&sql)
            .map_err(|error| error.to_string())?;
        let rows = self
            .executor
            .query_rows_as_strings("SELECT ROW_COUNT()")
            .map_err(|error| error.to_string())?;
        let affected = rows
            .first()
            .and_then(|row| row.first())
            .and_then(Clone::clone)
            .ok_or_else(|| format!("{operation} did not return ROW_COUNT()"))?
            .parse::<u64>()
            .map_err(|error| format!("invalid {operation} ROW_COUNT(): {error}"))?;
        if affected != 1 {
            return Err(format!(
                "{operation} affected {affected} rows instead of one"
            ));
        }
        Ok(())
    }
}

impl LostBinlogRecoveryStore for MySqlLostBinlogRecoveryStore {
    fn acquire_stream_lease(&self, lease_name: &str) -> Result<(), String> {
        TransactionalTargetExecutor::acquire_stream_lease(&self.executor, lease_name)
            .map_err(|error| error.to_string())
    }

    fn begin_transaction(&self) -> Result<(), String> {
        TransactionalTargetExecutor::begin_transaction(&self.executor)
            .map_err(|error| error.to_string())
    }

    fn load_checkpoint_for_update(
        &self,
        checkpoint_name: &str,
    ) -> Result<Option<Checkpoint>, String> {
        TransactionalTargetExecutor::load_transaction_checkpoint_for_update(
            &self.executor,
            &self.checkpoint_table,
            checkpoint_name,
        )
        .map_err(|error| error.to_string())
    }

    fn load_barrier_for_update(
        &self,
        barrier: &LostBinlogBarrier,
    ) -> Result<Option<LostBinlogBarrier>, String> {
        let row = self.query_optional_row(build_barrier_cas_select_sql(
            &self.journal_table,
            &barrier.source_identity,
            &barrier.binlog_file,
            barrier.event_start_position,
            barrier.event_end_position,
            &barrier.raw_sql,
        ))?;
        row.map(parse_barrier_row).transpose()
    }

    fn load_recovery_for_update(
        &self,
        recovery_id: &str,
    ) -> Result<Option<LostBinlogRecoveryRecord>, String> {
        let row = self.query_optional_row(build_recovery_cas_select_sql(
            &self.recovery_table,
            recovery_id,
        ))?;
        row.map(parse_recovery_row).transpose()
    }

    fn load_barrier_recovery_owner_for_update(
        &self,
        barrier: &LostBinlogBarrier,
    ) -> Result<Option<LostBinlogRecoveryRecord>, String> {
        let row = self.query_optional_row(build_barrier_recovery_owner_select_sql(
            &self.recovery_table,
            barrier,
        ))?;
        row.map(parse_recovery_row).transpose()
    }

    fn mark_recovery_abandoned(
        &self,
        recovery: &LostBinlogRecoveryRecord,
        replacement_recovery_id: &str,
        evidence_json: &str,
    ) -> Result<(), String> {
        let sql = build_abandon_recovery_sql(
            &self.recovery_table,
            recovery,
            replacement_recovery_id,
            evidence_json,
        );
        self.execute_and_require_one(sql, "abandon lost-binlog recovery")
    }

    fn insert_prepared_recovery(&self, recovery: &LostBinlogRecoveryRecord) -> Result<(), String> {
        let old_checkpoint_json = serde_json::to_string(&recovery.expected_checkpoint)
            .map_err(|error| format!("encode old recovery checkpoint: {error}"))?;
        let new_checkpoint_json = serde_json::to_string(&recovery.new_checkpoint)
            .map_err(|error| format!("encode new recovery checkpoint: {error}"))?;
        let sql = build_insert_prepared_recovery_sql(
            &self.recovery_table,
            &PreparedRecoveryRecord {
                recovery_id: &recovery.recovery_id,
                checkpoint_name: &recovery.checkpoint_name,
                source_identity: &recovery.source_identity,
                scope_hash: &recovery.scope_hash,
                old_checkpoint_json: &old_checkpoint_json,
                new_checkpoint_json: &new_checkpoint_json,
                old_barrier_source_identity: &recovery.expected_barrier.source_identity,
                old_barrier_file: &recovery.expected_barrier.binlog_file,
                old_barrier_start_position: recovery.expected_barrier.event_start_position,
                old_barrier_end_position: recovery.expected_barrier.event_end_position,
                old_barrier_raw_sql: &recovery.expected_barrier.raw_sql,
                operator_identity: &recovery.operator_identity,
                reason: &recovery.reason,
                prepared_evidence_json: &recovery.prepared_evidence_json,
            },
        );
        self.execute_and_require_one(sql, "prepare lost-binlog recovery")
    }

    fn save_checkpoint(
        &self,
        checkpoint_name: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), String> {
        TransactionalTargetExecutor::save_transaction_checkpoint(
            &self.executor,
            &self.checkpoint_table,
            checkpoint_name,
            checkpoint,
        )
        .map_err(|error| error.to_string())
    }

    fn mark_recovery_committed(
        &self,
        recovery_id: &str,
        proof: &LostBinlogReconciliationProof,
    ) -> Result<(), String> {
        let sql = build_commit_recovery_sql(
            &self.recovery_table,
            recovery_id,
            &proof.source_identity,
            &proof.scope_hash,
            &proof.evidence_json,
        );
        self.execute_and_require_one(sql, "commit lost-binlog recovery")
    }

    fn commit_transaction(&self) -> Result<(), String> {
        TransactionalTargetExecutor::commit_transaction(&self.executor)
            .map_err(|error| error.to_string())
    }

    fn rollback_transaction(&self) -> Result<(), String> {
        TransactionalTargetExecutor::rollback_transaction(&self.executor)
            .map_err(|error| error.to_string())
    }
}

fn parse_barrier_row(row: Vec<Option<String>>) -> Result<LostBinlogBarrier, String> {
    Ok(LostBinlogBarrier {
        source_identity: required_row_value(&row, 0, "barrier source identity")?,
        binlog_file: required_row_value(&row, 1, "barrier file")?,
        event_start_position: parse_row_u64(&row, 2, "barrier start position")?,
        event_end_position: parse_row_u64(&row, 3, "barrier end position")?,
        raw_sql: required_row_value(&row, 4, "barrier raw SQL")?,
    })
}

fn parse_recovery_row(row: Vec<Option<String>>) -> Result<LostBinlogRecoveryRecord, String> {
    let status = parse_recovery_status(&row)?;
    let expected_checkpoint = decode_checkpoint(&row, 7, "old recovery checkpoint")?;
    let new_checkpoint = decode_checkpoint(&row, 8, "new recovery checkpoint")?;
    Ok(LostBinlogRecoveryRecord {
        recovery_id: required_row_value(&row, 0, "recovery ID")?,
        checkpoint_name: required_row_value(&row, 1, "checkpoint name")?,
        source_identity: required_row_value(&row, 2, "source identity")?,
        scope_hash: required_row_value(&row, 3, "scope hash")?,
        operator_identity: required_row_value(&row, 4, "operator identity")?,
        reason: required_row_value(&row, 5, "recovery reason")?,
        prepared_evidence_json: required_row_value(&row, 6, "prepared evidence")?,
        expected_checkpoint,
        expected_barrier: LostBinlogBarrier {
            source_identity: required_row_value(&row, 9, "barrier source identity")?,
            binlog_file: required_row_value(&row, 10, "barrier file")?,
            event_start_position: parse_row_u64(&row, 11, "barrier start position")?,
            event_end_position: parse_row_u64(&row, 12, "barrier end position")?,
            raw_sql: required_row_value(&row, 13, "barrier raw SQL")?,
        },
        new_checkpoint,
        status,
        abandoned_evidence_json: optional_row_value(&row, 15),
        abandoned_at: optional_row_value(&row, 16),
    })
}

fn parse_recovery_status(row: &[Option<String>]) -> Result<LostBinlogRecoveryStatus, String> {
    match required_row_value(row, 14, "recovery status")?.as_str() {
        "prepared" => Ok(LostBinlogRecoveryStatus::Prepared),
        "committed" => Ok(LostBinlogRecoveryStatus::Committed),
        "verified" => Ok(LostBinlogRecoveryStatus::Verified),
        "abandoned" => Ok(LostBinlogRecoveryStatus::Abandoned),
        value => Err(format!("unsupported lost-binlog recovery status {value}")),
    }
}

fn decode_checkpoint(
    row: &[Option<String>],
    index: usize,
    field: &str,
) -> Result<Checkpoint, String> {
    serde_json::from_str(&required_row_value(row, index, field)?)
        .map_err(|error| format!("decode {field}: {error}"))
}

fn parse_row_u64(row: &[Option<String>], index: usize, field: &str) -> Result<u64, String> {
    required_row_value(row, index, field)?
        .parse()
        .map_err(|error| format!("invalid {field}: {error}"))
}

fn required_row_value(row: &[Option<String>], index: usize, field: &str) -> Result<String, String> {
    row.get(index)
        .and_then(Clone::clone)
        .ok_or_else(|| format!("missing {field}"))
}

fn optional_row_value(row: &[Option<String>], index: usize) -> Option<String> {
    row.get(index).and_then(Clone::clone)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_prepared_recovery_as_immutable_record() {
        let sql = build_insert_prepared_recovery_sql(
            "cdc.stream_recovery_records",
            &PreparedRecoveryRecord {
                recovery_id: "recovery-1",
                checkpoint_name: "stream-binlog:source-1",
                source_identity: "source-1#server-id=3",
                scope_hash: "scope-sha256",
                old_checkpoint_json: "{\"source_file\":\"mysqld-bin.000001\"}",
                new_checkpoint_json: "{\"source_file\":\"mysqld-bin.000002\"}",
                old_barrier_source_identity: "source-1#server-id=3",
                old_barrier_file: "mysqld-bin.000001",
                old_barrier_start_position: 100,
                old_barrier_end_position: 200,
                old_barrier_raw_sql: "DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives",
                operator_identity: "operator@example.com",
                reason: "source binlog was purged",
                prepared_evidence_json: "{\"scope\":\"complete\"}",
            },
        );

        assert_eq!(
            sql,
            "INSERT INTO `cdc`.`stream_recovery_records` (recovery_id,checkpoint_name,source_identity,scope_hash,old_checkpoint_json,new_checkpoint_json,old_barrier_source_identity,old_barrier_file,old_barrier_start_position,old_barrier_end_position,old_barrier_raw_sql,operator_identity,reason,prepared_evidence_json,status) VALUES ('recovery-1','stream-binlog:source-1','source-1#server-id=3','scope-sha256','{\"source_file\":\"mysqld-bin.000001\"}','{\"source_file\":\"mysqld-bin.000002\"}','source-1#server-id=3','mysqld-bin.000001',100,200,'DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives','operator@example.com','source binlog was purged','{\"scope\":\"complete\"}','prepared')"
        );
        assert!(!sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    #[test]
    fn locks_exact_checkpoint_barrier_and_source_scope_rows_for_update() {
        assert_eq!(
            build_checkpoint_cas_select_sql("cdc.stream_checkpoint", "stream-binlog:source-1",),
            "SELECT checkpoint_json FROM `cdc`.`stream_checkpoint` WHERE checkpoint_name = 'stream-binlog:source-1' LIMIT 1 FOR UPDATE"
        );
        assert_eq!(
            build_barrier_cas_select_sql(
                "cdc.ddl_replay_journal",
                "source-1#server-id=3",
                "mysqld-bin.000001",
                100,
                200,
                "DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives",
            ),
            "SELECT source_identity,binlog_file,event_start_position,event_end_position,raw_sql,status FROM `cdc`.`ddl_replay_journal` WHERE source_identity = 'source-1#server-id=3' AND binlog_file = 'mysqld-bin.000001' AND event_start_position = 100 AND event_end_position = 200 AND raw_sql = 'DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives' AND status IN ('translation_pending','blocked') LIMIT 1 FOR UPDATE"
        );
        assert_eq!(
            build_recovery_cas_select_sql("cdc.stream_recovery_records", "recovery-1"),
            "SELECT recovery_id,checkpoint_name,source_identity,scope_hash,operator_identity,reason,prepared_evidence_json,old_checkpoint_json,new_checkpoint_json,old_barrier_source_identity,old_barrier_file,old_barrier_start_position,old_barrier_end_position,old_barrier_raw_sql,status,abandoned_evidence_json,CAST(abandoned_at AS CHAR) FROM `cdc`.`stream_recovery_records` WHERE recovery_id = 'recovery-1' LIMIT 1 FOR UPDATE"
        );
    }

    #[test]
    fn locks_exact_barrier_recovery_owner_and_abandons_only_prepared_identity() {
        let barrier = LostBinlogBarrier {
            source_identity: "source-1#server-id=3".to_string(),
            binlog_file: "mysqld-bin.000001".to_string(),
            event_start_position: 100,
            event_end_position: 200,
            raw_sql: "DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives".to_string(),
        };
        let owner_sql =
            build_barrier_recovery_owner_select_sql("cdc.stream_recovery_records", &barrier);
        assert!(owner_sql.contains("old_barrier_source_identity = 'source-1#server-id=3'"));
        assert!(owner_sql.contains("status WHEN 'prepared' THEN 0"));
        assert!(owner_sql.contains("LIMIT 1 FOR UPDATE"));

        let recovery = LostBinlogRecoveryRecord {
            recovery_id: "recovery-old".to_string(),
            checkpoint_name: "stream-binlog:source-1".to_string(),
            source_identity: barrier.source_identity.clone(),
            scope_hash: "scope-sha256".to_string(),
            operator_identity: "operator@example.com".to_string(),
            reason: "old reason".to_string(),
            prepared_evidence_json: "{\"prepared\":true}".to_string(),
            expected_checkpoint: Checkpoint {
                source_file: "mysqld-bin.000001".to_string(),
                source_position: 10,
                gtid: None,
                event_timestamp: 0,
                last_event: crate::checkpoint::LastEvent {
                    event_type: "QueryEvent".to_string(),
                    description: "old".to_string(),
                },
            },
            expected_barrier: barrier,
            new_checkpoint: Checkpoint {
                source_file: "mysqld-bin.000002".to_string(),
                source_position: 20,
                gtid: None,
                event_timestamp: 0,
                last_event: crate::checkpoint::LastEvent {
                    event_type: "QueryEvent".to_string(),
                    description: "new".to_string(),
                },
            },
            status: LostBinlogRecoveryStatus::Prepared,
            abandoned_evidence_json: None,
            abandoned_at: None,
        };
        let abandon_sql = build_abandon_recovery_sql(
            "cdc.stream_recovery_records",
            &recovery,
            "recovery-new",
            "{\"old_recovery_id\":\"recovery-old\",\"replacement_recovery_id\":\"recovery-new\"}",
        );
        assert!(abandon_sql.contains("status = 'abandoned'"));
        assert!(abandon_sql.contains("abandoned_at = UTC_TIMESTAMP(6)"));
        assert!(abandon_sql.contains("status = 'prepared'"));
        assert!(!abandon_sql.contains("abandoned_at = '"));
    }

    #[test]
    fn parses_abandoned_recovery_evidence_and_server_timestamp() {
        let row = vec![
            Some("recovery-old".to_string()),
            Some("stream-binlog:source-1".to_string()),
            Some("source-1#server-id=3".to_string()),
            Some("scope-sha256".to_string()),
            Some("operator@example.com".to_string()),
            Some("old reason".to_string()),
            Some("{\"prepared\":true}".to_string()),
            Some("{\"source_file\":\"mysqld-bin.000001\",\"source_position\":10,\"gtid\":null,\"event_timestamp\":0,\"last_event\":{\"event_type\":\"QueryEvent\",\"description\":\"old\"}}".to_string()),
            Some("{\"source_file\":\"mysqld-bin.000002\",\"source_position\":20,\"gtid\":null,\"event_timestamp\":0,\"last_event\":{\"event_type\":\"QueryEvent\",\"description\":\"new\"}}".to_string()),
            Some("source-1#server-id=3".to_string()),
            Some("mysqld-bin.000001".to_string()),
            Some("100".to_string()),
            Some("200".to_string()),
            Some("DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives".to_string()),
            Some("abandoned".to_string()),
            Some("{\"old_recovery_id\":\"recovery-old\"}".to_string()),
            Some("2026-08-13 01:02:03.000000".to_string()),
        ];
        let parsed = parse_recovery_row(row).expect("abandoned recovery row parses");
        assert_eq!(parsed.status, LostBinlogRecoveryStatus::Abandoned);
        assert_eq!(
            parsed.abandoned_evidence_json.as_deref(),
            Some("{\"old_recovery_id\":\"recovery-old\"}")
        );
        assert_eq!(
            parsed.abandoned_at.as_deref(),
            Some("2026-08-13 01:02:03.000000")
        );
    }

    #[test]
    fn builds_exact_checkpoint_and_recovery_commit_updates() {
        assert_eq!(
            build_checkpoint_update_sql(
                "cdc.stream_checkpoint",
                "stream-binlog:source-1",
                "{\"source_file\":\"mysqld-bin.000001\"}",
                "{\"source_file\":\"mysqld-bin.000002\"}",
            ),
            "UPDATE `cdc`.`stream_checkpoint` SET checkpoint_json = '{\"source_file\":\"mysqld-bin.000002\"}' WHERE checkpoint_name = 'stream-binlog:source-1' AND checkpoint_json = '{\"source_file\":\"mysqld-bin.000001\"}'"
        );
        assert_eq!(
            build_commit_recovery_sql(
                "cdc.stream_recovery_records",
                "recovery-1",
                "source-1#server-id=3",
                "scope-sha256",
                "{\"schema\":\"converged\",\"data\":\"converged\"}",
            ),
            "UPDATE `cdc`.`stream_recovery_records` SET status = 'committed', committed_evidence_json = '{\"schema\":\"converged\",\"data\":\"converged\"}', committed_at = UTC_TIMESTAMP(6) WHERE recovery_id = 'recovery-1' AND source_identity = 'source-1#server-id=3' AND scope_hash = 'scope-sha256' AND status = 'prepared'"
        );
    }

    #[test]
    fn excludes_only_exact_committed_recovery_barrier() {
        let sql = build_active_barrier_select_sql(
            "cdc.ddl_replay_journal",
            "cdc.stream_recovery_records",
            "source-1#server-id=3",
        );

        assert_eq!(
            sql,
            "SELECT journal.binlog_file,journal.event_start_position,journal.status FROM `cdc`.`ddl_replay_journal` journal WHERE journal.source_identity = 'source-1#server-id=3' AND journal.status IN ('translation_pending','prepared','blocked') AND NOT EXISTS (SELECT 1 FROM `cdc`.`stream_recovery_records` recovery WHERE recovery.status IN ('committed','verified') AND recovery.old_barrier_source_identity = journal.source_identity AND recovery.old_barrier_file = journal.binlog_file AND recovery.old_barrier_start_position = journal.event_start_position AND recovery.old_barrier_end_position = journal.event_end_position AND recovery.old_barrier_raw_sql_sha256 = SHA2(journal.raw_sql, 256)) ORDER BY journal.binlog_file,journal.event_start_position LIMIT 1"
        );
    }
}
