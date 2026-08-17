use crate::checkpoint::{Checkpoint, LastEvent};
use crate::inventory::{ColumnInventory, SchemaInventory, TableInventory};
use crate::live::TargetMySqlConfig;
use crate::lost_binlog_recovery::{
    LostBinlogBarrier, LostBinlogRecoveryRequest, RecoverLostBinlogConfig,
    recovery_reconciliation_proof, recovery_sync_config,
};
use crate::mysql_config::MySqlConnectionConfig;
use crate::sync::SyncChunkProgress;
use std::path::PathBuf;

#[test]
fn recovery_builds_one_unified_full_scope_configuration() {
    let config = recovery_config();
    let request = recovery_request();
    let inventory = inventory(["parents", "children"]);

    let unified = recovery_sync_config(&config, &request, &inventory);

    assert_eq!(unified.source.host, config.source.host);
    assert_eq!(unified.target.host, config.target.host);
    assert_eq!(unified.tables, ["parents", "children"]);
    assert_eq!(unified.chunk_size, 500);
    assert_eq!(unified.parallelism, 1);
    assert_eq!(unified.progress_table, "control.sync_runs");
    assert_eq!(unified.run_id.as_deref(), Some("recovery-42"));
    assert_eq!(unified.run_id_prefix, None);
}

#[test]
fn recovery_proof_binds_exact_run_and_complete_table_scope() {
    let request = recovery_request();
    let inventory = inventory(["parents", "children"]);
    let rows = vec![
        progress("recovery-42", "parents", true, 0, 0, 0),
        progress("recovery-42", "children", true, 2, 1, 3),
    ];

    let proof = recovery_reconciliation_proof(&request, "scope-42", &inventory, &rows)
        .expect("complete unified recovery proof");

    assert_eq!(proof.recovery_id, "recovery-42");
    assert_eq!(proof.source_identity, "source-1#server-id=7");
    assert_eq!(proof.scope_hash, "scope-42");
    assert!(proof.schema_converged);
    assert!(proof.data_converged);
    assert!(proof.unsupported_scope.is_empty());
    let evidence: serde_json::Value =
        serde_json::from_str(&proof.evidence_json).expect("proof evidence JSON");
    assert_eq!(evidence["compared_tables"], 2);
    assert_eq!(evidence["repaired_tables"], 1);
}

#[test]
fn recovery_proof_rejects_incomplete_missing_or_wrong_run_progress() {
    let request = recovery_request();
    let inventory = inventory(["parents", "children"]);

    let incomplete = vec![
        progress("recovery-42", "parents", true, 0, 0, 0),
        progress("recovery-42", "children", false, 0, 0, 0),
    ];
    assert_eq!(
        recovery_reconciliation_proof(&request, "scope-42", &inventory, &incomplete)
            .expect_err("incomplete progress"),
        "unified recovery progress for table `children` is incomplete"
    );

    let missing = vec![progress("recovery-42", "parents", true, 0, 0, 0)];
    assert_eq!(
        recovery_reconciliation_proof(&request, "scope-42", &inventory, &missing)
            .expect_err("missing progress"),
        "unified recovery progress scope differs: missing=children unexpected="
    );

    let wrong_run = vec![
        progress("recovery-42", "parents", true, 0, 0, 0),
        progress("other-run", "children", true, 0, 0, 0),
    ];
    assert_eq!(
        recovery_reconciliation_proof(&request, "scope-42", &inventory, &wrong_run)
            .expect_err("wrong run progress"),
        "unified recovery progress run id mismatch for table `children`: expected `recovery-42`, found `other-run`"
    );
}

fn recovery_config() -> RecoverLostBinlogConfig {
    let mut source = MySqlConnectionConfig::default();
    source.host = "source".to_string();
    source.user = "source-user".to_string();
    source.password = "source-password".to_string();
    source.database = "source-db".to_string();
    let target = TargetMySqlConfig {
        host: "target".to_string(),
        user: "target-user".to_string(),
        password: "target-password".to_string(),
        database: "target-db".to_string(),
        tls_ca_file: "/tmp/target-ca.pem".to_string(),
        ..TargetMySqlConfig::default()
    };
    RecoverLostBinlogConfig {
        source,
        source_identity: "source-1".to_string(),
        target,
        authorization_file: PathBuf::from("authorization.json"),
        checkpoint_table: "cdc.stream_checkpoint".to_string(),
        journal_table: "cdc.ddl_replay_journal".to_string(),
        recovery_table: "cdc.stream_recovery_records".to_string(),
        progress_table: "control.sync_runs".to_string(),
        chunk_size: 500,
    }
}

fn recovery_request() -> LostBinlogRecoveryRequest {
    LostBinlogRecoveryRequest {
        recovery_id: "recovery-42".to_string(),
        checkpoint_name: "stream-binlog:source-1".to_string(),
        expected_checkpoint: Checkpoint {
            source_file: "mysqld-bin.000001".to_string(),
            source_position: 100,
            gtid: None,
            event_timestamp: 0,
            last_event: LastEvent {
                event_type: "fixture".to_string(),
                description: "fixture".to_string(),
            },
        },
        expected_barrier: LostBinlogBarrier {
            source_identity: "source-1#server-id=7".to_string(),
            binlog_file: "mysqld-bin.000001".to_string(),
            event_start_position: 100,
            event_end_position: 200,
            raw_sql: "DROP TRIGGER fixture".to_string(),
        },
        scope_hash: "scope-42".to_string(),
        operator_identity: "operator@example.com".to_string(),
        reason: "authorized recovery".to_string(),
        prepared_evidence_json: "{}".to_string(),
    }
}

fn inventory<const N: usize>(names: [&str; N]) -> SchemaInventory {
    SchemaInventory {
        schema: "source-db".to_string(),
        tables: names.into_iter().map(table).collect(),
        indexes: vec![],
        foreign_keys: vec![],
        views: vec![],
        triggers: vec![],
        routines: vec![],
        events: vec![],
    }
}

fn table(name: &str) -> TableInventory {
    TableInventory {
        name: name.to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        collation: None,
        primary_key: vec!["id".to_string()],
        columns: vec![ColumnInventory {
            name: "id".to_string(),
            ordinal_position: 1,
            column_type: "bigint".to_string(),
            data_type: "bigint".to_string(),
            is_nullable: false,
            character_set: None,
            collation: None,
            default_value: None,
            extra: String::new(),
            comment: String::new(),
            generated: None,
        }],
    }
}

fn progress(
    run_id: &str,
    table: &str,
    complete: bool,
    inserts: u64,
    updates: u64,
    deletes: u64,
) -> SyncChunkProgress {
    SyncChunkProgress {
        run_id: run_id.to_string(),
        table: table.to_string(),
        run_spec_json: "{}".to_string(),
        last_primary_key: None,
        complete,
        chunks: 1,
        rows_scanned: 1,
        inserts,
        updates,
        deletes,
    }
}
