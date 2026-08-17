use crate::inventory::{ColumnInventory, SchemaInventory, TableInventory};
use crate::live::TargetMySqlConfig;
use crate::lost_binlog_recovery::{
    ResyncStreamConfig, resync_sync_config, resync_table_counts,
};
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::sync::SyncChunkProgress;

#[test]
fn resync_stream_builds_one_unified_all_table_configuration() {
    let config = resync_config();
    let inventory = inventory(["parents", "children"]);

    let unified = resync_sync_config(&config, &inventory);

    assert_eq!(unified.source.host, config.source.host);
    assert_eq!(unified.target.host, config.target.host);
    assert_eq!(unified.tables, ["parents", "children"]);
    assert_eq!(unified.chunk_size, 500);
    assert_eq!(unified.parallelism, 4);
    assert_eq!(unified.progress_table, "control.sync_runs");
    assert_eq!(
        unified.run_id.as_deref(),
        Some("resync-stream:source-incarnation")
    );
    assert_eq!(unified.run_id_prefix, None);
}

#[test]
fn resync_stream_reports_all_compared_tables_and_only_changed_tables_as_repaired() {
    let rows = vec![
        progress("parents", 0, 0, 0),
        progress("children", 2, 1, 3),
        progress("profiles", 0, 4, 0),
    ];

    assert_eq!(resync_table_counts(&rows), (2, 3));
}

fn resync_config() -> ResyncStreamConfig {
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
    ResyncStreamConfig {
        source,
        source_identity: "source-incarnation".to_string(),
        target,
        checkpoint_table: "cdc.stream_checkpoint".to_string(),
        progress_table: "control.sync_runs".to_string(),
        chunk_size: 500,
        parallelism: 4,
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
        collation: Some("utf8mb4_0900_ai_ci".to_string()),
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

fn progress(table: &str, inserts: u64, updates: u64, deletes: u64) -> SyncChunkProgress {
    SyncChunkProgress {
        run_id: "resync-stream:source-incarnation".to_string(),
        table: table.to_string(),
        run_spec_json: "{}".to_string(),
        last_primary_key: None,
        complete: true,
        chunks: 1,
        rows_scanned: 1,
        inserts,
        updates,
        deletes,
    }
}
