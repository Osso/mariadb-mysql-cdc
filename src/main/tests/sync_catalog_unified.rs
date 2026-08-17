use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::table_catalog::{
    CatalogConnectionConfig, SyncCatalogConfig, SyncableCatalog, SyncableTableEntry,
    sync_config_from_catalog,
};
use std::path::PathBuf;

#[test]
fn sync_catalog_builds_one_unified_sync_configuration() {
    let source = MySqlConnectionConfig {
        host: "source".to_string(),
        port: 3307,
        user: "source-user".to_string(),
        password: "source-password".to_string(),
        database: "source-db".to_string(),
    };
    let target = TargetMySqlConfig {
        host: "target".to_string(),
        port: 25060,
        user: "target-user".to_string(),
        password: "target-password".to_string(),
        database: "target-db".to_string(),
        tls_ca_file: "/tmp/target-ca.pem".to_string(),
        ..TargetMySqlConfig::default()
    };
    let config = SyncCatalogConfig {
        connections: CatalogConnectionConfig {
            source: source.clone(),
            target: target.clone(),
        },
        catalog: PathBuf::from("catalog.json"),
        progress_table: "control.sync_runs".to_string(),
        run_id_prefix: "nightly".to_string(),
        chunk_size: 500,
    };
    let catalog = SyncableCatalog {
        tables: vec![entry("parents"), entry("children")],
    };

    let unified = sync_config_from_catalog(&config, &catalog);

    assert_eq!(unified.source.host, source.host);
    assert_eq!(unified.source.port, source.port);
    assert_eq!(unified.target.host, target.host);
    assert_eq!(unified.target.port, target.port);
    assert_eq!(unified.tables, ["parents", "children"]);
    assert_eq!(unified.chunk_size, 500);
    assert_eq!(unified.parallelism, 16);
    assert_eq!(unified.progress_table, "control.sync_runs");
    assert_eq!(unified.run_id, None);
    assert_eq!(unified.run_id_prefix.as_deref(), Some("nightly"));
}

fn entry(name: &str) -> SyncableTableEntry {
    SyncableTableEntry {
        name: name.to_string(),
        primary_key: vec!["id".to_string()],
        primary_key_ordering: vec![],
        columns: vec!["id".to_string()],
        estimated_source_rows: 1,
        parent_dependencies: vec![],
    }
}
