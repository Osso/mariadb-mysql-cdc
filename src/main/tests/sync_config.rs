use crate::inventory::{ColumnInventory, GeneratedColumn, TableInventory};
use crate::sync::{
    SyncConfig, SyncPrimaryKeyOrdering, SyncTable, build_sync_run_identity,
    sync_table_from_inventory, validate_sync_config,
};

#[test]
fn sync_config_preserves_exact_run_id_across_invocation_changes() {
    let config = exact_run_config();
    let first = build_sync_run_identity(
        &config,
        vec![sync_table("zeta", "zeta_id"), sync_table("alpha", "alpha_id")],
    )
    .expect("first exact run identity");

    let mut changed = config;
    changed.source.host = "replacement-source.example".to_string();
    changed.source.database = "replacement_source".to_string();
    changed.target.host = "10.20.30.40".to_string();
    changed.target.database = "replacement_target".to_string();
    changed.target.tls_ca_file = "/tmp/replacement-ca.pem".to_string();
    changed.chunk_size = 37;
    changed.parallelism = 16;
    changed.progress_table = "other.sync_progress".to_string();
    changed.tables = strings(["replacement"]);
    let changed_tables = vec![sync_table("replacement", "replacement_id")];
    let resumed = build_sync_run_identity(&changed, changed_tables).expect("changed invocation");

    assert_eq!(first.run_id, "sync-run-42");
    assert_eq!(resumed, first);
}

#[test]
fn sync_config_preserves_known_v1_prefix_derived_run_id() {
    let config = prefixed_run_config();
    let tables = vec![sync_table("alpha", "alpha_id"), sync_table("zeta", "zeta_id")];

    let identity = build_sync_run_identity(&config, tables).expect("prefixed identity");

    assert_eq!(
        identity.run_id,
        "sync-v1-4badcffdeb82f66e8a205b63aebeded3079295bb22658ce4e59b9d8b163ef751"
    );
}

#[test]
fn sync_config_rejects_invalid_connections_scope_progress_and_identity() {
    let valid = exact_run_config();
    validate_sync_config(&valid).expect("valid config");

    assert_config_error(valid.clone(), |config| config.source.host.clear(), "source host is required");
    assert_config_error(valid.clone(), |config| config.source.user.clear(), "source user is required");
    assert_config_error(
        valid.clone(),
        |config| config.source.password.clear(),
        "source password is required",
    );
    assert_config_error(
        valid.clone(),
        |config| config.source.database.clear(),
        "source database is required",
    );
    assert_config_error(valid.clone(), |config| config.target.host.clear(), "target host is required");
    assert_config_error(valid.clone(), |config| config.target.user.clear(), "target user is required");
    assert_config_error(
        valid.clone(),
        |config| config.target.password.clear(),
        "target password is required",
    );
    assert_config_error(
        valid.clone(),
        |config| config.target.database.clear(),
        "target database is required",
    );
    assert_config_error(
        valid.clone(),
        |config| config.target.tls_ca_file.clear(),
        "target TLS CA file is required",
    );
    assert_config_error(valid.clone(), |config| config.tables.clear(), "at least one table is required");
    assert_config_error(valid.clone(), |config| config.chunk_size = 0, "chunk size must be greater than zero");
    assert_config_error(valid.clone(), |config| config.parallelism = 0, "parallelism must be greater than zero");

    for progress_table in ["sync_runs", ".sync_runs", "cdc.", "cdc.sync.runs"] {
        let mut config = valid.clone();
        config.progress_table = progress_table.to_string();
        assert_eq!(
            validate_sync_config(&config).expect_err("invalid progress table"),
            "progress table must be exactly schema-qualified with nonempty parts"
        );
    }

    let mut duplicate_tables = valid.clone();
    duplicate_tables.tables = strings(["episodes", "episodes"]);
    assert_eq!(
        validate_sync_config(&duplicate_tables).expect_err("duplicate selected table"),
        "selected table `episodes` is duplicated"
    );

    let mut both = valid.clone();
    both.run_id_prefix = Some("scheduled".to_string());
    assert_eq!(
        validate_sync_config(&both).expect_err("both run identities"),
        "exactly one of run_id or run_id_prefix is required"
    );

    let mut neither = valid.clone();
    neither.run_id = None;
    assert_eq!(
        validate_sync_config(&neither).expect_err("missing run identity"),
        "exactly one of run_id or run_id_prefix is required"
    );

    let mut empty_id = valid.clone();
    empty_id.run_id = Some(String::new());
    assert_eq!(
        validate_sync_config(&empty_id).expect_err("empty exact run id"),
        "run id is required"
    );

    let mut long_id = valid.clone();
    long_id.run_id = Some("é".repeat(65));
    assert_eq!(
        validate_sync_config(&long_id).expect_err("oversized exact run id"),
        "run id is 130 bytes; cdc.sync_runs.run_id allows at most 128"
    );

    let mut empty_prefix = prefixed_run_config();
    empty_prefix.run_id_prefix = Some(String::new());
    assert_eq!(
        validate_sync_config(&empty_prefix).expect_err("empty run id prefix"),
        "run id prefix is required"
    );
}

#[test]
fn sync_config_rejects_duplicate_concrete_tables() {
    let error = build_sync_run_identity(
        &exact_run_config(),
        vec![sync_table("episodes", "id"), sync_table("episodes", "id")],
    )
    .expect_err("duplicate concrete table");

    assert_eq!(error, "concrete sync table `episodes` is duplicated");
}

#[test]
fn sync_table_conversion_preserves_order_excludes_generated_columns_and_parses_enum_keys() {
    let table = inventory_table(
        vec!["id", "state"],
        vec![
            column("id", 1, "bigint unsigned", None),
            column("state", 2, "enum('draft','live','archived')", None),
            column("title", 3, "varchar(255)", None),
            column(
                "search_text",
                4,
                "text",
                Some(GeneratedColumn {
                    expression: "lower(`title`)".to_string(),
                    generation_kind: "STORED GENERATED".to_string(),
                }),
            ),
        ],
    );

    assert_eq!(
        sync_table_from_inventory(&table).expect("sync table"),
        SyncTable {
            name: "episodes".to_string(),
            primary_key: strings(["id", "state"]),
            primary_key_ordering: vec![
                SyncPrimaryKeyOrdering::Native,
                SyncPrimaryKeyOrdering::Enum(strings(["draft", "live", "archived"])),
            ],
            columns: strings(["id", "state", "title"]),
        }
    );
}

#[test]
fn sync_table_conversion_rejects_invalid_primary_keys_and_duplicate_columns() {
    let no_primary_key = inventory_table(Vec::new(), vec![column("id", 1, "bigint", None)]);
    assert_eq!(
        sync_table_from_inventory(&no_primary_key).expect_err("missing primary key"),
        "table `episodes` has no primary key"
    );

    let missing_primary_key = inventory_table(
        vec!["missing"],
        vec![column("id", 1, "bigint", None)],
    );
    assert_eq!(
        sync_table_from_inventory(&missing_primary_key).expect_err("absent primary key"),
        "primary-key column `missing` is absent from `episodes` inventory"
    );

    let generated_primary_key = inventory_table(
        vec!["generated_id"],
        vec![column(
            "generated_id",
            1,
            "bigint",
            Some(GeneratedColumn {
                expression: "1".to_string(),
                generation_kind: "STORED GENERATED".to_string(),
            }),
        )],
    );
    assert_eq!(
        sync_table_from_inventory(&generated_primary_key).expect_err("generated primary key"),
        "primary-key column `generated_id` is not writable in `episodes`"
    );

    let duplicate_columns = inventory_table(
        vec!["id"],
        vec![
            column("id", 1, "bigint", None),
            column("id", 2, "bigint", None),
        ],
    );
    assert_eq!(
        sync_table_from_inventory(&duplicate_columns).expect_err("duplicate columns"),
        "column `id` is duplicated in `episodes` inventory"
    );

    let duplicate_primary_key = inventory_table(
        vec!["id", "id"],
        vec![column("id", 1, "bigint", None)],
    );
    assert_eq!(
        sync_table_from_inventory(&duplicate_primary_key).expect_err("duplicate primary key"),
        "primary-key column `id` is duplicated in `episodes` inventory"
    );
}

fn exact_run_config() -> SyncConfig {
    SyncConfig {
        source: crate::mysql_config::MySqlConnectionConfig {
            host: "source-host".to_string(),
            port: 3307,
            user: "source-user".to_string(),
            password: "source-secret".to_string(),
            database: "source_database".to_string(),
        },
        target: crate::live::TargetMySqlConfig {
            host: "target-host".to_string(),
            port: 25060,
            user: "target-user".to_string(),
            password: "target-secret".to_string(),
            database: "target_database".to_string(),
            tls_ca_file: "/tmp/target-ca.pem".to_string(),
            insert_conflict_policy: crate::live::InsertConflictPolicy::Error,
        },
        tables: strings(["episodes"]),
        chunk_size: 500,
        parallelism: 4,
        progress_table: "cdc.sync_runs".to_string(),
        coordinator_session_wait_timeout_seconds: None,
        run_id: Some("sync-run-42".to_string()),
        run_id_prefix: None,
    }
}

fn prefixed_run_config() -> SyncConfig {
    let mut config = exact_run_config();
    config.run_id = None;
    config.run_id_prefix = Some("scheduled-sync".to_string());
    config
}

fn assert_config_error(
    mut config: SyncConfig,
    change: impl FnOnce(&mut SyncConfig),
    expected: &str,
) {
    change(&mut config);
    assert_eq!(validate_sync_config(&config).expect_err(expected), expected);
}

fn sync_table(name: &str, primary_key: &str) -> SyncTable {
    SyncTable {
        name: name.to_string(),
        primary_key: vec![primary_key.to_string()],
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns: vec![primary_key.to_string()],
    }
}

fn inventory_table(primary_key: Vec<&str>, columns: Vec<ColumnInventory>) -> TableInventory {
    TableInventory {
        name: "episodes".to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        collation: Some("utf8mb4_0900_ai_ci".to_string()),
        primary_key: primary_key.into_iter().map(str::to_string).collect(),
        columns,
    }
}

fn column(
    name: &str,
    ordinal_position: u32,
    column_type: &str,
    generated: Option<GeneratedColumn>,
) -> ColumnInventory {
    ColumnInventory {
        name: name.to_string(),
        ordinal_position,
        column_type: column_type.to_string(),
        data_type: column_type
            .split(['(', ' '])
            .next()
            .expect("column type has a data type")
            .to_string(),
        is_nullable: false,
        character_set: None,
        collation: None,
        default_value: None,
        extra: String::new(),
        comment: String::new(),
        generated,
    }
}

fn strings<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}
