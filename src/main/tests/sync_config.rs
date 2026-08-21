use crate::inventory::{ColumnInventory, GeneratedColumn, SchemaInventory, TableInventory};
use crate::sync::{
    SyncConfig, SyncPrimaryKeyOrdering, SyncRunSpec, SyncTable, build_sync_run_identity,
    plan_additive_run_spec_migration, sync_table_from_inventory, validate_sync_config,
};

#[test]
fn sync_config_builds_sorted_secret_free_spec_and_preserves_exact_run_id() {
    let config = exact_run_config();
    let identity = build_sync_run_identity(
        &config,
        vec![sync_table("zeta", "zeta_id"), sync_table("alpha", "alpha_id")],
    )
    .expect("valid exact sync run");

    assert_eq!(identity.run_id, "sync-run-42");
    assert_eq!(identity.run_spec.tables[0].name, "alpha");
    assert_eq!(identity.run_spec.tables[1].name, "zeta");
    assert_eq!(identity.run_spec.source.host, "source-host");
    assert_eq!(identity.run_spec.source.port, 3307);
    assert_eq!(identity.run_spec.source.database, "source_database");
    assert_eq!(identity.run_spec.target.host, "target-host");
    assert_eq!(identity.run_spec.target.port, 25060);
    assert_eq!(identity.run_spec.target.database, "target_database");
    assert_eq!(identity.run_spec.chunk_size, 500);
    assert_eq!(identity.run_spec.parallelism, 4);
    assert_eq!(identity.run_spec.progress_table, "cdc.sync_runs");
    assert_eq!(
        identity.run_spec_json,
        serde_json::to_string(&identity.run_spec).expect("serialize run spec")
    );
    for secret in [
        "source-secret",
        "target-secret",
        "/tmp/target-ca.pem",
        "source-user",
        "target-user",
    ] {
        assert!(
            !identity.run_spec_json.contains(secret),
            "run specification leaked `{secret}`: {}",
            identity.run_spec_json
        );
    }
}

#[test]
fn sync_config_derives_stable_domain_separated_run_ids_from_every_immutable_input() {
    let config = prefixed_run_config();
    let tables = vec![sync_table("alpha", "alpha_id"), sync_table("zeta", "zeta_id")];
    let first = build_sync_run_identity(&config, tables.clone()).expect("first identity");
    let reordered = build_sync_run_identity(
        &config,
        vec![sync_table("zeta", "zeta_id"), sync_table("alpha", "alpha_id")],
    )
    .expect("reordered identity");

    assert_eq!(first, reordered);
    assert!(first.run_id.starts_with("sync-v1-"));
    assert_eq!(first.run_id.len(), "sync-v1-".len() + 64);
    assert!(first.run_id.len() <= 128);

    let mut variants = Vec::new();

    let mut changed = config.clone();
    changed.source.host = "other-source".to_string();
    variants.push(build_sync_run_identity(&changed, tables.clone()).expect("source variant"));

    let mut changed = config.clone();
    changed.target.database = "other_target".to_string();
    variants.push(build_sync_run_identity(&changed, tables.clone()).expect("target variant"));

    let mut changed = config.clone();
    changed.chunk_size += 1;
    variants.push(build_sync_run_identity(&changed, tables.clone()).expect("chunk variant"));

    let mut changed = config.clone();
    changed.parallelism += 1;
    variants.push(build_sync_run_identity(&changed, tables.clone()).expect("parallel variant"));

    let mut changed = config.clone();
    changed.progress_table = "other.sync_runs".to_string();
    variants.push(build_sync_run_identity(&changed, tables.clone()).expect("progress variant"));

    let mut changed_tables = tables;
    changed_tables[0].columns.push("title".to_string());
    variants.push(build_sync_run_identity(&config, changed_tables).expect("table variant"));

    for variant in variants {
        assert_ne!(variant.run_id, first.run_id);
    }
}

#[test]
fn sync_additive_run_spec_migration_accepts_compatible_added_writable_columns() {
    let (persisted, current, source, target) = compatible_additive_migration();

    let plan = plan_additive_run_spec_migration(&persisted, &current, &source, &target)
        .expect("compatible additive migration");

    assert_eq!(plan.changed_tables.len(), 1);
    assert_eq!(plan.changed_tables[0].table, "alpha");
    assert_eq!(
        plan.changed_tables[0].added_columns,
        strings(["direct_seen_at", "sync_seen_at"])
    );
}

#[test]
fn sync_additive_run_spec_migration_rejects_endpoint_setting_and_scope_drift() {
    let (persisted, current, source, target) = compatible_additive_migration();
    let mut variants = Vec::new();

    let mut changed = current.clone();
    changed.source.host = "other-source".to_string();
    variants.push(("source endpoint changed", changed));

    let mut changed = current.clone();
    changed.target.database = "other-target".to_string();
    variants.push(("target endpoint changed", changed));

    let mut changed = current.clone();
    changed.chunk_size += 1;
    variants.push(("chunk size changed", changed));

    let mut changed = current.clone();
    changed.parallelism += 1;
    variants.push(("parallelism changed", changed));

    let mut changed = current.clone();
    changed.progress_table = "other.sync_runs".to_string();
    variants.push(("progress table changed", changed));

    let mut changed = current;
    changed
        .tables
        .push(migration_sync_table("gamma", &["id", "value"]));
    variants.push(("table scope or order changed", changed));

    for (expected, changed) in variants {
        assert_eq!(
            plan_additive_run_spec_migration(&persisted, &changed, &source, &target)
                .expect_err(expected),
            format!("additive run-spec migration {expected}")
        );
    }
}

#[test]
fn sync_additive_run_spec_migration_rejects_removed_or_reordered_writable_columns() {
    let (persisted, _, _, _) = compatible_additive_migration();

    let removed = migration_spec(vec![
        migration_sync_table("alpha", &["id", "direct_seen_at"]),
        migration_sync_table("beta", &["id", "value"]),
    ]);
    let removed_inventory = migration_inventory(vec![
        migration_inventory_table(
            "alpha",
            &["id"],
            &[("id", "bigint unsigned"), ("direct_seen_at", "timestamp")],
        ),
        migration_inventory_table(
            "beta",
            &["id"],
            &[("id", "bigint unsigned"), ("value", "varchar(255)")],
        ),
    ]);
    assert_eq!(
        plan_additive_run_spec_migration(
            &persisted,
            &removed,
            &removed_inventory,
            &removed_inventory,
        )
        .expect_err("removed column"),
        "additive run-spec migration table `alpha` removed writable columns: value"
    );

    let reordered = migration_spec(vec![
        migration_sync_table("alpha", &["value", "id", "direct_seen_at"]),
        migration_sync_table("beta", &["id", "value"]),
    ]);
    let reordered_inventory = migration_inventory(vec![
        migration_inventory_table(
            "alpha",
            &["id"],
            &[
                ("value", "varchar(255)"),
                ("id", "bigint unsigned"),
                ("direct_seen_at", "timestamp"),
            ],
        ),
        migration_inventory_table(
            "beta",
            &["id"],
            &[("id", "bigint unsigned"), ("value", "varchar(255)")],
        ),
    ]);
    assert_eq!(
        plan_additive_run_spec_migration(
            &persisted,
            &reordered,
            &reordered_inventory,
            &reordered_inventory,
        )
        .expect_err("reordered columns"),
        "additive run-spec migration table `alpha` reordered existing writable columns"
    );
}

#[test]
fn sync_additive_run_spec_migration_rejects_primary_key_and_ordering_drift() {
    let (persisted, current, source, target) = compatible_additive_migration();

    let mut changed_key = current.clone();
    changed_key.tables[0].primary_key = strings(["value"]);
    let mut key_source = source.clone();
    key_source.tables[0].primary_key = strings(["value"]);
    let mut key_target = target.clone();
    key_target.tables[0].primary_key = strings(["value"]);
    assert_eq!(
        plan_additive_run_spec_migration(
            &persisted,
            &changed_key,
            &key_source,
            &key_target,
        )
        .expect_err("primary-key drift"),
        "additive run-spec migration table `alpha` primary key changed"
    );

    let mut changed_ordering = current;
    changed_ordering.tables[0].primary_key_ordering = vec![
        SyncPrimaryKeyOrdering::Enum(strings(["one", "two"])),
    ];
    let mut ordering_source = source;
    ordering_source.tables[0].columns[0].column_type = "enum('one','two')".to_string();
    ordering_source.tables[0].columns[0].data_type = "enum".to_string();
    let ordering_target = ordering_source.clone();
    assert_eq!(
        plan_additive_run_spec_migration(
            &persisted,
            &changed_ordering,
            &ordering_source,
            &ordering_target,
        )
        .expect_err("primary-key ordering drift"),
        "additive run-spec migration table `alpha` primary-key ordering changed"
    );
}

#[test]
fn sync_additive_run_spec_migration_rejects_missing_or_incompatible_target() {
    let (persisted, current, source, mut target) = compatible_additive_migration();
    target.tables.retain(|table| table.name != "alpha");
    assert_eq!(
        plan_additive_run_spec_migration(&persisted, &current, &source, &target)
            .expect_err("missing target"),
        "additive run-spec migration target table `alpha` is missing"
    );

    let (_, current, source, mut target) = compatible_additive_migration();
    target
        .tables
        .iter_mut()
        .find(|table| table.name == "alpha")
        .expect("alpha target")
        .columns
        .retain(|column| column.name != "sync_seen_at");
    assert_eq!(
        plan_additive_run_spec_migration(&persisted, &current, &source, &target)
            .expect_err("incompatible target"),
        "additive run-spec migration table `alpha` current source and target schemas are incompatible"
    );
}

#[test]
fn sync_additive_run_spec_migration_rejects_no_additive_change() {
    let persisted = migration_spec(vec![
        migration_sync_table("alpha", &["id", "value"]),
        migration_sync_table("beta", &["id", "value"]),
    ]);
    let inventory = migration_inventory(vec![
        migration_inventory_table(
            "alpha",
            &["id"],
            &[("id", "bigint unsigned"), ("value", "varchar(255)")],
        ),
        migration_inventory_table(
            "beta",
            &["id"],
            &[("id", "bigint unsigned"), ("value", "varchar(255)")],
        ),
    ]);

    assert_eq!(
        plan_additive_run_spec_migration(&persisted, &persisted, &inventory, &inventory)
            .expect_err("no additive change"),
        "additive run-spec migration has no added writable columns"
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

fn compatible_additive_migration() -> (
    SyncRunSpec,
    SyncRunSpec,
    SchemaInventory,
    SchemaInventory,
) {
    let persisted = migration_spec(vec![
        migration_sync_table("alpha", &["id", "value"]),
        migration_sync_table("beta", &["id", "value"]),
    ]);
    let current = migration_spec(vec![
        migration_sync_table(
            "alpha",
            &["id", "direct_seen_at", "value", "sync_seen_at"],
        ),
        migration_sync_table("beta", &["id", "value"]),
    ]);
    let inventory = migration_inventory(vec![
        migration_inventory_table(
            "alpha",
            &["id"],
            &[
                ("id", "bigint unsigned"),
                ("direct_seen_at", "timestamp"),
                ("value", "varchar(255)"),
                ("sync_seen_at", "timestamp"),
            ],
        ),
        migration_inventory_table(
            "beta",
            &["id"],
            &[("id", "bigint unsigned"), ("value", "varchar(255)")],
        ),
    ]);
    (persisted, current, inventory.clone(), inventory)
}

fn migration_spec(tables: Vec<SyncTable>) -> SyncRunSpec {
    build_sync_run_identity(&exact_run_config(), tables)
        .expect("migration run specification")
        .run_spec
}

fn migration_sync_table(name: &str, columns: &[&str]) -> SyncTable {
    SyncTable {
        name: name.to_string(),
        primary_key: strings(["id"]),
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns: columns.iter().map(|column| (*column).to_string()).collect(),
    }
}

fn migration_inventory(tables: Vec<TableInventory>) -> SchemaInventory {
    SchemaInventory {
        schema: "source_database".to_string(),
        tables,
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        views: Vec::new(),
        triggers: Vec::new(),
        routines: Vec::new(),
        events: Vec::new(),
    }
}

fn migration_inventory_table(
    name: &str,
    primary_key: &[&str],
    columns: &[(&str, &str)],
) -> TableInventory {
    TableInventory {
        name: name.to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        collation: Some("utf8mb4_0900_ai_ci".to_string()),
        primary_key: primary_key
            .iter()
            .map(|column| (*column).to_string())
            .collect(),
        columns: columns
            .iter()
            .enumerate()
            .map(|(index, (name, column_type))| column(name, index as u32 + 1, column_type, None))
            .collect(),
    }
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
        run_id: Some("sync-run-42".to_string()),
        run_id_prefix: None,
        authorized_old_run_spec_sha256: None,
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
