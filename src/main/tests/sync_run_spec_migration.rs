use crate::inventory::{ColumnInventory, SchemaInventory, TableInventory};
use crate::sync::{
    LockedSyncProgressRow, SyncConfig, SyncPrimaryKeyOrdering, SyncRunSpec,
    SyncRunSpecMigrationDecision, SyncStage, SyncTable, build_sync_run_identity,
    decide_locked_run_spec_migration, validate_sync_config,
};
use crate::sync_cli::parse_sync_config;

const AUTHORIZED_OLD_SHA256: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FIXTURE_OLD_SHA256: &str =
    "01605f111206a2b2200c122431c9d5084bce7a4ee9eea14e79f5dda51cfb30a9";
const FIXTURE_CURRENT_SHA256: &str =
    "3ed93e95f1de300ff96801952df4d7852059aa1324deba6e27a1473298c1d07e";

#[test]
fn sync_authorization_cli_requires_lowercase_sha256_and_exact_run_id() {
    super::set_env(
        "CDC_SYNC_MIGRATION_SOURCE_PASSWORD",
        "source-password",
    );
    super::set_env(
        "CDC_SYNC_MIGRATION_TARGET_PASSWORD",
        "target-password",
    );

    let config = parse_sync_config(sync_cli_args(&[
        "--run-id",
        "sync-run-42",
        "--authorize-old-run-spec-sha256",
        AUTHORIZED_OLD_SHA256,
    ]))
    .expect("authorized exact sync config");
    assert_eq!(
        config.authorized_old_run_spec_sha256.as_deref(),
        Some(AUTHORIZED_OLD_SHA256)
    );

    for invalid in [
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "gaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        let error = parse_sync_config(sync_cli_args(&[
            "--run-id",
            "sync-run-42",
            "--authorize-old-run-spec-sha256",
            invalid,
        ]))
        .expect_err("invalid authorization hash");
        assert_eq!(
            error,
            "authorized old run-spec SHA-256 must be exactly 64 lowercase ASCII hex characters"
        );
    }

    let error = parse_sync_config(sync_cli_args(&[
        "--run-id-prefix",
        "scheduled-sync",
        "--authorize-old-run-spec-sha256",
        AUTHORIZED_OLD_SHA256,
    ]))
    .expect_err("authorization with derived run id");
    assert_eq!(
        error,
        "authorized old run-spec SHA-256 requires an exact run_id, not run_id_prefix"
    );
}

#[test]
fn sync_authorization_does_not_change_normal_run_identity() {
    let without_authorization = exact_run_config();
    let mut with_authorization = without_authorization.clone();
    with_authorization.authorized_old_run_spec_sha256 = Some(AUTHORIZED_OLD_SHA256.to_string());

    validate_sync_config(&with_authorization).expect("authorized sync config");
    let table = migration_sync_table("alpha", &["id", "value"]);
    let baseline = build_sync_run_identity(&without_authorization, vec![table.clone()])
        .expect("baseline identity");
    let authorized = build_sync_run_identity(&with_authorization, vec![table])
        .expect("authorized identity");

    assert_eq!(authorized, baseline);
    assert!(!authorized.run_spec_json.contains(AUTHORIZED_OLD_SHA256));
}

#[test]
fn locked_run_spec_migration_rejects_zero_or_multiple_persisted_specs() {
    let fixture = migration_fixture();
    let no_rows = decide_locked_run_spec_migration(
        &[],
        &fixture.authorized_old_sha256,
        &fixture.current_identity,
        &fixture.source,
        &fixture.target,
    )
    .expect_err("zero locked rows");
    assert_eq!(no_rows, "run-spec migration requires at least one locked progress row");

    let rows = vec![
        locked_row(SyncStage::PrerequisiteSchema, "alpha", &fixture.persisted_json),
        locked_row(
            SyncStage::PrerequisiteSchema,
            "beta",
            &fixture.current_identity.run_spec_json,
        ),
    ];
    let multiple = decide_locked_run_spec_migration(
        &rows,
        &fixture.authorized_old_sha256,
        &fixture.current_identity,
        &fixture.source,
        &fixture.target,
    )
    .expect_err("multiple persisted specs");
    assert_eq!(
        multiple,
        "run-spec migration locked progress rows contain 2 distinct raw run specifications"
    );
}

#[test]
fn locked_run_spec_migration_rejects_wrong_authorized_hash() {
    let fixture = migration_fixture();
    let rows = fixture.persisted_progress_rows();

    let error = decide_locked_run_spec_migration(
        &rows,
        AUTHORIZED_OLD_SHA256,
        &fixture.current_identity,
        &fixture.source,
        &fixture.target,
    )
    .expect_err("wrong authorized hash");

    assert_eq!(
        error,
        format!(
            "persisted run-spec SHA-256 {} does not match authorized SHA-256 {AUTHORIZED_OLD_SHA256}",
            fixture.authorized_old_sha256
        )
    );
}

#[test]
fn locked_run_spec_migration_rejects_unexpected_progress_table() {
    let fixture = migration_fixture();
    let mut rows = fixture.persisted_progress_rows();
    rows.push(locked_row(
        SyncStage::PrerequisiteSchema,
        "outside_scope",
        &fixture.persisted_json,
    ));

    let error = decide_locked_run_spec_migration(
        &rows,
        &fixture.authorized_old_sha256,
        &fixture.current_identity,
        &fixture.source,
        &fixture.target,
    )
    .expect_err("unexpected progress table");

    assert_eq!(
        error,
        "run-spec migration progress table `outside_scope` is outside the unchanged run scope"
    );
}

#[test]
fn locked_run_spec_migration_rejects_rows_stage_for_changed_table() {
    let fixture = migration_fixture();
    let mut rows = fixture.persisted_progress_rows();
    rows.push(locked_row(
        SyncStage::Rows,
        "alpha",
        &fixture.persisted_json,
    ));

    let error = decide_locked_run_spec_migration(
        &rows,
        &fixture.authorized_old_sha256,
        &fixture.current_identity,
        &fixture.source,
        &fixture.target,
    )
    .expect_err("changed table row progress");

    assert_eq!(
        error,
        "run-spec migration changed table `alpha` already has rows-stage progress"
    );
}

#[test]
fn locked_run_spec_migration_accepts_unchanged_table_row_progress_with_exact_decision() {
    let fixture = migration_fixture();
    let rows = fixture.persisted_progress_rows();

    let decision = decide_locked_run_spec_migration(
        &rows,
        &fixture.authorized_old_sha256,
        &fixture.current_identity,
        &fixture.source,
        &fixture.target,
    )
    .expect("authorized additive migration");

    assert_eq!(
        decision,
        SyncRunSpecMigrationDecision::UpdateRequired {
            expected_locked_row_count: 3,
            old_sha256: FIXTURE_OLD_SHA256.to_string(),
            new_sha256: FIXTURE_CURRENT_SHA256.to_string(),
            changed_tables: vec![crate::sync::AdditiveRunSpecTableChange {
                table: "alpha".to_string(),
                added_columns: strings(["direct_seen_at", "sync_seen_at"]),
            }],
        }
    );
}

#[test]
fn locked_run_spec_migration_rejects_outside_scope_already_current_progress() {
    let fixture = migration_fixture();
    let current_json = &fixture.current_identity.run_spec_json;
    let rows = vec![
        locked_row(SyncStage::PrerequisiteSchema, "alpha", current_json),
        locked_row(
            SyncStage::PrerequisiteSchema,
            "outside_scope",
            current_json,
        ),
    ];

    let error = decide_locked_run_spec_migration(
        &rows,
        AUTHORIZED_OLD_SHA256,
        &fixture.current_identity,
        &fixture.source,
        &fixture.target,
    )
    .expect_err("outside-scope already-current progress");

    assert_eq!(
        error,
        "run-spec migration progress table `outside_scope` is outside the unchanged run scope"
    );
}

#[test]
fn locked_run_spec_migration_returns_idempotent_already_current_decision() {
    let fixture = migration_fixture();
    let current_json = &fixture.current_identity.run_spec_json;
    let rows = vec![
        locked_row(SyncStage::PrerequisiteSchema, "alpha", current_json),
        locked_row(SyncStage::Rows, "alpha", current_json),
        locked_row(SyncStage::FinalConstraints, "beta", current_json),
    ];

    let decision = decide_locked_run_spec_migration(
        &rows,
        AUTHORIZED_OLD_SHA256,
        &fixture.current_identity,
        &fixture.source,
        &fixture.target,
    )
    .expect("already-current migration retry");

    assert_eq!(
        decision,
        SyncRunSpecMigrationDecision::AlreadyCurrent {
            locked_row_count: 3,
            current_sha256: FIXTURE_CURRENT_SHA256.to_string(),
        }
    );
}

struct MigrationFixture {
    persisted_json: String,
    authorized_old_sha256: String,
    current_identity: crate::sync::SyncRunIdentity,
    source: SchemaInventory,
    target: SchemaInventory,
}

impl MigrationFixture {
    fn persisted_progress_rows(&self) -> Vec<LockedSyncProgressRow> {
        vec![
            locked_row(
                SyncStage::PrerequisiteSchema,
                "alpha",
                &self.persisted_json,
            ),
            locked_row(
                SyncStage::PrerequisiteSchema,
                "beta",
                &self.persisted_json,
            ),
            locked_row(SyncStage::Rows, "beta", &self.persisted_json),
        ]
    }
}

fn migration_fixture() -> MigrationFixture {
    let persisted = migration_spec(vec![
        migration_sync_table("alpha", &["id", "value"]),
        migration_sync_table("beta", &["id", "value"]),
    ]);
    let current_tables = vec![
        migration_sync_table(
            "alpha",
            &["id", "direct_seen_at", "value", "sync_seen_at"],
        ),
        migration_sync_table("beta", &["id", "value"]),
    ];
    let current_identity = build_sync_run_identity(&exact_run_config(), current_tables)
        .expect("current identity");
    let inventory = migration_inventory(vec![
        migration_inventory_table(
            "alpha",
            &[
                ("id", "bigint unsigned"),
                ("direct_seen_at", "timestamp"),
                ("value", "varchar(255)"),
                ("sync_seen_at", "timestamp"),
            ],
        ),
        migration_inventory_table(
            "beta",
            &[("id", "bigint unsigned"), ("value", "varchar(255)")],
        ),
    ]);
    let persisted_json = serde_json::to_string(&persisted).expect("persisted JSON");
    let authorized_old_sha256 = FIXTURE_OLD_SHA256.to_string();

    MigrationFixture {
        persisted_json,
        authorized_old_sha256,
        current_identity,
        source: inventory.clone(),
        target: inventory,
    }
}

fn migration_spec(tables: Vec<SyncTable>) -> SyncRunSpec {
    build_sync_run_identity(&exact_run_config(), tables)
        .expect("migration run specification")
        .run_spec
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
        tables: strings(["alpha", "beta"]),
        chunk_size: 500,
        parallelism: 1,
        progress_table: "cdc.sync_runs".to_string(),
        run_id: Some("sync-run-42".to_string()),
        run_id_prefix: None,
        authorized_old_run_spec_sha256: None,
    }
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

fn migration_inventory_table(name: &str, columns: &[(&str, &str)]) -> TableInventory {
    TableInventory {
        name: name.to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        collation: Some("utf8mb4_0900_ai_ci".to_string()),
        primary_key: strings(["id"]),
        columns: columns
            .iter()
            .enumerate()
            .map(|(index, (name, column_type))| ColumnInventory {
                name: (*name).to_string(),
                ordinal_position: index as u32 + 1,
                column_type: (*column_type).to_string(),
                data_type: column_type
                    .split(['(', ' '])
                    .next()
                    .expect("column data type")
                    .to_string(),
                is_nullable: false,
                character_set: None,
                collation: None,
                default_value: None,
                extra: String::new(),
                comment: String::new(),
                generated: None,
            })
            .collect(),
    }
}

fn locked_row(stage: SyncStage, table_name: &str, run_spec_json: &str) -> LockedSyncProgressRow {
    LockedSyncProgressRow {
        stage,
        table_name: table_name.to_string(),
        run_spec_json: run_spec_json.to_string(),
    }
}

fn sync_cli_args(run_identity_and_authorization: &[&str]) -> Vec<String> {
    let mut values = vec![
        "--source-host",
        "source-db",
        "--source-user",
        "source-user",
        "--source-password-env",
        "CDC_SYNC_MIGRATION_SOURCE_PASSWORD",
        "--source-database",
        "source-schema",
        "--target-host",
        "target-db",
        "--target-user",
        "target-user",
        "--target-password-env",
        "CDC_SYNC_MIGRATION_TARGET_PASSWORD",
        "--target-database",
        "target-schema",
        "--target-tls-ca-file",
        "/tmp/target-ca.pem",
        "--table",
        "items",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    values.extend(
        run_identity_and_authorization
            .iter()
            .map(|value| (*value).to_string()),
    );
    values
}

fn strings<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}
