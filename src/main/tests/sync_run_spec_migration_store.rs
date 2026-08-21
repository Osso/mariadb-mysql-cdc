use crate::inventory::{ColumnInventory, SchemaInventory, TableInventory};
use crate::sync::{
    AdditiveRunSpecTableChange, LockedSyncProgressRow, SyncConfig, SyncPrimaryKeyOrdering,
    SyncRunSpecMigrationExecutor, SyncRunSpecMigrationOutcome, SyncRunSpecMigrationRequest,
    SyncStage, SyncTable, build_sync_run_identity, continue_after_sync_run_spec_migration,
    format_sync_run_spec_migration_audit, read_sync_run_spec_migration_target_inventory,
    run_locked_sync_run_spec_migration, run_optional_sync_run_spec_migration,
};
use std::cell::{Cell, RefCell};

const AUTHORIZED_OLD_SHA256: &str =
    "01605f111206a2b2200c122431c9d5084bce7a4ee9eea14e79f5dda51cfb30a9";
const CURRENT_SHA256: &str =
    "3ed93e95f1de300ff96801952df4d7852059aa1324deba6e27a1473298c1d07e";

#[derive(Clone, Debug, Eq, PartialEq)]
enum Operation {
    BeginSerializable,
    Lock(String),
    Update {
        run_id: String,
        old_json: String,
        current_json: String,
    },
    Verify {
        run_id: String,
        current_json: String,
    },
    Commit,
    Rollback,
}

struct ScriptedMigrationExecutor {
    locked_rows: Result<Vec<LockedSyncProgressRow>, String>,
    update_result: Result<u64, String>,
    verification_result: Result<u64, String>,
    commit_result: Result<(), String>,
    rollback_result: Result<(), String>,
    operations: Vec<Operation>,
}

impl ScriptedMigrationExecutor {
    fn with_rows(rows: Vec<LockedSyncProgressRow>) -> Self {
        Self {
            locked_rows: Ok(rows),
            update_result: Ok(3),
            verification_result: Ok(3),
            commit_result: Ok(()),
            rollback_result: Ok(()),
            operations: Vec::new(),
        }
    }
}

impl SyncRunSpecMigrationExecutor for ScriptedMigrationExecutor {
    fn begin_serializable_transaction(&mut self) -> Result<(), String> {
        self.operations.push(Operation::BeginSerializable);
        Ok(())
    }

    fn lock_run_rows(&mut self, run_id: &str) -> Result<Vec<LockedSyncProgressRow>, String> {
        self.operations.push(Operation::Lock(run_id.to_string()));
        self.locked_rows.clone()
    }

    fn update_run_spec(
        &mut self,
        run_id: &str,
        old_json: &str,
        current_json: &str,
    ) -> Result<u64, String> {
        self.operations.push(Operation::Update {
            run_id: run_id.to_string(),
            old_json: old_json.to_string(),
            current_json: current_json.to_string(),
        });
        self.update_result.clone()
    }

    fn count_run_rows_with_spec(
        &mut self,
        run_id: &str,
        current_json: &str,
    ) -> Result<u64, String> {
        self.operations.push(Operation::Verify {
            run_id: run_id.to_string(),
            current_json: current_json.to_string(),
        });
        self.verification_result.clone()
    }

    fn commit_transaction(&mut self) -> Result<(), String> {
        self.operations.push(Operation::Commit);
        self.commit_result.clone()
    }

    fn rollback_transaction(&mut self) -> Result<(), String> {
        self.operations.push(Operation::Rollback);
        self.rollback_result.clone()
    }
}

#[test]
fn sync_run_spec_migration_store_locks_updates_verifies_and_commits_every_row() {
    let fixture = fixture();
    let rows = fixture.persisted_rows();
    let mut executor = ScriptedMigrationExecutor::with_rows(rows);

    let request = migration_request(&fixture, AUTHORIZED_OLD_SHA256);
    let outcome = run_locked_sync_run_spec_migration(&mut executor, &request)
        .expect("transactional migration");

    assert_eq!(
        outcome,
        SyncRunSpecMigrationOutcome::Migrated {
            locked_row_count: 3,
            affected_row_count: 3,
            authorized_old_sha256: AUTHORIZED_OLD_SHA256.to_string(),
            old_sha256: AUTHORIZED_OLD_SHA256.to_string(),
            new_sha256: CURRENT_SHA256.to_string(),
            changed_tables: vec![AdditiveRunSpecTableChange {
                table: "alpha".to_string(),
                added_columns: strings(["direct_seen_at", "sync_seen_at"]),
            }],
        }
    );
    assert_eq!(
        executor.operations,
        [
            Operation::BeginSerializable,
            Operation::Lock("sync-run-42".to_string()),
            Operation::Update {
                run_id: "sync-run-42".to_string(),
                old_json: fixture.persisted_json,
                current_json: fixture.current.run_spec_json.clone(),
            },
            Operation::Verify {
                run_id: "sync-run-42".to_string(),
                current_json: fixture.current.run_spec_json,
            },
            Operation::Commit,
        ]
    );
}

#[test]
fn sync_run_spec_migration_store_already_current_commits_without_write_or_delta() {
    let fixture = fixture();
    let rows = fixture.current_rows();
    let mut executor = ScriptedMigrationExecutor::with_rows(rows);

    let request = migration_request(&fixture, AUTHORIZED_OLD_SHA256);
    let outcome = run_locked_sync_run_spec_migration(&mut executor, &request)
        .expect("idempotent migration");

    assert_eq!(
        outcome,
        SyncRunSpecMigrationOutcome::AlreadyCurrent {
            locked_row_count: 3,
            affected_row_count: 0,
            authorized_old_sha256: AUTHORIZED_OLD_SHA256.to_string(),
            current_sha256: CURRENT_SHA256.to_string(),
        }
    );
    assert_eq!(
        executor.operations,
        [
            Operation::BeginSerializable,
            Operation::Lock("sync-run-42".to_string()),
            Operation::Commit,
        ]
    );
}

#[test]
fn sync_run_spec_migration_store_lock_failure_rolls_back_without_write() {
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());
    executor.locked_rows = Err("forced lock failure".to_string());

    let error = run(&mut executor, &fixture).expect_err("lock failure");

    assert_eq!(error, "forced lock failure");
    assert_eq!(
        executor.operations,
        [
            Operation::BeginSerializable,
            Operation::Lock("sync-run-42".to_string()),
            Operation::Rollback,
        ]
    );
}

#[test]
fn sync_run_spec_migration_store_update_failure_rolls_back_without_later_operations() {
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());
    executor.update_result = Err("forced update failure".to_string());

    let error = run(&mut executor, &fixture).expect_err("update failure");

    assert_eq!(error, "forced update failure");
    assert_eq!(
        executor.operations,
        [
            Operation::BeginSerializable,
            Operation::Lock("sync-run-42".to_string()),
            Operation::Update {
                run_id: "sync-run-42".to_string(),
                old_json: fixture.persisted_json,
                current_json: fixture.current.run_spec_json,
            },
            Operation::Rollback,
        ]
    );
}

#[test]
fn sync_run_spec_migration_store_affected_count_mismatch_rolls_back_before_verification() {
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());
    executor.update_result = Ok(2);

    let error = run(&mut executor, &fixture).expect_err("affected count mismatch");

    assert_eq!(
        error,
        "run-spec migration updated 2 rows, expected 3 locked rows"
    );
    assert_eq!(executor.operations.last(), Some(&Operation::Rollback));
    assert!(!executor
        .operations
        .iter()
        .any(|operation| matches!(operation, Operation::Verify { .. } | Operation::Commit)));
}

#[test]
fn sync_run_spec_migration_store_verification_count_mismatch_rolls_back_before_commit() {
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());
    executor.verification_result = Ok(2);

    let error = run(&mut executor, &fixture).expect_err("verification count mismatch");

    assert_eq!(
        error,
        "run-spec migration verification found 2 current-spec rows, expected 3 locked rows"
    );
    assert_eq!(executor.operations.last(), Some(&Operation::Rollback));
    assert!(!executor
        .operations
        .iter()
        .any(|operation| matches!(operation, Operation::Commit)));
}

#[test]
fn sync_run_spec_migration_store_commit_failure_attempts_rollback() {
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());
    executor.commit_result = Err("forced commit failure".to_string());

    let error = run(&mut executor, &fixture).expect_err("commit failure");

    assert_eq!(error, "forced commit failure");
    assert_eq!(
        &executor.operations[executor.operations.len() - 2..],
        [Operation::Commit, Operation::Rollback]
    );
}

#[test]
fn sync_run_spec_migration_store_commit_failure_appends_rollback_failure() {
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());
    executor.commit_result = Err("forced commit failure".to_string());
    executor.rollback_result = Err("forced rollback failure".to_string());

    let error = run(&mut executor, &fixture).expect_err("commit and rollback failure");

    assert_eq!(
        error,
        "forced commit failure; additionally rollback sync run-spec migration failed: forced rollback failure"
    );
    assert_eq!(
        &executor.operations[executor.operations.len() - 2..],
        [Operation::Commit, Operation::Rollback]
    );
}

#[test]
fn sync_run_spec_migration_store_preserves_primary_error_and_appends_rollback_failure() {
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());
    executor.update_result = Err("forced update failure".to_string());
    executor.rollback_result = Err("forced rollback failure".to_string());

    let error = run(&mut executor, &fixture).expect_err("update and rollback failure");

    assert_eq!(
        error,
        "forced update failure; additionally rollback sync run-spec migration failed: forced rollback failure"
    );
    assert_eq!(executor.operations.last(), Some(&Operation::Rollback));
}

#[test]
fn sync_run_spec_migration_store_decision_failure_rolls_back_without_write() {
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());

    let request = migration_request(
        &fixture,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let error = run_locked_sync_run_spec_migration(&mut executor, &request)
        .expect_err("authorization failure");

    assert!(error.contains("does not match authorized SHA-256"));
    assert_eq!(executor.operations.last(), Some(&Operation::Rollback));
    assert!(!executor.operations.iter().any(|operation| {
        matches!(
            operation,
            Operation::Update { .. } | Operation::Verify { .. } | Operation::Commit
        )
    }));
}

#[test]
fn ordinary_sync_skips_target_inventory_and_run_spec_migration_transaction() {
    let config = config();
    let target_read = Cell::new(false);
    let target = read_sync_run_spec_migration_target_inventory(&config, |_| {
        target_read.set(true);
        Ok(inventory())
    })
    .expect("ordinary target inventory decision");
    let fixture = fixture();
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());

    let outcome = run_optional_sync_run_spec_migration(
        &config,
        &fixture.current,
        &fixture.inventory,
        target.as_ref(),
        &mut executor,
    )
    .expect("ordinary sync migration decision");

    assert_eq!(outcome, None);
    assert!(!target_read.get());
    assert!(executor.operations.is_empty());
}

#[test]
fn authorized_sync_reads_target_inventory_and_returns_exact_migration_outcome() {
    let fixture = fixture();
    let mut config = config();
    config.authorized_old_run_spec_sha256 = Some(AUTHORIZED_OLD_SHA256.to_string());
    let target_read = Cell::new(false);
    let target = read_sync_run_spec_migration_target_inventory(&config, |_| {
        target_read.set(true);
        Ok(fixture.inventory.clone())
    })
    .expect("authorized target inventory")
    .expect("target inventory required");
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());

    let outcome = run_optional_sync_run_spec_migration(
        &config,
        &fixture.current,
        &fixture.inventory,
        Some(&target),
        &mut executor,
    )
    .expect("authorized migration")
    .expect("migration outcome");

    assert!(target_read.get());
    assert_eq!(
        outcome,
        SyncRunSpecMigrationOutcome::Migrated {
            locked_row_count: 3,
            affected_row_count: 3,
            authorized_old_sha256: AUTHORIZED_OLD_SHA256.to_string(),
            old_sha256: AUTHORIZED_OLD_SHA256.to_string(),
            new_sha256: CURRENT_SHA256.to_string(),
            changed_tables: vec![AdditiveRunSpecTableChange {
                table: "alpha".to_string(),
                added_columns: strings(["direct_seen_at", "sync_seen_at"]),
            }],
        }
    );
}

#[test]
fn migration_error_prevents_subsequent_sync_action() {
    let fixture = fixture();
    let mut config = config();
    config.authorized_old_run_spec_sha256 = Some(AUTHORIZED_OLD_SHA256.to_string());
    let mut executor = ScriptedMigrationExecutor::with_rows(fixture.persisted_rows());
    executor.locked_rows = Err("forced migration failure".to_string());
    let migration = run_optional_sync_run_spec_migration(
        &config,
        &fixture.current,
        &fixture.inventory,
        Some(&fixture.inventory),
        &mut executor,
    );
    let action_called = Cell::new(false);

    let error = continue_after_sync_run_spec_migration(
        migration,
        |_| {},
        || {
            action_called.set(true);
            Ok(())
        },
    )
    .expect_err("migration failure");

    assert_eq!(error, "forced migration failure");
    assert!(!action_called.get());
}

#[test]
fn committed_migration_emits_audit_before_subsequent_sync_action() {
    let outcome = SyncRunSpecMigrationOutcome::AlreadyCurrent {
        locked_row_count: 576,
        affected_row_count: 0,
        authorized_old_sha256: AUTHORIZED_OLD_SHA256.to_string(),
        current_sha256: CURRENT_SHA256.to_string(),
    };
    let events = RefCell::new(Vec::new());

    continue_after_sync_run_spec_migration(
        Ok(Some(outcome)),
        |_| events.borrow_mut().push("audit"),
        || {
            events.borrow_mut().push("orchestration");
            Ok(())
        },
    )
    .expect("post-migration sequence");

    assert_eq!(*events.borrow(), ["audit", "orchestration"]);
}

#[test]
fn run_spec_migration_audit_is_stable_secret_free_json_with_exact_fields() {
    let outcome = SyncRunSpecMigrationOutcome::Migrated {
        locked_row_count: 576,
        affected_row_count: 576,
        authorized_old_sha256: AUTHORIZED_OLD_SHA256.to_string(),
        old_sha256: AUTHORIZED_OLD_SHA256.to_string(),
        new_sha256: CURRENT_SHA256.to_string(),
        changed_tables: vec![AdditiveRunSpecTableChange {
            table: "content_sections_events_raw".to_string(),
            added_columns: strings(["direct_seen_at", "sync_seen_at"]),
        }],
    };

    let audit = format_sync_run_spec_migration_audit("full-catalog-sync-20260819-01", &outcome);
    let parsed: serde_json::Value = serde_json::from_str(&audit).expect("audit JSON");

    assert_eq!(
        parsed,
        serde_json::json!({
            "event": "sync_run_spec_migration",
            "run_id": "full-catalog-sync-20260819-01",
            "status": "migrated",
            "authorized_old_sha256": AUTHORIZED_OLD_SHA256,
            "old_sha256": AUTHORIZED_OLD_SHA256,
            "new_sha256": CURRENT_SHA256,
            "locked_row_count": 576,
            "affected_row_count": 576,
            "delta": [{
                "table": "content_sections_events_raw",
                "added_columns": ["direct_seen_at", "sync_seen_at"]
            }]
        })
    );
    assert_eq!(
        audit,
        format_sync_run_spec_migration_audit("full-catalog-sync-20260819-01", &outcome)
    );
    assert!(!audit.contains("run_spec_json"));
    assert!(!audit.contains("source-secret"));
    assert!(!audit.contains("target-secret"));
}

fn run(
    executor: &mut ScriptedMigrationExecutor,
    fixture: &Fixture,
) -> Result<SyncRunSpecMigrationOutcome, String> {
    let request = migration_request(fixture, AUTHORIZED_OLD_SHA256);
    run_locked_sync_run_spec_migration(executor, &request)
}

fn migration_request<'a>(
    fixture: &'a Fixture,
    authorized_old_sha256: &'a str,
) -> SyncRunSpecMigrationRequest<'a> {
    SyncRunSpecMigrationRequest {
        run_id: "sync-run-42",
        authorized_old_sha256,
        current: &fixture.current,
        source: &fixture.inventory,
        target: &fixture.inventory,
    }
}

struct Fixture {
    persisted_json: String,
    current: crate::sync::SyncRunIdentity,
    inventory: SchemaInventory,
}

impl Fixture {
    fn persisted_rows(&self) -> Vec<LockedSyncProgressRow> {
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

    fn current_rows(&self) -> Vec<LockedSyncProgressRow> {
        vec![
            locked_row(
                SyncStage::PrerequisiteSchema,
                "alpha",
                &self.current.run_spec_json,
            ),
            locked_row(SyncStage::Rows, "alpha", &self.current.run_spec_json),
            locked_row(
                SyncStage::FinalConstraints,
                "beta",
                &self.current.run_spec_json,
            ),
        ]
    }
}

fn fixture() -> Fixture {
    let persisted = build_sync_run_identity(
        &config(),
        vec![
            sync_table("alpha", &["id", "value"]),
            sync_table("beta", &["id", "value"]),
        ],
    )
    .expect("persisted identity")
    .run_spec;
    let current = build_sync_run_identity(
        &config(),
        vec![
            sync_table(
                "alpha",
                &["id", "direct_seen_at", "value", "sync_seen_at"],
            ),
            sync_table("beta", &["id", "value"]),
        ],
    )
    .expect("current identity");
    let inventory = inventory();
    Fixture {
        persisted_json: serde_json::to_string(&persisted).expect("persisted JSON"),
        current,
        inventory,
    }
}

fn config() -> SyncConfig {
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

fn inventory() -> SchemaInventory {
    SchemaInventory {
        schema: "source_database".to_string(),
        tables: vec![
            inventory_table(
                "alpha",
                &[
                    ("id", "bigint unsigned"),
                    ("direct_seen_at", "timestamp"),
                    ("value", "varchar(255)"),
                    ("sync_seen_at", "timestamp"),
                ],
            ),
            inventory_table(
                "beta",
                &[("id", "bigint unsigned"), ("value", "varchar(255)")],
            ),
        ],
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        views: Vec::new(),
        triggers: Vec::new(),
        routines: Vec::new(),
        events: Vec::new(),
    }
}

fn sync_table(name: &str, columns: &[&str]) -> SyncTable {
    SyncTable {
        name: name.to_string(),
        primary_key: strings(["id"]),
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns: columns.iter().map(|column| (*column).to_string()).collect(),
    }
}

fn inventory_table(name: &str, columns: &[(&str, &str)]) -> TableInventory {
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

fn strings<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}
