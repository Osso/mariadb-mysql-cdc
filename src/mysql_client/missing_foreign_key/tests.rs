use super::*;
use std::collections::VecDeque;

struct FakeRepairExecutor {
    outcomes: BTreeMap<String, VecDeque<Result<(), TargetExecuteError>>>,
    parents: BTreeMap<String, MissingForeignKeyParent>,
    duplicate_reconciliations:
        BTreeMap<String, Result<DuplicateParentReconciliation, TargetExecuteError>>,
    duplicate_reconciliations_loaded: Vec<String>,
    duplicate_verification_outcomes: BTreeMap<String, Result<(), TargetExecuteError>>,
    duplicate_verifications: Vec<String>,
    executed: Vec<String>,
}

impl MissingForeignKeyRepairExecutor for FakeRepairExecutor {
    fn execute_row_change_statement(
        &mut self,
        change: &TargetRowChange,
    ) -> Result<(), TargetExecuteError> {
        self.executed.push(change.table.clone());
        self.outcomes
            .get_mut(&change.table)
            .and_then(VecDeque::pop_front)
            .unwrap_or(Ok(()))
    }

    fn load_missing_foreign_key_parent(
        &mut self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<MissingForeignKeyParent, TargetExecuteError> {
        assert_eq!(error.mysql_code(), Some(1452));
        self.parents.get(&change.table).cloned().ok_or_else(|| {
            TargetExecuteError::new(format!(
                "missing fake parent for {}.{}",
                change.schema, change.table
            ))
        })
    }

    fn load_duplicate_parent_reconciliation(
        &mut self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<DuplicateParentReconciliation, TargetExecuteError> {
        assert_eq!(error.mysql_code(), Some(1062));
        self.duplicate_reconciliations_loaded
            .push(change.table.clone());
        self.duplicate_reconciliations
            .get(&change.table)
            .cloned()
            .unwrap_or_else(|| {
                Err(TargetExecuteError::new(format!(
                    "missing fake duplicate reconciliation for {}.{}",
                    change.schema, change.table
                )))
            })
    }

    fn verify_duplicate_parent_reconciliation(
        &mut self,
        change: &TargetRowChange,
        _reconciliation: &DuplicateParentReconciliation,
    ) -> Result<(), TargetExecuteError> {
        self.duplicate_verifications.push(change.table.clone());
        self.duplicate_verification_outcomes
            .remove(&change.table)
            .unwrap_or(Ok(()))
    }
}

#[test]
fn recursively_repairs_nested_parents_before_retrying_child() {
    let mut executor = FakeRepairExecutor {
        outcomes: BTreeMap::from([
            ("sessions".to_string(), outcomes([missing_fk(), Ok(())])),
            ("guests".to_string(), outcomes([missing_fk(), Ok(())])),
            ("utms".to_string(), outcomes([Ok(())])),
        ]),
        parents: BTreeMap::from([
            (
                "sessions".to_string(),
                fake_parent("sessions", "sessions_guest", "guests"),
            ),
            (
                "guests".to_string(),
                fake_parent("guests", "guests_utm", "utms"),
            ),
        ]),
        duplicate_reconciliations: BTreeMap::new(),
        duplicate_reconciliations_loaded: Vec::new(),
        duplicate_verification_outcomes: BTreeMap::new(),
        duplicate_verifications: Vec::new(),
        executed: Vec::new(),
    };

    execute_row_change_with_missing_foreign_key_repair(&mut executor, &row_change("sessions"))
        .expect("repair nested parents");

    assert_eq!(
        executor.executed,
        ["sessions", "guests", "utms", "guests", "sessions"]
    );
}

#[test]
fn reconciles_same_primary_key_parent_before_retrying_child() {
    let mut executor = duplicate_parent_executor(duplicate_reconciliation(
        row_change_kind("users_update", TargetRowChangeKind::Update),
        false,
    ));

    execute_row_change_with_missing_foreign_key_repair(
        &mut executor,
        &row_change("artists_favorites"),
    )
    .expect("reconcile same-primary-key parent");

    assert_eq!(
        executor.executed,
        [
            "artists_favorites",
            "users",
            "users_update",
            "artists_favorites"
        ]
    );
    assert_eq!(executor.duplicate_reconciliations_loaded, ["users"]);
    assert_eq!(executor.duplicate_verifications, ["users"]);
}

#[test]
fn reconciles_different_primary_key_owner_before_reinserting_parent() {
    let mut executor = duplicate_parent_executor(duplicate_reconciliation(
        row_change_kind("stale_comic_update", TargetRowChangeKind::Update),
        true,
    ));
    executor
        .outcomes
        .insert("users".to_string(), outcomes([duplicate(), Ok(())]));

    execute_row_change_with_missing_foreign_key_repair(
        &mut executor,
        &row_change("artists_favorites"),
    )
    .expect("reconcile different-primary-key owner");

    assert_eq!(
        executor.executed,
        [
            "artists_favorites",
            "users",
            "stale_comic_update",
            "users",
            "artists_favorites"
        ]
    );
    assert_eq!(executor.duplicate_verifications, ["users"]);
}

#[test]
fn deletes_source_absent_duplicate_owner_before_reinserting_parent() {
    let mut executor = duplicate_parent_executor(duplicate_reconciliation(
        row_change_kind("stale_owner_delete", TargetRowChangeKind::Delete),
        true,
    ));
    executor
        .outcomes
        .insert("users".to_string(), outcomes([duplicate(), Ok(())]));

    execute_row_change_with_missing_foreign_key_repair(
        &mut executor,
        &row_change("artists_favorites"),
    )
    .expect("delete source-absent duplicate owner");

    assert_eq!(
        executor.executed,
        [
            "artists_favorites",
            "users",
            "stale_owner_delete",
            "users",
            "artists_favorites"
        ]
    );
    assert_eq!(executor.duplicate_verifications, ["users"]);
}

#[test]
fn rejects_ambiguous_duplicate_owner_without_retrying_child() {
    let mut executor = duplicate_parent_executor(Err(TargetExecuteError::new(
        "duplicate parent owner is ambiguous",
    )));

    let error = execute_row_change_with_missing_foreign_key_repair(
        &mut executor,
        &row_change("artists_favorites"),
    )
    .expect_err("ambiguous owner must fail closed");

    assert!(error.to_string().contains("owner is ambiguous"));
    assert_eq!(executor.executed, ["artists_favorites", "users"]);
    assert!(executor.duplicate_verifications.is_empty());
}

#[test]
fn rejects_parent_verification_failure_without_retrying_child() {
    let mut executor = duplicate_parent_executor(duplicate_reconciliation(
        row_change_kind("users_update", TargetRowChangeKind::Update),
        false,
    ));
    executor.duplicate_verification_outcomes.insert(
        "users".to_string(),
        Err(TargetExecuteError::new("parent verification mismatch")),
    );

    let error = execute_row_change_with_missing_foreign_key_repair(
        &mut executor,
        &row_change("artists_favorites"),
    )
    .expect_err("verification failure must fail closed");

    assert!(error.to_string().contains("verification mismatch"));
    assert_eq!(
        executor.executed,
        ["artists_favorites", "users", "users_update"]
    );
    assert_eq!(executor.duplicate_verifications, ["users"]);
}

#[test]
fn rejects_duplicate_that_remains_after_owner_reconciliation() {
    let mut executor = duplicate_parent_executor(duplicate_reconciliation(
        row_change_kind("stale_owner_update", TargetRowChangeKind::Update),
        true,
    ));
    executor
        .outcomes
        .insert("users".to_string(), outcomes([duplicate(), duplicate()]));

    let error = execute_row_change_with_missing_foreign_key_repair(
        &mut executor,
        &row_change("artists_favorites"),
    )
    .expect_err("repeated duplicate must fail closed");

    assert_eq!(error.mysql_code(), Some(1062));
    assert_eq!(
        executor.executed,
        ["artists_favorites", "users", "stale_owner_update", "users"]
    );
    assert!(executor.duplicate_verifications.is_empty());
}

#[test]
fn rejects_repeated_duplicate_parent_repair_key_as_a_cycle() {
    let mut executor = duplicate_parent_executor(duplicate_reconciliation(
        row_change_kind("owner_update", TargetRowChangeKind::Update),
        false,
    ));
    executor
        .outcomes
        .insert("users".to_string(), outcomes([duplicate(), duplicate()]));
    executor
        .outcomes
        .insert("owner_update".to_string(), outcomes([missing_fk()]));
    executor.parents.insert(
        "owner_update".to_string(),
        fake_parent("owner_update", "owner_missing_parent", "users"),
    );

    let error = execute_row_change_with_missing_foreign_key_repair(
        &mut executor,
        &row_change("artists_favorites"),
    )
    .expect_err("duplicate repair cycle must fail closed");

    assert!(error.to_string().contains("duplicate-parent repair cycle"));
    assert_eq!(
        executor.executed,
        ["artists_favorites", "users", "owner_update", "users"]
    );
    assert!(executor.duplicate_verifications.is_empty());
}

#[test]
fn keeps_ignoring_duplicate_on_original_source_insert() {
    let mut executor = FakeRepairExecutor {
        outcomes: BTreeMap::from([("accounts".to_string(), outcomes([duplicate()]))]),
        parents: BTreeMap::new(),
        duplicate_reconciliations: BTreeMap::new(),
        duplicate_reconciliations_loaded: Vec::new(),
        duplicate_verification_outcomes: BTreeMap::new(),
        duplicate_verifications: Vec::new(),
        executed: Vec::new(),
    };

    execute_row_change_with_missing_foreign_key_repair(&mut executor, &row_change("accounts"))
        .expect("original source duplicate remains idempotent");

    assert_eq!(executor.executed, ["accounts"]);
    assert!(executor.duplicate_reconciliations_loaded.is_empty());
}

#[test]
fn rejects_repeated_repair_key_as_a_cycle() {
    let mut executor = FakeRepairExecutor {
        outcomes: BTreeMap::from([
            ("alpha".to_string(), outcomes([missing_fk(), missing_fk()])),
            ("beta".to_string(), outcomes([missing_fk()])),
        ]),
        parents: BTreeMap::from([
            (
                "alpha".to_string(),
                fake_parent("alpha", "alpha_beta", "beta"),
            ),
            (
                "beta".to_string(),
                fake_parent("beta", "beta_alpha", "alpha"),
            ),
        ]),
        duplicate_reconciliations: BTreeMap::new(),
        duplicate_reconciliations_loaded: Vec::new(),
        duplicate_verification_outcomes: BTreeMap::new(),
        duplicate_verifications: Vec::new(),
        executed: Vec::new(),
    };

    let error =
        execute_row_change_with_missing_foreign_key_repair(&mut executor, &row_change("alpha"))
            .expect_err("cycle must fail closed");

    assert!(error.to_string().contains("repair cycle detected"));
    assert_eq!(executor.executed, ["alpha", "beta", "alpha"]);
}

#[test]
fn rejects_parent_chain_beyond_bounded_depth() {
    let mut outcomes_by_table = BTreeMap::new();
    let mut parents = BTreeMap::new();
    for depth in 0..=MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH {
        let child = format!("table_{depth}");
        let parent = format!("table_{}", depth + 1);
        outcomes_by_table.insert(child.clone(), outcomes([missing_fk()]));
        parents.insert(
            child.clone(),
            fake_parent(&child, &format!("constraint_{depth}"), &parent),
        );
    }
    let mut executor = FakeRepairExecutor {
        outcomes: outcomes_by_table,
        parents,
        duplicate_reconciliations: BTreeMap::new(),
        duplicate_reconciliations_loaded: Vec::new(),
        duplicate_verification_outcomes: BTreeMap::new(),
        duplicate_verifications: Vec::new(),
        executed: Vec::new(),
    };

    let error =
        execute_row_change_with_missing_foreign_key_repair(&mut executor, &row_change("table_0"))
            .expect_err("over-depth repair must fail closed");

    assert!(error.to_string().contains("exceeded maximum depth"));
    assert_eq!(
        executor.executed.len(),
        MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH + 1
    );
}

fn duplicate_parent_executor(
    reconciliation: Result<DuplicateParentReconciliation, TargetExecuteError>,
) -> FakeRepairExecutor {
    FakeRepairExecutor {
        outcomes: BTreeMap::from([
            (
                "artists_favorites".to_string(),
                outcomes([missing_fk(), Ok(())]),
            ),
            ("users".to_string(), outcomes([duplicate()])),
        ]),
        parents: BTreeMap::from([(
            "artists_favorites".to_string(),
            fake_parent("artists_favorites", "artists_favorites_user", "users"),
        )]),
        duplicate_reconciliations: BTreeMap::from([("users".to_string(), reconciliation)]),
        duplicate_reconciliations_loaded: Vec::new(),
        duplicate_verification_outcomes: BTreeMap::new(),
        duplicate_verifications: Vec::new(),
        executed: Vec::new(),
    }
}

fn duplicate_reconciliation(
    owner_change: TargetRowChange,
    retry_parent_insert: bool,
) -> Result<DuplicateParentReconciliation, TargetExecuteError> {
    let table = owner_change.table.clone();
    Ok(DuplicateParentReconciliation {
        owner_change,
        retry_parent_insert,
        verification: SqlStatement {
            sql: "SELECT 1".to_string(),
            params: Vec::new(),
        },
        repair_key: DuplicateParentRepairKey {
            schema: "globalcomix".to_string(),
            table,
            index: "PRIMARY".to_string(),
            values: vec!["1".to_string()],
        },
    })
}

fn outcomes<const N: usize>(
    values: [Result<(), TargetExecuteError>; N],
) -> VecDeque<Result<(), TargetExecuteError>> {
    VecDeque::from(values)
}

fn missing_fk() -> Result<(), TargetExecuteError> {
    Err(TargetExecuteError::from_mysql(1452, "missing parent"))
}

fn duplicate() -> Result<(), TargetExecuteError> {
    Err(TargetExecuteError::from_mysql(1062, "duplicate parent"))
}

fn fake_parent(child_table: &str, constraint: &str, parent_table: &str) -> MissingForeignKeyParent {
    MissingForeignKeyParent {
        change: row_change(parent_table),
        constraint: constraint.to_string(),
        repair_key: MissingForeignKeyRepairKey {
            child_schema: "globalcomix".to_string(),
            child_table: child_table.to_string(),
            constraint: constraint.to_string(),
            values: vec![parent_table.to_string()],
        },
    }
}

fn row_change(table: &str) -> TargetRowChange {
    row_change_kind(table, TargetRowChangeKind::Insert)
}

fn row_change_kind(table: &str, kind: TargetRowChangeKind) -> TargetRowChange {
    TargetRowChange {
        statement: SqlStatement {
            sql: format!("INSERT INTO `{table}` (`id`) VALUES (?)"),
            params: vec![Value::UInt(1)],
        },
        kind,
        schema: "globalcomix".to_string(),
        table: table.to_string(),
        values: BTreeMap::from([("id".to_string(), Value::UInt(1))]),
    }
}
