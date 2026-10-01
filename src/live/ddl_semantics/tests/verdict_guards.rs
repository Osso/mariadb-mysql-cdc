use super::super::canonical::observe_operation_state as canonical_observed_state;
use super::*;

const GUARDED: &str = "ALTER TABLE accounts ADD UNIQUE KEY IF NOT EXISTS uk_handle (handle), DROP KEY IF EXISTS obsolete, ADD CONSTRAINT IF NOT EXISTS chk_handle CHECK (handle IN ('Random','experiment'))";
const CHECK_CLAUSE: &str = "(`handle` IN ('Random','experiment'))";

fn captured_absent() -> SemanticSchemaSnapshot {
    let mut target = semantic_snapshot(0, None);
    target
        .check_constraints
        .insert("accounts".into(), Vec::new());
    target
}

fn final_target() -> SemanticSchemaSnapshot {
    let mut target = captured_absent();
    let mut key = target.inventory.indexes[0].clone();
    key.name = "uk_handle".into();
    key.unique = true;
    key.columns[0].prefix_length = None;
    target.inventory.indexes.push(key);
    target.check_constraints.get_mut("accounts").unwrap().push((
        "chk_handle".into(),
        CHECK_CLAUSE.into(),
        true,
    ));
    target
}

fn evidence(target: &SemanticSchemaSnapshot) -> Result<DdlSemanticEvidence, String> {
    let operation = parse_ddl_operation(GUARDED).expect("generic guarded operation");
    assert!(operation.alter_table_ast.is_some());
    build_semantic_evidence(&operation, target, target)
}

#[test]
fn verdict_guards_absent_apply_is_one_atomic_alter() {
    let target = captured_absent();
    let captured = evidence(&target).expect("absent evidence");
    assert_ne!(captured.pre_state, captured.expected_post_state);
    let translated =
        transform::transform_production_alter_table_with_target(GUARDED, &target).unwrap();
    assert_eq!(
        translated.target_sql.as_deref(),
        Some(
            "ALTER TABLE `accounts` ADD UNIQUE KEY `uk_handle` (`handle`), ADD CONSTRAINT `chk_handle` CHECK (`handle` IN ('Random','experiment'))"
        )
    );
    let operation = parse_ddl_operation(GUARDED).unwrap();
    assert_eq!(
        canonical_observed_state(&final_target(), &operation).unwrap(),
        captured.expected_post_state
    );
}

#[test]
fn verdict_guards_final_state_is_restart_noop() {
    let target = final_target();
    let first = evidence(&target).unwrap();
    let restarted = evidence(&target).unwrap();
    assert_eq!(first, restarted);
    assert_eq!(first.pre_state, first.expected_post_state);
    assert_eq!(
        transform::transform_production_alter_table_with_target(GUARDED, &target)
            .unwrap()
            .target_sql,
        None
    );
}

#[test]
fn verdict_guards_reject_partial_divergent_disabled_and_uncaptured() {
    let mut partial = final_target();
    partial
        .check_constraints
        .get_mut("accounts")
        .unwrap()
        .clear();
    let mut only_check = final_target();
    only_check
        .inventory
        .indexes
        .retain(|index| index.name != "uk_handle");
    let mut divergent = final_target();
    divergent.check_constraints.get_mut("accounts").unwrap()[0].1 =
        "(handle IN ('random','experiment'))".into();
    let mut disabled = final_target();
    disabled.check_constraints.get_mut("accounts").unwrap()[0].2 = false;
    let mut divergent_key = final_target();
    divergent_key.inventory.indexes.last_mut().unwrap().unique = false;
    for target in [
        partial,
        only_check,
        divergent,
        disabled,
        divergent_key,
        semantic_snapshot(0, None),
    ] {
        assert!(evidence(&target).is_err());
        assert!(transform::transform_production_alter_table_with_target(GUARDED, &target).is_err());
    }
}

#[test]
fn verdict_guards_drop_requires_final_absence_or_absent_additions() {
    let mut initial = captured_absent();
    let mut old = initial.inventory.indexes[0].clone();
    old.name = "obsolete".into();
    initial.inventory.indexes.push(old.clone());
    assert!(evidence(&initial).is_ok());
    assert_eq!(
        transform::transform_production_alter_table_with_target(GUARDED, &initial)
            .unwrap()
            .target_sql
            .as_deref(),
        Some(
            "ALTER TABLE `accounts` ADD UNIQUE KEY `uk_handle` (`handle`), DROP INDEX `obsolete`, ADD CONSTRAINT `chk_handle` CHECK (`handle` IN ('Random','experiment'))"
        )
    );
    let mut partial_final = final_target();
    partial_final.inventory.indexes.push(old);
    assert!(evidence(&partial_final).is_err());
    assert!(
        transform::transform_production_alter_table_with_target(GUARDED, &partial_final).is_err()
    );
}

#[test]
fn verdict_guards_missing_or_disabled_check_does_not_match_expected_observation() {
    let target = captured_absent();
    let captured = evidence(&target).unwrap();
    let operation = parse_ddl_operation(GUARDED).unwrap();
    let mut observed = final_target();
    observed.check_constraints.get_mut("accounts").unwrap()[0].2 = false;
    assert_ne!(
        canonical_observed_state(&observed, &operation).unwrap(),
        captured.expected_post_state
    );
    observed
        .check_constraints
        .get_mut("accounts")
        .unwrap()
        .clear();
    assert_ne!(
        canonical_observed_state(&observed, &operation).unwrap(),
        captured.expected_post_state
    );
}

#[test]
fn verdict_guards_unguarded_check_has_enforced_postcondition() {
    let target = captured_absent();
    let operation = parse_ddl_operation(
        "ALTER TABLE accounts ADD CONSTRAINT chk_handle CHECK (handle IN ('Random','experiment'))",
    )
    .unwrap();
    let captured = build_semantic_evidence(&operation, &target, &target).unwrap();
    assert_ne!(captured.pre_state, captured.expected_post_state);
    let mut observed = target.clone();
    observed
        .check_constraints
        .get_mut("accounts")
        .unwrap()
        .push(("chk_handle".into(), CHECK_CLAUSE.into(), true));
    assert_eq!(
        canonical_observed_state(&observed, &operation).unwrap(),
        captured.expected_post_state
    );
    assert!(build_semantic_evidence(&operation, &observed, &observed).is_err());
}

#[test]
fn verdict_guards_metadata_drift_fails_existing_snapshot_fence() {
    let before = final_target();
    let mut after = before.clone();
    after.check_constraints.get_mut("accounts").unwrap()[0].2 = false;
    assert!(validate_target_snapshot_consistency(&before, &after).is_err());
}

#[test]
fn verdict_guards_noncheck_operation_ignores_unrelated_check_metadata() {
    let before = semantic_snapshot(0, None);
    let mut after = before.clone();
    after.check_constraints.insert(
        "accounts".into(),
        vec![(
            "unrelated".into(),
            "unsupported arbitrary expression".into(),
            false,
        )],
    );
    let operation = parse_ddl_operation("ALTER TABLE accounts ADD COLUMN test INT NULL").unwrap();
    assert_eq!(
        canonical_observed_state(&before, &operation).unwrap(),
        canonical_observed_state(&after, &operation).unwrap()
    );
}

#[test]
fn verdict_guards_only_touched_checks_are_compared() {
    let before = final_target();
    let mut after = before.clone();
    after.check_constraints.get_mut("accounts").unwrap().push((
        "unrelated".into(),
        "unsupported arbitrary expression".into(),
        true,
    ));
    let operation = parse_ddl_operation(GUARDED).unwrap();
    assert_eq!(
        canonical_observed_state(&before, &operation).unwrap(),
        canonical_observed_state(&after, &operation).unwrap()
    );
    assert_eq!(
        evidence(&before).unwrap().expected_post_state,
        evidence(&after).unwrap().expected_post_state
    );
}

#[test]
fn verdict_guards_drop_only_absent_noop_and_unguarded_drop_required() {
    let target = semantic_snapshot(0, None);
    assert_eq!(
        transform::transform_production_alter_table_with_target(
            "ALTER TABLE accounts DROP KEY IF EXISTS obsolete",
            &target
        )
        .unwrap()
        .target_sql,
        None
    );
    assert!(
        transform::transform_production_alter_table_with_target(
            "ALTER TABLE accounts DROP KEY obsolete",
            &target
        )
        .is_err()
    );
}

#[test]
fn verdict_guards_cannot_strip_new_guards_without_target_evidence() {
    assert!(transform::transform_production_alter_table(GUARDED).is_err());
}
