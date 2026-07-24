//! The fixture is the stall cleared manually at `mysqld-bin.002709:753030230`:
//! `paid_subscriptions_users_pages` 172056641 referenced `sessions.98587059`, which the target did
//! not hold.

use super::*;
use std::collections::BTreeMap;

fn violation() -> ForeignKeyViolation {
    ForeignKeyViolation {
        child_schema: "globalcomix".to_string(),
        child_table: "paid_subscriptions_users_pages".to_string(),
        constraint: "fk_paid_subscriptions_users_pages_session_id".to_string(),
        child_columns: vec!["session_id".to_string()],
        parent_schema: None,
        parent_table: "sessions".to_string(),
        parent_columns: vec!["session_id".to_string()],
    }
}

fn session_row(session_id: &str, guest_id: &str) -> SnapshotRow {
    SnapshotRow {
        primary_key: vec![session_id.to_string()],
        values: BTreeMap::from([
            ("session_id".to_string(), Some(session_id.to_string())),
            ("guest_id".to_string(), Some(guest_id.to_string())),
        ]),
    }
}

fn plan(
    source_parent_rows: &[SnapshotRow],
    target_parent_rows: &[SnapshotRow],
) -> Result<MissingParentPlan, MissingParentRejection> {
    let violation = violation();
    plan_missing_parent_recovery(&MissingParentInput {
        violation: &violation,
        child_foreign_key_values: &[Some("98587059".to_string())],
        source_parent_rows,
        target_parent_rows,
    })
}

#[test]
fn inserts_the_exact_source_parent_when_the_target_lacks_it() {
    let source = session_row("98587059", "46604143");

    let planned = plan(std::slice::from_ref(&source), &[]).expect("plan");

    assert_eq!(planned, MissingParentPlan::InsertParent(source));
}

/// A concurrent recovery or replay can install the parent first; replay alone then resolves it.
#[test]
fn reports_already_reconciled_when_the_target_holds_an_equal_parent() {
    let source = session_row("98587059", "46604143");

    let planned = plan(std::slice::from_ref(&source), std::slice::from_ref(&source)).expect("plan");

    assert_eq!(planned, MissingParentPlan::AlreadyReconciled);
}

/// A target parent that differs from source is drift, not a missing parent, so it must fail closed
/// rather than have recovery overwrite it.
#[test]
fn fails_closed_when_the_target_parent_diverges() {
    let source = session_row("98587059", "46604143");
    let divergent = session_row("98587059", "99999999");

    assert_eq!(
        plan(
            std::slice::from_ref(&source),
            std::slice::from_ref(&divergent)
        )
        .expect_err("divergent"),
        MissingParentRejection::TargetParentDiverges
    );
}

#[test]
fn fails_closed_when_the_source_parent_is_absent() {
    assert_eq!(
        plan(&[], &[]).expect_err("absent"),
        MissingParentRejection::SourceParentAbsent
    );
}

#[test]
fn fails_closed_when_the_source_parent_is_ambiguous() {
    let rows = vec![
        session_row("98587059", "46604143"),
        session_row("98587059", "46604144"),
    ];

    assert_eq!(
        plan(&rows, &[]).expect_err("ambiguous"),
        MissingParentRejection::SourceParentAmbiguous
    );
}

#[test]
fn fails_closed_when_the_target_parent_is_ambiguous() {
    let source = session_row("98587059", "46604143");
    let targets = vec![
        session_row("98587059", "46604143"),
        session_row("98587059", "46604143"),
    ];

    assert_eq!(
        plan(std::slice::from_ref(&source), &targets).expect_err("ambiguous"),
        MissingParentRejection::TargetParentAmbiguous
    );
}

/// The source row must actually own the referenced identity, or the read was not the parent lookup.
#[test]
fn fails_closed_when_the_source_parent_identity_does_not_match() {
    let source = session_row("98587060", "46604143");

    assert_eq!(
        plan(std::slice::from_ref(&source), &[]).expect_err("mismatch"),
        MissingParentRejection::SourceParentIdentityMismatch
    );
}

/// A NULL foreign-key value cannot violate the constraint, so it is not this class.
#[test]
fn fails_closed_on_a_null_foreign_key_value() {
    let violation = violation();
    let source = session_row("98587059", "46604143");

    let rejection = plan_missing_parent_recovery(&MissingParentInput {
        violation: &violation,
        child_foreign_key_values: &[None],
        source_parent_rows: std::slice::from_ref(&source),
        target_parent_rows: &[],
    })
    .expect_err("null");

    assert_eq!(rejection, MissingParentRejection::NullForeignKeyValue);
}

#[test]
fn fails_closed_when_the_child_image_lacks_a_foreign_key_column() {
    let violation = violation();
    let source = session_row("98587059", "46604143");

    let rejection = plan_missing_parent_recovery(&MissingParentInput {
        violation: &violation,
        child_foreign_key_values: &[],
        source_parent_rows: std::slice::from_ref(&source),
        target_parent_rows: &[],
    })
    .expect_err("missing column");

    assert_eq!(rejection, MissingParentRejection::MissingChildColumn);
}

/// Multi-column parents use every referenced column for the identity check.
#[test]
fn matches_a_multi_column_parent_identity() {
    let violation = ForeignKeyViolation {
        child_schema: "globalcomix".to_string(),
        child_table: "sessions".to_string(),
        constraint: "fk_sessions_guest".to_string(),
        child_columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
        parent_schema: None,
        parent_table: "guests".to_string(),
        parent_columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
    };
    let source = SnapshotRow {
        primary_key: vec!["78011674".to_string()],
        values: BTreeMap::from([
            ("guest_id".to_string(), Some("78011674".to_string())),
            ("guest_hash".to_string(), Some("fb42c5a9".to_string())),
        ]),
    };

    let planned = plan_missing_parent_recovery(&MissingParentInput {
        violation: &violation,
        child_foreign_key_values: &[Some("78011674".to_string()), Some("fb42c5a9".to_string())],
        source_parent_rows: std::slice::from_ref(&source),
        target_parent_rows: &[],
    })
    .expect("plan");

    assert_eq!(planned, MissingParentPlan::InsertParent(source));
}

/// A second referenced column that disagrees is not the referenced parent.
#[test]
fn fails_closed_when_a_multi_column_parent_identity_partly_matches() {
    let violation = ForeignKeyViolation {
        child_schema: "globalcomix".to_string(),
        child_table: "sessions".to_string(),
        constraint: "fk_sessions_guest".to_string(),
        child_columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
        parent_schema: None,
        parent_table: "guests".to_string(),
        parent_columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
    };
    let source = SnapshotRow {
        primary_key: vec!["78011674".to_string()],
        values: BTreeMap::from([
            ("guest_id".to_string(), Some("78011674".to_string())),
            ("guest_hash".to_string(), Some("different".to_string())),
        ]),
    };

    let rejection = plan_missing_parent_recovery(&MissingParentInput {
        violation: &violation,
        child_foreign_key_values: &[Some("78011674".to_string()), Some("fb42c5a9".to_string())],
        source_parent_rows: std::slice::from_ref(&source),
        target_parent_rows: &[],
    })
    .expect_err("mismatch");

    assert_eq!(
        rejection,
        MissingParentRejection::SourceParentIdentityMismatch
    );
}
