//! The violations here are the shapes seen on the production stream on 2026-07-24.

use super::*;

/// `comics -> artists(id, name)`: `id` is the parent key, `name` the cascaded attribute.
fn comics_artist_violation() -> ForeignKeyViolation {
    ForeignKeyViolation {
        child_schema: "globalcomix".to_string(),
        child_table: "comics".to_string(),
        constraint: "comics_ibfk_5".to_string(),
        child_columns: vec!["artist_id".to_string(), "artist_name".to_string()],
        parent_schema: None,
        parent_table: "artists".to_string(),
        parent_columns: vec!["id".to_string(), "name".to_string()],
    }
}

/// `paid_subscriptions_users_pages -> sessions(session_id)`: single column, no derived attribute.
fn session_violation() -> ForeignKeyViolation {
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

fn locked(columns: &[&str], rows: Vec<Vec<Value>>) -> LockedParentRows {
    LockedParentRows {
        columns: columns.iter().map(|column| (*column).to_string()).collect(),
        rows,
    }
}

fn columns(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

struct Fixture {
    violation: ForeignKeyViolation,
    foreign_key: ForeignKeyInventory,
    parent_primary_key: Vec<String>,
    child_columns: Vec<String>,
    child_values: Vec<Value>,
    locked_parent: LockedParentRows,
}

impl Fixture {
    fn plan(&self) -> Result<ForeignKeyRepairPlan, ForeignKeyRepairRejection> {
        plan_foreign_key_repair(&ForeignKeyRepairInput {
            violation: &self.violation,
            foreign_key: &self.foreign_key,
            operation: ConflictOperation::Insert,
            error_code: 1452,
            parent_primary_key: &self.parent_primary_key,
            child_columns: &self.child_columns,
            child_values: &self.child_values,
            locked_parent: &self.locked_parent,
        })
    }
}

/// The child carries the artist's old name; the locked parent already holds the new one.
fn superseded_artist_fixture() -> Fixture {
    let violation = comics_artist_violation();
    Fixture {
        foreign_key: foreign_key_from_violation(&violation),
        violation,
        parent_primary_key: columns(&["id"]),
        child_columns: columns(&["id", "title", "artist_id", "artist_name"]),
        child_values: vec![
            Value::UInt(500),
            Value::Bytes(b"Some Comic".to_vec()),
            Value::UInt(42),
            Value::Bytes(b"Old Name".to_vec()),
        ],
        locked_parent: locked(
            &["id", "name"],
            vec![vec![Value::UInt(42), Value::Bytes(b"New Name".to_vec())]],
        ),
    }
}

#[test]
fn installs_the_parent_when_the_locked_identity_is_absent() {
    let mut fixture = superseded_artist_fixture();
    fixture.locked_parent = locked(&["id", "name"], Vec::new());

    assert_eq!(
        fixture.plan().expect("plan"),
        ForeignKeyRepairPlan::InstallParent
    );
}

/// A single-column foreign key has no derived attribute, so an absent parent is the only cause it
/// can have. This is the constraint that stalled production twice.
#[test]
fn installs_the_parent_for_a_single_column_constraint() {
    let violation = session_violation();
    let fixture = Fixture {
        foreign_key: foreign_key_from_violation(&violation),
        violation,
        parent_primary_key: columns(&["session_id"]),
        child_columns: columns(&["id", "session_id"]),
        child_values: vec![Value::UInt(8812), Value::Bytes(b"sess-abc".to_vec())],
        locked_parent: locked(&["session_id"], Vec::new()),
    };

    assert_eq!(
        fixture.plan().expect("plan"),
        ForeignKeyRepairPlan::InstallParent
    );
}

#[test]
fn fast_forwards_only_the_derived_column_when_the_parent_is_present() {
    let fixture = superseded_artist_fixture();

    let ForeignKeyRepairPlan::FastForwardChild(plan) = fixture.plan().expect("plan") else {
        panic!("a present parent with a moved attribute must fast-forward");
    };

    assert_eq!(plan.constraint, "comics_ibfk_5");
    assert_eq!(plan.parent_table, "artists");
    assert_eq!(plan.substitutions.len(), 1, "only the derived column moves");
    assert_eq!(plan.substitutions[0].child_column, "artist_name");
    assert_eq!(
        plan.substitutions[0].parent_value,
        Value::Bytes(b"New Name".to_vec())
    );
}

#[test]
fn fails_closed_when_more_than_one_parent_owns_the_identity() {
    let mut fixture = superseded_artist_fixture();
    fixture.locked_parent = locked(
        &["id", "name"],
        vec![
            vec![Value::UInt(42), Value::Bytes(b"One".to_vec())],
            vec![Value::UInt(42), Value::Bytes(b"Two".to_vec())],
        ],
    );

    assert_eq!(
        fixture
            .plan()
            .expect_err("ambiguous parent must fail closed"),
        ForeignKeyRepairRejection::ParentAmbiguous
    );
}

/// The locked read must have selected exactly the columns the error named, or the values compared
/// below would be the wrong columns.
#[test]
fn fails_closed_when_the_locked_read_shape_does_not_match_the_error() {
    let mut fixture = superseded_artist_fixture();
    fixture.locked_parent = locked(
        &["id", "slug"],
        vec![vec![Value::UInt(42), Value::Bytes(b"x".to_vec())]],
    );

    assert_eq!(
        fixture.plan().expect_err("shape mismatch must fail closed"),
        ForeignKeyRepairRejection::LockedParentShapeMismatch
    );
}

/// The parent key itself differing is not a superseded attribute; it means the locked read did not
/// return the parent that was asked for.
#[test]
fn fails_closed_when_the_parent_key_itself_differs() {
    let mut fixture = superseded_artist_fixture();
    fixture.locked_parent = locked(
        &["id", "name"],
        vec![vec![Value::UInt(99), Value::Bytes(b"New Name".to_vec())]],
    );

    let rejection = fixture.plan().expect_err("key mismatch must fail closed");
    assert!(
        rejection.reason().contains("ParentIdentityMismatch"),
        "{rejection}"
    );
}

#[test]
fn rebuilds_the_constraint_from_the_error_and_defaults_the_parent_schema() {
    let foreign_key = foreign_key_from_violation(&comics_artist_violation());

    assert_eq!(foreign_key.name, "comics_ibfk_5");
    assert_eq!(foreign_key.table, "comics");
    assert_eq!(foreign_key.columns, columns(&["artist_id", "artist_name"]));
    assert_eq!(foreign_key.referenced_table, "artists");
    assert_eq!(foreign_key.referenced_columns, columns(&["id", "name"]));
    assert_eq!(
        foreign_key.referenced_schema, "globalcomix",
        "an unqualified parent shares the child schema"
    );
}

#[test]
fn keeps_a_qualified_parent_schema_when_the_error_states_one() {
    let mut violation = comics_artist_violation();
    violation.parent_schema = Some("other".to_string());

    assert_eq!(
        foreign_key_from_violation(&violation).referenced_schema,
        "other"
    );
}

/// The predicate must use the parent key alone, taking the child value that references it.
#[test]
fn builds_the_locked_predicate_from_the_parent_key_only() {
    let fixture = superseded_artist_fixture();

    let predicate = parent_primary_key_predicate(
        &fixture.violation,
        &fixture.parent_primary_key,
        &fixture.child_columns,
        &fixture.child_values,
    )
    .expect("predicate");

    assert_eq!(predicate, vec![("id".to_string(), Value::UInt(42))]);
}

#[test]
fn builds_a_composite_locked_predicate_in_parent_key_order() {
    let violation = ForeignKeyViolation {
        child_schema: "globalcomix".to_string(),
        child_table: "sessions".to_string(),
        constraint: "fk_sessions_guest".to_string(),
        child_columns: columns(&["guest_id", "guest_hash"]),
        parent_schema: None,
        parent_table: "guests".to_string(),
        parent_columns: columns(&["guest_id", "guest_hash"]),
    };

    let predicate = parent_primary_key_predicate(
        &violation,
        &columns(&["guest_id", "guest_hash"]),
        &columns(&["session_id", "guest_hash", "guest_id"]),
        &[
            Value::Bytes(b"s1".to_vec()),
            Value::Bytes(b"h1".to_vec()),
            Value::UInt(5),
        ],
    )
    .expect("predicate");

    assert_eq!(
        predicate,
        vec![
            ("guest_id".to_string(), Value::UInt(5)),
            ("guest_hash".to_string(), Value::Bytes(b"h1".to_vec())),
        ]
    );
}

#[test]
fn declines_a_locked_predicate_when_the_child_value_is_null() {
    let fixture = superseded_artist_fixture();
    let child_values = vec![
        Value::UInt(500),
        Value::Bytes(b"Some Comic".to_vec()),
        Value::NULL,
        Value::Bytes(b"Old Name".to_vec()),
    ];

    assert!(
        parent_primary_key_predicate(
            &fixture.violation,
            &fixture.parent_primary_key,
            &fixture.child_columns,
            &child_values,
        )
        .is_none()
    );
}

#[test]
fn declines_a_locked_predicate_when_the_image_lacks_the_referencing_column() {
    let fixture = superseded_artist_fixture();

    assert!(
        parent_primary_key_predicate(
            &fixture.violation,
            &fixture.parent_primary_key,
            &columns(&["id", "title", "artist_name"]),
            &[
                Value::UInt(500),
                Value::Bytes(b"Some Comic".to_vec()),
                Value::Bytes(b"Old Name".to_vec()),
            ],
        )
        .is_none()
    );
}

/// Only the substituted column changes; every other column keeps its historical value.
#[test]
fn rebuilds_the_child_image_with_only_the_derived_column_moved() {
    let fixture = superseded_artist_fixture();
    let ForeignKeyRepairPlan::FastForwardChild(plan) = fixture.plan().expect("plan") else {
        panic!("expected a fast-forward plan");
    };

    let row = fast_forwarded_child_row(&fixture.child_columns, &fixture.child_values, &plan)
        .expect("rebuilt child image");

    assert_eq!(row.columns, fixture.child_columns);
    assert_eq!(
        row.values,
        vec![
            Value::UInt(500),
            Value::Bytes(b"Some Comic".to_vec()),
            Value::UInt(42),
            Value::Bytes(b"New Name".to_vec()),
        ]
    );
}

#[test]
fn declines_to_rebuild_a_child_image_of_mismatched_length() {
    let fixture = superseded_artist_fixture();
    let ForeignKeyRepairPlan::FastForwardChild(plan) = fixture.plan().expect("plan") else {
        panic!("expected a fast-forward plan");
    };

    assert!(fast_forwarded_child_row(&fixture.child_columns, &[Value::UInt(1)], &plan).is_none());
}
