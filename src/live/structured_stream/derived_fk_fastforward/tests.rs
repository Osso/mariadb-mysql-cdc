//! Fixtures reproduce the two production stalls observed on `mysqld-bin.002709`:
//! `comics` / `comics_ibfk_5` at position 564272818 and `releases` / `releases_ibfk_3` at
//! position 531921789.

use super::*;

fn comics_artists_foreign_key() -> ForeignKeyInventory {
    ForeignKeyInventory {
        table: "comics".to_string(),
        name: "comics_ibfk_5".to_string(),
        columns: vec!["artist_id".to_string(), "artist_name".to_string()],
        referenced_schema: "globalcomix".to_string(),
        referenced_table: "artists".to_string(),
        referenced_columns: vec!["id".to_string(), "name".to_string()],
    }
}

fn releases_comics_foreign_key() -> ForeignKeyInventory {
    ForeignKeyInventory {
        table: "releases".to_string(),
        name: "releases_ibfk_3".to_string(),
        columns: vec!["comic_id".to_string(), "comic_is_visible".to_string()],
        referenced_schema: "globalcomix".to_string(),
        referenced_table: "comics".to_string(),
        referenced_columns: vec!["id".to_string(), "is_visible".to_string()],
    }
}

fn guests_foreign_key() -> ForeignKeyInventory {
    ForeignKeyInventory {
        table: "users_search_queries_history".to_string(),
        name: "fk_users_search_queries_history_guest_id".to_string(),
        columns: vec!["guest_id".to_string()],
        referenced_schema: "globalcomix".to_string(),
        referenced_table: "guests".to_string(),
        referenced_columns: vec!["guest_id".to_string()],
    }
}

fn bytes(text: &str) -> Value {
    Value::Bytes(text.as_bytes().to_vec())
}

/// `comics` 48057 carried `artist_name='kalyancomics'` while the target artist had already been
/// renamed to `721e2822-4e99-4d56-963f-4029271e74d2RqFh`.
#[test]
fn substitutes_only_the_renamed_artist_name_for_the_comics_insert() {
    let foreign_key = comics_artists_foreign_key();
    let parent_primary_key = vec!["id".to_string()];
    let child_columns = vec![
        "id".to_string(),
        "slug".to_string(),
        "artist_id".to_string(),
        "artist_name".to_string(),
    ];
    let child_values = vec![
        Value::UInt(48057),
        bytes("venuvupuram"),
        Value::UInt(32168),
        bytes("kalyancomics"),
    ];
    let locked_parent = LockedParentRows {
        columns: vec!["id".to_string(), "name".to_string()],
        rows: vec![vec![
            Value::UInt(32168),
            bytes("721e2822-4e99-4d56-963f-4029271e74d2RqFh"),
        ]],
    };
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "comics",
        operation: ConflictOperation::Insert,
        error_code: 1452,
        constraint: "comics_ibfk_5",
        foreign_key: Some(&foreign_key),
        parent_primary_key: &parent_primary_key,
        child_columns: &child_columns,
        child_values: &child_values,
        locked_parent: &locked_parent,
    };

    let plan = plan_derived_fk_fastforward(&input).expect("plan");

    assert_eq!(
        plan.substitutions,
        vec![DerivedFkSubstitution {
            child_column: "artist_name".to_string(),
            referenced_column: "name".to_string(),
            historical_value: bytes("kalyancomics"),
            parent_value: bytes("721e2822-4e99-4d56-963f-4029271e74d2RqFh"),
        }]
    );
    assert_eq!(plan.parent_table, "artists");
    assert!(plan.evidence().contains("comics_ibfk_5"));
    assert!(
        plan.evidence()
            .contains("artist_name: kalyancomics -> 721e2822-4e99-4d56-963f-4029271e74d2RqFh")
    );
}

/// `releases` 384447 carried `comic_is_visible=1` while the target comic was already hidden.
#[test]
fn substitutes_the_hidden_comic_visibility_for_the_releases_insert() {
    let foreign_key = releases_comics_foreign_key();
    let parent_primary_key = vec!["id".to_string()];
    let child_columns = vec![
        "id".to_string(),
        "comic_id".to_string(),
        "comic_is_visible".to_string(),
    ];
    let child_values = vec![Value::UInt(384447), Value::UInt(48054), Value::UInt(1)];
    let locked_parent = LockedParentRows {
        columns: vec!["id".to_string(), "is_visible".to_string()],
        rows: vec![vec![Value::UInt(48054), Value::UInt(0)]],
    };
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "releases",
        operation: ConflictOperation::Insert,
        error_code: 1452,
        constraint: "releases_ibfk_3",
        foreign_key: Some(&foreign_key),
        parent_primary_key: &parent_primary_key,
        child_columns: &child_columns,
        child_values: &child_values,
        locked_parent: &locked_parent,
    };

    let plan = plan_derived_fk_fastforward(&input).expect("plan");

    assert_eq!(plan.substitutions.len(), 1);
    assert_eq!(plan.substitutions[0].child_column, "comic_is_visible");
    assert_eq!(plan.substitutions[0].parent_value, Value::UInt(0));
}

/// A bare integer child value and a text parent value are the same foreign-key value.
#[test]
fn treats_text_and_integer_encodings_of_the_same_key_as_equal() {
    let foreign_key = releases_comics_foreign_key();
    let parent_primary_key = vec!["id".to_string()];
    let child_columns = vec!["comic_id".to_string(), "comic_is_visible".to_string()];
    let child_values = vec![bytes("48054"), Value::UInt(1)];
    let locked_parent = LockedParentRows {
        columns: vec!["id".to_string(), "is_visible".to_string()],
        rows: vec![vec![Value::UInt(48054), bytes("0")]],
    };
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "releases",
        operation: ConflictOperation::Insert,
        error_code: 1452,
        constraint: "releases_ibfk_3",
        foreign_key: Some(&foreign_key),
        parent_primary_key: &parent_primary_key,
        child_columns: &child_columns,
        child_values: &child_values,
        locked_parent: &locked_parent,
    };

    let plan = plan_derived_fk_fastforward(&input).expect("plan");

    assert_eq!(plan.substitutions.len(), 1);
    assert_eq!(plan.substitutions[0].child_column, "comic_is_visible");
}

fn releases_rejection_input(
    foreign_key: &ForeignKeyInventory,
    parent_primary_key: &[String],
    child_columns: &[String],
    child_values: &[Value],
    locked_parent: &LockedParentRows,
) -> DerivedFkRejection {
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "releases",
        operation: ConflictOperation::Insert,
        error_code: 1452,
        constraint: "releases_ibfk_3",
        foreign_key: Some(foreign_key),
        parent_primary_key,
        child_columns,
        child_values,
        locked_parent,
    };
    plan_derived_fk_fastforward(&input).expect_err("rejection")
}

/// A missing parent row is the separate missing-parent class and must not be fast-forwarded.
#[test]
fn rejects_a_missing_parent_row() {
    let foreign_key = releases_comics_foreign_key();
    let rejection = releases_rejection_input(
        &foreign_key,
        &["id".to_string()],
        &["comic_id".to_string(), "comic_is_visible".to_string()],
        &[Value::UInt(48054), Value::UInt(1)],
        &LockedParentRows {
            columns: vec!["id".to_string(), "is_visible".to_string()],
            rows: vec![],
        },
    );

    assert_eq!(rejection, DerivedFkRejection::ParentRowNotUnique);
}

#[test]
fn rejects_an_ambiguous_parent_row() {
    let foreign_key = releases_comics_foreign_key();
    let rejection = releases_rejection_input(
        &foreign_key,
        &["id".to_string()],
        &["comic_id".to_string(), "comic_is_visible".to_string()],
        &[Value::UInt(48054), Value::UInt(1)],
        &LockedParentRows {
            columns: vec!["id".to_string(), "is_visible".to_string()],
            rows: vec![
                vec![Value::UInt(48054), Value::UInt(0)],
                vec![Value::UInt(48054), Value::UInt(1)],
            ],
        },
    );

    assert_eq!(rejection, DerivedFkRejection::ParentRowNotUnique);
}

/// A different parent identity is unrelated drift, not a superseded derived attribute.
#[test]
fn rejects_a_parent_identity_mismatch() {
    let foreign_key = releases_comics_foreign_key();
    let rejection = releases_rejection_input(
        &foreign_key,
        &["id".to_string()],
        &["comic_id".to_string(), "comic_is_visible".to_string()],
        &[Value::UInt(48054), Value::UInt(1)],
        &LockedParentRows {
            columns: vec!["id".to_string(), "is_visible".to_string()],
            rows: vec![vec![Value::UInt(99999), Value::UInt(1)]],
        },
    );

    assert_eq!(rejection, DerivedFkRejection::ParentIdentityMismatch);
}

#[test]
fn rejects_when_no_derived_column_drifted() {
    let foreign_key = releases_comics_foreign_key();
    let rejection = releases_rejection_input(
        &foreign_key,
        &["id".to_string()],
        &["comic_id".to_string(), "comic_is_visible".to_string()],
        &[Value::UInt(48054), Value::UInt(1)],
        &LockedParentRows {
            columns: vec!["id".to_string(), "is_visible".to_string()],
            rows: vec![vec![Value::UInt(48054), Value::UInt(1)]],
        },
    );

    assert_eq!(rejection, DerivedFkRejection::NoDerivedDrift);
}

#[test]
fn rejects_a_null_foreign_key_value() {
    let foreign_key = releases_comics_foreign_key();
    let rejection = releases_rejection_input(
        &foreign_key,
        &["id".to_string()],
        &["comic_id".to_string(), "comic_is_visible".to_string()],
        &[Value::UInt(48054), Value::NULL],
        &LockedParentRows {
            columns: vec!["id".to_string(), "is_visible".to_string()],
            rows: vec![vec![Value::UInt(48054), Value::UInt(0)]],
        },
    );

    assert_eq!(rejection, DerivedFkRejection::NullForeignKeyValue);
}

#[test]
fn rejects_a_child_column_missing_from_the_event_image() {
    let foreign_key = releases_comics_foreign_key();
    let rejection = releases_rejection_input(
        &foreign_key,
        &["id".to_string()],
        &["comic_id".to_string()],
        &[Value::UInt(48054)],
        &LockedParentRows {
            columns: vec!["id".to_string(), "is_visible".to_string()],
            rows: vec![vec![Value::UInt(48054), Value::UInt(0)]],
        },
    );

    assert_eq!(rejection, DerivedFkRejection::MissingChildColumn);
}

/// A single-column foreign key has no derived attribute to fast-forward.
#[test]
fn rejects_a_single_column_foreign_key() {
    let foreign_key = guests_foreign_key();
    let parent_primary_key = vec!["guest_id".to_string()];
    let child_columns = vec!["guest_id".to_string()];
    let child_values = vec![Value::UInt(86371285)];
    let locked_parent = LockedParentRows {
        columns: vec!["guest_id".to_string()],
        rows: vec![],
    };
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "users_search_queries_history",
        operation: ConflictOperation::Insert,
        error_code: 1452,
        constraint: "fk_users_search_queries_history_guest_id",
        foreign_key: Some(&foreign_key),
        parent_primary_key: &parent_primary_key,
        child_columns: &child_columns,
        child_values: &child_values,
        locked_parent: &locked_parent,
    };

    assert_eq!(
        plan_derived_fk_fastforward(&input).expect_err("rejection"),
        DerivedFkRejection::SingleColumnForeignKey
    );
}

/// Every referenced column being part of the parent primary key leaves nothing derived.
#[test]
fn rejects_a_foreign_key_that_references_only_the_parent_primary_key() {
    let foreign_key = ForeignKeyInventory {
        table: "sessions".to_string(),
        name: "fk_sessions_guest".to_string(),
        columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
        referenced_schema: "globalcomix".to_string(),
        referenced_table: "guests".to_string(),
        referenced_columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
    };
    let parent_primary_key = vec!["guest_id".to_string(), "guest_hash".to_string()];
    let child_columns = vec!["guest_id".to_string(), "guest_hash".to_string()];
    let child_values = vec![Value::UInt(78011674), bytes("fb42c5a9")];
    let locked_parent = LockedParentRows {
        columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
        rows: vec![vec![Value::UInt(78011674), bytes("fb42c5a9")]],
    };
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "sessions",
        operation: ConflictOperation::Insert,
        error_code: 1452,
        constraint: "fk_sessions_guest",
        foreign_key: Some(&foreign_key),
        parent_primary_key: &parent_primary_key,
        child_columns: &child_columns,
        child_values: &child_values,
        locked_parent: &locked_parent,
    };

    assert_eq!(
        plan_derived_fk_fastforward(&input).expect_err("rejection"),
        DerivedFkRejection::NoDerivedDrift
    );
}

#[test]
fn rejects_a_non_foreign_key_error_code() {
    let foreign_key = releases_comics_foreign_key();
    let parent_primary_key = vec!["id".to_string()];
    let child_columns = vec!["comic_id".to_string(), "comic_is_visible".to_string()];
    let child_values = vec![Value::UInt(48054), Value::UInt(1)];
    let locked_parent = LockedParentRows {
        columns: vec!["id".to_string(), "is_visible".to_string()],
        rows: vec![vec![Value::UInt(48054), Value::UInt(0)]],
    };
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "releases",
        operation: ConflictOperation::Insert,
        error_code: 1062,
        constraint: "releases_ibfk_3",
        foreign_key: Some(&foreign_key),
        parent_primary_key: &parent_primary_key,
        child_columns: &child_columns,
        child_values: &child_values,
        locked_parent: &locked_parent,
    };

    assert_eq!(
        plan_derived_fk_fastforward(&input).expect_err("rejection"),
        DerivedFkRejection::WrongErrorCode
    );
}

#[test]
fn rejects_an_unknown_constraint() {
    let parent_primary_key = vec!["id".to_string()];
    let child_columns = vec!["comic_id".to_string(), "comic_is_visible".to_string()];
    let child_values = vec![Value::UInt(48054), Value::UInt(1)];
    let locked_parent = LockedParentRows {
        columns: vec!["id".to_string(), "is_visible".to_string()],
        rows: vec![vec![Value::UInt(48054), Value::UInt(0)]],
    };
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "releases",
        operation: ConflictOperation::Insert,
        error_code: 1452,
        constraint: "releases_ibfk_3",
        foreign_key: None,
        parent_primary_key: &parent_primary_key,
        child_columns: &child_columns,
        child_values: &child_values,
        locked_parent: &locked_parent,
    };

    assert_eq!(
        plan_derived_fk_fastforward(&input).expect_err("rejection"),
        DerivedFkRejection::UnknownConstraint
    );
}

#[test]
fn rejects_locked_parent_columns_that_do_not_match_the_constraint() {
    let foreign_key = releases_comics_foreign_key();
    let rejection = releases_rejection_input(
        &foreign_key,
        &["id".to_string()],
        &["comic_id".to_string(), "comic_is_visible".to_string()],
        &[Value::UInt(48054), Value::UInt(1)],
        &LockedParentRows {
            columns: vec!["id".to_string(), "show_in_list".to_string()],
            rows: vec![vec![Value::UInt(48054), Value::UInt(0)]],
        },
    );

    assert_eq!(rejection, DerivedFkRejection::LockedParentColumnsMismatch);
}

#[test]
fn rejects_a_delete_operation() {
    let foreign_key = releases_comics_foreign_key();
    let parent_primary_key = vec!["id".to_string()];
    let child_columns = vec!["comic_id".to_string(), "comic_is_visible".to_string()];
    let child_values = vec![Value::UInt(48054), Value::UInt(1)];
    let locked_parent = LockedParentRows {
        columns: vec!["id".to_string(), "is_visible".to_string()],
        rows: vec![vec![Value::UInt(48054), Value::UInt(0)]],
    };
    let input = DerivedFkFastForwardInput {
        schema: "globalcomix",
        child_table: "releases",
        operation: ConflictOperation::Delete,
        error_code: 1452,
        constraint: "releases_ibfk_3",
        foreign_key: Some(&foreign_key),
        parent_primary_key: &parent_primary_key,
        child_columns: &child_columns,
        child_values: &child_values,
        locked_parent: &locked_parent,
    };

    assert_eq!(
        plan_derived_fk_fastforward(&input).expect_err("rejection"),
        DerivedFkRejection::WrongOperation
    );
}
