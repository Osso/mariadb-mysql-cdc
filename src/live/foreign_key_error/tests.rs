//! Fixtures are the verbatim `1452` errors observed on the production stream and in
//! `cdc.row_conflicts` on 2026-07-24.

use super::*;

fn violation(error_text: &str) -> ForeignKeyViolation {
    parse_foreign_key_violation(error_text).expect("foreign key violation")
}

/// The constraint that stalled the stream twice at `mysqld-bin.002709:753030230` and `:744193436`.
#[test]
fn parses_the_paid_subscriptions_session_violation() {
    let parsed = violation(
        "target mysql query failed: MySqlError { ERROR 1452 (23000): Cannot add or update a child \
         row: a foreign key constraint fails (`globalcomix`.`paid_subscriptions_users_pages`, \
         CONSTRAINT `fk_paid_subscriptions_users_pages_session_id` FOREIGN KEY (`session_id`) \
         REFERENCES `sessions` (`session_id`)) }",
    );

    assert_eq!(
        parsed,
        ForeignKeyViolation {
            child_schema: "globalcomix".to_string(),
            child_table: "paid_subscriptions_users_pages".to_string(),
            constraint: "fk_paid_subscriptions_users_pages_session_id".to_string(),
            child_columns: vec!["session_id".to_string()],
            parent_schema: None,
            parent_table: "sessions".to_string(),
            parent_columns: vec!["session_id".to_string()],
        }
    );
}

#[test]
fn parses_the_users_search_queries_history_guest_violation() {
    let parsed = violation(
        "target mysql query failed: MySqlError { ERROR 1452 (23000): Cannot add or update a child \
         row: a foreign key constraint fails (`globalcomix`.`users_search_queries_history`, \
         CONSTRAINT `fk_users_search_queries_history_guest_id` FOREIGN KEY (`guest_id`) \
         REFERENCES `guests` (`guest_id`)) }",
    );

    assert_eq!(parsed.child_table, "users_search_queries_history");
    assert_eq!(parsed.parent_table, "guests");
    assert_eq!(parsed.child_columns, vec!["guest_id".to_string()]);
}

/// The multi-column constraint that already had a hardcoded recovery path.
#[test]
fn parses_the_multi_column_sessions_guest_violation() {
    let parsed = violation(
        "target mysql query failed: MySqlError { ERROR 1452 (23000): Cannot add or update a child \
         row: a foreign key constraint fails (`globalcomix`.`sessions`, CONSTRAINT \
         `fk_sessions_guest` FOREIGN KEY (`guest_id`, `guest_hash`) REFERENCES `guests` \
         (`guest_id`, `guest_hash`)) }",
    );

    assert_eq!(parsed.constraint, "fk_sessions_guest");
    assert_eq!(
        parsed.child_columns,
        vec!["guest_id".to_string(), "guest_hash".to_string()]
    );
    assert_eq!(
        parsed.parent_columns,
        vec!["guest_id".to_string(), "guest_hash".to_string()]
    );
}

/// The denormalised cascade pair that stalled the stream at `mysqld-bin.002709:564272818`.
#[test]
fn parses_the_comics_artist_violation_with_cascade_clauses() {
    let parsed = violation(
        "target mysql query failed: MySqlError { ERROR 1452 (23000): Cannot add or update a child \
         row: a foreign key constraint fails (`globalcomix`.`comics`, CONSTRAINT `comics_ibfk_5` \
         FOREIGN KEY (`artist_id`, `artist_name`) REFERENCES `artists` (`id`, `name`) ON DELETE \
         RESTRICT ON UPDATE CASCADE) }",
    );

    assert_eq!(parsed.child_table, "comics");
    assert_eq!(parsed.constraint, "comics_ibfk_5");
    assert_eq!(
        parsed.child_columns,
        vec!["artist_id".to_string(), "artist_name".to_string()]
    );
    assert_eq!(parsed.parent_table, "artists");
    assert_eq!(
        parsed.parent_columns,
        vec!["id".to_string(), "name".to_string()]
    );
}

/// The release visibility pair that stalled the stream at `mysqld-bin.002709:531921789`.
#[test]
fn parses_the_releases_visibility_violation() {
    let parsed = violation(
        "Cannot add or update a child row: a foreign key constraint fails \
         (`globalcomix`.`releases`, CONSTRAINT `releases_ibfk_3` FOREIGN KEY (`comic_id`, \
         `comic_is_visible`) REFERENCES `comics` (`id`, `is_visible`) ON DELETE RESTRICT ON \
         UPDATE CASCADE)",
    );

    assert_eq!(parsed.parent_table, "comics");
    assert_eq!(
        parsed.parent_columns,
        vec!["id".to_string(), "is_visible".to_string()]
    );
}

#[test]
fn parses_a_schema_qualified_parent_reference() {
    let parsed = violation(
        "Cannot add or update a child row: a foreign key constraint fails (`globalcomix`.`orders`, \
         CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `other`.`users` (`id`))",
    );

    assert_eq!(parsed.parent_schema, Some("other".to_string()));
    assert_eq!(parsed.parent_table, "users");
}

#[test]
fn decodes_a_doubled_backtick_in_an_identifier() {
    let parsed = violation(
        "Cannot add or update a child row: a foreign key constraint fails \
         (`globalcomix`.`od``d`, CONSTRAINT `c1` FOREIGN KEY (`a`) REFERENCES `p` (`b`))",
    );

    assert_eq!(parsed.child_table, "od`d");
}

#[test]
fn rejects_a_non_foreign_key_error() {
    assert!(
        parse_foreign_key_violation(
            "target mysql query failed: MySqlError { ERROR 1062 (23000): Duplicate entry \
             'abc' for key 'guests.idx_guest_hash' }"
        )
        .is_none()
    );
}

#[test]
fn rejects_a_truncated_foreign_key_error() {
    assert!(
        parse_foreign_key_violation(
            "Cannot add or update a child row: a foreign key constraint fails \
             (`globalcomix`.`sessions`, CONSTRAINT `fk_sessions_guest` FOREIGN KEY (`guest_id`"
        )
        .is_none()
    );
}

/// A column count mismatch means the text was not the documented shape, so it must not be trusted.
#[test]
fn rejects_mismatched_column_counts() {
    assert!(
        parse_foreign_key_violation(
            "Cannot add or update a child row: a foreign key constraint fails \
             (`globalcomix`.`sessions`, CONSTRAINT `fk_sessions_guest` FOREIGN KEY (`guest_id`, \
             `guest_hash`) REFERENCES `guests` (`guest_id`))"
        )
        .is_none()
    );
}
