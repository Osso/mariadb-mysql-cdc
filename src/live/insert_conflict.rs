#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum InsertConflictPolicy {
    #[default]
    Error,
    IgnoreDuplicate,
    ReplaceDivergentPk,
}

pub fn should_ignore_duplicate_insert(
    policy: InsertConflictPolicy,
    sql: &str,
    stderr: &str,
) -> bool {
    policy == InsertConflictPolicy::IgnoreDuplicate
        && starts_with_insert(sql)
        && is_duplicate_error(stderr)
}

pub fn should_ignore_duplicate_row_change(
    policy: InsertConflictPolicy,
    sql: &str,
    stderr: &str,
) -> bool {
    policy == InsertConflictPolicy::IgnoreDuplicate
        && starts_with_row_change(sql)
        && is_duplicate_error(stderr)
}

pub(crate) fn should_replace_divergent_primary(
    policy: InsertConflictPolicy,
    duplicate_index: Option<&str>,
    rows_equal: bool,
) -> bool {
    policy == InsertConflictPolicy::ReplaceDivergentPk
        && !rows_equal
        && duplicate_index.is_some_and(is_primary_duplicate_index)
}

fn is_primary_duplicate_index(index: &str) -> bool {
    index.eq_ignore_ascii_case("PRIMARY")
        || index
            .rsplit_once('.')
            .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case("PRIMARY"))
}

fn starts_with_row_change(sql: &str) -> bool {
    let sql = sql.trim_start().to_ascii_uppercase();
    sql.starts_with("INSERT INTO ") || sql.starts_with("UPDATE ")
}

fn is_duplicate_error(stderr: &str) -> bool {
    stderr.contains("ERROR 1062")
}

fn starts_with_insert(sql: &str) -> bool {
    sql.trim_start()
        .to_ascii_uppercase()
        .starts_with("INSERT INTO ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_insert_errors_can_be_ignored_in_catchup_mode() {
        assert!(should_ignore_duplicate_insert(
            InsertConflictPolicy::IgnoreDuplicate,
            "INSERT INTO accounts (id) VALUES (1)",
            "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'",
        ));
    }

    #[test]
    fn duplicate_errors_on_other_statements_still_fail() {
        assert!(!should_ignore_duplicate_insert(
            InsertConflictPolicy::IgnoreDuplicate,
            "UPDATE accounts SET name = 'x' WHERE id = 1",
            "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'",
        ));
    }

    #[test]
    fn duplicate_row_inserts_and_updates_can_be_skipped() {
        let duplicate = "ERROR 1062 (23000): Duplicate entry 'email' for key 'users.email'";

        assert!(should_ignore_duplicate_row_change(
            InsertConflictPolicy::IgnoreDuplicate,
            "INSERT INTO users (id, email) VALUES (?, ?)",
            duplicate,
        ));
        assert!(should_ignore_duplicate_row_change(
            InsertConflictPolicy::IgnoreDuplicate,
            "UPDATE users SET email = ? WHERE id = ?",
            duplicate,
        ));
    }

    #[test]
    fn duplicate_row_changes_fail_under_default_policy() {
        assert!(!should_ignore_duplicate_row_change(
            InsertConflictPolicy::Error,
            "INSERT INTO users (id) VALUES (?)",
            "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'",
        ));
    }

    #[test]
    fn duplicate_inserts_fail_under_default_policy() {
        assert!(!should_ignore_duplicate_insert(
            InsertConflictPolicy::Error,
            "INSERT INTO accounts (id) VALUES (1)",
            "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'",
        ));
    }

    #[test]
    fn replacement_requires_divergent_primary_duplicate() {
        assert!(should_replace_divergent_primary(
            InsertConflictPolicy::ReplaceDivergentPk,
            Some("PRIMARY"),
            false,
        ));
        assert!(should_replace_divergent_primary(
            InsertConflictPolicy::ReplaceDivergentPk,
            Some("accounts.PRIMARY"),
            false,
        ));
        assert!(!should_replace_divergent_primary(
            InsertConflictPolicy::ReplaceDivergentPk,
            Some("uq_accounts_email"),
            false,
        ));
        assert!(!should_replace_divergent_primary(
            InsertConflictPolicy::ReplaceDivergentPk,
            Some("PRIMARY"),
            true,
        ));
        assert!(!should_replace_divergent_primary(
            InsertConflictPolicy::Error,
            Some("PRIMARY"),
            false,
        ));
    }
}
