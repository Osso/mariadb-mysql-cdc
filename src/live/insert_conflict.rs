#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum InsertConflictPolicy {
    #[default]
    Error,
    IgnoreDuplicate,
}

pub fn should_ignore_duplicate_insert(
    policy: InsertConflictPolicy,
    sql: &str,
    stderr: &str,
) -> bool {
    policy == InsertConflictPolicy::IgnoreDuplicate
        && starts_with_insert(sql)
        && stderr.contains("ERROR 1062")
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
    fn duplicate_inserts_fail_under_default_policy() {
        assert!(!should_ignore_duplicate_insert(
            InsertConflictPolicy::Error,
            "INSERT INTO accounts (id) VALUES (1)",
            "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'",
        ));
    }
}
