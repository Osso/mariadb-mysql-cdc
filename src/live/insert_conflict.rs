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
    error: &str,
) -> bool {
    policy == InsertConflictPolicy::IgnoreDuplicate
        && sql
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("INSERT INTO ")
        && error.contains("ERROR 1062")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_duplicate_insert_policy_ignores_only_insert_1062() {
        assert!(should_ignore_duplicate_insert(
            InsertConflictPolicy::IgnoreDuplicate,
            "INSERT INTO accounts (id) VALUES (1)",
            "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'",
        ));
        assert!(!should_ignore_duplicate_insert(
            InsertConflictPolicy::IgnoreDuplicate,
            "UPDATE accounts SET name = 'x' WHERE id = 1",
            "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'",
        ));
        assert!(!should_ignore_duplicate_insert(
            InsertConflictPolicy::Error,
            "INSERT INTO accounts (id) VALUES (1)",
            "ERROR 1062 (23000): Duplicate entry '1' for key 'PRIMARY'",
        ));
    }
}
