use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CanonicalForeignKey {
    pub constraint_schema: String,
    pub constraint_name: String,
    pub child_schema: String,
    pub child_table: String,
    pub child_columns: Vec<String>,
    pub parent_schema: String,
    pub parent_table: String,
    pub parent_columns: Vec<String>,
    pub update_rule: String,
    pub delete_rule: String,
    pub match_option: String,
    pub enforced: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalForeignKeyRow {
    pub constraint_schema: String,
    pub constraint_name: String,
    pub child_schema: String,
    pub child_table: String,
    pub child_column: String,
    pub ordinal_position: u32,
    pub parent_schema: String,
    pub parent_table: String,
    pub parent_column: String,
    pub update_rule: String,
    pub delete_rule: String,
    pub match_option: String,
    pub enforced: bool,
}

pub fn canonicalize_foreign_keys(
    rows: Vec<CanonicalForeignKeyRow>,
) -> Result<Vec<CanonicalForeignKey>, String> {
    group_foreign_key_rows(sort_foreign_key_rows(rows))
        .into_iter()
        .map(build_canonical_foreign_key)
        .collect()
}

fn sort_foreign_key_rows(mut rows: Vec<CanonicalForeignKeyRow>) -> Vec<CanonicalForeignKeyRow> {
    rows.sort_by(|left, right| {
        (
            &left.constraint_schema,
            &left.constraint_name,
            &left.child_schema,
            &left.child_table,
            left.ordinal_position,
        )
            .cmp(&(
                &right.constraint_schema,
                &right.constraint_name,
                &right.child_schema,
                &right.child_table,
                right.ordinal_position,
            ))
    });
    rows
}

fn group_foreign_key_rows(
    rows: Vec<CanonicalForeignKeyRow>,
) -> BTreeMap<(String, String, String, String), Vec<CanonicalForeignKeyRow>> {
    rows.into_iter().fold(BTreeMap::new(), |mut grouped, row| {
        grouped
            .entry((
                row.constraint_schema.clone(),
                row.constraint_name.clone(),
                row.child_schema.clone(),
                row.child_table.clone(),
            ))
            .or_default()
            .push(row);
        grouped
    })
}

type ForeignKeyGroup = (
    (String, String, String, String),
    Vec<CanonicalForeignKeyRow>,
);

fn build_canonical_foreign_key(
    ((constraint_schema, constraint_name, child_schema, child_table), rows): ForeignKeyGroup,
) -> Result<CanonicalForeignKey, String> {
    let first = first_foreign_key_row(&rows)?;
    let update_rule = normalize_fk_rule(&first.update_rule);
    let delete_rule = normalize_fk_rule(&first.delete_rule);
    validate_foreign_key_group(
        &constraint_schema,
        &constraint_name,
        first,
        &rows,
        &update_rule,
        &delete_rule,
    )?;
    Ok(CanonicalForeignKey {
        constraint_schema,
        constraint_name,
        child_schema,
        child_table,
        child_columns: rows.iter().map(|row| row.child_column.clone()).collect(),
        parent_schema: first.parent_schema.clone(),
        parent_table: first.parent_table.clone(),
        parent_columns: rows.iter().map(|row| row.parent_column.clone()).collect(),
        update_rule,
        delete_rule,
        match_option: first.match_option.clone(),
        enforced: first.enforced,
    })
}

fn first_foreign_key_row(
    rows: &[CanonicalForeignKeyRow],
) -> Result<&CanonicalForeignKeyRow, String> {
    rows.first()
        .ok_or_else(|| "empty foreign-key group".to_string())
}

fn validate_foreign_key_group(
    schema: &str,
    name: &str,
    first: &CanonicalForeignKeyRow,
    rows: &[CanonicalForeignKeyRow],
    update_rule: &str,
    delete_rule: &str,
) -> Result<(), String> {
    let consistent = rows.iter().skip(1).all(|row| {
        row.child_schema == first.child_schema
            && row.child_table == first.child_table
            && row.parent_schema == first.parent_schema
            && row.parent_table == first.parent_table
            && normalize_fk_rule(&row.update_rule) == update_rule
            && normalize_fk_rule(&row.delete_rule) == delete_rule
            && row.match_option == first.match_option
            && row.enforced == first.enforced
    });
    consistent
        .then_some(())
        .ok_or_else(|| format!("foreign-key constraint {schema}.{name} has inconsistent metadata"))
}

fn normalize_fk_rule(rule: &str) -> String {
    if rule.eq_ignore_ascii_case("NO ACTION") {
        "RESTRICT".to_string()
    } else {
        rule.to_ascii_uppercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_inventory_preserves_schema_columns_and_rules() {
        let inventory =
            canonicalize_foreign_keys(vec![foreign_key_row("children", "RESTRICT", "CASCADE")])
                .expect("canonical inventory");

        assert_eq!(inventory[0].child_schema, "app");
        assert_eq!(inventory[0].parent_schema, "app");
        assert_eq!(inventory[0].child_columns, vec!["parent_id"]);
        assert_eq!(inventory[0].parent_columns, vec!["id"]);
        assert_eq!(inventory[0].delete_rule, "CASCADE");
        assert!(inventory[0].enforced);
    }

    #[test]
    fn same_constraint_name_remains_distinct_across_child_tables() {
        let inventory = canonicalize_foreign_keys(vec![
            foreign_key_row("child_a", "RESTRICT", "CASCADE"),
            foreign_key_row("child_b", "RESTRICT", "CASCADE"),
        ])
        .expect("canonical inventory");

        assert_eq!(inventory.len(), 2);
        assert_eq!(
            inventory
                .iter()
                .map(|foreign_key| foreign_key.child_table.as_str())
                .collect::<Vec<_>>(),
            vec!["child_a", "child_b"]
        );
    }

    #[test]
    fn no_action_rules_normalize_to_restrict() {
        let canonical =
            canonicalize_foreign_keys(vec![foreign_key_row("children", "NO ACTION", "NO ACTION")])
                .expect("canonical foreign key");

        assert_eq!(canonical[0].update_rule, "RESTRICT");
        assert_eq!(canonical[0].delete_rule, "RESTRICT");
    }

    fn foreign_key_row(
        child_table: &str,
        update_rule: &str,
        delete_rule: &str,
    ) -> CanonicalForeignKeyRow {
        CanonicalForeignKeyRow {
            constraint_schema: "app".to_string(),
            constraint_name: "parent_fk".to_string(),
            child_schema: "app".to_string(),
            child_table: child_table.to_string(),
            child_column: "parent_id".to_string(),
            ordinal_position: 1,
            parent_schema: "app".to_string(),
            parent_table: "parents".to_string(),
            parent_column: "id".to_string(),
            update_rule: update_rule.to_string(),
            delete_rule: delete_rule.to_string(),
            match_option: "NONE".to_string(),
            enforced: true,
        }
    }
}
