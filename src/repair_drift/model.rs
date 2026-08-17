use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

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

pub(crate) fn normalize_fk_rule(rule: &str) -> String {
    if rule.eq_ignore_ascii_case("NO ACTION") {
        "RESTRICT".to_string()
    } else {
        rule.to_ascii_uppercase()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepairInventory {
    pub schema: String,
    pub tables: Vec<String>,
    pub foreign_keys: Vec<CanonicalForeignKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairPlanError {
    SchemaMismatch(String),
    CrossSchema(String),
    Cycle(Vec<String>),
}

impl fmt::Display for RepairPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaMismatch(message) | Self::CrossSchema(message) => {
                formatter.write_str(message)
            }
            Self::Cycle(tables) => write!(
                formatter,
                "foreign-key cycle blocks repair: {}",
                tables.join(" -> ")
            ),
        }
    }
}

impl std::error::Error for RepairPlanError {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepairPlan {
    pub run_id: String,
    pub source_identity: String,
    pub target_identity: String,
    pub inventory_hash: String,
    pub plan_hash: String,
    pub tables: Vec<String>,
    pub delete_order: Vec<String>,
    pub insert_order: Vec<String>,
    pub update_order: Vec<String>,
}
