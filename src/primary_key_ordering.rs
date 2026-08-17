use crate::inventory::TableInventory;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "labels")]
pub enum PrimaryKeyOrdering {
    Native,
    Enum(Vec<String>),
}

pub(crate) fn primary_key_ordering_from_inventory(
    table: &TableInventory,
) -> Result<Vec<PrimaryKeyOrdering>, String> {
    table
        .primary_key
        .iter()
        .map(|name| primary_key_column_ordering(table, name))
        .collect()
}

fn primary_key_column_ordering(
    table: &TableInventory,
    name: &str,
) -> Result<PrimaryKeyOrdering, String> {
    let column = table
        .columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| {
            format!(
                "primary-key column `{name}` is absent from `{}` inventory",
                table.name
            )
        })?;
    Ok(
        match crate::sql_type::parse_enum_column_type(&column.column_type) {
            Some(labels) => PrimaryKeyOrdering::Enum(labels),
            None => PrimaryKeyOrdering::Native,
        },
    )
}
