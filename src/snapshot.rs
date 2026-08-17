use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotTable {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
}

impl From<&crate::inventory::TableInventory> for SnapshotTable {
    fn from(table: &crate::inventory::TableInventory) -> Self {
        Self {
            name: table.name.clone(),
            primary_key: table.primary_key.clone(),
            columns: table
                .columns
                .iter()
                .filter(|column| column.generated.is_none())
                .map(|column| column.name.clone())
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotRow {
    pub primary_key: Vec<String>,
    pub values: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum SnapshotError {
    InvalidTable(String),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTable(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for SnapshotError {}
