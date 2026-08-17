use crate::canonical_foreign_key::CanonicalForeignKey;
use serde::{Deserialize, Serialize};
use std::fmt;

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
