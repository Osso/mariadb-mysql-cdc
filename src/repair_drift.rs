mod config;
mod plan;
mod run;

#[cfg(test)]
mod tests;

pub use crate::conflict_repair::{
    CanonicalForeignKey, MySqlConflictStore, RepairInventory, RepairPlan, RepairPlanError,
    build_repair_plan,
};
use crate::table_sync::{self, SyncMode};
use crate::{live::TargetMySqlConfig, mysql_snapshot::MySqlConnectionConfig};
use std::fmt;

#[derive(Clone, Debug)]
pub struct RepairDriftConfig {
    pub source: MySqlConnectionConfig,
    pub source_identity: String,
    pub target: TargetMySqlConfig,
    pub tables: Vec<String>,
    pub parent_first: Vec<String>,
    pub start_after: Option<Vec<String>>,
    pub end_at: Option<Vec<String>>,
    pub content_check: bool,
    pub mode: SyncMode,
    pub chunk_size: usize,
    pub progress_table: String,
    pub max_deletes: Option<u64>,
    pub max_deletes_explicit: bool,
    pub run_id: Option<String>,
    pub run_id_prefix: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairDriftTableReport {
    pub table: String,
    pub run_id: String,
    pub source_count: u64,
    pub target_count: u64,
    pub sync_report: table_sync::SyncTableReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairDriftSkip {
    pub table: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairDriftReport {
    pub run_id: String,
    pub source_tables: usize,
    pub target_tables: usize,
    pub compared_tables: usize,
    pub drifted_tables: usize,
    pub repaired: Vec<RepairDriftTableReport>,
    pub skipped: Vec<RepairDriftSkip>,
}

#[derive(Debug)]
pub enum RepairDriftError {
    Config(String),
    Inventory(String),
    DriftCheck(String),
    Repair(String),
}

impl fmt::Display for RepairDriftError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => formatter.write_str(message),
            Self::Inventory(message) => {
                write!(formatter, "repair drift inventory failed: {message}")
            }
            Self::DriftCheck(message) => {
                write!(formatter, "repair drift count check failed: {message}")
            }
            Self::Repair(message) => {
                write!(formatter, "repair drift table repair failed: {message}")
            }
        }
    }
}

impl std::error::Error for RepairDriftError {}

pub fn run_repair_drift(config: &RepairDriftConfig) -> Result<RepairDriftReport, RepairDriftError> {
    run::run_repair_drift(config)
}

pub fn run_repair_drift_command(args: Vec<String>, usage: &str) {
    run::run_repair_drift_command(args, usage);
}

pub fn build_fk_aware_repair_plan(
    run_id: &str,
    source_identity: &str,
    target_identity: &str,
    source: &RepairInventory,
    target: &RepairInventory,
    max_deletes: u64,
) -> Result<RepairPlan, RepairPlanError> {
    plan::build_fk_aware_repair_plan(
        run_id,
        source_identity,
        target_identity,
        source,
        target,
        max_deletes,
    )
}

pub fn order_table_names(
    all_tables: &[String],
    parent_first: &[String],
) -> Result<Vec<String>, String> {
    plan::order_table_names(all_tables, parent_first)
}

pub fn drifted_table_names(comparisons: &[crate::drift_check::DriftComparison]) -> Vec<String> {
    plan::drifted_table_names(comparisons)
}
