use super::AdditiveRunSpecTableChange;
use super::config::{SyncRunIdentity, SyncRunSpec, plan_additive_run_spec_migration};
use super::model::SyncStage;
use crate::inventory::SchemaInventory;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LockedSyncProgressRow {
    pub(crate) stage: SyncStage,
    pub(crate) table_name: String,
    pub(crate) run_spec_json: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SyncRunSpecMigrationDecision {
    AlreadyCurrent {
        locked_row_count: usize,
        current_sha256: String,
    },
    UpdateRequired {
        expected_locked_row_count: usize,
        old_sha256: String,
        new_sha256: String,
        changed_tables: Vec<AdditiveRunSpecTableChange>,
    },
}

pub(crate) fn decide_locked_run_spec_migration(
    locked_rows: &[LockedSyncProgressRow],
    authorized_old_sha256: &str,
    current: &SyncRunIdentity,
    source: &SchemaInventory,
    target: &SchemaInventory,
) -> Result<SyncRunSpecMigrationDecision, String> {
    let persisted_json = require_one_persisted_spec(locked_rows)?;
    let current_sha256 = sha256_hex(&current.run_spec_json);
    if persisted_json == current.run_spec_json {
        validate_progress_scope(locked_rows, &current.run_spec)?;
        return Ok(SyncRunSpecMigrationDecision::AlreadyCurrent {
            locked_row_count: locked_rows.len(),
            current_sha256,
        });
    }

    let old_sha256 = sha256_hex(persisted_json);
    require_authorized_hash(&old_sha256, authorized_old_sha256)?;
    let persisted = deserialize_persisted_spec(persisted_json)?;
    let plan = plan_additive_run_spec_migration(&persisted, &current.run_spec, source, target)?;
    validate_progress_scope(locked_rows, &persisted)?;
    validate_changed_tables_have_no_row_progress(locked_rows, &plan.changed_tables)?;

    Ok(SyncRunSpecMigrationDecision::UpdateRequired {
        expected_locked_row_count: locked_rows.len(),
        old_sha256,
        new_sha256: current_sha256,
        changed_tables: plan.changed_tables,
    })
}

fn require_one_persisted_spec(rows: &[LockedSyncProgressRow]) -> Result<&str, String> {
    if rows.is_empty() {
        return Err("run-spec migration requires at least one locked progress row".to_string());
    }
    let specs = rows
        .iter()
        .map(|row| row.run_spec_json.as_str())
        .collect::<BTreeSet<_>>();
    if specs.len() != 1 {
        return Err(format!(
            "run-spec migration locked progress rows contain {} distinct raw run specifications",
            specs.len()
        ));
    }
    Ok(specs
        .into_iter()
        .next()
        .expect("one persisted specification"))
}

fn require_authorized_hash(actual: &str, authorized: &str) -> Result<(), String> {
    if actual == authorized {
        return Ok(());
    }
    Err(format!(
        "persisted run-spec SHA-256 {actual} does not match authorized SHA-256 {authorized}"
    ))
}

fn deserialize_persisted_spec(raw: &str) -> Result<SyncRunSpec, String> {
    serde_json::from_str(raw)
        .map_err(|error| format!("decode persisted sync run specification: {error}"))
}

fn validate_progress_scope(
    rows: &[LockedSyncProgressRow],
    persisted: &SyncRunSpec,
) -> Result<(), String> {
    let scope = persisted
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    let unexpected = rows
        .iter()
        .find(|row| !scope.contains(row.table_name.as_str()));
    if let Some(row) = unexpected {
        return Err(format!(
            "run-spec migration progress table `{}` is outside the unchanged run scope",
            row.table_name
        ));
    }
    Ok(())
}

fn validate_changed_tables_have_no_row_progress(
    rows: &[LockedSyncProgressRow],
    changed_tables: &[AdditiveRunSpecTableChange],
) -> Result<(), String> {
    let changed = changed_tables
        .iter()
        .map(|change| change.table.as_str())
        .collect::<BTreeSet<_>>();
    let started = rows
        .iter()
        .find(|row| row.stage == SyncStage::Rows && changed.contains(row.table_name.as_str()));
    if let Some(row) = started {
        return Err(format!(
            "run-spec migration changed table `{}` already has rows-stage progress",
            row.table_name
        ));
    }
    Ok(())
}

fn sha256_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
