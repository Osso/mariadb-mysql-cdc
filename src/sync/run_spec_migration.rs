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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SyncRunSpecMigrationOutcome {
    AlreadyCurrent {
        locked_row_count: usize,
        affected_row_count: u64,
        authorized_old_sha256: String,
        current_sha256: String,
    },
    Migrated {
        locked_row_count: usize,
        affected_row_count: u64,
        authorized_old_sha256: String,
        old_sha256: String,
        new_sha256: String,
        changed_tables: Vec<AdditiveRunSpecTableChange>,
    },
}

pub(crate) trait SyncRunSpecMigrationExecutor {
    fn begin_serializable_transaction(&mut self) -> Result<(), String>;
    fn lock_run_rows(&mut self, run_id: &str) -> Result<Vec<LockedSyncProgressRow>, String>;
    fn update_run_spec(
        &mut self,
        run_id: &str,
        old_json: &str,
        current_json: &str,
    ) -> Result<u64, String>;
    fn count_run_rows_with_spec(&mut self, run_id: &str, current_json: &str)
    -> Result<u64, String>;
    fn commit_transaction(&mut self) -> Result<(), String>;
    fn rollback_transaction(&mut self) -> Result<(), String>;
}

pub(crate) struct SyncRunSpecMigrationRequest<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) authorized_old_sha256: &'a str,
    pub(crate) current: &'a SyncRunIdentity,
    pub(crate) source: &'a SchemaInventory,
    pub(crate) target: &'a SchemaInventory,
}

struct RequiredRunSpecMigration {
    expected_locked_row_count: usize,
    old_sha256: String,
    new_sha256: String,
    changed_tables: Vec<AdditiveRunSpecTableChange>,
}

pub(crate) fn run_locked_sync_run_spec_migration(
    executor: &mut impl SyncRunSpecMigrationExecutor,
    request: &SyncRunSpecMigrationRequest<'_>,
) -> Result<SyncRunSpecMigrationOutcome, String> {
    executor.begin_serializable_transaction()?;
    let result = run_started_sync_run_spec_migration(executor, request);
    match result {
        Ok(outcome) => commit_migration(executor, outcome),
        Err(error) => Err(rollback_after_migration_error(executor, error)),
    }
}

fn run_started_sync_run_spec_migration(
    executor: &mut impl SyncRunSpecMigrationExecutor,
    request: &SyncRunSpecMigrationRequest<'_>,
) -> Result<SyncRunSpecMigrationOutcome, String> {
    let locked_rows = executor.lock_run_rows(request.run_id)?;
    let decision = decide_locked_run_spec_migration(
        &locked_rows,
        request.authorized_old_sha256,
        request.current,
        request.source,
        request.target,
    )?;
    outcome_for_migration_decision(executor, request, &locked_rows, decision)
}

fn outcome_for_migration_decision(
    executor: &mut impl SyncRunSpecMigrationExecutor,
    request: &SyncRunSpecMigrationRequest<'_>,
    locked_rows: &[LockedSyncProgressRow],
    decision: SyncRunSpecMigrationDecision,
) -> Result<SyncRunSpecMigrationOutcome, String> {
    match decision {
        SyncRunSpecMigrationDecision::AlreadyCurrent {
            locked_row_count,
            current_sha256,
        } => Ok(already_current_outcome(
            request,
            locked_row_count,
            current_sha256,
        )),
        update @ SyncRunSpecMigrationDecision::UpdateRequired { .. } => {
            update_locked_run_rows(executor, request, locked_rows, required_migration(update)?)
        }
    }
}

fn required_migration(
    decision: SyncRunSpecMigrationDecision,
) -> Result<RequiredRunSpecMigration, String> {
    match decision {
        SyncRunSpecMigrationDecision::UpdateRequired {
            expected_locked_row_count,
            old_sha256,
            new_sha256,
            changed_tables,
        } => Ok(RequiredRunSpecMigration {
            expected_locked_row_count,
            old_sha256,
            new_sha256,
            changed_tables,
        }),
        SyncRunSpecMigrationDecision::AlreadyCurrent { .. } => {
            Err("update helper received an already-current decision".to_string())
        }
    }
}

fn already_current_outcome(
    request: &SyncRunSpecMigrationRequest<'_>,
    locked_row_count: usize,
    current_sha256: String,
) -> SyncRunSpecMigrationOutcome {
    SyncRunSpecMigrationOutcome::AlreadyCurrent {
        locked_row_count,
        affected_row_count: 0,
        authorized_old_sha256: request.authorized_old_sha256.to_string(),
        current_sha256,
    }
}

fn update_locked_run_rows(
    executor: &mut impl SyncRunSpecMigrationExecutor,
    request: &SyncRunSpecMigrationRequest<'_>,
    locked_rows: &[LockedSyncProgressRow],
    migration: RequiredRunSpecMigration,
) -> Result<SyncRunSpecMigrationOutcome, String> {
    let persisted_json = first_locked_run_spec_json(locked_rows);
    let affected_row_count = executor.update_run_spec(
        request.run_id,
        persisted_json,
        &request.current.run_spec_json,
    )?;
    require_affected_row_count(affected_row_count, migration.expected_locked_row_count)?;
    let current_row_count =
        executor.count_run_rows_with_spec(request.run_id, &request.current.run_spec_json)?;
    require_current_row_count(current_row_count, migration.expected_locked_row_count)?;
    Ok(SyncRunSpecMigrationOutcome::Migrated {
        locked_row_count: migration.expected_locked_row_count,
        affected_row_count,
        authorized_old_sha256: request.authorized_old_sha256.to_string(),
        old_sha256: migration.old_sha256,
        new_sha256: migration.new_sha256,
        changed_tables: migration.changed_tables,
    })
}

fn first_locked_run_spec_json(rows: &[LockedSyncProgressRow]) -> &str {
    &rows
        .first()
        .expect("update decision requires locked progress rows")
        .run_spec_json
}

fn require_affected_row_count(actual: u64, expected: usize) -> Result<(), String> {
    if actual == expected as u64 {
        return Ok(());
    }
    Err(format!(
        "run-spec migration updated {actual} rows, expected {expected} locked rows"
    ))
}

fn require_current_row_count(actual: u64, expected: usize) -> Result<(), String> {
    if actual == expected as u64 {
        return Ok(());
    }
    Err(format!(
        "run-spec migration verification found {actual} current-spec rows, expected {expected} locked rows"
    ))
}

fn commit_migration(
    executor: &mut impl SyncRunSpecMigrationExecutor,
    outcome: SyncRunSpecMigrationOutcome,
) -> Result<SyncRunSpecMigrationOutcome, String> {
    executor
        .commit_transaction()
        .map(|()| outcome)
        .map_err(|error| rollback_after_migration_error(executor, error))
}

fn rollback_after_migration_error(
    executor: &mut impl SyncRunSpecMigrationExecutor,
    primary_error: String,
) -> String {
    match executor.rollback_transaction() {
        Ok(()) => primary_error,
        Err(rollback_error) => format!(
            "{primary_error}; additionally rollback sync run-spec migration failed: {rollback_error}"
        ),
    }
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
