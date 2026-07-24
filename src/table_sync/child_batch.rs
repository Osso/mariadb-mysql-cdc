//! Insert reconciliation for one child row batch.
//!
//! A batch insert can fail because a foreign-key parent is missing or because some rows already
//! exist on the target. Duplicate reconciliation reads the target once per row in the batch, so
//! retrying the whole batch after every duplicate costs `O(batch^2)` target round-trips while the
//! live stream keeps inserting the same rows concurrently.
//!
//! Reconciliation already proves which rows are absent, so those rows are inserted individually.
//! One concurrent duplicate then costs one round-trip instead of another full-batch pass.

use super::model::TableSyncError;
use crate::snapshot::SnapshotRow;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ChildInsertOutcome {
    Applied,
    /// Carries the MySQL `1062` text so reconciliation can name the colliding index as evidence.
    DuplicateKey(String),
    MissingParent,
}

/// Target operations the reconciliation loop needs.
pub(crate) trait ChildBatchInserter {
    fn table_name(&self) -> &str;

    fn insert(&mut self, rows: &[SnapshotRow]) -> Result<ChildInsertOutcome, TableSyncError>;

    fn repair_parents(&mut self, rows: &[SnapshotRow]) -> Result<(), TableSyncError>;

    /// Rows of `rows` that are still absent from the target. Diverging or foreign-owned
    /// duplicates fail closed instead of being reported as absent. `duplicate_error` is the MySQL
    /// text that triggered reconciliation and is recorded as evidence on failure.
    fn reconcile_duplicates(
        &mut self,
        rows: &[SnapshotRow],
        duplicate_error: &str,
    ) -> Result<Vec<SnapshotRow>, TableSyncError>;
}

pub(crate) fn insert_child_batch_with_reconciliation<I>(
    inserter: &mut I,
    batch: &[SnapshotRow],
) -> Result<(), TableSyncError>
where
    I: ChildBatchInserter + ?Sized,
{
    let mut repaired_parents = false;
    loop {
        match inserter.insert(batch)? {
            ChildInsertOutcome::Applied => return Ok(()),
            ChildInsertOutcome::MissingParent if !repaired_parents => {
                inserter.repair_parents(batch)?;
                repaired_parents = true;
            }
            ChildInsertOutcome::MissingParent => {
                return Err(TableSyncError::Repair(format!(
                    "foreign-key parent is still missing for `{}` after parent repair",
                    inserter.table_name()
                )));
            }
            ChildInsertOutcome::DuplicateKey(duplicate_error) => {
                let absent = inserter.reconcile_duplicates(batch, &duplicate_error)?;
                if absent.is_empty() {
                    return Ok(());
                }
                return insert_absent_rows_individually(inserter, &absent);
            }
        }
    }
}

/// Insert rows already proven absent. A duplicate here means another target identity owns a
/// secondary unique key, which reconciliation reports as a closed failure.
fn insert_absent_rows_individually<I>(
    inserter: &mut I,
    rows: &[SnapshotRow],
) -> Result<(), TableSyncError>
where
    I: ChildBatchInserter + ?Sized,
{
    for row in rows {
        let single = std::slice::from_ref(row);
        match inserter.insert(single)? {
            ChildInsertOutcome::Applied => {}
            ChildInsertOutcome::DuplicateKey(duplicate_error) => {
                inserter.reconcile_duplicates(single, &duplicate_error)?;
            }
            ChildInsertOutcome::MissingParent => {
                return Err(TableSyncError::Repair(format!(
                    "foreign-key parent is still missing for `{}` after parent repair",
                    inserter.table_name()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
