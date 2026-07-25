//! Plans recovery for a MySQL `1452` whose parent row is absent from the target.
//!
//! The stream replays historical events, so a child can reference a parent that table sync has not
//! copied yet. Recovery inserts the exact source parent row and then replays the child from the
//! unchanged checkpoint. A parent is never updated or deleted, and the checkpoint never advances
//! during recovery.
//!
//! UNWIRED as of 2026-07-25: generic deferral is disabled in `row::conflict` while the backfill is
//! incomplete, because recovery cannot succeed when the parent is absent from source and target
//! alike, and a failed recovery aborted the stream instead of skipping the row. Re-enable
//! `is_deferred_foreign_key_conflict` once the parent tables are fully loaded; this planner is kept
//! intact for that.
#![allow(dead_code)]

//! This generalises the two previously hardcoded constraints to any foreign key, using the identity
//! parsed from the error text. Every predicate fails closed so an unproven case keeps the ordinary
//! durable-abort path.

use super::foreign_key_error::ForeignKeyViolation;
use crate::snapshot::SnapshotRow;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MissingParentPlan {
    /// The parent is absent from the target and exactly one complete source parent row exists.
    InsertParent(SnapshotRow),
    /// The target already holds an equal parent row, so replay alone can resolve the conflict.
    AlreadyReconciled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MissingParentRejection {
    NullForeignKeyValue,
    MissingChildColumn,
    SourceParentAbsent,
    SourceParentAmbiguous,
    SourceParentIdentityMismatch,
    TargetParentAmbiguous,
    TargetParentDiverges,
}

impl MissingParentRejection {
    /// Stable snake_case reason so an operator can grep a stall back to the predicate that failed.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::NullForeignKeyValue => "null_foreign_key_value",
            Self::MissingChildColumn => "missing_child_column",
            Self::SourceParentAbsent => "source_parent_absent",
            Self::SourceParentAmbiguous => "source_parent_ambiguous",
            Self::SourceParentIdentityMismatch => "source_parent_identity_mismatch",
            Self::TargetParentAmbiguous => "target_parent_ambiguous",
            Self::TargetParentDiverges => "target_parent_diverges",
        }
    }
}

impl std::fmt::Display for MissingParentRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.reason())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MissingParentInput<'a> {
    pub(crate) violation: &'a ForeignKeyViolation,
    /// Child foreign-key values in `violation.child_columns` order.
    pub(crate) child_foreign_key_values: &'a [Option<String>],
    /// Source rows matching the parent columns, cardinality preserved.
    pub(crate) source_parent_rows: &'a [SnapshotRow],
    /// Target rows matching the same parent columns, cardinality preserved.
    pub(crate) target_parent_rows: &'a [SnapshotRow],
}

pub(crate) fn plan_missing_parent_recovery(
    input: &MissingParentInput<'_>,
) -> Result<MissingParentPlan, MissingParentRejection> {
    let violation = input.violation;
    if input.child_foreign_key_values.len() != violation.child_columns.len() {
        return Err(MissingParentRejection::MissingChildColumn);
    }
    // A NULL foreign-key value never violates the constraint, so such a conflict is not this class.
    let expected = input
        .child_foreign_key_values
        .iter()
        .map(|value| {
            value
                .clone()
                .ok_or(MissingParentRejection::NullForeignKeyValue)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let source_parent = match input.source_parent_rows {
        [] => return Err(MissingParentRejection::SourceParentAbsent),
        [row] => row,
        _ => return Err(MissingParentRejection::SourceParentAmbiguous),
    };
    if !row_matches_parent_columns(source_parent, &violation.parent_columns, &expected) {
        return Err(MissingParentRejection::SourceParentIdentityMismatch);
    }

    match input.target_parent_rows {
        [] => Ok(MissingParentPlan::InsertParent(source_parent.clone())),
        [row] if row.values == source_parent.values => Ok(MissingParentPlan::AlreadyReconciled),
        [_] => Err(MissingParentRejection::TargetParentDiverges),
        _ => Err(MissingParentRejection::TargetParentAmbiguous),
    }
}

fn row_matches_parent_columns(row: &SnapshotRow, columns: &[String], expected: &[String]) -> bool {
    columns
        .iter()
        .zip(expected)
        .all(|(column, expected)| match row.values.get(column) {
            Some(Some(value)) => value == expected,
            _ => false,
        })
}

#[cfg(test)]
mod tests;
