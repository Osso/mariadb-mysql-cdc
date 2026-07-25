//! Unified resolution of a deferred foreign-key conflict, decided under the parent row lock.
//!
//! A MySQL `1452` on the replayed stream has two causes, and they are only distinguishable by
//! looking at the parent:
//!
//! - The parent identity is absent from the target, because table sync has not copied it yet.
//! - The parent is present but a referenced non-key attribute has moved on, because catch-up copied
//!   current state while the stream replays older events. Those columns are maintained by
//!   `ON UPDATE CASCADE`, so they are derived data.
//!
//! Both are decided here, from one locked read taken inside the applying transaction. The lock is
//! what makes the decision sound: the parent cannot change between classifying it and writing.
//!
//! Neither outcome mutates a parent row. The absent case inserts the exact source parent and
//! replays the child unchanged; the superseded case rewrites only the child's derived referenced
//! columns and leaves every other column historical.

use super::derived_fk_fastforward::{
    DerivedFkFastForwardInput, DerivedFkFastForwardPlan, DerivedFkRejection, LockedParentRows,
    plan_derived_fk_fastforward,
};
use super::superseded_source::CanonicalSourceRow;
use crate::conflict_repair::ConflictOperation;
use crate::inventory::ForeignKeyInventory;
use crate::live::ForeignKeyViolation;
use mysql::Value;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ForeignKeyRepairPlan {
    /// The parent identity is absent, so the exact source parent is installed and the child image
    /// replays unchanged.
    InstallParent,
    /// The parent holds a superseded attribute, so the child's derived columns fast-forward to it.
    FastForwardChild(DerivedFkFastForwardPlan),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForeignKeyRepairRejection {
    ParentAmbiguous,
    LockedParentShapeMismatch,
    Derived(DerivedFkRejection),
}

impl ForeignKeyRepairRejection {
    /// Stable snake_case reason so an operator can grep a stall back to the failed predicate.
    pub(crate) fn reason(self) -> String {
        match self {
            Self::ParentAmbiguous => "parent_ambiguous".to_string(),
            Self::LockedParentShapeMismatch => "locked_parent_shape_mismatch".to_string(),
            Self::Derived(rejection) => format!("derived_{rejection:?}"),
        }
    }
}

impl std::fmt::Display for ForeignKeyRepairRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.reason())
    }
}

/// A planned foreign-key repair, reduced to the statements that carry it out and durable evidence.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ForeignKeyRepairProof {
    pub(crate) statements: Vec<crate::target::SqlStatement>,
    pub(crate) evidence: String,
}

pub(crate) struct ForeignKeyRepairInput<'a> {
    pub(crate) violation: &'a ForeignKeyViolation,
    /// The constraint rebuilt from the violation by `foreign_key_from_violation`.
    pub(crate) foreign_key: &'a ForeignKeyInventory,
    pub(crate) operation: ConflictOperation,
    pub(crate) error_code: u16,
    /// The parent table's primary key, which the error does not name.
    pub(crate) parent_primary_key: &'a [String],
    pub(crate) child_columns: &'a [String],
    pub(crate) child_values: &'a [Value],
    pub(crate) locked_parent: &'a LockedParentRows,
}

/// Classifies the conflict from the locked parent image and plans the matching repair.
pub(crate) fn plan_foreign_key_repair(
    input: &ForeignKeyRepairInput<'_>,
) -> Result<ForeignKeyRepairPlan, ForeignKeyRepairRejection> {
    if input.locked_parent.columns != input.violation.parent_columns {
        return Err(ForeignKeyRepairRejection::LockedParentShapeMismatch);
    }
    match input.locked_parent.rows.len() {
        // Nothing owns the referenced primary key, so no attribute could have been superseded.
        0 => Ok(ForeignKeyRepairPlan::InstallParent),
        1 => plan_derived_fk_fastforward(&derived_input(input))
            .map(ForeignKeyRepairPlan::FastForwardChild)
            .map_err(ForeignKeyRepairRejection::Derived),
        _ => Err(ForeignKeyRepairRejection::ParentAmbiguous),
    }
}

fn derived_input<'a>(input: &'a ForeignKeyRepairInput<'a>) -> DerivedFkFastForwardInput<'a> {
    DerivedFkFastForwardInput {
        schema: &input.violation.child_schema,
        child_table: &input.violation.child_table,
        operation: input.operation,
        error_code: input.error_code,
        constraint: &input.violation.constraint,
        foreign_key: Some(input.foreign_key),
        parent_primary_key: input.parent_primary_key,
        child_columns: input.child_columns,
        child_values: input.child_values,
        locked_parent: input.locked_parent,
    }
}

/// Rebuilds the constraint the Class 2 planner validates against.
///
/// MySQL names the child columns, the parent table and the referenced columns in the error itself,
/// so the constraint does not have to be read back from the schema. Only the parent's primary key
/// has to come from the inventory, because the error never states it.
pub(crate) fn foreign_key_from_violation(violation: &ForeignKeyViolation) -> ForeignKeyInventory {
    ForeignKeyInventory {
        table: violation.child_table.clone(),
        name: violation.constraint.clone(),
        columns: violation.child_columns.clone(),
        referenced_schema: violation
            .parent_schema
            .clone()
            .unwrap_or_else(|| violation.child_schema.clone()),
        referenced_table: violation.parent_table.clone(),
        referenced_columns: violation.parent_columns.clone(),
    }
}

/// The locked read selects a parent by its primary key only, so pair each parent primary key column
/// with the child's historical value for the child column that references it.
///
/// Returns `None` when the child image does not carry one of those columns, which fails closed.
pub(crate) fn parent_primary_key_predicate(
    violation: &ForeignKeyViolation,
    parent_primary_key: &[String],
    child_columns: &[String],
    child_values: &[Value],
) -> Option<Vec<(String, Value)>> {
    parent_primary_key
        .iter()
        .map(|parent_column| {
            let position = violation
                .parent_columns
                .iter()
                .position(|column| column == parent_column)?;
            let child_column = violation.child_columns.get(position)?;
            let child_position = child_columns
                .iter()
                .position(|column| column == child_column)?;
            let value = child_values.get(child_position)?;
            match value {
                // A NULL child column never violates the constraint, so it cannot identify a parent.
                Value::NULL => None,
                value => Some((parent_column.clone(), value.clone())),
            }
        })
        .collect()
}

/// Applies the planned substitutions to the child image, leaving every other column historical.
///
/// Returns `None` when a substituted column is absent from the image, which fails closed.
pub(crate) fn fast_forwarded_child_row(
    child_columns: &[String],
    child_values: &[Value],
    plan: &DerivedFkFastForwardPlan,
) -> Option<CanonicalSourceRow> {
    if child_columns.len() != child_values.len() {
        return None;
    }
    let mut values = child_values.to_vec();
    for substitution in &plan.substitutions {
        let position = child_columns
            .iter()
            .position(|column| *column == substitution.child_column)?;
        values[position] = substitution.parent_value.clone();
    }
    Some(CanonicalSourceRow {
        columns: child_columns.to_vec(),
        values,
        // The hash identifies a source evidence row; a rebuilt child image is not one, and the
        // insert builder reads only the columns and values.
        hash: String::new(),
    })
}

#[cfg(test)]
mod tests;
