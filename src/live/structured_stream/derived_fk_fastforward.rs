//! Fast-forward of denormalized foreign-key columns on a historical child row event.
//!
//! A multi-column foreign key whose referenced columns extend the parent primary key with a
//! mutable parent attribute keeps a denormalized copy of that attribute in the child table. The
//! copy is maintained by `ON UPDATE CASCADE`, so it is derived data rather than independent row
//! content.
//!
//! While the stream replays historical events, catch-up and table sync can already have installed
//! a later parent image on the target. Replaying the child event then fails with MySQL `1452`
//! because the historical attribute pair no longer exists in the parent.
//!
//! This module proves that exact situation and plans the minimal repair: substitute only the
//! derived referenced columns of the child image with the locked parent's current values. The
//! parent is never mutated, every other child column keeps its historical value, and the next
//! replayed parent update cascades the same values the substitution already wrote.

use crate::conflict_repair::ConflictOperation;
use crate::inventory::ForeignKeyInventory;
use mysql::Value;

/// Referenced columns of the conflicting foreign key, read from the target under the active
/// transaction. Cardinality is preserved so callers must reject missing or ambiguous parents.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LockedParentRows {
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<Value>>,
}

#[derive(Clone, Debug)]
pub(crate) struct DerivedFkFastForwardInput<'a> {
    pub(crate) schema: &'a str,
    pub(crate) child_table: &'a str,
    pub(crate) operation: ConflictOperation,
    pub(crate) error_code: u16,
    pub(crate) constraint: &'a str,
    pub(crate) foreign_key: Option<&'a ForeignKeyInventory>,
    pub(crate) parent_primary_key: &'a [String],
    pub(crate) child_columns: &'a [String],
    pub(crate) child_values: &'a [Value],
    pub(crate) locked_parent: &'a LockedParentRows,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DerivedFkRejection {
    WrongErrorCode,
    WrongOperation,
    UnknownConstraint,
    ConstraintTableMismatch,
    SingleColumnForeignKey,
    ForeignKeyShapeMismatch,
    ParentSchemaMismatch,
    ParentPrimaryKeyUnknown,
    ParentPrimaryKeyNotReferenced,
    LockedParentColumnsMismatch,
    MissingChildColumn,
    ChildImageLengthMismatch,
    NullForeignKeyValue,
    ParentRowNotUnique,
    ParentIdentityMismatch,
    NoDerivedDrift,
}

/// One derived referenced column whose child value must be replaced by the parent's current value.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DerivedFkSubstitution {
    pub(crate) child_column: String,
    pub(crate) referenced_column: String,
    pub(crate) historical_value: Value,
    pub(crate) parent_value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DerivedFkFastForwardPlan {
    pub(crate) constraint: String,
    pub(crate) parent_table: String,
    pub(crate) substitutions: Vec<DerivedFkSubstitution>,
}

impl DerivedFkFastForwardPlan {
    /// Durable evidence naming the constraint and every substituted column with both values.
    pub(crate) fn evidence(&self) -> String {
        let columns = self
            .substitutions
            .iter()
            .map(|substitution| {
                format!(
                    "{}: {} -> {}",
                    substitution.child_column,
                    format_value(&substitution.historical_value),
                    format_value(&substitution.parent_value)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "derived foreign-key columns fast-forwarded to the locked parent image; \
             constraint={} parent={} substituted={}",
            self.constraint, self.parent_table, columns
        )
    }
}

pub(crate) fn plan_derived_fk_fastforward(
    input: &DerivedFkFastForwardInput<'_>,
) -> Result<DerivedFkFastForwardPlan, DerivedFkRejection> {
    if input.error_code != 1452 {
        return Err(DerivedFkRejection::WrongErrorCode);
    }
    if !matches!(
        input.operation,
        ConflictOperation::Insert | ConflictOperation::Update
    ) {
        return Err(DerivedFkRejection::WrongOperation);
    }
    let foreign_key = input
        .foreign_key
        .ok_or(DerivedFkRejection::UnknownConstraint)?;
    if foreign_key.name != input.constraint {
        return Err(DerivedFkRejection::UnknownConstraint);
    }
    if foreign_key.table != input.child_table {
        return Err(DerivedFkRejection::ConstraintTableMismatch);
    }
    if foreign_key.referenced_schema != input.schema {
        return Err(DerivedFkRejection::ParentSchemaMismatch);
    }
    if foreign_key.referenced_columns.len() < 2 {
        return Err(DerivedFkRejection::SingleColumnForeignKey);
    }
    if foreign_key.columns.len() != foreign_key.referenced_columns.len() {
        return Err(DerivedFkRejection::ForeignKeyShapeMismatch);
    }
    if input.parent_primary_key.is_empty() {
        return Err(DerivedFkRejection::ParentPrimaryKeyUnknown);
    }
    if !input
        .parent_primary_key
        .iter()
        .all(|column| foreign_key.referenced_columns.contains(column))
    {
        return Err(DerivedFkRejection::ParentPrimaryKeyNotReferenced);
    }
    if input.parent_primary_key.len() == foreign_key.referenced_columns.len() {
        return Err(DerivedFkRejection::NoDerivedDrift);
    }
    if input.child_columns.len() != input.child_values.len() {
        return Err(DerivedFkRejection::ChildImageLengthMismatch);
    }
    if input.locked_parent.columns != foreign_key.referenced_columns {
        return Err(DerivedFkRejection::LockedParentColumnsMismatch);
    }
    if input.locked_parent.rows.len() != 1 {
        return Err(DerivedFkRejection::ParentRowNotUnique);
    }
    let parent_row = &input.locked_parent.rows[0];
    if parent_row.len() != foreign_key.referenced_columns.len() {
        return Err(DerivedFkRejection::LockedParentColumnsMismatch);
    }

    let mut substitutions = Vec::new();
    for (position, referenced_column) in foreign_key.referenced_columns.iter().enumerate() {
        let child_column = &foreign_key.columns[position];
        let child_value = child_value(input, child_column)?;
        if matches!(child_value, Value::NULL) {
            return Err(DerivedFkRejection::NullForeignKeyValue);
        }
        let parent_value = &parent_row[position];
        let identical = values_equal(child_value, parent_value);
        if input.parent_primary_key.contains(referenced_column) {
            if !identical {
                return Err(DerivedFkRejection::ParentIdentityMismatch);
            }
            continue;
        }
        if !identical {
            substitutions.push(DerivedFkSubstitution {
                child_column: child_column.clone(),
                referenced_column: referenced_column.clone(),
                historical_value: child_value.clone(),
                parent_value: parent_value.clone(),
            });
        }
    }
    if substitutions.is_empty() {
        return Err(DerivedFkRejection::NoDerivedDrift);
    }

    Ok(DerivedFkFastForwardPlan {
        constraint: foreign_key.name.clone(),
        parent_table: foreign_key.referenced_table.clone(),
        substitutions,
    })
}

fn child_value<'a>(
    input: &'a DerivedFkFastForwardInput<'_>,
    child_column: &str,
) -> Result<&'a Value, DerivedFkRejection> {
    let position = input
        .child_columns
        .iter()
        .position(|column| column == child_column)
        .ok_or(DerivedFkRejection::MissingChildColumn)?;
    input
        .child_values
        .get(position)
        .ok_or(DerivedFkRejection::ChildImageLengthMismatch)
}

/// Foreign keys compare by stored value, so a bare integer and its text form are the same key.
fn values_equal(left: &Value, right: &Value) -> bool {
    if left == right {
        return true;
    }
    match (comparable_bytes(left), comparable_bytes(right)) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

fn comparable_bytes(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::NULL => None,
        Value::Bytes(bytes) => Some(bytes.clone()),
        Value::Int(number) => Some(number.to_string().into_bytes()),
        Value::UInt(number) => Some(number.to_string().into_bytes()),
        Value::Float(_) | Value::Double(_) | Value::Date(..) | Value::Time(..) => None,
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::NULL => "NULL".to_string(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Value::Int(number) => number.to_string(),
        Value::UInt(number) => number.to_string(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests;
