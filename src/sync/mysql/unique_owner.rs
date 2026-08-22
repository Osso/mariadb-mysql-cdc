use super::super::model::{
    SyncInsertFailure, SyncUniqueIndex, SyncUniqueOwnerAction, SyncUniqueOwnerConflict,
};
use crate::database_row::DatabaseRow;
use crate::target::duplicate_index_from_error;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncUniqueIndexColumn {
    pub(crate) index: String,
    pub(crate) column: Option<String>,
    pub(crate) sequence: u64,
    pub(crate) prefix_length: Option<u64>,
}

#[derive(Serialize)]
struct SyncUniqueOwnerReconciliationEvent<'a> {
    event: &'static str,
    table: &'a str,
    index: &'a str,
    action: &'static str,
    intended_primary_key: &'a [String],
    owner_primary_key: &'a [String],
}

pub(crate) fn resolve_sync_unique_index(
    table: &str,
    error: &str,
    rows: Vec<SyncUniqueIndexColumn>,
) -> Result<SyncUniqueIndex, String> {
    let error_index = duplicate_index_from_error(error).ok_or_else(|| {
        format!("duplicate insert error does not identify an index for `{table}`")
    })?;
    let mut indexes = BTreeMap::<String, Vec<SyncUniqueIndexColumn>>::new();
    for row in rows {
        indexes.entry(row.index.clone()).or_default().push(row);
    }
    let index_name = resolve_sync_unique_index_name(table, &error_index, &indexes)?;
    if index_name == "PRIMARY" {
        return Err(format!(
            "duplicate insert named PRIMARY for `{table}`; secondary unique reconciliation refused"
        ));
    }
    let mut columns = indexes
        .remove(&index_name)
        .ok_or_else(|| format!("unique index `{index_name}` metadata is absent for `{table}`"))?;
    columns.sort_by_key(|column| column.sequence);
    let columns = columns
        .into_iter()
        .enumerate()
        .map(|(position, column)| {
            resolve_full_unique_index_column(table, &index_name, position, column)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err(format!(
            "unique index `{index_name}` has no columns for `{table}`"
        ));
    }
    Ok(SyncUniqueIndex {
        name: index_name,
        columns,
    })
}

fn resolve_sync_unique_index_name(
    table: &str,
    error_index: &str,
    indexes: &BTreeMap<String, Vec<SyncUniqueIndexColumn>>,
) -> Result<String, String> {
    if indexes.contains_key(error_index) {
        return Ok(error_index.to_string());
    }
    let matches = indexes
        .keys()
        .filter(|index| error_index.ends_with(&format!(".{index}")))
        .cloned()
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Ok(index.clone()),
        [] => Err(format!(
            "duplicate insert index `{error_index}` is absent for `{table}`"
        )),
        _ => Err(format!(
            "duplicate insert index `{error_index}` is ambiguous for `{table}`: {}",
            matches.join(", ")
        )),
    }
}

fn resolve_full_unique_index_column(
    table: &str,
    index: &str,
    position: usize,
    column: SyncUniqueIndexColumn,
) -> Result<String, String> {
    let expected_sequence = u64::try_from(position + 1).expect("index position fits u64");
    if column.sequence != expected_sequence {
        return Err(format!(
            "unique index `{index}` metadata is non-contiguous for `{table}`"
        ));
    }
    if column.prefix_length.is_some() {
        return Err(format!(
            "unique index `{index}` has a prefixed column for `{table}`"
        ));
    }
    column
        .column
        .ok_or_else(|| format!("unique index `{index}` has an expression column for `{table}`"))
}

pub(crate) fn build_sync_insert_failure(
    rows: &[DatabaseRow],
    failed_batch_start: usize,
    failed_batch_len: usize,
    mysql_code: Option<u16>,
    message: String,
) -> SyncInsertFailure {
    let failed_batch_end = failed_batch_start + failed_batch_len;
    assert!(
        failed_batch_end <= rows.len(),
        "failed insert batch is in bounds"
    );
    SyncInsertFailure {
        mysql_code,
        message,
        failed_batch: rows[failed_batch_start..failed_batch_end].to_vec(),
        remaining_rows: rows[failed_batch_end..].to_vec(),
    }
}

pub(crate) fn format_unique_owner_reconciliation_event(
    table: &str,
    conflict: &SyncUniqueOwnerConflict,
    action: &SyncUniqueOwnerAction,
) -> String {
    serde_json::to_string(&SyncUniqueOwnerReconciliationEvent {
        event: "sync_unique_owner_reconciliation",
        table,
        index: &conflict.index.name,
        action: action.as_str(),
        intended_primary_key: &conflict.intended.primary_key,
        owner_primary_key: &conflict.owner.primary_key,
    })
    .expect("unique-owner reconciliation event is serializable")
}

pub(super) fn validate_unique_owner(
    table: &str,
    index: &SyncUniqueIndex,
    intended: &DatabaseRow,
    owner: &DatabaseRow,
) -> Result<(), String> {
    if owner.primary_key == intended.primary_key {
        return Err(format!(
            "unique index `{}` owner has the intended primary key for `{table}`",
            index.name
        ));
    }
    let intended_identity = index.values(intended, "intended")?;
    let owner_identity = index.values(owner, "target owner")?;
    if owner_identity != intended_identity {
        return Err(format!(
            "unique index `{}` target owner identity disagrees with intended row for `{table}`",
            index.name
        ));
    }
    Ok(())
}

pub(super) fn verify_exact_row(
    actual: Option<DatabaseRow>,
    expected: Option<&DatabaseRow>,
    label: &str,
) -> Result<(), String> {
    if actual.as_ref() == expected {
        return Ok(());
    }
    Err(format!("{label} verification failed"))
}

pub(super) fn mysql_error_code(error: &mysql::Error) -> Option<u16> {
    match error {
        mysql::Error::MySqlError(error) => Some(error.code),
        _ => None,
    }
}
