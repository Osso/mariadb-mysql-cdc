use super::CatchupSnapshotConfig;
use crate::mysql_client::{PersistentMySqlSource, PersistentTargetExecutor};
use crate::snapshot::{SnapshotError, SnapshotTable};
use crate::target::{SnapshotInsertMode, SqlStatement, TargetExecutor, TargetMySqlWriter};

pub(super) fn snapshot_target_for_table(
    config: &CatchupSnapshotConfig,
    source: &PersistentMySqlSource,
    table: &SnapshotTable,
) -> Result<TargetMySqlWriter<PersistentTargetExecutor>, SnapshotError> {
    let executor = PersistentTargetExecutor::new_for_sync(&config.target)
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))?;
    let target_columns = read_or_create_target_table(source, &executor, table)?;
    validate_target_table_columns(table, &target_columns)?;
    Ok(TargetMySqlWriter::from_snapshot_table(
        table,
        executor,
        SnapshotInsertMode::IgnoreDuplicate,
    ))
}

fn read_or_create_target_table(
    source: &PersistentMySqlSource,
    executor: &PersistentTargetExecutor,
    table: &SnapshotTable,
) -> Result<Vec<String>, SnapshotError> {
    let target_columns = read_target_column_names(executor, &table.name)?;
    if !target_columns.is_empty() {
        return Ok(target_columns);
    }

    create_missing_target_table(source, executor, &table.name)?;
    read_target_column_names(executor, &table.name)
}

fn read_target_column_names(
    executor: &PersistentTargetExecutor,
    table: &str,
) -> Result<Vec<String>, SnapshotError> {
    executor
        .read_column_names(table)
        .map_err(|error| SnapshotError::InvalidTable(error.to_string()))
}

fn create_missing_target_table(
    source: &PersistentMySqlSource,
    executor: &PersistentTargetExecutor,
    table: &str,
) -> Result<(), SnapshotError> {
    let source_ddl = source.read_create_table(table)?;
    let target_ddl = crate::live::mysql_compatible_create_table(&source_ddl);
    executor
        .execute(&SqlStatement {
            sql: target_ddl,
            params: Vec::new(),
        })
        .map_err(|error| {
            SnapshotError::InvalidTable(format!(
                "failed to create missing target table {table}: {error}"
            ))
        })
}

#[cfg(test)]
pub(super) fn validate_target_table_columns(
    table: &SnapshotTable,
    target_columns: &[String],
) -> Result<(), SnapshotError> {
    validate_target_table_columns_inner(table, target_columns)
}

#[cfg(not(test))]
fn validate_target_table_columns(
    table: &SnapshotTable,
    target_columns: &[String],
) -> Result<(), SnapshotError> {
    validate_target_table_columns_inner(table, target_columns)
}

fn validate_target_table_columns_inner(
    table: &SnapshotTable,
    target_columns: &[String],
) -> Result<(), SnapshotError> {
    let missing_columns = table
        .columns
        .iter()
        .filter(|column| !target_columns.contains(column))
        .cloned()
        .collect::<Vec<_>>();
    if missing_columns.is_empty() {
        return Ok(());
    }

    Err(SnapshotError::InvalidTable(format!(
        "target table {} is missing source columns: {}",
        table.name,
        missing_columns.join(",")
    )))
}
