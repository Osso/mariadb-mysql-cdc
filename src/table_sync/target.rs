use super::{SyncTable, TableSyncError};
use crate::snapshot::SnapshotRow;
use crate::target::PrimaryKey;
use mysql::Value;
use std::collections::BTreeMap;

pub trait SyncRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
    fn update_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        for row in rows {
            self.update_row(row)?;
        }
        Ok(())
    }
    fn delete_row(&mut self, primary_key: &[String]) -> Result<(), TableSyncError>;

    fn restore_displaced_owner_and_insert(
        &mut self,
        _table: &SyncTable,
        _displaced_source: &SnapshotRow,
        _displaced_target: &SnapshotRow,
        _missing_source: &SnapshotRow,
        _progress_sql: &str,
    ) -> Result<(), TableSyncError> {
        Err(TableSyncError::Repair(
            "transactional two-parent collision repair is unavailable".to_string(),
        ))
    }
}

impl<E> SyncRepairTarget for crate::target::TargetMySqlWriter<E>
where
    E: crate::target::TargetExecutor,
{
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        self.insert_rows(std::slice::from_ref(row))
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        crate::target::TargetMySqlWriter::update_row(self, row)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn update_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        crate::target::TargetMySqlWriter::update_rows(self, rows)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn delete_row(&mut self, primary_key: &[String]) -> Result<(), TableSyncError> {
        let primary_key = PrimaryKey::new(primary_key.iter().cloned().map(Value::from).collect());
        crate::target::TargetMySqlWriter::delete_row(self, &primary_key)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }
}

pub(crate) struct MySqlSyncRepairTarget {
    writer: crate::target::TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
}

impl MySqlSyncRepairTarget {
    pub(crate) fn new(
        writer: crate::target::TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
    ) -> Self {
        Self { writer }
    }
}

impl SyncRepairTarget for MySqlSyncRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        self.writer
            .insert_rows(std::slice::from_ref(row))
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        crate::target::TargetMySqlWriter::update_row(&self.writer, row)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn update_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        crate::target::TargetMySqlWriter::update_rows(&self.writer, rows)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn delete_row(&mut self, primary_key: &[String]) -> Result<(), TableSyncError> {
        let primary_key = PrimaryKey::new(primary_key.iter().cloned().map(Value::from).collect());
        crate::target::TargetMySqlWriter::delete_row(&self.writer, &primary_key)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn restore_displaced_owner_and_insert(
        &mut self,
        table: &SyncTable,
        displaced_source: &SnapshotRow,
        displaced_target: &SnapshotRow,
        missing_source: &SnapshotRow,
        progress_sql: &str,
    ) -> Result<(), TableSyncError> {
        self.writer
            .restore_displaced_owner_and_insert_transactionally(
                table,
                displaced_source,
                displaced_target,
                missing_source,
                progress_sql,
            )
    }
}

impl crate::target::TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor> {
    pub(crate) fn restore_displaced_owner_and_insert_transactionally(
        &mut self,
        table: &SyncTable,
        displaced_source: &SnapshotRow,
        displaced_target: &SnapshotRow,
        missing_source: &SnapshotRow,
        progress_sql: &str,
    ) -> Result<(), TableSyncError> {
        let executor = &self.executor;
        executor
            .begin_sync_transaction()
            .map_err(target_repair_error)?;
        let result = (|| {
            lock_parent_rows(executor, table, displaced_source, missing_source)?;
            let dependencies_before = dependency_fingerprint(
                executor,
                table,
                &[displaced_source, displaced_target, missing_source],
            )?;
            crate::target::TargetMySqlWriter::update_row(self, displaced_source)
                .map_err(|error| TableSyncError::Repair(error.to_string()))?;
            self.insert_rows(std::slice::from_ref(missing_source))
                .map_err(|error| TableSyncError::Repair(error.to_string()))?;
            verify_parent_rows(executor, table, displaced_source, missing_source)?;
            let dependencies_after = dependency_fingerprint(
                executor,
                table,
                &[displaced_source, displaced_target, missing_source],
            )?;
            if dependencies_after != dependencies_before {
                return Err(TableSyncError::Repair(
                    "two-parent collision repair changed dependent rows".to_string(),
                ));
            }
            executor
                .execute_raw_sql(progress_sql)
                .map_err(target_repair_error)
        })();
        match result {
            Ok(()) => executor
                .commit_sync_transaction()
                .map_err(target_repair_error),
            Err(error) => {
                executor
                    .rollback_sync_transaction()
                    .map_err(target_repair_error)?;
                Err(error)
            }
        }
    }
}

fn target_repair_error(error: crate::target::TargetExecuteError) -> TableSyncError {
    TableSyncError::Repair(error.to_string())
}

fn lock_parent_rows(
    executor: &crate::mysql_client::PersistentTargetExecutor,
    table: &SyncTable,
    displaced_source: &SnapshotRow,
    missing_source: &SnapshotRow,
) -> Result<(), TableSyncError> {
    let predicate = parent_identity_predicate(table, &[displaced_source, missing_source])?;
    let sql = format!(
        "SELECT {} FROM {} WHERE {predicate} FOR UPDATE",
        quote_ident_list(&table.primary_key),
        quote_ident(&table.name),
    );
    executor
        .query_rows_as_strings(&sql)
        .map(|_| ())
        .map_err(target_repair_error)
}

fn verify_parent_rows(
    executor: &crate::mysql_client::PersistentTargetExecutor,
    table: &SyncTable,
    displaced_source: &SnapshotRow,
    missing_source: &SnapshotRow,
) -> Result<(), TableSyncError> {
    let predicate = parent_identity_predicate(table, &[displaced_source, missing_source])?;
    let sql = format!(
        "SELECT {} FROM {} WHERE {predicate} ORDER BY {}",
        quote_ident_list(&table.columns),
        quote_ident(&table.name),
        quote_ident_list(&table.primary_key),
    );
    let actual = executor
        .query_rows_as_strings(&sql)
        .map_err(target_repair_error)?;
    let mut expected = vec![
        row_values(table, displaced_source)?,
        row_values(table, missing_source)?,
    ];
    expected.sort();
    let mut actual = actual;
    actual.sort();
    if actual != expected {
        return Err(TableSyncError::Repair(
            "two-parent collision repair verification mismatch".to_string(),
        ));
    }
    Ok(())
}

type DependencyFingerprint = Vec<(String, String, Vec<Vec<Option<String>>>)>;

fn dependency_fingerprint(
    executor: &crate::mysql_client::PersistentTargetExecutor,
    table: &SyncTable,
    parent_images: &[&SnapshotRow],
) -> Result<DependencyFingerprint, TableSyncError> {
    let metadata_sql = format!(
        "SELECT TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_NAME,COLUMN_NAME,REFERENCED_COLUMN_NAME \
         FROM information_schema.KEY_COLUMN_USAGE \
         WHERE REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME={} \
         ORDER BY TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_NAME,ORDINAL_POSITION",
        quote_literal(Some(&table.name)),
    );
    let metadata = executor
        .query_rows_as_strings(&metadata_sql)
        .map_err(target_repair_error)?;
    let constraints = group_foreign_keys(metadata)?;
    constraints
        .into_iter()
        .map(|((child_schema, child_table, constraint), columns)| {
            let predicate = child_identity_predicate(&columns, parent_images)?;
            let sql = format!(
                "SELECT * FROM {}.{} WHERE {predicate}",
                quote_ident(&child_schema),
                quote_ident(&child_table),
            );
            let mut rows = executor
                .query_rows_as_strings(&sql)
                .map_err(target_repair_error)?;
            rows.sort();
            Ok((format!("{child_schema}.{child_table}"), constraint, rows))
        })
        .collect()
}

type ForeignKeyColumns = BTreeMap<(String, String, String), Vec<(String, String)>>;

fn group_foreign_keys(rows: Vec<Vec<Option<String>>>) -> Result<ForeignKeyColumns, TableSyncError> {
    let mut constraints = BTreeMap::new();
    for row in rows {
        if row.len() != 5 {
            return Err(TableSyncError::Repair(
                "foreign-key inventory returned malformed row".to_string(),
            ));
        }
        let child_schema = required_field(&row[0], "child schema")?;
        let child_table = required_field(&row[1], "child table")?;
        let constraint = required_field(&row[2], "constraint")?;
        let child_column = required_field(&row[3], "child column")?;
        let parent_column = required_field(&row[4], "parent column")?;
        constraints
            .entry((child_schema, child_table, constraint))
            .or_insert_with(Vec::new)
            .push((child_column, parent_column));
    }
    Ok(constraints)
}

fn child_identity_predicate(
    columns: &[(String, String)],
    parents: &[&SnapshotRow],
) -> Result<String, TableSyncError> {
    parents
        .iter()
        .map(|parent| {
            columns
                .iter()
                .map(|(child, referenced)| {
                    let value = parent.values.get(referenced).ok_or_else(|| {
                        TableSyncError::Repair(format!(
                            "source row lacks referenced column `{referenced}`"
                        ))
                    })?;
                    Ok(equality_predicate(child, value.as_deref()))
                })
                .collect::<Result<Vec<_>, TableSyncError>>()
                .map(|parts| format!("({})", parts.join(" AND ")))
        })
        .collect::<Result<Vec<_>, TableSyncError>>()
        .map(|parts| parts.join(" OR "))
}

fn parent_identity_predicate(
    table: &SyncTable,
    parents: &[&SnapshotRow],
) -> Result<String, TableSyncError> {
    parents
        .iter()
        .map(|parent| {
            table
                .primary_key
                .iter()
                .map(|column| {
                    let value = parent.values.get(column).ok_or_else(|| {
                        TableSyncError::Repair(format!(
                            "source row lacks primary-key column `{column}`"
                        ))
                    })?;
                    Ok(equality_predicate(column, value.as_deref()))
                })
                .collect::<Result<Vec<_>, TableSyncError>>()
                .map(|parts| format!("({})", parts.join(" AND ")))
        })
        .collect::<Result<Vec<_>, TableSyncError>>()
        .map(|parts| parts.join(" OR "))
}

fn row_values(table: &SyncTable, row: &SnapshotRow) -> Result<Vec<Option<String>>, TableSyncError> {
    table
        .columns
        .iter()
        .map(|column| {
            row.values.get(column).cloned().ok_or_else(|| {
                TableSyncError::Repair(format!("source row lacks column `{column}`"))
            })
        })
        .collect()
}

fn required_field(value: &Option<String>, label: &str) -> Result<String, TableSyncError> {
    value
        .clone()
        .ok_or_else(|| TableSyncError::Repair(format!("{label} was NULL")))
}

fn equality_predicate(column: &str, value: Option<&str>) -> String {
    match value {
        Some(value) => format!("{} = {}", quote_ident(column), quote_literal(Some(value))),
        None => format!("{} IS NULL", quote_ident(column)),
    }
}

fn quote_ident_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn quote_literal(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''")),
        None => "NULL".to_string(),
    }
}
