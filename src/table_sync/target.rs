use super::fk_parent_repair::{
    ForeignKeyColumn, ForeignKeyEdge, ParentIdentity, ParentRepairRow, ParentRepairStore,
    repair_fk_parents_and_retry,
};
use super::mysql::MySqlSyncReader;
use super::{SyncTable, TableSyncError};
use crate::inventory::{ForeignKeyInventory, SchemaInventory, TableInventory};
use crate::snapshot::{SnapshotRow, SnapshotTable};
use crate::target::{PrimaryKey, SnapshotInsertMode, TargetMySqlWriter};
use mysql::Value;
use std::collections::{BTreeMap, BTreeSet};

pub trait SyncRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
    fn insert_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        for row in rows {
            self.insert_row(row)?;
        }
        Ok(())
    }
    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
    fn update_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        for row in rows {
            self.update_row(row)?;
        }
        Ok(())
    }
    fn update_batch_size(&self) -> usize {
        usize::MAX
    }
    fn verify_rows(&self, _rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        Ok(())
    }
    fn verify_deleted_rows(&self, _primary_keys: &[Vec<String>]) -> Result<(), TableSyncError> {
        Ok(())
    }
    fn requires_terminal_verification(&self) -> bool {
        false
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
        crate::target::TargetMySqlWriter::insert_rows(self, std::slice::from_ref(row))
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn insert_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        let rows = rows.iter().map(|row| (*row).clone()).collect::<Vec<_>>();
        crate::target::TargetMySqlWriter::insert_rows(self, &rows)
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
    writer: TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
    fk_repair: Option<MySqlFkRepairContext>,
}

struct MySqlFkRepairContext {
    source: MySqlSyncReader,
    target: MySqlSyncReader,
    tables: BTreeMap<String, TableInventory>,
    edges: Vec<ForeignKeyEdge>,
}

impl MySqlSyncRepairTarget {
    pub(crate) fn new(
        writer: TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
    ) -> Self {
        Self {
            writer,
            fk_repair: None,
        }
    }

    pub(crate) fn new_with_fk_repair(
        writer: TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
        source: MySqlSyncReader,
        target: MySqlSyncReader,
        source_inventory: SchemaInventory,
        target_inventory: SchemaInventory,
    ) -> Self {
        let tables = source_inventory
            .tables
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect();
        let edges = merged_fk_edges(
            &source_inventory.schema,
            &target_inventory.schema,
            source_inventory.foreign_keys,
            target_inventory.foreign_keys,
        );
        Self {
            writer,
            fk_repair: Some(MySqlFkRepairContext {
                source,
                target,
                tables,
                edges,
            }),
        }
    }

    fn repair_fk_parents_and_retry(&mut self, rows: &[SnapshotRow]) -> Result<(), TableSyncError> {
        let child_table = self.writer.table_name().to_string();
        let child_rows = rows
            .iter()
            .map(|row| ParentRepairRow {
                table: child_table.clone(),
                values: row.values.clone(),
            })
            .collect::<Vec<_>>();
        let Some(mut context) = self.fk_repair.take() else {
            return Err(TableSyncError::Repair(
                "foreign-key parent repair context is unavailable".to_string(),
            ));
        };
        let result = repair_fk_parents_and_retry(
            &child_table,
            &child_rows,
            &context.edges.clone(),
            &mut MySqlParentRepairStore {
                writer: &self.writer,
                context: &mut context,
            },
        )
        .map_err(|error| TableSyncError::Repair(error.to_string()));
        self.fk_repair = Some(context);
        result
    }

    fn verify_exact_rows(
        &self,
        rows: &[&SnapshotRow],
        operation: &str,
    ) -> Result<(), TableSyncError> {
        let Some(context) = &self.fk_repair else {
            return Ok(());
        };
        let table_name = self.writer.table_name();
        let table = context.tables.get(table_name).ok_or_else(|| {
            TableSyncError::Repair(format!("source inventory is missing table `{table_name}`"))
        })?;
        for source_row in rows {
            let identity = row_identity(table, source_row)?;
            let target_rows = context.target.read_exact_inventory_rows(table, &identity)?;
            if target_rows.len() != 1 || target_rows.first() != Some(*source_row) {
                return Err(TableSyncError::Repair(format!(
                    "post-{operation} verification failed for `{table_name}` identity {identity:?}"
                )));
            }
        }
        Ok(())
    }

    fn verify_child_rows(&self, rows: &[SnapshotRow]) -> Result<(), TableSyncError> {
        self.verify_exact_rows(&rows.iter().collect::<Vec<_>>(), "insert")
    }

    fn rows_missing_after_duplicate(
        &self,
        rows: &[SnapshotRow],
        duplicate_error: &str,
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let Some(context) = &self.fk_repair else {
            return Err(TableSyncError::Repair(
                "duplicate reconciliation context is unavailable".to_string(),
            ));
        };
        let table_name = self.writer.table_name();
        let table = context.tables.get(table_name).ok_or_else(|| {
            TableSyncError::Repair(format!("source inventory is missing table `{table_name}`"))
        })?;
        let mut missing = Vec::new();
        for source_row in rows {
            let identity = row_identity(table, source_row)?;
            let target_rows = context.target.read_exact_inventory_rows(table, &identity)?;
            match target_rows.as_slice() {
                [] => missing.push(source_row.clone()),
                [target_row] if target_row == source_row => {}
                _ => {
                    return Err(TableSyncError::Repair(format!(
                        "concurrent duplicate owner diverges from source for `{table_name}` identity {identity:?}"
                    )));
                }
            }
        }
        if missing.len() == rows.len() {
            return Err(TableSyncError::Repair(format!(
                "duplicate key for `{table_name}` is owned by a different target identity; {}",
                foreign_owned_duplicate_evidence(rows, duplicate_error)
            )));
        }
        Ok(missing)
    }

    fn insert_child_batch(&mut self, batch: &[SnapshotRow]) -> Result<(), TableSyncError> {
        super::child_batch::insert_child_batch_with_reconciliation(self, batch)?;
        self.verify_child_rows(batch)
    }
}

impl super::child_batch::ChildBatchInserter for MySqlSyncRepairTarget {
    fn table_name(&self) -> &str {
        self.writer.table_name()
    }

    fn insert(
        &mut self,
        rows: &[SnapshotRow],
    ) -> Result<super::child_batch::ChildInsertOutcome, TableSyncError> {
        match self.writer.insert_rows(rows) {
            Ok(()) => Ok(super::child_batch::ChildInsertOutcome::Applied),
            Err(error) if error.mysql_code() == Some(1452) => {
                Ok(super::child_batch::ChildInsertOutcome::MissingParent)
            }
            Err(error) if error.mysql_code() == Some(1062) => Ok(
                super::child_batch::ChildInsertOutcome::DuplicateKey(error.to_string()),
            ),
            Err(error) => Err(TableSyncError::Repair(error.to_string())),
        }
    }

    fn repair_parents(&mut self, rows: &[SnapshotRow]) -> Result<(), TableSyncError> {
        self.repair_fk_parents_and_retry(rows)
    }

    fn reconcile_duplicates(
        &mut self,
        rows: &[SnapshotRow],
        duplicate_error: &str,
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        self.rows_missing_after_duplicate(rows, duplicate_error)
    }
}

struct MySqlParentRepairStore<'a> {
    writer: &'a TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
    context: &'a mut MySqlFkRepairContext,
}

impl ParentRepairStore for MySqlParentRepairStore<'_> {
    fn read_source_parent(
        &mut self,
        identity: &ParentIdentity,
    ) -> Result<Option<ParentRepairRow>, String> {
        self.read_parent(&self.context.source, identity)
    }

    fn read_target_parent(
        &mut self,
        identity: &ParentIdentity,
    ) -> Result<Option<ParentRepairRow>, String> {
        self.read_parent(&self.context.target, identity)
    }

    fn repair_parent(&mut self, row: &ParentRepairRow) -> Result<(), String> {
        let table = self.table(&row.table)?;
        let snapshot_row = parent_snapshot_row(table, row)?;
        let target_rows = self
            .context
            .target
            .read_exact_inventory_rows(
                table,
                &row_identity(table, &snapshot_row).map_err(|e| e.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        let parent_writer = TargetMySqlWriter::from_snapshot_table(
            &SnapshotTable::from(table),
            self.writer.executor.clone(),
            SnapshotInsertMode::Insert,
        );
        match target_rows.as_slice() {
            [] => parent_writer
                .insert_rows(std::slice::from_ref(&snapshot_row))
                .map_err(|error| error.to_string()),
            [_] => parent_writer
                .update_row(&snapshot_row)
                .map_err(|error| error.to_string()),
            rows => Err(format!(
                "target parent identity for `{}` is ambiguous: {} rows",
                row.table,
                rows.len()
            )),
        }
    }
}

impl MySqlParentRepairStore<'_> {
    fn table(&self, table: &str) -> Result<&TableInventory, String> {
        self.context
            .tables
            .get(table)
            .ok_or_else(|| format!("source inventory is missing parent table `{table}`"))
    }

    fn read_parent(
        &self,
        reader: &MySqlSyncReader,
        identity: &ParentIdentity,
    ) -> Result<Option<ParentRepairRow>, String> {
        let table = self.table(&identity.table)?;
        let rows = reader
            .read_exact_inventory_rows(table, &identity.values)
            .map_err(|error| error.to_string())?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(ParentRepairRow {
                table: identity.table.clone(),
                values: row.values.clone(),
            })),
            rows => Err(format!(
                "exact parent identity for `{}` is ambiguous: {} rows",
                identity.table,
                rows.len()
            )),
        }
    }
}

fn merged_fk_edges(
    source_schema: &str,
    target_schema: &str,
    source: Vec<ForeignKeyInventory>,
    target: Vec<ForeignKeyInventory>,
) -> Vec<ForeignKeyEdge> {
    source
        .into_iter()
        .filter(|foreign_key| foreign_key.referenced_schema == source_schema)
        .chain(
            target
                .into_iter()
                .filter(|foreign_key| foreign_key.referenced_schema == target_schema),
        )
        .map(|foreign_key| ForeignKeyEdge {
            child_table: foreign_key.table,
            parent_table: foreign_key.referenced_table,
            columns: foreign_key
                .columns
                .into_iter()
                .zip(foreign_key.referenced_columns)
                .map(|(child, parent)| ForeignKeyColumn { child, parent })
                .collect(),
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn parent_snapshot_row(
    table: &TableInventory,
    row: &ParentRepairRow,
) -> Result<SnapshotRow, String> {
    let primary_key = table
        .primary_key
        .iter()
        .map(|column| {
            row.values
                .get(column)
                .and_then(Option::clone)
                .ok_or_else(|| {
                    format!(
                        "parent `{}` has null or missing primary key `{column}`",
                        table.name
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SnapshotRow {
        primary_key,
        values: row.values.clone(),
    })
}

fn row_identity(
    table: &TableInventory,
    row: &SnapshotRow,
) -> Result<Vec<(String, String)>, TableSyncError> {
    table
        .primary_key
        .iter()
        .map(|column| {
            row.values
                .get(column)
                .and_then(Option::clone)
                .map(|value| (column.clone(), value))
                .ok_or_else(|| {
                    TableSyncError::Repair(format!(
                        "row in `{}` has null or missing primary key `{column}`",
                        table.name
                    ))
                })
        })
        .collect()
}

impl SyncRepairTarget for MySqlSyncRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        self.writer
            .insert_rows(std::slice::from_ref(row))
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn insert_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        let rows = rows.iter().map(|row| (*row).clone()).collect::<Vec<_>>();
        for batch in rows.chunks(self.writer.insert_batch_size()) {
            self.insert_child_batch(batch)?;
        }
        Ok(())
    }

    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        crate::target::TargetMySqlWriter::update_row(&self.writer, row)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn update_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        match crate::target::TargetMySqlWriter::update_rows(&self.writer, rows) {
            Ok(()) => Ok(()),
            Err(error) if error.mysql_code() == Some(1452) => {
                let rows = rows.iter().map(|row| (*row).clone()).collect::<Vec<_>>();
                self.repair_fk_parents_and_retry(&rows)?;
                crate::target::TargetMySqlWriter::update_rows(
                    &self.writer,
                    &rows.iter().collect::<Vec<_>>(),
                )
                .map_err(|retry_error| TableSyncError::Repair(retry_error.to_string()))
            }
            Err(error) => Err(TableSyncError::Repair(error.to_string())),
        }
    }

    fn update_batch_size(&self) -> usize {
        self.writer.update_batch_size()
    }

    fn verify_rows(&self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        self.verify_exact_rows(rows, "update")
    }

    fn verify_deleted_rows(&self, primary_keys: &[Vec<String>]) -> Result<(), TableSyncError> {
        let Some(context) = &self.fk_repair else {
            return Ok(());
        };
        let table_name = self.writer.table_name();
        let table = context.tables.get(table_name).ok_or_else(|| {
            TableSyncError::Repair(format!("source inventory is missing table `{table_name}`"))
        })?;
        for primary_key in primary_keys {
            if primary_key.len() != table.primary_key.len() {
                return Err(TableSyncError::Repair(format!(
                    "post-delete verification primary key width mismatch for `{table_name}`"
                )));
            }
            let identity = table
                .primary_key
                .iter()
                .cloned()
                .zip(primary_key.iter().cloned())
                .collect::<Vec<_>>();
            if !context
                .target
                .read_exact_inventory_rows(table, &identity)?
                .is_empty()
            {
                return Err(TableSyncError::Repair(format!(
                    "post-delete verification failed for `{table_name}` identity {identity:?}"
                )));
            }
        }
        Ok(())
    }

    fn requires_terminal_verification(&self) -> bool {
        self.fk_repair.is_some()
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
            crate::target::TargetMySqlWriter::insert_rows(
                self,
                std::slice::from_ref(missing_source),
            )
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

/// Rows named in a foreign-owned duplicate failure. Enough to reproduce it after the run exits, and
/// bounded so one wide batch cannot produce an unbounded error string.
const FOREIGN_OWNED_DUPLICATE_EVIDENCE_KEYS: usize = 10;

/// Every row of the batch is absent by its own identity yet the insert reported a duplicate, so some
/// other unique index owns the key. Without the index name and the source keys, the failure cannot be
/// reproduced once the live stream has moved the target on.
fn foreign_owned_duplicate_evidence(rows: &[SnapshotRow], duplicate_error: &str) -> String {
    let index = crate::conflict_repair::duplicate_key_name(duplicate_error)
        .unwrap_or_else(|| "<unparsed>".to_string());
    let shown = rows
        .iter()
        .take(FOREIGN_OWNED_DUPLICATE_EVIDENCE_KEYS)
        .map(|row| format!("{:?}", row.primary_key))
        .collect::<Vec<_>>()
        .join(", ");
    let omitted = rows
        .len()
        .saturating_sub(FOREIGN_OWNED_DUPLICATE_EVIDENCE_KEYS);
    let suffix = if omitted == 0 {
        String::new()
    } else {
        format!(" (+{omitted} more)")
    };
    format!(
        "duplicate_index={index}; batch_rows={}; source_primary_keys=[{shown}]{suffix}; \
         mysql_error={duplicate_error}",
        rows.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence_row(primary_key: &str) -> SnapshotRow {
        SnapshotRow {
            primary_key: vec![primary_key.to_string()],
            values: BTreeMap::from([("id".to_string(), Some(primary_key.to_string()))]),
        }
    }

    /// `users_actions_timestamps` aborted with only the table name, so the collision could not be
    /// reproduced once the live stream had moved the target on.
    #[test]
    fn foreign_owned_duplicate_evidence_names_the_index_and_source_keys() {
        let rows = vec![evidence_row("155476744"), evidence_row("155476745")];

        let evidence = foreign_owned_duplicate_evidence(
            &rows,
            "Duplicate entry '1916023-first_page_read' for key \
             'users_actions_timestamps.uidx_owner_key'",
        );

        assert!(evidence.contains("duplicate_index=users_actions_timestamps.uidx_owner_key"));
        assert!(evidence.contains("batch_rows=2"));
        assert!(evidence.contains("\"155476744\""));
        assert!(evidence.contains("\"155476745\""));
        assert!(!evidence.contains("more)"));
    }

    /// A wide batch must not produce an unbounded error string.
    #[test]
    fn foreign_owned_duplicate_evidence_bounds_the_key_list() {
        let rows = (0..25)
            .map(|index| evidence_row(&index.to_string()))
            .collect::<Vec<_>>();

        let evidence = foreign_owned_duplicate_evidence(&rows, "Duplicate entry 'x' for key 't.u'");

        assert!(evidence.contains("batch_rows=25"));
        assert!(evidence.contains("(+15 more)"));
        assert!(evidence.contains("\"9\""));
        assert!(!evidence.contains("\"10\""));
    }

    /// An unparseable duplicate message must still yield the source keys rather than nothing.
    #[test]
    fn foreign_owned_duplicate_evidence_survives_an_unparseable_error() {
        let evidence =
            foreign_owned_duplicate_evidence(&[evidence_row("7")], "some other target failure");

        assert!(evidence.contains("duplicate_index=<unparsed>"));
        assert!(evidence.contains("\"7\""));
        assert!(evidence.contains("mysql_error=some other target failure"));
    }

    fn foreign_key(referenced_schema: &str) -> ForeignKeyInventory {
        ForeignKeyInventory {
            table: "guests".to_string(),
            name: "fk_guests_utm_id".to_string(),
            columns: vec!["utm_id".to_string()],
            referenced_schema: referenced_schema.to_string(),
            referenced_table: "utms".to_string(),
            referenced_columns: vec!["id".to_string()],
        }
    }

    #[test]
    fn merges_local_source_and_target_fk_schemas() {
        let edges = merged_fk_edges(
            "source_db",
            "target_db",
            Vec::new(),
            vec![foreign_key("target_db")],
        );

        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].child_table, "guests");
        assert_eq!(edges[0].parent_table, "utms");
    }
}
