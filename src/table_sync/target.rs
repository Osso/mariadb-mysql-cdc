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

/// At most this many misfiled owners may be deleted for one duplicate batch.
///
/// The fault seen in production arrives in small contiguous blocks. A larger batch means the
/// premise is wrong, so the repair stops instead of deleting at scale.
const MISFILED_OWNER_RECLAIM_LIMIT: usize = 64;

/// The single target row that holds `source_row`'s unique key under a different primary key.
///
/// Returns `None` when nothing may be deleted: no owner, an ambiguous owner, the owner is the
/// rightful row, or the source agrees the owner's primary key owns this unique value.
fn misfiled_duplicate_owner(
    context: &MySqlFkRepairContext,
    table: &TableInventory,
    key_columns: &[String],
    source_row: &SnapshotRow,
) -> Result<Option<SnapshotRow>, TableSyncError> {
    let key_identity = column_identity(table, source_row, key_columns)?;
    let owners = context
        .target
        .read_exact_inventory_rows(table, &key_identity)?;
    let [owner] = owners.as_slice() else {
        return Ok(None);
    };
    if owner.primary_key == source_row.primary_key {
        return Ok(None);
    }
    let owner_identity = table
        .primary_key
        .iter()
        .cloned()
        .zip(owner.primary_key.iter().cloned())
        .collect::<Vec<_>>();
    let source_at_owner_key = context
        .source
        .read_exact_inventory_rows(table, &owner_identity)?;
    let source_key_at_owner = match source_at_owner_key.as_slice() {
        [] => None,
        [source_owner] => Some(column_identity(table, source_owner, key_columns)?),
        // An ambiguous source read proves nothing.
        _ => return Ok(None),
    };
    Ok(owner_is_misfiled(&key_identity, source_key_at_owner.as_deref()).then(|| owner.clone()))
}

/// Whether the target owner of `key_identity` may be deleted.
///
/// `source_key_at_owner` is the unique value the source stores at the owner's primary key, or `None`
/// when the source has no row there. The owner is misfiled only when the source does not agree that
/// its primary key owns this unique value: an absent source row, or a source row holding a different
/// value. Agreement means the owner is the rightful row and must survive.
fn owner_is_misfiled(
    key_identity: &[(String, String)],
    source_key_at_owner: Option<&[(String, String)]>,
) -> bool {
    match source_key_at_owner {
        None => true,
        Some(source_key) => source_key != key_identity,
    }
}

/// The named columns of a row as (column, value) pairs, in the order given.
fn duplicate_index_name(duplicate_error: &str) -> Result<String, String> {
    let index = crate::target::duplicate_index_from_error(duplicate_error)
        .ok_or_else(|| format!("duplicate error has no index name: {duplicate_error}"))?;
    let index = index.rsplit('.').next().unwrap_or(&index).to_string();
    if index.eq_ignore_ascii_case("PRIMARY") {
        return Err("cannot restore a stale owner for a PRIMARY duplicate".to_string());
    }
    Ok(index)
}

fn rows_match_columns(left: &SnapshotRow, right: &SnapshotRow, columns: &[String]) -> bool {
    columns
        .iter()
        .all(|column| left.values.get(column) == right.values.get(column))
}

fn column_identity(
    table: &TableInventory,
    row: &SnapshotRow,
    columns: &[String],
) -> Result<Vec<(String, String)>, TableSyncError> {
    columns
        .iter()
        .map(|column| {
            let value = row.values.get(column).cloned().flatten().ok_or_else(|| {
                TableSyncError::Repair(format!(
                    "unique key column `{column}` is NULL or missing on `{}`; a NULL key cannot \
                     collide",
                    table.name
                ))
            })?;
            Ok((column.clone(), value))
        })
        .collect()
}

/// What the target holds at a source row's primary key when an insert reported a duplicate.
#[derive(Debug, Eq, PartialEq)]
enum DuplicateOwner {
    /// No row: the duplicate belongs to some other primary key, so this row is still missing.
    Absent,
    /// The source row exactly: nothing to do.
    Equal,
    /// One row with different values. The insert lost a race - the stream applied the row between the
    /// comparison and the insert - or a mutable column moved on, which is routine against a live
    /// source: `comics_top_stats` differed only in the rolling `value_365_days`, 4895 against 4891, at
    /// the same primary key and the same update_time. Applying the source image converges it.
    Divergent,
    /// More than one row claims the identity.
    ///
    /// Unreachable while the schemas agree: the read is by primary key, MySQL enforces its uniqueness,
    /// the catalog rejects a table whose source has no primary key, and `schemas_are_compatible`
    /// requires the target's primary key to equal the source's. This arm is defence against a target
    /// whose primary key no longer covers those columns, which the `LIMIT 2` read exists to detect.
    Ambiguous,
}

fn classify_duplicate_owner(
    target_rows: &[SnapshotRow],
    source_row: &SnapshotRow,
) -> DuplicateOwner {
    match target_rows {
        [] => DuplicateOwner::Absent,
        [target_row] if target_row == source_row => DuplicateOwner::Equal,
        [_] => DuplicateOwner::Divergent,
        _ => DuplicateOwner::Ambiguous,
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
    /// Source secondary unique indexes by table, as (index name, key columns).
    unique_indexes: BTreeMap<String, Vec<(String, Vec<String>)>>,
}

impl MySqlSyncRepairTarget {
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
        let mut unique_indexes: BTreeMap<String, Vec<(String, Vec<String>)>> = BTreeMap::new();
        for index in &source_inventory.indexes {
            if !index.unique || index.name.eq_ignore_ascii_case("PRIMARY") {
                continue;
            }
            unique_indexes
                .entry(index.table.clone())
                .or_default()
                .push((
                    index.name.clone(),
                    index.columns.iter().map(|part| part.name.clone()).collect(),
                ));
        }
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
                unique_indexes,
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
        let identities = rows
            .iter()
            .map(|row| row_identity(table, row))
            .collect::<Result<Vec<_>, _>>()?;
        let target_rows = context
            .target
            .read_exact_inventory_rows_batch(table, &identities)?;
        verify_batch_convergence(table, rows, &identities, &target_rows, operation)
    }

    fn verify_child_rows(&self, rows: &[SnapshotRow]) -> Result<(), TableSyncError> {
        self.verify_exact_rows(&rows.iter().collect::<Vec<_>>(), "insert")
    }

    fn rows_missing_after_duplicate(
        &mut self,
        rows: &[SnapshotRow],
        duplicate_error: &str,
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        let Some(context) = &self.fk_repair else {
            return Err(TableSyncError::Repair(
                "duplicate reconciliation context is unavailable".to_string(),
            ));
        };
        let table_name = self.writer.table_name().to_string();
        let table = context.tables.get(&table_name).ok_or_else(|| {
            TableSyncError::Repair(format!("source inventory is missing table `{table_name}`"))
        })?;
        let mut missing = Vec::new();
        let mut divergent = Vec::new();
        for source_row in rows {
            let identity = row_identity(table, source_row)?;
            let target_rows = context.target.read_exact_inventory_rows(table, &identity)?;
            match classify_duplicate_owner(&target_rows, source_row) {
                DuplicateOwner::Absent => missing.push(source_row.clone()),
                DuplicateOwner::Equal => {}
                DuplicateOwner::Divergent => divergent.push(source_row.clone()),
                DuplicateOwner::Ambiguous => {
                    return Err(TableSyncError::Repair(format!(
                        "more than one target row owns identity {identity:?} on `{table_name}`"
                    )));
                }
            }
        }
        for source_row in &divergent {
            self.update_row(source_row)?;
        }
        if missing.len() == rows.len() {
            let reclaimed = self.reclaim_misfiled_duplicate_owners(rows, duplicate_error)?;
            if reclaimed == 0 {
                return Err(TableSyncError::Repair(format!(
                    "duplicate key for `{table_name}` is owned by a different target identity; {}",
                    foreign_owned_duplicate_evidence(rows, duplicate_error)
                )));
            }
        }
        Ok(missing)
    }

    /// Deletes target rows that hold a source row's unique key under the wrong primary key.
    ///
    /// A row copied without preserving the source primary key lands on a fresh auto_increment value.
    /// Its unique key then belongs to the wrong primary key, so the rightful row can never be
    /// inserted, and every update addressed by primary key silently finds nothing. The only way to
    /// converge is to remove the misfiled row, which is destructive, so each deletion must be proven:
    ///
    ///   - the source row being inserted is absent from the target by primary key, already
    ///     established by the caller;
    ///   - exactly one target row owns the conflicting unique value;
    ///   - that owner's primary key differs from the source row's;
    ///   - the source row at the owner's primary key is absent, or present with a different unique
    ///     value. This is the decisive check: if the source agrees that the owner's primary key owns
    ///     this unique value, the owner is legitimate and nothing may be deleted.
    ///
    /// Anything else fails closed and leaves the conflict for an operator.
    fn reclaim_misfiled_duplicate_owners(
        &mut self,
        rows: &[SnapshotRow],
        duplicate_error: &str,
    ) -> Result<usize, TableSyncError> {
        let Some(index) = crate::target::duplicate_index_from_error(duplicate_error) else {
            return Ok(0);
        };
        let index = index.rsplit('.').next().unwrap_or(&index).to_string();
        if index.eq_ignore_ascii_case("PRIMARY") {
            return Ok(0);
        }
        let table_name = self.writer.table_name().to_string();
        let mut owners = Vec::new();
        {
            let Some(context) = &self.fk_repair else {
                return Ok(0);
            };
            let table = context.tables.get(&table_name).ok_or_else(|| {
                TableSyncError::Repair(format!("source inventory is missing table `{table_name}`"))
            })?;
            let Some(key_columns) = context
                .unique_indexes
                .get(&table_name)
                .and_then(|indexes| {
                    indexes
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(&index))
                })
                .map(|(_, columns)| columns.clone())
            else {
                return Ok(0);
            };
            for source_row in rows {
                let Some(owner) =
                    misfiled_duplicate_owner(context, table, &key_columns, source_row)?
                else {
                    continue;
                };
                owners.push((owner, source_row.primary_key.clone()));
            }
        }
        if owners.len() > MISFILED_OWNER_RECLAIM_LIMIT {
            return Err(TableSyncError::Repair(format!(
                "refusing to reclaim {} misfiled duplicate owners on `{table_name}`; the limit is {MISFILED_OWNER_RECLAIM_LIMIT}",
                owners.len()
            )));
        }
        for (owner, rightful_primary_key) in &owners {
            println!(
                "cdc_misfiled_duplicate_owner_reclaimed table={table_name} index={index} \
                 deleted_primary_key={:?} rightful_primary_key={:?}",
                owner.primary_key, rightful_primary_key
            );
            self.delete_row(&owner.primary_key)?;
        }
        Ok(owners.len())
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

    fn converged_parents(
        &mut self,
        identities: &[ParentIdentity],
    ) -> Result<BTreeSet<ParentIdentity>, String> {
        let mut converged = BTreeSet::new();
        for (table_name, table_identities) in group_identities_by_table(identities) {
            let table = self.table(&table_name)?;
            let source = self.read_parents_batch(&self.context.source, table, &table_identities)?;
            if source.is_empty() {
                continue;
            }
            let target = self.read_parents_batch(&self.context.target, table, &table_identities)?;
            for (identity, source_row) in source {
                if target.get(&identity) == Some(&source_row) {
                    converged.insert(identity);
                }
            }
        }
        Ok(converged)
    }

    fn repair_parent(&mut self, row: &ParentRepairRow) -> Result<(), String> {
        let table = self.table(&row.table)?.clone();
        let snapshot_row = parent_snapshot_row(&table, row)?;
        let target_rows = self
            .context
            .target
            .read_exact_inventory_rows(
                &table,
                &row_identity(&table, &snapshot_row).map_err(|e| e.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        let parent_writer = TargetMySqlWriter::from_snapshot_table(
            &SnapshotTable::from(&table),
            self.writer.executor.clone(),
            SnapshotInsertMode::Insert,
        );
        match target_rows.as_slice() {
            [] => self.insert_parent_after_restoring_stale_unique_owners(
                &table,
                &parent_writer,
                &snapshot_row,
            ),
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

    fn insert_parent_after_restoring_stale_unique_owners(
        &mut self,
        table: &TableInventory,
        parent_writer: &TargetMySqlWriter<crate::mysql_client::PersistentTargetExecutor>,
        source_parent: &SnapshotRow,
    ) -> Result<(), String> {
        let restore_limit = self
            .context
            .unique_indexes
            .get(&table.name)
            .map_or(0, Vec::len);
        for _ in 0..=restore_limit {
            match parent_writer.insert_rows(std::slice::from_ref(source_parent)) {
                Ok(()) => return Ok(()),
                Err(error) if error.mysql_code() == Some(1062) => {
                    self.restore_stale_unique_owner(table, source_parent, &error.to_string())?;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
        Err(format!(
            "target parent insert for `{}` exceeded the unique-owner restore limit",
            table.name
        ))
    }

    fn restore_stale_unique_owner(
        &mut self,
        table: &TableInventory,
        source_parent: &SnapshotRow,
        duplicate_error: &str,
    ) -> Result<(), String> {
        let index = duplicate_index_name(duplicate_error)?;
        let key_columns = self.unique_index_columns(table, &index)?;
        let key_identity = column_identity(table, source_parent, key_columns)
            .map_err(|error| error.to_string())?;
        let target_owner = self.read_unique_owner(table, &key_identity)?;
        if target_owner.primary_key == source_parent.primary_key {
            return Err(format!(
                "target parent primary key already owns duplicate index `{index}` on `{}`",
                table.name
            ));
        }
        let owner_identity =
            row_identity(table, &target_owner).map_err(|error| error.to_string())?;
        let source_owner = self.read_source_owner(table, &owner_identity)?;
        if rows_match_columns(&source_owner, source_parent, key_columns) {
            return Err(format!(
                "source agrees target owner {:?} owns duplicate index `{index}` on `{}`",
                target_owner.primary_key, table.name
            ));
        }

        self.restore_owner_row(table, &owner_identity, &source_owner)?;
        println!(
            "cdc_stale_unique_owner_restored table={} index={} owner_primary_key={:?} \
             desired_primary_key={:?}",
            table.name, index, target_owner.primary_key, source_parent.primary_key
        );
        Ok(())
    }

    fn unique_index_columns<'b>(
        &'b self,
        table: &TableInventory,
        index: &str,
    ) -> Result<&'b [String], String> {
        self.context
            .unique_indexes
            .get(&table.name)
            .and_then(|indexes| {
                indexes
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(index))
            })
            .map(|(_, columns)| columns.as_slice())
            .ok_or_else(|| format!("unknown duplicate index `{index}` on `{}`", table.name))
    }

    fn read_unique_owner(
        &self,
        table: &TableInventory,
        identity: &[(String, String)],
    ) -> Result<SnapshotRow, String> {
        Self::read_exact_owner(&self.context.target, table, identity, "target")
    }

    fn read_source_owner(
        &self,
        table: &TableInventory,
        identity: &[(String, String)],
    ) -> Result<SnapshotRow, String> {
        Self::read_exact_owner(&self.context.source, table, identity, "source")
    }

    fn read_exact_owner(
        reader: &MySqlSyncReader,
        table: &TableInventory,
        identity: &[(String, String)],
        endpoint: &str,
    ) -> Result<SnapshotRow, String> {
        let owners = reader
            .read_exact_inventory_rows(table, identity)
            .map_err(|error| error.to_string())?;
        let [owner] = owners.as_slice() else {
            return Err(format!(
                "identity {identity:?} on `{}` has {} {endpoint} owners",
                table.name,
                owners.len()
            ));
        };
        Ok(owner.clone())
    }

    fn restore_owner_row(
        &self,
        table: &TableInventory,
        identity: &[(String, String)],
        source_owner: &SnapshotRow,
    ) -> Result<(), String> {
        let owner_writer = TargetMySqlWriter::from_snapshot_table(
            &SnapshotTable::from(table),
            self.writer.executor.clone(),
            SnapshotInsertMode::Insert,
        );
        owner_writer
            .update_row(source_owner)
            .map_err(|error| error.to_string())?;
        self.verify_restored_owner(table, identity, source_owner)
    }

    fn verify_restored_owner(
        &self,
        table: &TableInventory,
        identity: &[(String, String)],
        source_owner: &SnapshotRow,
    ) -> Result<(), String> {
        let restored = self
            .context
            .target
            .read_exact_inventory_rows(table, identity)
            .map_err(|error| error.to_string())?;
        if restored.as_slice() == std::slice::from_ref(source_owner) {
            return Ok(());
        }
        Err(format!(
            "target owner identity {identity:?} on `{}` did not match source after restore",
            table.name
        ))
    }

    /// Reads one parent table's identities in a single statement, keeping only unambiguous matches.
    ///
    /// An identity that returns more than one row is omitted rather than reported, so the caller
    /// cannot treat it as converged and the per-identity path still raises the ambiguity error.
    fn read_parents_batch(
        &self,
        reader: &MySqlSyncReader,
        table: &TableInventory,
        identities: &[ParentIdentity],
    ) -> Result<BTreeMap<ParentIdentity, ParentRepairRow>, String> {
        let keys = identities
            .iter()
            .map(|identity| identity.values.clone())
            .collect::<Vec<_>>();
        let rows = reader
            .read_exact_inventory_rows_batch(table, &keys)
            .map_err(|error| error.to_string())?;
        let mut by_identity: BTreeMap<ParentIdentity, Vec<ParentRepairRow>> = BTreeMap::new();
        for row in rows {
            let values = row_identity(table, &row).map_err(|error| error.to_string())?;
            by_identity
                .entry(ParentIdentity {
                    table: table.name.clone(),
                    values,
                })
                .or_default()
                .push(ParentRepairRow {
                    table: table.name.clone(),
                    values: row.values.clone(),
                });
        }
        Ok(by_identity
            .into_iter()
            .filter_map(|(identity, mut rows)| match rows.len() {
                1 => Some((identity, rows.remove(0))),
                _ => None,
            })
            .collect())
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

/// Groups parent identities by table so each table is read in one statement.
///
/// A child batch usually references several parent tables, and one row-constructor `IN` cannot span
/// tables. Grouping keeps the round-trip count at one per parent table per side.
fn group_identities_by_table(
    identities: &[ParentIdentity],
) -> BTreeMap<String, Vec<ParentIdentity>> {
    let mut grouped: BTreeMap<String, Vec<ParentIdentity>> = BTreeMap::new();
    for identity in identities {
        grouped
            .entry(identity.table.clone())
            .or_default()
            .push(identity.clone());
    }
    grouped
}

/// Fails closed unless every repaired row has exactly one equal row on the target.
///
/// One batched read replaces one read per row, so the rows arrive interleaved and must be matched
/// back by identity. A missing, divergent, or duplicated identity is a verification failure, which
/// leaves the chunk uncheckpointed for retry.
pub(super) fn verify_batch_convergence(
    table: &TableInventory,
    rows: &[&SnapshotRow],
    identities: &[Vec<(String, String)>],
    target_rows: &[SnapshotRow],
    operation: &str,
) -> Result<(), TableSyncError> {
    let mut rows_by_identity: BTreeMap<Vec<(String, String)>, Vec<&SnapshotRow>> = BTreeMap::new();
    for target_row in target_rows {
        rows_by_identity
            .entry(row_identity(table, target_row)?)
            .or_default()
            .push(target_row);
    }
    for (identity, source_row) in identities.iter().zip(rows) {
        let found = rows_by_identity
            .get(identity)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if found.len() != 1 || found.first().copied() != Some(*source_row) {
            return Err(TableSyncError::Repair(format!(
                "post-{operation} verification failed for `{}` identity {identity:?}",
                table.name
            )));
        }
    }
    Ok(())
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
    let index = crate::conflict_ledger::duplicate_key_name(duplicate_error)
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

    fn stat_row(views: &str) -> SnapshotRow {
        SnapshotRow {
            primary_key: vec!["13553".to_string(), "views".to_string()],
            values: [
                ("comic_id".to_string(), Some("13553".to_string())),
                ("statistic".to_string(), Some("views".to_string())),
                ("value_365_days".to_string(), Some(views.to_string())),
            ]
            .into_iter()
            .collect(),
        }
    }

    /// The observed production case: one row at the source primary key whose rolling counter moved on.
    /// It is divergent, so the source image is applied; failing the run rejected a converging target.
    #[test]
    fn one_target_row_with_different_values_is_divergent() {
        let source = stat_row("4895");
        let target = stat_row("4891");

        assert_eq!(
            classify_duplicate_owner(&[target], &source),
            DuplicateOwner::Divergent
        );
    }

    #[test]
    fn an_equal_target_row_needs_no_repair() {
        let source = stat_row("4895");

        assert_eq!(
            classify_duplicate_owner(std::slice::from_ref(&source), &source),
            DuplicateOwner::Equal
        );
    }

    /// No row at the primary key means the duplicate is owned by a different one, so the row is still
    /// missing and the misfiled-owner proof decides what happens next.
    #[test]
    fn no_target_row_leaves_the_source_row_missing() {
        assert_eq!(
            classify_duplicate_owner(&[], &stat_row("4895")),
            DuplicateOwner::Absent
        );
    }

    #[test]
    fn more_than_one_owner_is_ambiguous_even_though_a_primary_key_read_cannot_return_two() {
        let source = stat_row("4895");

        assert_eq!(
            classify_duplicate_owner(&[stat_row("4891"), stat_row("4892")], &source),
            DuplicateOwner::Ambiguous
        );
    }

    fn identity(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(column, value)| (column.to_string(), value.to_string()))
            .collect()
    }

    /// The observed production fault: the source has no row at the owner's primary key, because the
    /// owner was inserted on a fresh auto_increment value.
    #[test]
    fn an_owner_absent_from_the_source_is_misfiled() {
        let key = identity(&[("token", "62935919")]);

        assert!(owner_is_misfiled(&key, None));
    }

    /// The source stores a different value at that primary key, so the owner cannot belong there.
    #[test]
    fn an_owner_whose_source_row_holds_another_value_is_misfiled() {
        let key = identity(&[("user_id", "1916267"), ("entity_id", "3")]);
        let at_owner = identity(&[("user_id", "1917680"), ("entity_id", "3")]);

        assert!(owner_is_misfiled(&key, Some(&at_owner)));
    }

    /// The decisive negative: the source agrees this primary key owns the value, so the target row is
    /// the rightful one and deleting it would destroy live data.
    #[test]
    fn an_owner_the_source_agrees_with_is_never_misfiled() {
        let key = identity(&[("token", "62935919")]);

        assert!(!owner_is_misfiled(&key, Some(&key)));
    }

    #[test]
    fn a_null_unique_key_column_cannot_be_reclaimed() {
        let table = TableInventory {
            name: "users_offers".to_string(),
            table_type: "BASE TABLE".to_string(),
            engine: None,
            collation: None,
            primary_key: vec!["id".to_string()],
            columns: Vec::new(),
        };
        let row = SnapshotRow {
            primary_key: vec!["7".to_string()],
            values: [("uuid".to_string(), None)].into_iter().collect(),
        };

        let error = column_identity(&table, &row, &["uuid".to_string()])
            .expect_err("a NULL unique key cannot collide");

        assert!(error.to_string().contains("NULL"), "{error}");
    }

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
