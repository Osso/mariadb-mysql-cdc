use super::*;
use crate::inventory::{ForeignKeyInventory, SchemaInventory};
use crate::sync::component_locks::selected_fk_component;
use crate::sync::config::sync_table_from_inventory;
use crate::sync::fk_transition::{
    self as engine, Backend, Key, Limits, ParentReference, Relation, Row, RowKey,
};
use crate::sync::sql::build_related_rows_select_statement;

fn describe_transition_error(error: engine::Error<String>) -> String {
    match error {
        engine::Error::Backend(message) => message,
        engine::Error::Cycle(table) => format!("dependency cycle in `{table}`"),
        engine::Error::WorkLimit => "transition work limit reached".into(),
        engine::Error::InvalidPage => "invalid dependent key page".into(),
        engine::Error::InvalidRelation => "invalid foreign-key relation".into(),
        engine::Error::MissingColumn(column) => format!("missing column `{column}`"),
        engine::Error::MissingTarget(key) => format!("target row missing in `{}`", key.table),
        engine::Error::MissingParent { child, parent } => format!(
            "required parent in `{}` missing for `{}`",
            parent.table, child.table,
        ),
    }
}

pub(super) struct TransitionContext {
    source: Conn,
    tables: BTreeMap<String, SyncTable>,
    foreign_keys: Vec<ForeignKeyInventory>,
    pub(super) component: Vec<String>,
}

impl MySqlSyncTargetSession {
    pub(crate) fn configure_fk_transitions(
        &mut self,
        source: &MySqlConnectionConfig,
        inventory: &SchemaInventory,
        selected: &[String],
    ) -> Result<(), String> {
        let component = selected_fk_component(inventory, selected, &self.table.name)?;
        let tables = inventory
            .tables
            .iter()
            .filter(|table| component.contains(&table.name))
            .map(|table| {
                sync_table_from_inventory(table).map(|metadata| (table.name.clone(), metadata))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let foreign_keys = inventory
            .foreign_keys
            .iter()
            .filter(|key| {
                key.referenced_schema == inventory.schema && component.contains(&key.table)
            })
            .cloned()
            .collect();
        let source = open_sync_connection(sync_source_opts(source)?)
            .map_err(|error| format!("connect source for FK transition: {error}"))?;
        self.transitions = Some(TransitionContext {
            source,
            tables,
            foreign_keys,
            component,
        });
        Ok(())
    }

    pub(super) fn replace_source_absent_unique_owners(
        &mut self,
        failure: &mut SyncMutationFailure,
    ) -> Result<bool, String> {
        let conflicts = self.inspect_unique_owner_conflicts(failure)?;
        let mut context = self
            .transitions
            .take()
            .ok_or("FK transitions not configured")?;
        let result = (|| {
            let mut replaced = false;
            for conflict in conflicts {
                if self.replace_source_absent_unique_owner(&mut context, &conflict)? {
                    failure
                        .failed_batch
                        .retain(|row| row.primary_key != conflict.intended.primary_key);
                    replaced = true;
                }
            }
            Ok(replaced)
        })();
        self.transitions = Some(context);
        result
    }

    fn replace_source_absent_unique_owner(
        &mut self,
        context: &mut TransitionContext,
        conflict: &SyncUniqueOwnerConflict,
    ) -> Result<bool, String> {
        let owner = RowKey {
            table: self.table.name.clone(),
            pk: conflict.owner.primary_key.clone(),
        };
        let intended = RowKey {
            table: self.table.name.clone(),
            pk: conflict.intended.primary_key.clone(),
        };
        let mut backend = TransitionBackend {
            target: self,
            context,
            visiting: BTreeSet::new(),
        };
        if backend.source_row(&owner)?.is_some() {
            return Ok(false);
        }
        let intended_exists = backend.target_row(&intended)?.is_some();
        engine::replace_source_absent_owner(
            &mut backend,
            (&owner, &conflict.owner.values),
            (&intended, &conflict.intended.values),
            intended_exists,
            Limits {
                page_rows: 1000,
                max_keys: usize::MAX,
            },
        )
        .map_err(|error| {
            format!(
                "coordinated unique-owner replacement: {}",
                describe_transition_error(error)
            )
        })?;
        self.verify_and_record_owner_replacement(conflict)?;
        Ok(true)
    }

    fn verify_and_record_owner_replacement(
        &mut self,
        conflict: &SyncUniqueOwnerConflict,
    ) -> Result<(), String> {
        verify_exact_row(
            self.query_exact_row(&conflict.owner.primary_key)?,
            None,
            "replaced unique owner",
        )?;
        verify_exact_row(
            self.query_unique_owner(&conflict.index, &conflict.intended)?,
            Some(&conflict.intended),
            "replacement unique owner",
        )?;
        self.pending_reconciliation_events
            .push(format_unique_owner_reconciliation_event(
                &self.table.name,
                conflict,
                &SyncUniqueOwnerAction::Delete,
            ));
        Ok(())
    }

    pub(super) fn repair_insert_prerequisites(
        &mut self,
        rows: &[DatabaseRow],
    ) -> Result<(), String> {
        let mut context = self
            .transitions
            .take()
            .ok_or("FK transitions not configured")?;
        let table = self.table.name.clone();
        let result = (|| {
            let mut backend = TransitionBackend {
                target: self,
                context: &mut context,
                visiting: BTreeSet::new(),
            };
            let mut repaired = 0;
            for row in rows {
                let key = RowKey {
                    table: table.clone(),
                    pk: row.primary_key.clone(),
                };
                backend.visiting.insert(key.clone());
                let result = backend.ensure_required_parents(&table, &row.values);
                backend.visiting.remove(&key);
                repaired += result?;
            }
            if repaired == 0 {
                return Err(format!("no missing FK prerequisites found for `{table}`"));
            }
            Ok(())
        })();
        self.transitions = Some(context);
        result
    }

    pub(super) fn repair_restricted_updates(
        &mut self,
        failure: SyncMutationFailure,
    ) -> Result<(), SyncMutationFailure> {
        if !matches!(failure.mysql_code, Some(1217 | 1451)) || self.transitions.is_none() {
            return Err(failure);
        }
        let mut context = self.transitions.take().expect("configured transitions");
        let root_table = self.table.name.clone();
        let rows = failure.retry_rows();
        let result = (|| {
            let mut backend = TransitionBackend {
                target: self,
                context: &mut context,
                visiting: BTreeSet::new(),
            };
            for desired in &rows {
                let root = RowKey {
                    table: root_table.clone(),
                    pk: desired.primary_key.clone(),
                };
                let old = backend
                    .target_row(&root)?
                    .ok_or_else(|| format!("FK transition root missing in `{root_table}`"))?;
                engine::transition(
                    &mut backend,
                    &root,
                    &desired.values,
                    &old,
                    Limits {
                        page_rows: 1000,
                        max_keys: usize::MAX,
                    },
                )
                .map_err(|error| {
                    format!(
                        "coordinated FK update for `{root_table}`: {}",
                        describe_transition_error(error)
                    )
                })?;
            }
            Ok(())
        })();
        self.transitions = Some(context);
        result.map_err(|message| SyncMutationFailure { message, ..failure })
    }
}

struct TransitionBackend<'a> {
    target: &'a mut MySqlSyncTargetSession,
    context: &'a mut TransitionContext,
    visiting: BTreeSet<RowKey>,
}

impl TransitionBackend<'_> {
    fn table(&self, name: &str) -> Result<SyncTable, String> {
        self.context
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| format!("FK transition table `{name}` outside locked scope"))
    }

    fn exact(&mut self, key: &RowKey, source: bool) -> Result<Option<Row>, String> {
        let table = self.table(&key.table)?;
        let statement = build_exact_primary_key_select_statement(&table, &key.pk)?;
        let conn = if source {
            &mut self.context.source
        } else {
            &mut self.target.conn
        };
        let rows = query_statement_rows_as_strings(conn, &statement, "FK transition exact read")?;
        Ok(decode_optional_exact_row(&table, rows, "FK transition")?.map(|row| row.values))
    }

    fn read_parent_reference(
        &mut self,
        reference: &ParentReference,
        source: bool,
    ) -> Result<Option<DatabaseRow>, String> {
        let table = self.table(&reference.table)?;
        let values = reference
            .values
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>();
        let statement = build_related_rows_select_statement(
            &table,
            &reference.columns,
            &values,
            &SyncChunkReadRequest {
                start_after: None,
                end_at: None,
                limit: if source { 2 } else { 1 },
            },
        )?;
        let conn = if source {
            &mut self.context.source
        } else {
            &mut self.target.conn
        };
        let rows = query_statement_rows_as_strings(conn, &statement, "FK prerequisite exact read")?;
        decode_optional_exact_row(&table, rows, "FK prerequisite")
    }

    fn read_missing_parents(
        &mut self,
        table: &str,
        desired: &Row,
    ) -> Result<Vec<ParentReference>, String> {
        let references = self
            .context
            .foreign_keys
            .iter()
            .filter(|key| key.table == table)
            .map(|key| parent_reference_from_row(key, desired))
            .collect::<Result<Vec<_>, _>>()?;
        let mut missing = Vec::new();
        for reference in references.into_iter().flatten() {
            if self.read_parent_reference(&reference, false)?.is_none() {
                missing.push(reference);
            }
        }
        Ok(missing)
    }

    fn ensure_required_parents(&mut self, table: &str, desired: &Row) -> Result<usize, String> {
        let missing = self.read_missing_parents(table, desired)?;
        for reference in &missing {
            self.repair_required_parent(reference)?;
        }
        if !self.read_missing_parents(table, desired)?.is_empty() {
            return Err(format!("FK prerequisites remain missing for `{table}`"));
        }
        Ok(missing.len())
    }

    fn repair_required_parent(&mut self, reference: &ParentReference) -> Result<(), String> {
        let source = self
            .read_parent_reference(reference, true)?
            .ok_or_else(|| format!("source FK parent missing in `{}`", reference.table))?;
        let key = RowKey {
            table: reference.table.clone(),
            pk: source.primary_key.clone(),
        };
        if !self.visiting.insert(key.clone()) {
            return Err(format!("FK prerequisite cycle in `{}`", key.table));
        }
        let result = (|| {
            self.ensure_required_parents(&key.table, &source.values)?;
            match self.target_row(&key)? {
                None => self.write(&key, &source.values, true),
                Some(old) if old != source.values => {
                    self.update_required_parent(&key, &source, &old)
                }
                Some(_) => Ok(()),
            }
        })();
        self.visiting.remove(&key);
        result
    }

    fn update_required_parent(
        &mut self,
        key: &RowKey,
        source: &DatabaseRow,
        old: &Row,
    ) -> Result<(), String> {
        let table = self.table(&key.table)?;
        let statement = build_strict_update_rows_statement(&table, std::slice::from_ref(source))?;
        match self
            .target
            .conn
            .exec_drop(&statement.sql, Params::Positional(statement.params))
        {
            Ok(()) => self.verify_written(key, &source.values),
            Err(error) if matches!(mysql_error_code(&error), Some(1217 | 1451)) => {
                engine::transition(
                    self,
                    key,
                    &source.values,
                    old,
                    Limits {
                        page_rows: 1000,
                        max_keys: usize::MAX,
                    },
                )
                .map_err(|error| {
                    format!(
                        "coordinated FK prerequisite update for `{}`: {}",
                        key.table,
                        describe_transition_error(error)
                    )
                })
            }
            Err(error) => Err(format!(
                "FK prerequisite update for `{}` failed: {error}",
                key.table
            )),
        }
    }

    fn write(&mut self, key: &RowKey, desired: &Row, insert: bool) -> Result<(), String> {
        let table = self.table(&key.table)?;
        let row = DatabaseRow {
            primary_key: key.pk.clone(),
            values: desired.clone(),
        };
        let statement = if insert {
            build_strict_insert_statement(&table, std::slice::from_ref(&row))?
        } else {
            build_strict_update_rows_statement(&table, std::slice::from_ref(&row))?
        };
        self.target.execute_statement(statement)?;
        self.verify_written(key, desired)
    }

    fn verify_written(&mut self, key: &RowKey, desired: &Row) -> Result<(), String> {
        if self.exact(key, false)?.as_ref() != Some(desired) {
            return Err(format!(
                "FK transition write readback differs in `{}`",
                key.table
            ));
        }
        Ok(())
    }
}

fn parent_reference_from_row(
    key: &ForeignKeyInventory,
    desired: &Row,
) -> Result<Option<ParentReference>, String> {
    let values = key
        .columns
        .iter()
        .map(|column| {
            desired
                .get(column)
                .cloned()
                .ok_or_else(|| format!("missing FK child column `{column}`"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.iter().any(Option::is_none) {
        return Ok(None);
    }
    Ok(Some(ParentReference {
        table: key.referenced_table.clone(),
        columns: key.referenced_columns.clone(),
        values: values.into_iter().map(Option::unwrap).collect(),
    }))
}

fn build_dependent_page_statement(
    table: &SyncTable,
    relation: &Relation,
    old: &Row,
    after: Option<&Key>,
    limit: usize,
) -> Result<Option<SqlStatement>, String> {
    let values = relation
        .parent_columns
        .iter()
        .map(|column| {
            old.get(column)
                .cloned()
                .ok_or_else(|| format!("missing FK parent column `{column}`"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.iter().any(Option::is_none) {
        return Ok(None);
    }
    let request = SyncChunkReadRequest {
        start_after: after.cloned(),
        end_at: None,
        limit,
    };
    build_related_rows_select_statement(table, &relation.child_columns, &values, &request).map(Some)
}

fn read_dependent_keys(
    conn: &mut Conn,
    table: &SyncTable,
    statement: SqlStatement,
    byte_limit: usize,
) -> Result<Vec<Key>, String> {
    let mut result = conn
        .exec_iter(&statement.sql, Params::Positional(statement.params))
        .map_err(|error| format!("read FK dependents in `{}`: {error}", table.name))?;
    let mut keys = Vec::new();
    let mut bytes = 0;
    let mut retain = true;
    if let Some(rows) = result.iter() {
        for row in rows {
            let row = row.map_err(|error| format!("read FK dependent row: {error}"))?;
            if !retain {
                continue;
            }
            let decoded = decode_sync_row(table, mysql_row_to_strings(row))?;
            let size = decoded.primary_key.iter().map(String::len).sum::<usize>();
            if !keys.is_empty() && bytes + size > byte_limit {
                retain = false;
                continue;
            }
            bytes += size;
            keys.push(decoded.primary_key);
        }
    }
    Ok(keys)
}

impl Backend for TransitionBackend<'_> {
    type Error = String;

    fn incoming(&mut self, table: &str) -> Result<Vec<Relation>, String> {
        Ok(self
            .context
            .foreign_keys
            .iter()
            .filter(|key| key.referenced_table == table)
            .map(|key| Relation {
                child_table: key.table.clone(),
                child_columns: key.columns.clone(),
                parent_columns: key.referenced_columns.clone(),
            })
            .collect())
    }

    fn dependent_page(
        &mut self,
        relation: &Relation,
        old: &Row,
        after: Option<&Key>,
        limit: usize,
        byte_limit: usize,
    ) -> Result<Vec<Key>, String> {
        let table = self.table(&relation.child_table)?;
        let Some(statement) = build_dependent_page_statement(&table, relation, old, after, limit)?
        else {
            return Ok(Vec::new());
        };
        read_dependent_keys(&mut self.target.conn, &table, statement, byte_limit)
    }

    fn target_row(&mut self, key: &RowKey) -> Result<Option<Row>, String> {
        self.exact(key, false)
    }
    fn source_row(&mut self, key: &RowKey) -> Result<Option<Row>, String> {
        self.exact(key, true)
    }

    fn ensure_parents(
        &mut self,
        table: &str,
        desired: &Row,
    ) -> Result<Vec<ParentReference>, String> {
        self.ensure_required_parents(table, desired)?;
        self.read_missing_parents(table, desired)
    }

    fn delete(&mut self, key: &RowKey) -> Result<(), String> {
        let table = self.table(&key.table)?;
        for statement in build_strict_delete_batches(&table, std::slice::from_ref(&key.pk))? {
            self.target.execute_statement(statement)?;
        }
        if self.exact(key, false)?.is_some() {
            return Err(format!("FK transition delete remains in `{}`", key.table));
        }
        Ok(())
    }
    fn update(&mut self, key: &RowKey, desired: &Row) -> Result<(), String> {
        self.write(key, desired, false)
    }
    fn insert(&mut self, key: &RowKey, desired: &Row) -> Result<(), String> {
        self.write(key, desired, true)
    }
}
