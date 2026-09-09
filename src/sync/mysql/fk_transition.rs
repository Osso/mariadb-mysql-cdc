use super::*;
use crate::inventory::{ForeignKeyInventory, SchemaInventory};
use crate::sync::component_locks::selected_fk_component;
use crate::sync::config::sync_table_from_inventory;
use crate::sync::fk_transition::{
    self as engine, Backend, Key, Limits, ParentReference, Relation, Row, RowKey,
};
use crate::sync::sql::build_related_rows_select_statement;

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
                        max_keys: 1_000_000,
                    },
                )
                .map_err(|error| format!("coordinated FK update for `{root_table}`: {error:?}"))?;
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
        if self.exact(key, false)?.as_ref() != Some(desired) {
            return Err(format!(
                "FK transition write readback differs in `{}`",
                key.table
            ));
        }
        Ok(())
    }
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
            return Ok(Vec::new());
        }
        let request = SyncChunkReadRequest {
            start_after: after.cloned(),
            end_at: None,
            limit,
        };
        let statement = build_related_rows_select_statement(
            &table,
            &relation.child_columns,
            &values,
            &request,
        )?;
        let mut result = self
            .target
            .conn
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
                let decoded = decode_sync_row(&table, mysql_row_to_strings(row))?;
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

    fn target_row(&mut self, key: &RowKey) -> Result<Option<Row>, String> {
        self.exact(key, false)
    }
    fn source_row(&mut self, key: &RowKey) -> Result<Option<Row>, String> {
        self.exact(key, true)
    }

    fn missing_parents(
        &mut self,
        table: &str,
        desired: &Row,
    ) -> Result<Vec<ParentReference>, String> {
        let foreign_keys = self
            .context
            .foreign_keys
            .iter()
            .filter(|key| key.table == table)
            .cloned()
            .collect::<Vec<_>>();
        let mut missing = Vec::new();
        for key in foreign_keys {
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
                continue;
            }
            let parent = self.table(&key.referenced_table)?;
            let statement = build_related_rows_select_statement(
                &parent,
                &key.referenced_columns,
                &values,
                &SyncChunkReadRequest {
                    start_after: None,
                    end_at: None,
                    limit: 1,
                },
            )?;
            if query_statement_rows_as_strings(
                &mut self.target.conn,
                &statement,
                "FK parent existence",
            )?
            .is_empty()
            {
                missing.push(ParentReference {
                    table: key.referenced_table,
                    columns: key.referenced_columns,
                    values: values.into_iter().map(Option::unwrap).collect(),
                });
            }
        }
        Ok(missing)
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
