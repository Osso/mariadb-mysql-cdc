use crate::conflict_repair::CanonicalForeignKey;
use crate::inventory::{
    ColumnInventory, ForeignKeyInventory, IndexColumnInventory, IndexInventory, InventoryConfig,
    InventoryEndpointRole, MariaDbInventoryReader, SchemaInventory, TableInventory,
    build_canonical_foreign_key_inventory, build_inventory,
};
use crate::live::ddl_semantics::{DDL_TRANSFORMATION_VERSION, translate_modeled_ddl};
use crate::target::{SqlStatement, TargetExecutor};
use mysql::prelude::Queryable;
use mysql::{Conn, Params, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SchemaPhase {
    Create,
    Columns,
    Keys,
    Constraints,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TableSchemaStatus {
    Planned,
    Failed,
    Skipped,
    Converged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OverallSchemaStatus {
    Converged,
    Partial,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct PlannedSchemaStatement {
    pub(crate) phase: SchemaPhase,
    pub(crate) sql: String,
    pub(crate) objects: Vec<String>,
    pub(crate) prerequisites: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PreflightStatus {
    Passed,
    Blocked,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct CoercionPreflightEvent {
    pub(crate) column: String,
    pub(crate) predicate: Option<String>,
    pub(crate) count: u64,
    pub(crate) sample_primary_keys: Vec<Vec<String>>,
    pub(crate) status: PreflightStatus,
    pub(crate) error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct TableSchemaPlan {
    pub(crate) table: String,
    pub(crate) source_fingerprint: String,
    pub(crate) target_fingerprint: String,
    pub(crate) dependencies: Vec<String>,
    pub(crate) status: TableSchemaStatus,
    pub(crate) blockers: Vec<String>,
    pub(crate) preflights: Vec<CoercionPreflightEvent>,
    pub(crate) statements: Vec<PlannedSchemaStatement>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SchemaConvergencePlan {
    pub(crate) source_fingerprint: String,
    pub(crate) target_fingerprint: String,
    pub(crate) tables: Vec<TableSchemaPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct StatementExecution {
    pub(crate) phase: SchemaPhase,
    pub(crate) sql: String,
    pub(crate) status: String,
    pub(crate) error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct TableSchemaReport {
    pub(crate) table: String,
    pub(crate) source_fingerprint: String,
    pub(crate) target_fingerprint: String,
    pub(crate) status: TableSchemaStatus,
    pub(crate) blockers: Vec<String>,
    pub(crate) preflights: Vec<CoercionPreflightEvent>,
    pub(crate) skipped_dependencies: Vec<String>,
    pub(crate) planned_statements: Vec<PlannedSchemaStatement>,
    pub(crate) executions: Vec<StatementExecution>,
    pub(crate) final_differences: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SchemaConvergenceReport {
    pub(crate) transformation_version: String,
    pub(crate) source_fingerprint: String,
    pub(crate) target_fingerprint: String,
    pub(crate) overall_status: OverallSchemaStatus,
    pub(crate) error: Option<String>,
    pub(crate) tables: Vec<TableSchemaReport>,
}

/// `clause` keeps the endpoint's own text so an addition renders the source expression, while
/// identity ignores the parentheses and charset introducers MySQL adds when it re-renders one.
#[derive(Clone, Debug, Serialize)]
struct CheckConstraint {
    table: String,
    name: String,
    clause: String,
}

impl CheckConstraint {
    fn identity(&self) -> (&str, &str, String) {
        (
            &self.table,
            &self.name,
            canonical_sql_expression(&self.clause),
        )
    }
}

impl PartialEq for CheckConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for CheckConstraint {}

impl PartialOrd for CheckConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CheckConstraint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.identity().cmp(&other.identity())
    }
}

pub(crate) trait SchemaStatementExecutor {
    fn execute(&mut self, table: &str, sql: &str) -> Result<(), String>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CoercionBlockers {
    pub(crate) predicate: Option<String>,
    pub(crate) count: u64,
    pub(crate) sample_primary_keys: Vec<Vec<String>>,
    pub(crate) sample_error: Option<String>,
}

pub(crate) trait SchemaCoercionPreflight {
    fn inspect(
        &self,
        table: &TableInventory,
        source: &ColumnInventory,
        target: &ColumnInventory,
    ) -> Result<CoercionBlockers, String>;
}

/// How the selected-table set is determined before the source inventory is read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SchemaSelection {
    /// Exactly the named tables, from `--table` and `--catalog`.
    Named(Vec<String>),
    /// Every source base table, resolved from the source inventory.
    AllSourceTables,
}

pub(crate) fn schema_selection(
    repeated: &[String],
    catalog: Option<&[u8]>,
    all_tables: bool,
) -> Result<SchemaSelection, String> {
    if all_tables {
        if !repeated.is_empty() || catalog.is_some() {
            return Err(
                "sync-schema --all-tables cannot be combined with --table or --catalog".to_string(),
            );
        }
        return Ok(SchemaSelection::AllSourceTables);
    }
    Ok(SchemaSelection::Named(selected_tables(repeated, catalog)?))
}

pub(crate) fn resolve_schema_selection(
    selection: &SchemaSelection,
    source: &SchemaInventory,
) -> Result<Vec<String>, String> {
    match selection {
        SchemaSelection::Named(tables) => Ok(tables.clone()),
        SchemaSelection::AllSourceTables => all_source_tables(source),
    }
}

/// The schema inventory holds base tables only, so every inventoried table is selectable.
fn all_source_tables(source: &SchemaInventory) -> Result<Vec<String>, String> {
    let selected = source
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    if selected.is_empty() {
        return Err("sync-schema --all-tables found no source base tables".to_string());
    }
    if let Some(invalid) = selected.iter().find(|table| !valid_identifier(table)) {
        return Err(format!(
            "sync-schema --all-tables found invalid source table identifier `{invalid}`"
        ));
    }
    Ok(selected.into_iter().collect())
}

pub(crate) fn selected_tables(
    repeated: &[String],
    catalog: Option<&[u8]>,
) -> Result<Vec<String>, String> {
    #[derive(Deserialize)]
    struct Catalog {
        tables: Vec<CatalogTable>,
    }
    #[derive(Deserialize)]
    struct CatalogTable {
        name: String,
    }

    let mut selected = repeated.iter().cloned().collect::<BTreeSet<_>>();
    if let Some(bytes) = catalog {
        let catalog: Catalog = serde_json::from_slice(bytes)
            .map_err(|error| format!("invalid sync-schema catalog: {error}"))?;
        selected.extend(catalog.tables.into_iter().map(|table| table.name));
    }
    if selected.is_empty() {
        return Err(
            "sync-schema requires at least one --table, --catalog, or --all-tables true"
                .to_string(),
        );
    }
    if selected.iter().any(|table| !valid_identifier(table)) {
        return Err("sync-schema selection contains invalid table identifier".to_string());
    }
    Ok(selected.into_iter().collect())
}

/// The schema the target must hold: the source schema plus the unique parent indexes MySQL
/// requires but MariaDB does not.
///
/// MariaDB accepts a foreign key whose referenced columns are only covered by a non-unique
/// index. MySQL requires those columns to be the leftmost prefix of a UNIQUE or PRIMARY key
/// and rejects the constraint otherwise, so the translated target carries one synthesized
/// unique index per such parent identity.
pub(crate) fn expected_target_inventory(source: &SchemaInventory) -> SchemaInventory {
    let mut expected = source.clone();
    expected.indexes.extend(synthesized_parent_indexes(source));
    expected
}

fn synthesized_parent_indexes(source: &SchemaInventory) -> Vec<IndexInventory> {
    let mut synthesized = BTreeMap::new();
    for foreign_key in &source.foreign_keys {
        if foreign_key.referenced_schema != source.schema
            || standard_parent_key_exists(
                source,
                &foreign_key.referenced_table,
                &foreign_key.referenced_columns,
            )
        {
            continue;
        }
        let name = synthesized_parent_index_name(
            &foreign_key.referenced_table,
            &foreign_key.referenced_columns,
        );
        synthesized
            .entry((foreign_key.referenced_table.clone(), name.clone()))
            .or_insert_with(|| IndexInventory {
                table: foreign_key.referenced_table.clone(),
                name,
                unique: true,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: None,
                columns: foreign_key
                    .referenced_columns
                    .iter()
                    .enumerate()
                    // Ascending, as the inventory reader describes an ascending B-tree column.
                    .map(|(position, column)| IndexColumnInventory {
                        name: column.clone(),
                        sequence: position as u32 + 1,
                        prefix_length: None,
                        collation: Some("A".to_string()),
                        order: "ASC".to_string(),
                    })
                    .collect(),
            });
    }
    synthesized.into_values().collect()
}

fn synthesized_parent_index_name(table: &str, columns: &[String]) -> String {
    format!("uq_cdc_{table}_{}", columns.join("_"))
}

/// True when `columns` are the leftmost prefix of the table's primary key or of a unique index.
fn standard_parent_key_exists(
    source: &SchemaInventory,
    table_name: &str,
    columns: &[String],
) -> bool {
    let primary_key_covers = source
        .tables
        .iter()
        .find(|table| table.name == table_name)
        .is_some_and(|table| table.primary_key.starts_with(columns));
    primary_key_covers
        || indexes_for(source, table_name).into_iter().any(|index| {
            index.unique
                && index.columns.len() >= columns.len()
                && index
                    .columns
                    .iter()
                    .map(|column| &column.name)
                    .zip(columns)
                    .all(|(actual, required)| actual == required)
        })
}

pub(crate) fn plan_schema_convergence(
    source: &SchemaInventory,
    target: &SchemaInventory,
    selected: &[String],
    preflight: &dyn SchemaCoercionPreflight,
) -> Result<SchemaConvergencePlan, String> {
    let source_fingerprint = fingerprint(source)?;
    let source = &expected_target_inventory(source);
    let source_tables = table_map(source);
    let target_tables = table_map(target);
    let mut tables = Vec::new();
    let (ordered, cyclic) = dependency_order(source, selected);
    for name in &ordered {
        let target_table = target_tables.get(name.as_str()).copied();
        let Some(source_table) = source_tables.get(name.as_str()).copied() else {
            tables.push(failed_table_plan(
                name,
                None,
                target_table,
                format!("selected source table `{name}` is missing"),
            )?);
            continue;
        };
        let table = match plan_table(source, target, source_table, target_table, preflight) {
            Ok(table) => table,
            Err(error) => failed_table_plan(name, Some(source_table), target_table, error)?,
        };
        tables.push(table);
    }
    for name in cyclic {
        tables.push(failed_table_plan(
            &name,
            source_tables.get(name.as_str()).copied(),
            target_tables.get(name.as_str()).copied(),
            format!("selected schema dependency cycle includes `{name}`"),
        )?);
    }
    Ok(SchemaConvergencePlan {
        source_fingerprint,
        target_fingerprint: fingerprint(target)?,
        tables,
    })
}

fn failed_table_plan(
    table: &str,
    source: Option<&TableInventory>,
    target: Option<&TableInventory>,
    error: String,
) -> Result<TableSchemaPlan, String> {
    Ok(TableSchemaPlan {
        table: table.to_string(),
        source_fingerprint: source
            .map(expected_target_table_fingerprint)
            .transpose()?
            .unwrap_or_default(),
        target_fingerprint: target
            .map(observed_target_table_fingerprint)
            .transpose()?
            .unwrap_or_default(),
        dependencies: Vec::new(),
        status: TableSchemaStatus::Failed,
        blockers: vec![error],
        preflights: Vec::new(),
        statements: Vec::new(),
    })
}

fn preflight_event(column: &str, blockers: CoercionBlockers) -> CoercionPreflightEvent {
    let status = if blockers.count > 0 {
        PreflightStatus::Blocked
    } else if blockers.sample_error.is_some() {
        PreflightStatus::Error
    } else {
        PreflightStatus::Passed
    };
    CoercionPreflightEvent {
        column: column.to_string(),
        predicate: blockers.predicate,
        count: blockers.count,
        sample_primary_keys: blockers.sample_primary_keys,
        status,
        error: blockers.sample_error,
    }
}

fn plan_table(
    source: &SchemaInventory,
    target: &SchemaInventory,
    source_table: &TableInventory,
    target_table: Option<&TableInventory>,
    preflight: &dyn SchemaCoercionPreflight,
) -> Result<TableSchemaPlan, String> {
    let dependencies = source
        .foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.table == source_table.name)
        .map(|foreign_key| foreign_key.referenced_table.clone())
        .filter(|parent| parent != &source_table.name)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut plan = TableSchemaPlan {
        table: source_table.name.clone(),
        source_fingerprint: expected_target_table_fingerprint(source_table)?,
        target_fingerprint: target_table
            .map(observed_target_table_fingerprint)
            .transpose()?
            .unwrap_or_default(),
        dependencies,
        status: TableSchemaStatus::Planned,
        blockers: Vec::new(),
        preflights: Vec::new(),
        statements: Vec::new(),
    };
    let Some(target_table) = target_table else {
        let table_object = format!("table:{}", source_table.name);
        plan.statements.push(translate_statement(
            SchemaPhase::Create,
            render_create_table(source, source_table, false),
            vec![table_object.clone()],
            &[],
        )?);
        plan.statements
            .extend(
                render_foreign_keys(source, source_table)?
                    .into_iter()
                    .map(|mut statement| {
                        statement.prerequisites.push(table_object.clone());
                        statement.prerequisites.sort();
                        statement.prerequisites.dedup();
                        statement
                    }),
            );
        return Ok(plan);
    };

    if source_table.engine != target_table.engine
        || source_table.collation != target_table.collation
    {
        plan.statements
            .push(modeled_table_options_statement(source_table)?);
    }

    let source_columns = column_map(source_table);
    let target_columns = column_map(target_table);
    for (name, source_column) in &source_columns {
        let Some(target_column) = target_columns.get(name) else {
            continue;
        };
        if !column_change_requires_data_preflight(source_column, target_column) {
            continue;
        }
        let event = match preflight.inspect(target_table, source_column, target_column) {
            Ok(blockers) => preflight_event(name, blockers),
            Err(error) => CoercionPreflightEvent {
                column: name.to_string(),
                predicate: None,
                count: 0,
                sample_primary_keys: Vec::new(),
                status: PreflightStatus::Error,
                error: Some(error),
            },
        };
        if event.status != PreflightStatus::Passed {
            plan.blockers
                .push(format!("column {name} coercion preflight did not pass"));
        }
        plan.preflights.push(event);
    }

    let target_column_names = target_table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let key_actions = plan_keys(source, target, source_table, target_table)?;
    let constraint_actions = plan_foreign_keys(source, target, source_table)?;
    plan.statements.extend(
        constraint_actions
            .iter()
            .filter(|statement| is_drop_statement(statement))
            .cloned(),
    );
    plan.statements.extend(
        key_actions
            .iter()
            .filter(|statement| is_drop_statement(statement))
            .cloned(),
    );
    for column in target_table.columns.iter().rev() {
        if !source_columns.contains_key(column.name.as_str()) {
            let prerequisites =
                target_column_drop_prerequisites(target, target_table, &column.name);
            let statement = translate_statement(
                SchemaPhase::Columns,
                format!(
                    "ALTER TABLE `{}` DROP COLUMN IF EXISTS `{}`",
                    source_table.name, column.name
                ),
                vec![format!("column:{}.{}", source_table.name, column.name)],
                &target_column_names,
            )?;
            plan.statements
                .push(with_prerequisites(statement, prerequisites));
        }
    }
    let mut planned_columns = BTreeSet::new();
    for (index, source_column) in source_table.columns.iter().enumerate() {
        if plan.preflights.iter().any(|event| {
            event.column == source_column.name && event.status != PreflightStatus::Passed
        }) {
            continue;
        }
        let clause = render_column(source_column);
        let position = column_position(source_table, index);
        let sql = match target_columns.get(source_column.name.as_str()) {
            None => format!(
                "ALTER TABLE `{}` ADD COLUMN {clause}{position}",
                source_table.name
            ),
            Some(target_column) if !columns_equal(source_column, target_column) => format!(
                "ALTER TABLE `{}` MODIFY COLUMN {clause}{position}",
                source_table.name
            ),
            Some(_) => continue,
        };
        let object = format!("column:{}.{}", source_table.name, source_column.name);
        let prerequisites = index
            .checked_sub(1)
            .map(|previous| {
                format!(
                    "column:{}.{}",
                    source_table.name, source_table.columns[previous].name
                )
            })
            .filter(|previous| planned_columns.contains(previous))
            .into_iter()
            .collect();
        let statement = translate_statement(
            SchemaPhase::Columns,
            sql,
            vec![object.clone()],
            &target_column_names,
        )?;
        plan.statements
            .push(with_prerequisites(statement, prerequisites));
        planned_columns.insert(object);
    }
    plan.statements.extend(
        key_actions
            .into_iter()
            .filter(|statement| !is_drop_statement(statement)),
    );
    plan.statements.extend(
        constraint_actions
            .into_iter()
            .filter(|statement| !is_drop_statement(statement)),
    );
    Ok(plan)
}

fn plan_keys(
    source: &SchemaInventory,
    target: &SchemaInventory,
    source_table: &TableInventory,
    target_table: &TableInventory,
) -> Result<Vec<PlannedSchemaStatement>, String> {
    let mut statements = Vec::new();
    let source_indexes = indexes_for(source, &source_table.name);
    let target_indexes = indexes_for(target, &target_table.name);
    for target_index in &target_indexes {
        if !source_indexes
            .iter()
            .any(|source_index| indexes_equal(source_index, target_index))
        {
            statements.push(translate_statement(
                SchemaPhase::Keys,
                format!(
                    "DROP INDEX `{}` ON `{}`",
                    target_index.name, target_table.name
                ),
                vec![format!("index:{}.{}", target_table.name, target_index.name)],
                &[],
            )?);
        }
    }
    if source_table.primary_key != target_table.primary_key {
        if !target_table.primary_key.is_empty() {
            statements.push(translate_statement(
                SchemaPhase::Keys,
                format!("ALTER TABLE `{}` DROP PRIMARY KEY", source_table.name),
                vec![format!("primary_key:{}", source_table.name)],
                &[],
            )?);
        }
        if !source_table.primary_key.is_empty() {
            let statement = translate_statement(
                SchemaPhase::Keys,
                format!(
                    "ALTER TABLE `{}` ADD PRIMARY KEY ({})",
                    source_table.name,
                    quoted_list(&source_table.primary_key)
                ),
                vec![format!("primary_key:{}", source_table.name)],
                &[],
            )?;
            statements.push(with_prerequisites(
                statement,
                column_prerequisites(&source_table.name, &source_table.primary_key),
            ));
        }
    }
    for source_index in source_indexes {
        if !target_indexes
            .iter()
            .any(|target_index| indexes_equal(source_index, target_index))
        {
            let statement = translate_statement(
                SchemaPhase::Keys,
                render_create_index(source_index),
                vec![format!(
                    "index:{}.{}",
                    source_index.table, source_index.name
                )],
                &[],
            )?;
            let columns = source_index
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let mut prerequisites = column_prerequisites(&source_index.table, &columns);
            if target_indexes.iter().any(|target_index| {
                target_index.name == source_index.name && !indexes_equal(source_index, target_index)
            }) {
                prerequisites.push(format!(
                    "index:{}.{}",
                    source_index.table, source_index.name
                ));
            }
            statements.push(with_prerequisites(statement, prerequisites));
        }
    }
    Ok(statements)
}

fn render_foreign_keys(
    source: &SchemaInventory,
    table: &TableInventory,
) -> Result<Vec<PlannedSchemaStatement>, String> {
    source
        .foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.table == table.name)
        .map(|foreign_key| {
            let statement = translate_statement(
                SchemaPhase::Constraints,
                render_foreign_key(foreign_key, &source.schema),
                vec![format!("foreign_key:{}.{}", table.name, foreign_key.name)],
                &[],
            )?;
            Ok(with_prerequisites(
                statement,
                foreign_key_prerequisites(source, foreign_key),
            ))
        })
        .collect()
}

fn plan_foreign_keys(
    source: &SchemaInventory,
    target: &SchemaInventory,
    table: &TableInventory,
) -> Result<Vec<PlannedSchemaStatement>, String> {
    let source_keys = source
        .foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.table == table.name)
        .collect::<Vec<_>>();
    let target_keys = target
        .foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.table == table.name)
        .collect::<Vec<_>>();
    let mut statements = Vec::new();
    for target_key in &target_keys {
        if !source_keys.iter().any(|source_key| {
            foreign_keys_equal(source_key, &source.schema, target_key, &target.schema)
        }) {
            statements.push(translate_statement(
                SchemaPhase::Constraints,
                format!(
                    "ALTER TABLE `{}` DROP FOREIGN KEY `{}`",
                    table.name, target_key.name
                ),
                vec![format!("foreign_key:{}.{}", table.name, target_key.name)],
                &[],
            )?);
        }
    }
    for source_key in source_keys {
        if !target_keys.iter().any(|target_key| {
            foreign_keys_equal(source_key, &source.schema, target_key, &target.schema)
        }) {
            let statement = translate_statement(
                SchemaPhase::Constraints,
                render_foreign_key(source_key, &source.schema),
                vec![format!("foreign_key:{}.{}", table.name, source_key.name)],
                &[],
            )?;
            statements.push(with_prerequisites(
                statement,
                foreign_key_prerequisites(source, source_key),
            ));
        }
    }
    Ok(statements)
}

fn foreign_key_prerequisites(
    source: &SchemaInventory,
    foreign_key: &ForeignKeyInventory,
) -> Vec<String> {
    let mut prerequisites = column_prerequisites(&foreign_key.table, &foreign_key.columns);
    prerequisites.extend(index_prerequisites(
        source,
        &foreign_key.table,
        &foreign_key.columns,
    ));
    prerequisites.push(format!("table:{}", foreign_key.referenced_table));
    prerequisites.extend(column_prerequisites(
        &foreign_key.referenced_table,
        &foreign_key.referenced_columns,
    ));
    prerequisites.extend(index_prerequisites(
        source,
        &foreign_key.referenced_table,
        &foreign_key.referenced_columns,
    ));
    prerequisites.sort();
    prerequisites.dedup();
    prerequisites
}

fn index_prerequisites(
    source: &SchemaInventory,
    table_name: &str,
    required_columns: &[String],
) -> Vec<String> {
    let mut prerequisites = Vec::new();
    if source
        .tables
        .iter()
        .find(|table| table.name == table_name)
        .is_some_and(|table| table.primary_key == required_columns)
    {
        prerequisites.push(format!("primary_key:{table_name}"));
    }
    prerequisites.extend(
        indexes_for(source, table_name)
            .into_iter()
            .filter(|index| {
                index.columns.len() >= required_columns.len()
                    && index
                        .columns
                        .iter()
                        .map(|column| &column.name)
                        .zip(required_columns)
                        .all(|(actual, required)| actual == required)
            })
            .map(|index| format!("index:{}.{}", index.table, index.name)),
    );
    prerequisites
}

pub(crate) fn execute_schema_plan(
    plan: SchemaConvergencePlan,
    executor: &mut dyn SchemaStatementExecutor,
    final_differences: &dyn Fn(&str) -> Vec<String>,
) -> SchemaConvergenceReport {
    let mut table_status = BTreeMap::new();
    let mut reports = Vec::new();
    let total = plan.tables.len();
    for (position, table) in plan.tables.into_iter().enumerate() {
        let report = execute_table_plan(table, &table_status, executor, final_differences);
        log_table_progress(position + 1, total, &report);
        table_status.insert(report.table.clone(), report.status);
        reports.push(report);
    }
    let overall_status = overall_schema_status(&reports);
    SchemaConvergenceReport {
        transformation_version: DDL_TRANSFORMATION_VERSION.to_string(),
        source_fingerprint: plan.source_fingerprint,
        target_fingerprint: plan.target_fingerprint,
        overall_status,
        error: (overall_status != OverallSchemaStatus::Converged)
            .then(|| "one or more selected tables remain divergent".to_string()),
        tables: reports,
    }
}

/// A table-by-table trace on stderr, because the JSON report only appears once the whole run
/// finishes and a long run is otherwise indistinguishable from a hang.
fn log_table_progress(position: usize, total: usize, report: &TableSchemaReport) {
    let executed = report
        .executions
        .iter()
        .filter(|execution| execution.status == "executed")
        .count();
    let failed = report
        .executions
        .iter()
        .filter(|execution| execution.status == "failed")
        .count();
    let skipped = report
        .executions
        .iter()
        .filter(|execution| execution.status == "skipped")
        .count();
    eprintln!(
        "cdc_sync_schema_table table={} progress={position}/{total} status={} executed={executed} failed={failed} skipped={skipped} differences={} blockers={}",
        report.table,
        serde_json::to_value(report.status)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_else(|| "unknown".to_string()),
        report.final_differences.len(),
        report.blockers.len(),
    );
}

fn execute_table_plan(
    table: TableSchemaPlan,
    table_status: &BTreeMap<String, TableSchemaStatus>,
    executor: &mut dyn SchemaStatementExecutor,
    final_differences: &dyn Fn(&str) -> Vec<String>,
) -> TableSchemaReport {
    let failed_dependencies = failed_dependencies(&table, table_status);
    let mut report = table_report(&table, failed_dependencies.clone());
    let may_execute = report.status != TableSchemaStatus::Failed && failed_dependencies.is_empty();
    if !failed_dependencies.is_empty() {
        report.status = TableSchemaStatus::Skipped;
    }
    if may_execute {
        execute_table_statements(&table, executor, &mut report);
    }
    report.final_differences = final_differences(&table.table);
    if !failed_dependencies.is_empty() && report.final_differences.is_empty() {
        report
            .final_differences
            .push("dependency prerequisite failed".to_string());
    }
    if report.status == TableSchemaStatus::Planned {
        report.status = if report.final_differences.is_empty() {
            TableSchemaStatus::Converged
        } else {
            TableSchemaStatus::Failed
        };
    }
    report
}

fn failed_dependencies(
    table: &TableSchemaPlan,
    table_status: &BTreeMap<String, TableSchemaStatus>,
) -> Vec<String> {
    table
        .dependencies
        .iter()
        .filter(|dependency| {
            table_status
                .get(dependency.as_str())
                .is_some_and(|status| *status != TableSchemaStatus::Converged)
        })
        .cloned()
        .collect()
}

fn table_report(table: &TableSchemaPlan, skipped_dependencies: Vec<String>) -> TableSchemaReport {
    TableSchemaReport {
        table: table.table.clone(),
        source_fingerprint: table.source_fingerprint.clone(),
        target_fingerprint: table.target_fingerprint.clone(),
        status: table.status,
        blockers: table.blockers.clone(),
        preflights: table.preflights.clone(),
        skipped_dependencies,
        planned_statements: table.statements.clone(),
        executions: Vec::new(),
        final_differences: Vec::new(),
    }
}

fn execute_table_statements(
    table: &TableSchemaPlan,
    executor: &mut dyn SchemaStatementExecutor,
    report: &mut TableSchemaReport,
) {
    let mut failed_objects = table
        .preflights
        .iter()
        .filter(|event| event.status != PreflightStatus::Passed)
        .map(|event| format!("column:{}.{}", table.table, event.column))
        .collect::<BTreeSet<_>>();
    for statement in &table.statements {
        let failed_prerequisites = failed_statement_prerequisites(statement, &failed_objects);
        let execution = if failed_prerequisites.is_empty() {
            execute_planned_statement(executor, &table.table, statement)
        } else {
            skipped_statement_execution(statement, &failed_prerequisites)
        };
        let failed = execution.status != "executed";
        report.executions.push(execution);
        if failed {
            failed_objects.extend(statement.objects.iter().cloned());
            report.status = TableSchemaStatus::Failed;
        }
    }
}

fn failed_statement_prerequisites(
    statement: &PlannedSchemaStatement,
    failed_objects: &BTreeSet<String>,
) -> Vec<String> {
    statement
        .prerequisites
        .iter()
        .filter(|prerequisite| failed_objects.contains(*prerequisite))
        .cloned()
        .collect()
}

fn execute_planned_statement(
    executor: &mut dyn SchemaStatementExecutor,
    table: &str,
    statement: &PlannedSchemaStatement,
) -> StatementExecution {
    let error = executor.execute(table, &statement.sql).err();
    StatementExecution {
        phase: statement.phase,
        sql: statement.sql.clone(),
        status: if error.is_none() {
            "executed"
        } else {
            "failed"
        }
        .to_string(),
        error,
    }
}

fn skipped_statement_execution(
    statement: &PlannedSchemaStatement,
    failed_prerequisites: &[String],
) -> StatementExecution {
    StatementExecution {
        phase: statement.phase,
        sql: statement.sql.clone(),
        status: "skipped".to_string(),
        error: Some(format!(
            "failed prerequisites: {}",
            failed_prerequisites.join(",")
        )),
    }
}

fn overall_schema_status(reports: &[TableSchemaReport]) -> OverallSchemaStatus {
    let converged = reports
        .iter()
        .filter(|report| report.status == TableSchemaStatus::Converged)
        .count();
    if converged == reports.len() {
        OverallSchemaStatus::Converged
    } else if converged == 0 {
        OverallSchemaStatus::Failed
    } else {
        OverallSchemaStatus::Partial
    }
}

#[derive(Clone, Debug)]
struct SyncSchemaConfig {
    source: crate::mysql_snapshot::MySqlConnectionConfig,
    target: crate::live::TargetMySqlConfig,
    tables: Vec<String>,
    catalog: Option<PathBuf>,
    all_tables: bool,
}

pub(crate) fn run_sync_schema_command(args: Vec<String>, _usage: &str) {
    let result = parse_sync_schema_config(args).and_then(run_sync_schema);
    let (json, exit_code) = render_sync_schema_termination(result);
    println!("{json}");
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn render_sync_schema_termination(
    result: Result<SchemaConvergenceReport, String>,
) -> (String, i32) {
    let report = match result {
        Ok(report) => report,
        Err(error) => SchemaConvergenceReport {
            transformation_version: DDL_TRANSFORMATION_VERSION.to_string(),
            source_fingerprint: String::new(),
            target_fingerprint: String::new(),
            overall_status: OverallSchemaStatus::Failed,
            error: Some(error),
            tables: Vec::new(),
        },
    };
    let exit_code = match report.overall_status {
        OverallSchemaStatus::Converged => 0,
        OverallSchemaStatus::Partial => 1,
        OverallSchemaStatus::Failed if report.tables.is_empty() => 2,
        OverallSchemaStatus::Failed => 1,
    };
    (
        serde_json::to_string_pretty(&report).expect("schema report JSON"),
        exit_code,
    )
}

fn run_sync_schema(config: SyncSchemaConfig) -> Result<SchemaConvergenceReport, String> {
    let catalog = config
        .catalog
        .as_ref()
        .map(fs::read)
        .transpose()
        .map_err(|error| format!("failed to read sync-schema catalog: {error}"))?;
    let selection = schema_selection(&config.tables, catalog.as_deref(), config.all_tables)?;
    let source_reader = MariaDbInventoryReader::new(inventory_config_source(&config.source));
    let target_reader = MariaDbInventoryReader::new(inventory_config_target(&config.target));
    let source = build_inventory(&config.source.database, &source_reader)
        .map_err(|error| format!("source schema inventory failed: {error}"))?;
    let target = build_inventory(&config.target.database, &target_reader)
        .map_err(|error| format!("target schema inventory failed: {error}"))?;
    let selected = resolve_schema_selection(&selection, &source)?;
    let preflight = MySqlCoercionPreflight {
        config: inventory_config_target(&config.target),
        schema: config.target.database.clone(),
    };
    let mut plan = plan_schema_convergence(&source, &target, &selected, &preflight)?;
    let source_checks = CheckConstraintReader::new(inventory_config_source(&config.source))
        .read(&config.source.database, None)?;
    let target_checks = CheckConstraintReader::new(inventory_config_target(&config.target))
        .read(&config.target.database, None)?;
    append_check_constraint_plan(&mut plan, &source, &source_checks, &target_checks);
    let source_canonical_foreign_keys =
        build_canonical_foreign_key_inventory(&config.source.database, &source_reader)
            .map_err(|error| format!("source canonical foreign key inventory failed: {error}"))?;
    let target_canonical_foreign_keys =
        build_canonical_foreign_key_inventory(&config.target.database, &target_reader)
            .map_err(|error| format!("target canonical foreign key inventory failed: {error}"))?;
    append_canonical_foreign_key_plan(
        &mut plan,
        &source,
        &source_canonical_foreign_keys,
        &target_canonical_foreign_keys,
        &config.target.database,
    );
    let executor = crate::mysql_client::PersistentTargetExecutor::new(&config.target)
        .map_err(|error| error.to_string())?;
    let mut executor = MySqlSchemaExecutor { executor };
    let target_config = inventory_config_target(&config.target);
    let source_checks_by_table = checks_by_table(&target_check_constraints(&source_checks));
    let source_table_map = table_map(&source);
    let expected = expected_target_inventory(&source);
    // One reader and one check-constraint connection serve every table, because a fresh TLS
    // handshake per table costs more than the metadata reads themselves.
    let verification_reader = MariaDbInventoryReader::new(target_config.clone());
    let check_reader = RefCell::new(CheckConstraintReader::new(target_config.clone()));
    Ok(execute_schema_plan(plan, &mut executor, &|table| {
        // Verification describes one table, so it must not re-read the whole schema per table.
        verification_reader.scope_to_table(table);
        let target_inventory = match build_inventory(&config.target.database, &verification_reader)
        {
            Ok(inventory) => inventory,
            Err(error) => return vec![format!("target re-inventory failed: {error}")],
        };
        if !source_table_map.contains_key(table) {
            return vec!["source table disappeared during verification".to_string()];
        }
        let mut differences = schema_table_differences(&expected, &target_inventory, table);
        let target_checks = check_reader
            .borrow_mut()
            .read(&config.target.database, Some(table))
            .map(|checks| {
                checks_by_table(&checks)
                    .get(table)
                    .cloned()
                    .unwrap_or_default()
            });
        match target_checks {
            Ok(target_checks)
                if target_checks
                    == source_checks_by_table
                        .get(table)
                        .cloned()
                        .unwrap_or_default() => {}
            Ok(_) => differences.push("check constraints differ".to_string()),
            Err(error) => differences.push(format!("check verification failed: {error}")),
        }
        let target_canonical =
            build_canonical_foreign_key_inventory(&config.target.database, &verification_reader);
        let source_table_foreign_keys = relative_canonical_foreign_keys(
            &canonical_foreign_keys_for(&source_canonical_foreign_keys, table),
            &config.source.database,
        );
        match target_canonical {
            Ok(target_keys)
                if relative_canonical_foreign_keys(
                    &canonical_foreign_keys_for(&target_keys, table),
                    &config.target.database,
                ) == source_table_foreign_keys => {}
            Ok(_) => differences.push("foreign key rules differ".to_string()),
            Err(error) => differences.push(format!("foreign key verification failed: {error}")),
        }
        differences
    }))
}

struct MySqlSchemaExecutor {
    executor: crate::mysql_client::PersistentTargetExecutor,
}

struct MySqlCoercionPreflight {
    config: InventoryConfig,
    schema: String,
}

impl MySqlCoercionPreflight {
    fn connect(&self) -> Result<Conn, String> {
        let opts = crate::inventory::reader::inventory_opts(&self.config)?;
        Conn::new(opts).map_err(|error| format!("coercion preflight connection failed: {error}"))
    }

    fn count_blockers(
        &self,
        connection: &mut Conn,
        table: &str,
        predicate: &str,
    ) -> Result<u64, String> {
        let sql = format!(
            "SELECT COUNT(*) FROM {}.{} WHERE {}",
            quoted_identifier(&self.schema)?,
            quoted_identifier(table)?,
            predicate
        );
        connection
            .query_first::<u64, _>(sql)
            .map_err(|error| format!("coercion blocker count failed: {error}"))
            .map(|count| count.unwrap_or(0))
    }

    fn sample_blocker_primary_keys(
        &self,
        connection: &mut Conn,
        table: &TableInventory,
        predicate: &str,
    ) -> Result<Vec<Vec<String>>, String> {
        if table.primary_key.is_empty() {
            return Err("target table has no primary key for blocker samples".to_string());
        }
        let primary_key = table
            .primary_key
            .iter()
            .map(|column| quoted_identifier(column))
            .collect::<Result<Vec<_>, _>>()?
            .join(",");
        let sql = format!(
            "SELECT {primary_key} FROM {}.{} WHERE {} ORDER BY {primary_key} LIMIT 10",
            quoted_identifier(&self.schema)?,
            quoted_identifier(&table.name)?,
            predicate
        );
        connection
            .query::<mysql::Row, _>(sql)
            .map_err(|error| format!("coercion blocker samples failed: {error}"))
            .map(|rows| {
                rows.into_iter()
                    .map(|row| row.unwrap().iter().map(mysql_value_text).collect())
                    .collect()
            })
    }
}

impl SchemaCoercionPreflight for MySqlCoercionPreflight {
    fn inspect(
        &self,
        table: &TableInventory,
        source: &ColumnInventory,
        target: &ColumnInventory,
    ) -> Result<CoercionBlockers, String> {
        let predicate = coercion_blocker_predicate(source, target).ok();
        let query_predicate = predicate.as_deref().unwrap_or("TRUE");
        let mut connection = self.connect()?;
        let count = self.count_blockers(&mut connection, &table.name, query_predicate)?;
        let samples = if count == 0 {
            Ok(Vec::new())
        } else {
            self.sample_blocker_primary_keys(&mut connection, table, query_predicate)
        };
        Ok(coercion_blockers(predicate, count, samples))
    }
}

fn coercion_blockers(
    predicate: Option<String>,
    count: u64,
    samples: Result<Vec<Vec<String>>, String>,
) -> CoercionBlockers {
    match samples {
        Ok(sample_primary_keys) => CoercionBlockers {
            predicate,
            count,
            sample_primary_keys,
            sample_error: None,
        },
        Err(error) => CoercionBlockers {
            predicate,
            count,
            sample_primary_keys: Vec::new(),
            sample_error: Some(error),
        },
    }
}

impl SchemaStatementExecutor for MySqlSchemaExecutor {
    fn execute(&mut self, _table: &str, sql: &str) -> Result<(), String> {
        self.executor
            .execute(&SqlStatement {
                sql: sql.to_string(),
                params: Vec::new(),
            })
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

fn parse_sync_schema_config(args: Vec<String>) -> Result<SyncSchemaConfig, String> {
    let mut values = BTreeMap::<String, String>::new();
    let mut tables = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let flag = args[index].as_str();
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        if flag == "--table" {
            tables.push(value.clone());
        } else {
            values.insert(flag.to_string(), value.clone());
        }
        index += 2;
    }
    let source_password_env = required(&values, "--source-password-env")?;
    let target_password_env = required(&values, "--target-password-env")?;
    Ok(SyncSchemaConfig {
        source: crate::mysql_snapshot::MySqlConnectionConfig {
            host: required(&values, "--source-host")?,
            port: optional_u16(&values, "--source-port", 3306)?,
            user: required(&values, "--source-user")?,
            password: crate::read_env_password(&source_password_env)?,
            database: required(&values, "--source-database")?,
        },
        target: crate::live::TargetMySqlConfig {
            host: required(&values, "--target-host")?,
            port: optional_u16(&values, "--target-port", 3306)?,
            user: required(&values, "--target-user")?,
            password: crate::read_env_password(&target_password_env)?,
            database: required(&values, "--target-database")?,
            tls_ca_file: required(&values, "--target-tls-ca-file")?,
            insert_conflict_policy: crate::live::InsertConflictPolicy::Error,
        },
        tables,
        catalog: values.get("--catalog").map(PathBuf::from),
        all_tables: match values.get("--all-tables") {
            Some(value) => crate::parse_bool("--all-tables", value)?,
            None => false,
        },
    })
}

/// Holds one connection for repeated check-constraint reads, because a fresh TLS handshake per
/// table costs more than the read.
struct CheckConstraintReader {
    config: InventoryConfig,
    connection: Option<Conn>,
}

impl CheckConstraintReader {
    fn new(config: InventoryConfig) -> Self {
        Self {
            config,
            connection: None,
        }
    }

    fn read(
        &mut self,
        schema: &str,
        table_scope: Option<&str>,
    ) -> Result<Vec<CheckConstraint>, String> {
        if self.connection.is_none() {
            let opts = crate::inventory::reader::inventory_opts(&self.config)?;
            self.connection = Some(
                Conn::new(opts)
                    .map_err(|error| format!("check inventory connection failed: {error}"))?,
            );
        }
        let connection = self
            .connection
            .as_mut()
            .expect("check inventory connection");
        let result =
            query_check_constraints(connection, self.config.endpoint_role, schema, table_scope);
        if result.is_err() {
            self.connection = None;
        }
        result
    }
}

fn query_check_constraints(
    connection: &mut Conn,
    endpoint_role: InventoryEndpointRole,
    schema: &str,
    table_scope: Option<&str>,
) -> Result<Vec<CheckConstraint>, String> {
    let sql = check_constraint_query(endpoint_role, table_scope);
    let mut parameters = vec![Value::Bytes(schema.as_bytes().to_vec())];
    if let Some(table_scope) = table_scope {
        parameters.push(Value::Bytes(table_scope.as_bytes().to_vec()));
    }
    connection
        .exec_map(
            sql,
            Params::Positional(parameters),
            |(table, name, clause): (String, String, String)| CheckConstraint {
                table,
                name,
                clause,
            },
        )
        .map_err(|error| format!("check constraint inventory failed: {error}"))
}

/// MariaDB reports the owning table in `CHECK_CONSTRAINTS` and allows one check name on
/// several tables. MySQL's view has no table column, but its check names are unique per
/// schema, so only there is the `TABLE_CONSTRAINTS` join both needed and unambiguous.
fn check_constraint_query(
    endpoint_role: InventoryEndpointRole,
    table_scope: Option<&str>,
) -> String {
    let (columns_and_from, predicate, order) = match endpoint_role {
        InventoryEndpointRole::Source => (
            "SELECT TABLE_NAME,CONSTRAINT_NAME,CHECK_CLAUSE \
               FROM information_schema.CHECK_CONSTRAINTS",
            "WHERE CONSTRAINT_SCHEMA=?",
            "ORDER BY TABLE_NAME,CONSTRAINT_NAME",
        ),
        InventoryEndpointRole::Target => (
            "SELECT tc.TABLE_NAME,tc.CONSTRAINT_NAME,cc.CHECK_CLAUSE \
               FROM information_schema.TABLE_CONSTRAINTS tc \
               JOIN information_schema.CHECK_CONSTRAINTS cc \
                 ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA \
                AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME",
            "WHERE tc.CONSTRAINT_SCHEMA=? AND tc.CONSTRAINT_TYPE='CHECK'",
            "ORDER BY tc.TABLE_NAME,tc.CONSTRAINT_NAME",
        ),
    };
    let scope = match (endpoint_role, table_scope) {
        (_, None) => "",
        (InventoryEndpointRole::Source, Some(_)) => " AND TABLE_NAME=?",
        (InventoryEndpointRole::Target, Some(_)) => " AND tc.TABLE_NAME=?",
    };
    format!("{columns_and_from} {predicate}{scope} {order}")
}

fn checks_by_table(checks: &[CheckConstraint]) -> BTreeMap<String, Vec<CheckConstraint>> {
    let mut grouped = BTreeMap::<String, Vec<CheckConstraint>>::new();
    for check in checks {
        grouped
            .entry(check.table.clone())
            .or_default()
            .push(check.clone());
    }
    grouped
}

fn append_check_constraint_plan(
    plan: &mut SchemaConvergencePlan,
    source_inventory: &SchemaInventory,
    source: &[CheckConstraint],
    target: &[CheckConstraint],
) {
    let source = checks_by_table(&target_check_constraints(source));
    let target = checks_by_table(target);
    for table in &mut plan.tables {
        let source_checks = source.get(&table.table).cloned().unwrap_or_default();
        let target_checks = target.get(&table.table).cloned().unwrap_or_default();
        let drops = target_checks
            .iter()
            .filter(|target_check| !source_checks.contains(target_check))
            .filter_map(|check| {
                translate_for_table(
                    table,
                    SchemaPhase::Constraints,
                    format!("ALTER TABLE `{}` DROP CHECK `{}`", table.table, check.name),
                    vec![format!("check:{}.{}", table.table, check.name)],
                )
            })
            .collect::<Vec<_>>();
        let additions = source_checks
            .iter()
            .filter(|source_check| !target_checks.contains(source_check))
            .filter_map(|check| {
                let statement = translate_for_table(
                    table,
                    SchemaPhase::Constraints,
                    format!(
                        "ALTER TABLE `{}` ADD CONSTRAINT `{}` CHECK ({})",
                        table.table, check.name, check.clause
                    ),
                    vec![format!("check:{}.{}", table.table, check.name)],
                )?;
                Some(with_prerequisites(
                    statement,
                    check_constraint_prerequisites(source_inventory, check),
                ))
            })
            .collect::<Vec<_>>();
        table.statements.splice(0..0, drops);
        table.statements.extend(additions);
    }
}

/// The check constraints the target must hold.
///
/// MariaDB only requires a check name to be unique within its table, and this source reuses
/// several across tables. MySQL requires schema-wide uniqueness, so a reused name is qualified
/// with its table. Names already unique keep their source spelling.
fn target_check_constraints(source: &[CheckConstraint]) -> Vec<CheckConstraint> {
    let mut tables_per_name = BTreeMap::<&str, BTreeSet<&str>>::new();
    for check in source {
        tables_per_name
            .entry(check.name.as_str())
            .or_default()
            .insert(check.table.as_str());
    }
    source
        .iter()
        .map(|check| {
            let reused = tables_per_name
                .get(check.name.as_str())
                .is_some_and(|tables| tables.len() > 1);
            if !reused {
                return check.clone();
            }
            CheckConstraint {
                name: format!("{}_{}", check.table, check.name),
                ..check.clone()
            }
        })
        .collect()
}

fn check_constraint_prerequisites(
    source: &SchemaInventory,
    check: &CheckConstraint,
) -> Vec<String> {
    let Some(table) = source.tables.iter().find(|table| table.name == check.table) else {
        return Vec::new();
    };
    let selected_columns = table
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut prerequisites = selected_columns
        .into_iter()
        .filter(|column| check_clause_references_column(&check.clause, column))
        .map(|column| format!("column:{}.{}", check.table, column))
        .collect::<Vec<_>>();
    prerequisites.sort();
    prerequisites.dedup();
    prerequisites
}

fn check_clause_references_column(clause: &str, column: &str) -> bool {
    let quoted = format!("`{column}`");
    if clause.contains(&quoted) {
        return true;
    }
    clause.match_indices(column).any(|(start, _)| {
        let end = start + column.len();
        let starts_at_boundary = start == 0
            || (!clause.as_bytes()[start - 1].is_ascii_alphanumeric()
                && clause.as_bytes()[start - 1] != b'_');
        let ends_at_boundary = end == clause.len()
            || (!clause.as_bytes()[end].is_ascii_alphanumeric() && clause.as_bytes()[end] != b'_');
        starts_at_boundary && ends_at_boundary
    })
}

fn append_canonical_foreign_key_plan(
    plan: &mut SchemaConvergencePlan,
    source_inventory: &SchemaInventory,
    source: &[CanonicalForeignKey],
    target: &[CanonicalForeignKey],
    target_schema: &str,
) {
    for table in &mut plan.tables {
        let foreign_key_prerequisites = table
            .statements
            .iter()
            .flat_map(|statement| {
                statement
                    .objects
                    .iter()
                    .filter(|object| object.starts_with("foreign_key:"))
                    .map(|object| (object.clone(), statement.prerequisites.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        table.statements.retain(|statement| {
            !statement
                .objects
                .iter()
                .any(|object| object.starts_with("foreign_key:"))
        });
        let source_keys = relative_canonical_foreign_keys(
            &canonical_foreign_keys_for(source, &table.table),
            &source_inventory.schema,
        );
        let target_keys = relative_canonical_foreign_keys(
            &canonical_foreign_keys_for(target, &table.table),
            target_schema,
        );
        let drops = target_keys
            .iter()
            .filter(|target_key| !source_keys.contains(target_key))
            .filter_map(|key| {
                translate_for_table(
                    table,
                    SchemaPhase::Constraints,
                    format!(
                        "ALTER TABLE `{}` DROP FOREIGN KEY `{}`",
                        table.table, key.constraint_name
                    ),
                    vec![format!(
                        "foreign_key:{}.{}",
                        table.table, key.constraint_name
                    )],
                )
            })
            .collect::<Vec<_>>();
        let additions = source_keys
            .iter()
            .filter(|source_key| !target_keys.contains(source_key))
            .filter_map(|key| {
                let object = format!("foreign_key:{}.{}", table.table, key.constraint_name);
                let statement = translate_for_table(
                    table,
                    SchemaPhase::Constraints,
                    render_canonical_foreign_key(key, &source_inventory.schema),
                    vec![object.clone()],
                )?;
                let prerequisites = foreign_key_prerequisites
                    .get(&object)
                    .cloned()
                    .unwrap_or_else(|| canonical_foreign_key_prerequisites(source_inventory, key));
                Some(with_prerequisites(statement, prerequisites))
            })
            .collect::<Vec<_>>();
        table.statements.splice(0..0, drops);
        table.statements.extend(additions);
    }
}

fn canonical_foreign_key_prerequisites(
    source: &SchemaInventory,
    key: &CanonicalForeignKey,
) -> Vec<String> {
    foreign_key_prerequisites(
        source,
        &ForeignKeyInventory {
            table: key.child_table.clone(),
            name: key.constraint_name.clone(),
            columns: key.child_columns.clone(),
            referenced_schema: key.parent_schema.clone(),
            referenced_table: key.parent_table.clone(),
            referenced_columns: key.parent_columns.clone(),
        },
    )
}

fn canonical_foreign_keys_for(
    foreign_keys: &[CanonicalForeignKey],
    table: &str,
) -> Vec<CanonicalForeignKey> {
    foreign_keys
        .iter()
        .filter(|key| key.child_table == table)
        .cloned()
        .collect()
}

fn render_canonical_foreign_key(key: &CanonicalForeignKey, schema: &str) -> String {
    format!(
        "ALTER TABLE `{}` ADD CONSTRAINT `{}` FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
        key.child_table,
        key.constraint_name,
        quoted_list(&key.child_columns),
        referenced_table_reference(schema, &key.parent_schema, &key.parent_table),
        quoted_list(&key.parent_columns),
        key.update_rule,
        key.delete_rule,
    )
}

fn inventory_config_source(
    config: &crate::mysql_snapshot::MySqlConnectionConfig,
) -> InventoryConfig {
    InventoryConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.user.clone(),
        password: config.password.clone(),
        endpoint_role: InventoryEndpointRole::Source,
        use_tls: false,
        tls_ca_file: None,
        ..InventoryConfig::default()
    }
}

fn inventory_config_target(config: &crate::live::TargetMySqlConfig) -> InventoryConfig {
    InventoryConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.user.clone(),
        password: config.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(config.tls_ca_file.clone()),
        ..InventoryConfig::default()
    }
}

fn modeled_table_options_statement(
    table: &TableInventory,
) -> Result<PlannedSchemaStatement, String> {
    let options = render_table_options(table);
    let modeled_probe = format!("CREATE TABLE `sync_schema_options_probe` (`id` BIGINT) {options}");
    let translated = translate_modeled_ddl(&modeled_probe, &[])
        .map_err(|error| format!("table option DDL is not modeled: {error}"))?;
    require_current_translation_version(&translated)?;
    Ok(raw_statement(
        SchemaPhase::Columns,
        format!("ALTER TABLE `{}` {options}", table.name),
        vec![format!("table:{}.options", table.name)],
    ))
}

fn render_table_options(table: &TableInventory) -> String {
    let engine = table.engine.as_deref().unwrap_or("InnoDB");
    let collation = table.collation.as_deref().map(|collation| {
        let collation = canonical_collation(collation);
        let character_set = collation.split('_').next().unwrap_or(&collation);
        format!(" DEFAULT CHARACTER SET={character_set} COLLATE={collation}")
    });
    format!(
        "ENGINE={engine}{}",
        collation.as_deref().unwrap_or_default()
    )
}

fn render_create_table(
    inventory: &SchemaInventory,
    table: &TableInventory,
    include_foreign_keys: bool,
) -> String {
    let mut definitions = table.columns.iter().map(render_column).collect::<Vec<_>>();
    if !table.primary_key.is_empty() {
        definitions.push(format!("PRIMARY KEY ({})", quoted_list(&table.primary_key)));
    }
    definitions.extend(
        indexes_for(inventory, &table.name)
            .into_iter()
            .map(render_inline_index),
    );
    if include_foreign_keys {
        definitions.extend(
            inventory
                .foreign_keys
                .iter()
                .filter(|foreign_key| foreign_key.table == table.name)
                .map(|foreign_key| render_inline_foreign_key(foreign_key, &inventory.schema)),
        );
    }
    let collation = table.collation.as_deref().map(|collation| {
        let collation = canonical_collation(collation);
        let character_set = collation.split('_').next().unwrap_or(&collation);
        format!(" DEFAULT CHARACTER SET={character_set} COLLATE={collation}")
    });
    format!(
        "CREATE TABLE `{}` ({}) ENGINE={}{}",
        table.name,
        definitions.join(", "),
        table.engine.as_deref().unwrap_or("InnoDB"),
        collation.as_deref().unwrap_or_default(),
    )
}

fn render_column(column: &ColumnInventory) -> String {
    let mut rendered = format!(
        "`{}` {}",
        column.name,
        uppercase_type_name(&column.column_type)
    );
    // `CHARACTER SET` and `COLLATE` belong to the data type, so MySQL rejects them after
    // nullability, a default, or a generated expression.
    if let Some(character_set) = &column.character_set {
        rendered.push_str(&format!(" CHARACTER SET {character_set}"));
    }
    if let Some(collation) = &column.collation {
        rendered.push_str(&format!(" COLLATE {}", canonical_collation(collation)));
    }
    if let Some(generated) = &column.generated {
        rendered.push_str(&format!(
            " GENERATED ALWAYS AS ({}) {}",
            generated.expression,
            generated.generation_kind.to_ascii_uppercase()
        ));
        return rendered;
    }
    rendered.push_str(if column.is_nullable {
        " NULL"
    } else {
        " NOT NULL"
    });
    if let Some(default) = &column.default_value {
        rendered.push_str(" DEFAULT ");
        rendered.push_str(&render_default(default));
    } else if column.is_nullable {
        rendered.push_str(" DEFAULT NULL");
    }
    let lower_extra = column.extra.to_ascii_lowercase();
    if lower_extra.contains("auto_increment") {
        rendered.push_str(" AUTO_INCREMENT");
    }
    if let Some(index) = lower_extra.find("on update") {
        rendered.push(' ');
        rendered.push_str(&column.extra[index..]);
    }
    if !column.comment.is_empty() {
        rendered.push_str(&format!(
            " COMMENT '{}'",
            column.comment.replace('\'', "''")
        ));
    }
    rendered
}

/// MariaDB's `information_schema` reports a literal default already written as SQL: strings
/// carry their quotes and bit literals their `b'..'` form. Only a bare value needs quoting.
fn render_default(default: &str) -> String {
    if default.eq_ignore_ascii_case("current_timestamp")
        || default
            .to_ascii_lowercase()
            .starts_with("current_timestamp(")
        || default.eq_ignore_ascii_case("null")
    {
        default.to_ascii_uppercase()
    } else if is_sql_literal(default) {
        default.to_string()
    } else {
        format!("'{}'", default.replace('\'', "''"))
    }
}

fn is_sql_literal(default: &str) -> bool {
    let bit_literal = default.len() > 3
        && default.starts_with("b'")
        && default.ends_with('\'')
        && default[2..default.len() - 1]
            .bytes()
            .all(|byte| byte == b'0' || byte == b'1');
    bit_literal || quoted_literal_body(default).is_some()
}

/// The body of a single-quoted SQL literal, with doubled quotes collapsed. `None` when the
/// value is not one quoted literal.
fn quoted_literal_body(value: &str) -> Option<String> {
    let inner = value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))?;
    let mut body = String::with_capacity(inner.len());
    let mut characters = inner.chars();
    while let Some(character) = characters.next() {
        match character {
            '\'' => {
                if characters.next() != Some('\'') {
                    // An unescaped quote means the value is not one quoted literal.
                    return None;
                }
                body.push('\'');
            }
            '\\' => {
                body.push('\\');
                body.push(characters.next()?);
            }
            other => body.push(other),
        }
    }
    Some(body)
}

fn column_position(table: &TableInventory, index: usize) -> String {
    if index == 0 {
        " FIRST".to_string()
    } else {
        format!(" AFTER `{}`", table.columns[index - 1].name)
    }
}

fn translate_statement(
    phase: SchemaPhase,
    sql: String,
    objects: Vec<String>,
    target_columns: &[String],
) -> Result<PlannedSchemaStatement, String> {
    let translated = translate_modeled_ddl(&sql, target_columns)?;
    require_current_translation_version(&translated)?;
    let sql = translated
        .target_sql
        .ok_or_else(|| format!("schema DDL translated to no-op: {sql}"))?;
    Ok(raw_statement(phase, sql, objects))
}

fn require_current_translation_version(
    translated: &crate::live::ddl_semantics::DdlTransformation,
) -> Result<(), String> {
    (translated.version == DDL_TRANSFORMATION_VERSION)
        .then_some(())
        .ok_or_else(|| {
            format!(
                "schema DDL translation version mismatch: expected {DDL_TRANSFORMATION_VERSION}, got {}",
                translated.version
            )
        })
}

fn translate_for_table(
    table: &mut TableSchemaPlan,
    phase: SchemaPhase,
    sql: String,
    objects: Vec<String>,
) -> Option<PlannedSchemaStatement> {
    match translate_statement(phase, sql, objects, &[]) {
        Ok(statement) => Some(statement),
        Err(error) => {
            table.status = TableSchemaStatus::Failed;
            table
                .blockers
                .push(format!("DDL translation failed: {error}"));
            None
        }
    }
}

fn raw_statement(phase: SchemaPhase, sql: String, objects: Vec<String>) -> PlannedSchemaStatement {
    PlannedSchemaStatement {
        phase,
        sql,
        objects,
        prerequisites: Vec::new(),
    }
}

fn with_prerequisites(
    mut statement: PlannedSchemaStatement,
    prerequisites: Vec<String>,
) -> PlannedSchemaStatement {
    statement.prerequisites = prerequisites;
    statement
}

fn column_prerequisites(table: &str, columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|column| format!("column:{table}.{column}"))
        .collect()
}

fn target_column_drop_prerequisites(
    target: &SchemaInventory,
    table: &TableInventory,
    column: &str,
) -> Vec<String> {
    let mut prerequisites = indexes_for(target, &table.name)
        .into_iter()
        .filter(|index| index.columns.iter().any(|part| part.name == column))
        .map(|index| format!("index:{}.{}", table.name, index.name))
        .collect::<Vec<_>>();
    prerequisites.extend(
        target
            .foreign_keys
            .iter()
            .filter(|key| key.table == table.name && key.columns.iter().any(|name| name == column))
            .map(|key| format!("foreign_key:{}.{}", table.name, key.name)),
    );
    prerequisites.sort();
    prerequisites.dedup();
    prerequisites
}

fn is_drop_statement(statement: &PlannedSchemaStatement) -> bool {
    statement.sql.to_ascii_uppercase().contains(" DROP ")
}

fn schema_table_differences(
    source: &SchemaInventory,
    target: &SchemaInventory,
    table: &str,
) -> Vec<String> {
    let Some(source_table) = source
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
    else {
        return vec!["source table missing".to_string()];
    };
    let Some(target_table) = target
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
    else {
        return vec!["target table missing".to_string()];
    };
    let mut differences = Vec::new();
    if expected_target_table_fingerprint(source_table).ok()
        != observed_target_table_fingerprint(target_table).ok()
    {
        differences.push(
            "columns, generated expressions, primary key, engine, or collation differ".to_string(),
        );
    }
    let source_indexes = indexes_for(source, table);
    let target_indexes = indexes_for(target, table);
    if source_indexes.len() != target_indexes.len()
        || source_indexes.iter().any(|source_index| {
            !target_indexes
                .iter()
                .any(|target_index| indexes_equal(source_index, target_index))
        })
    {
        differences.push("indexes differ".to_string());
    }
    let source_foreign_keys = source
        .foreign_keys
        .iter()
        .filter(|key| key.table == table)
        .collect::<Vec<_>>();
    let target_foreign_keys = target
        .foreign_keys
        .iter()
        .filter(|key| key.table == table)
        .collect::<Vec<_>>();
    if source_foreign_keys.len() != target_foreign_keys.len()
        || source_foreign_keys.iter().any(|source_key| {
            !target_foreign_keys.iter().any(|target_key| {
                foreign_keys_equal(source_key, &source.schema, target_key, &target.schema)
            })
        })
    {
        differences.push("foreign keys differ".to_string());
    }
    differences
}

fn coercion_blocker_predicate(
    source: &ColumnInventory,
    target: &ColumnInventory,
) -> Result<String, String> {
    validate_predicate_metadata(source, target)?;
    let column = quoted_identifier(&source.name)?;
    let mut predicates = nullability_predicates(source, target, &column);
    predicates.extend(type_coercion_predicates(source, target, &column)?);
    if predicates.is_empty() {
        return Err(format!(
            "no target-data predicate for {} to {} conversion",
            target.column_type, source.column_type
        ));
    }
    Ok(format!("({})", predicates.join(") OR (")))
}

fn validate_predicate_metadata(
    source: &ColumnInventory,
    target: &ColumnInventory,
) -> Result<(), String> {
    if source.generated != target.generated {
        return Err("generated-column conversion has no safe target-data predicate".to_string());
    }
    if source.character_set != target.character_set || source.collation != target.collation {
        return Err(
            "character set or collation conversion requires explicit validation".to_string(),
        );
    }
    Ok(())
}

fn nullability_predicates(
    source: &ColumnInventory,
    target: &ColumnInventory,
    column: &str,
) -> Vec<String> {
    (target.is_nullable && !source.is_nullable)
        .then(|| format!("{column} IS NULL"))
        .into_iter()
        .collect()
}

fn type_coercion_predicates(
    source: &ColumnInventory,
    target: &ColumnInventory,
    column: &str,
) -> Result<Vec<String>, String> {
    let source_type = normalized_data_type(source);
    let target_type = normalized_data_type(target);
    if source_type == "varchar" && target_type == "varchar" {
        return varchar_coercion_predicates(source, target, column);
    }
    if integer_rank(&source_type).is_some() && integer_rank(&target_type).is_some() {
        let (minimum, maximum) = integer_bounds(source)?;
        return Ok(vec![format!(
            "{column} < {minimum} OR {column} > {maximum}"
        )]);
    }
    if source_type == "decimal" && target_type == "decimal" {
        return decimal_coercion_predicates(source, column);
    }
    if source_type == target_type {
        return Ok(Vec::new());
    }
    Err(format!(
        "unsupported target-data predicate for {} to {} conversion",
        target.column_type, source.column_type
    ))
}

fn varchar_coercion_predicates(
    source: &ColumnInventory,
    target: &ColumnInventory,
    column: &str,
) -> Result<Vec<String>, String> {
    let (source_length, target_length) = varchar_length(source)
        .zip(varchar_length(target))
        .ok_or_else(|| "VARCHAR length metadata is incomplete".to_string())?;
    Ok((source_length < target_length)
        .then(|| format!("CHAR_LENGTH({column}) > {source_length}"))
        .into_iter()
        .collect())
}

fn decimal_coercion_predicates(
    source: &ColumnInventory,
    column: &str,
) -> Result<Vec<String>, String> {
    let (precision, scale) =
        decimal_shape(source).ok_or_else(|| "DECIMAL shape metadata is incomplete".to_string())?;
    let integer_digits = precision.saturating_sub(scale);
    let maximum = decimal_maximum(integer_digits, scale);
    Ok(vec![format!(
        "{column} < -{maximum} OR {column} > {maximum} OR {column} <> ROUND({column}, {scale})"
    )])
}

fn decimal_maximum(integer_digits: u64, scale: u64) -> String {
    if scale == 0 {
        "9".repeat(integer_digits as usize)
    } else {
        format!(
            "{}.{}",
            "9".repeat(integer_digits as usize),
            "9".repeat(scale as usize)
        )
    }
}

fn integer_bounds(column: &ColumnInventory) -> Result<(&'static str, &'static str), String> {
    let unsigned = is_unsigned(column);
    match (normalized_data_type(column).as_str(), unsigned) {
        ("tinyint", false) => Ok(("-128", "127")),
        ("tinyint", true) => Ok(("0", "255")),
        ("smallint", false) => Ok(("-32768", "32767")),
        ("smallint", true) => Ok(("0", "65535")),
        ("mediumint", false) => Ok(("-8388608", "8388607")),
        ("mediumint", true) => Ok(("0", "16777215")),
        ("int" | "integer", false) => Ok(("-2147483648", "2147483647")),
        ("int" | "integer", true) => Ok(("0", "4294967295")),
        ("bigint", false) => Ok(("-9223372036854775808", "9223372036854775807")),
        ("bigint", true) => Ok(("0", "18446744073709551615")),
        _ => Err(format!("unsupported integer type {}", column.column_type)),
    }
}

fn quoted_identifier(identifier: &str) -> Result<String, String> {
    valid_identifier(identifier)
        .then(|| format!("`{identifier}`"))
        .ok_or_else(|| format!("invalid MySQL identifier {identifier}"))
}

fn mysql_value_text(value: &Value) -> String {
    match value {
        Value::NULL => "NULL".to_string(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}")
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => format!(
            "{}{days} {hours:02}:{minutes:02}:{seconds:02}.{micros:06}",
            if *negative { "-" } else { "" }
        ),
    }
}

fn column_change_requires_data_preflight(
    source: &ColumnInventory,
    target: &ColumnInventory,
) -> bool {
    if source.generated != target.generated {
        return true;
    }
    if target.is_nullable && !source.is_nullable {
        return true;
    }
    let source_type = normalized_data_type(source);
    let target_type = normalized_data_type(target);
    if source_type == target_type {
        if source.character_set != target.character_set || source.collation != target.collation {
            return true;
        }
        if let Some((source_length, target_length)) =
            varchar_length(source).zip(varchar_length(target))
        {
            return source_length < target_length;
        }
        if integer_rank(&source_type).is_some() {
            return is_unsigned(source) != is_unsigned(target);
        }
        if source_type == "decimal" {
            return decimal_shape(source).zip(decimal_shape(target)).is_none_or(
                |(source_shape, target_shape)| {
                    source_shape.0 - source_shape.1 < target_shape.0 - target_shape.1
                        || source_shape.1 < target_shape.1
                },
            );
        }
        let is_timestamp_mapping = source.data_type.eq_ignore_ascii_case("timestamp")
            && target.data_type.eq_ignore_ascii_case("datetime");
        return !is_timestamp_mapping
            && !source.column_type.eq_ignore_ascii_case(&target.column_type);
    }
    integer_rank(&source_type)
        .zip(integer_rank(&target_type))
        .is_none_or(|(source_rank, target_rank)| {
            is_unsigned(source) != is_unsigned(target) || source_rank < target_rank
        })
}

fn normalized_data_type(column: &ColumnInventory) -> String {
    if column.data_type.eq_ignore_ascii_case("timestamp") {
        "datetime".to_string()
    } else {
        column.data_type.to_ascii_lowercase()
    }
}

fn varchar_length(column: &ColumnInventory) -> Option<u64> {
    let column_type = column.column_type.to_ascii_lowercase();
    let length = column_type.strip_prefix("varchar(")?.strip_suffix(')')?;
    length.parse().ok()
}

fn is_unsigned(column: &ColumnInventory) -> bool {
    column
        .column_type
        .split_ascii_whitespace()
        .any(|part| part.eq_ignore_ascii_case("unsigned"))
}

fn decimal_shape(column: &ColumnInventory) -> Option<(u64, u64)> {
    let column_type = column.column_type.to_ascii_lowercase();
    let shape = column_type.strip_prefix("decimal(")?.strip_suffix(')')?;
    let mut parts = shape.split(',');
    let precision = parts.next()?.trim().parse().ok()?;
    let scale = parts.next()?.trim().parse().ok()?;
    Some((precision, scale))
}

fn integer_rank(data_type: &str) -> Option<u8> {
    match data_type {
        "tinyint" => Some(1),
        "smallint" => Some(2),
        "mediumint" => Some(3),
        "int" | "integer" => Some(4),
        "bigint" => Some(5),
        _ => None,
    }
}

/// Compare a source column against a target column through the MySQL form the translated DDL
/// produces, so an already-converged column is not modified again.
fn columns_equal(source: &ColumnInventory, target: &ColumnInventory) -> bool {
    expected_target_column(source) == observed_target_column(target)
}

/// The MySQL column the translated source definition produces.
fn expected_target_column(source: &ColumnInventory) -> ColumnInventory {
    let mut expected = observed_target_column(source);
    if expected.data_type.eq_ignore_ascii_case("timestamp") {
        expected.data_type = "datetime".to_string();
        expected.column_type = expected.column_type.replacen("timestamp", "datetime", 1);
    }
    expected
}

/// The same column described the way MySQL's `information_schema` reports it.
///
/// MariaDB and MySQL describe an identical column differently: MariaDB keeps the integer
/// display widths MySQL 8 dropped, quotes literal defaults MySQL reports bare, spells a NULL
/// default as the literal `NULL`, writes `current_timestamp()` where MySQL writes
/// `CURRENT_TIMESTAMP`, omits MySQL's `DEFAULT_GENERATED` marker, names its UCA-1400
/// collations differently, and prints generated expressions without MySQL's added
/// parentheses and charset introducers.
fn observed_target_column(column: &ColumnInventory) -> ColumnInventory {
    let mut canonical = column.clone();
    canonical.column_type = canonical_column_type(&column.column_type);
    canonical.data_type = column.data_type.to_ascii_lowercase();
    canonical.default_value = canonical_default(column.default_value.as_deref());
    canonical.extra = canonical_extra(&column.extra);
    canonical.collation = column.collation.as_deref().map(canonical_collation);
    canonical.generated =
        column
            .generated
            .as_ref()
            .map(|generated| crate::inventory::GeneratedColumn {
                expression: canonical_sql_expression(&generated.expression),
                generation_kind: generated.generation_kind.to_ascii_lowercase(),
            });
    canonical
}

const INTEGER_TYPES: [&str; 6] = [
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
];

/// Change the case of the type name and its trailing attributes only, because `ENUM` values
/// are data and their case is significant.
fn recase_type_name(column_type: &str, recase: fn(&str) -> String) -> String {
    match type_parameter_span(column_type) {
        Some((open, close)) => format!(
            "{}{}{}",
            recase(&column_type[..open]),
            &column_type[open..=close],
            recase(&column_type[close + 1..])
        ),
        None => recase(column_type),
    }
}

fn uppercase_type_name(column_type: &str) -> String {
    recase_type_name(column_type, str::to_ascii_uppercase)
}

/// Byte offsets of the type's opening and closing parenthesis, when it has parameters.
fn type_parameter_span(column_type: &str) -> Option<(usize, usize)> {
    let open = column_type.find('(')?;
    let close = column_type.rfind(')')?;
    (close > open).then_some((open, close))
}

/// MySQL 8 dropped integer display widths, so `int(11) unsigned` and `int unsigned` describe
/// the same column and only the attributes after the width carry meaning.
fn canonical_column_type(column_type: &str) -> String {
    let lowered = recase_type_name(column_type, str::to_ascii_lowercase);
    let Some((open, close)) = type_parameter_span(&lowered) else {
        return lowered;
    };
    let base = lowered[..open].trim_end();
    if !INTEGER_TYPES.contains(&base) {
        return lowered;
    }
    let suffix = lowered[close + 1..].trim_start();
    if suffix.is_empty() {
        base.to_string()
    } else {
        format!("{base} {suffix}")
    }
}

fn canonical_default(default: Option<&str>) -> Option<String> {
    let default = default?;
    if default.eq_ignore_ascii_case("null") {
        return None;
    }
    if let Some(body) = quoted_literal_body(default) {
        return Some(body);
    }
    Some(canonical_current_timestamp(default))
}

/// `current_timestamp()` and `CURRENT_TIMESTAMP` are the same default; only an explicit
/// fractional-second precision distinguishes them. Any other value is data, so its case stands.
fn canonical_current_timestamp(value: &str) -> String {
    let lowered = value.to_ascii_lowercase();
    if lowered.starts_with("current_timestamp") {
        if lowered == "current_timestamp()" {
            return "current_timestamp".to_string();
        }
        return lowered;
    }
    value.to_string()
}

/// MySQL adds a `DEFAULT_GENERATED` marker for expression defaults that MariaDB never reports.
fn canonical_extra(extra: &str) -> String {
    let lowered = extra.to_ascii_lowercase();
    lowered
        .replace("default_generated", "")
        .replace("current_timestamp()", "current_timestamp")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// MariaDB 11 names its Unicode collations `*_uca1400_*`; MySQL spells the same ordering
/// `utf8mb4_0900_*`, and has no UCA-1400 utf8mb3 collation beyond its general one.
fn canonical_collation(collation: &str) -> String {
    let lowered = collation.to_ascii_lowercase();
    match lowered.as_str() {
        "utf8mb4_uca1400_ai_ci" => "utf8mb4_0900_ai_ci".to_string(),
        "utf8mb4_uca1400_as_cs" => "utf8mb4_0900_as_cs".to_string(),
        "utf8mb3_uca1400_ai_ci" => "utf8mb3_general_ci".to_string(),
        _ => lowered,
    }
}

/// MySQL re-renders a stored expression - a generated column or a check clause - with its own
/// parentheses, charset introducers, and spacing. Comparison therefore ignores the formatting
/// MySQL controls, not the operands or operators.
fn canonical_sql_expression(expression: &str) -> String {
    let mut canonical = String::with_capacity(expression.len());
    let lowered = expression.to_ascii_lowercase();
    let mut rest = lowered.as_str();
    while !rest.is_empty() {
        if let Some(remainder) = rest
            .strip_prefix("_utf8mb4")
            .or_else(|| rest.strip_prefix("_utf8mb3"))
        {
            rest = remainder;
            continue;
        }
        let mut characters = rest.chars();
        let character = characters.next().expect("non-empty remainder");
        if !matches!(character, '(' | ')' | ' ' | '\t' | '\n' | '\\') {
            canonical.push(character);
        }
        rest = characters.as_str();
    }
    canonical
}

fn indexes_equal(left: &IndexInventory, right: &IndexInventory) -> bool {
    left.name == right.name
        && left.unique == right.unique
        && left.index_type.eq_ignore_ascii_case(&right.index_type)
        && left.visible == right.visible
        && left.comment == right.comment
        && left.columns == right.columns
}

/// Replaces an endpoint's own database name, so a same-schema reference compares equal even
/// when the target database is named differently from the source.
const OWN_SCHEMA: &str = "<own schema>";

fn relative_schema<'a>(inventory_schema: &str, schema: &'a str) -> &'a str {
    if schema == inventory_schema {
        OWN_SCHEMA
    } else {
        schema
    }
}

/// The same canonical foreign key with every database name expressed relative to the endpoint
/// that reported it.
fn relative_canonical_foreign_key(key: &CanonicalForeignKey, schema: &str) -> CanonicalForeignKey {
    CanonicalForeignKey {
        constraint_schema: relative_schema(schema, &key.constraint_schema).to_string(),
        child_schema: relative_schema(schema, &key.child_schema).to_string(),
        parent_schema: relative_schema(schema, &key.parent_schema).to_string(),
        ..key.clone()
    }
}

fn relative_canonical_foreign_keys(
    keys: &[CanonicalForeignKey],
    schema: &str,
) -> Vec<CanonicalForeignKey> {
    keys.iter()
        .map(|key| relative_canonical_foreign_key(key, schema))
        .collect()
}

fn foreign_keys_equal(
    left: &ForeignKeyInventory,
    left_schema: &str,
    right: &ForeignKeyInventory,
    right_schema: &str,
) -> bool {
    left.name == right.name
        && left.columns == right.columns
        && relative_schema(left_schema, &left.referenced_schema)
            == relative_schema(right_schema, &right.referenced_schema)
        && left.referenced_table == right.referenced_table
        && left.referenced_columns == right.referenced_columns
}

fn render_create_index(index: &IndexInventory) -> String {
    let visibility = if index.visible { "" } else { " INVISIBLE" };
    let comment = index
        .comment
        .as_ref()
        .filter(|comment| !comment.is_empty())
        .map(|comment| format!(" COMMENT '{}'", escape_mysql_string(comment)))
        .unwrap_or_default();
    format!(
        "CREATE {}INDEX `{}` ON `{}` ({}) USING {}{visibility}{comment}",
        if index.unique { "UNIQUE " } else { "" },
        index.name,
        index.table,
        index
            .columns
            .iter()
            .map(render_index_column)
            .collect::<Vec<_>>()
            .join(","),
        index.index_type,
    )
}

fn render_inline_index(index: &IndexInventory) -> String {
    let visibility = if index.visible { "" } else { " INVISIBLE" };
    let comment = index
        .comment
        .as_ref()
        .filter(|comment| !comment.is_empty())
        .map(|comment| format!(" COMMENT '{}'", escape_mysql_string(comment)))
        .unwrap_or_default();
    format!(
        "{}KEY `{}` ({}) USING {}{visibility}{comment}",
        if index.unique { "UNIQUE " } else { "" },
        index.name,
        index
            .columns
            .iter()
            .map(render_index_column)
            .collect::<Vec<_>>()
            .join(","),
        index.index_type,
    )
}

fn escape_mysql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

fn render_index_column(column: &crate::inventory::IndexColumnInventory) -> String {
    let prefix = column
        .prefix_length
        .map(|length| format!("({length})"))
        .unwrap_or_default();
    let order = if column.order.is_empty() {
        String::new()
    } else {
        format!(" {}", column.order)
    };
    format!("`{}`{prefix}{order}", column.name)
}

fn render_foreign_key(foreign_key: &ForeignKeyInventory, schema: &str) -> String {
    format!(
        "ALTER TABLE `{}` ADD CONSTRAINT `{}` FOREIGN KEY ({}) REFERENCES {} ({})",
        foreign_key.table,
        foreign_key.name,
        quoted_list(&foreign_key.columns),
        referenced_table_reference(
            schema,
            &foreign_key.referenced_schema,
            &foreign_key.referenced_table
        ),
        quoted_list(&foreign_key.referenced_columns)
    )
}

fn render_inline_foreign_key(foreign_key: &ForeignKeyInventory, schema: &str) -> String {
    format!(
        "CONSTRAINT `{}` FOREIGN KEY ({}) REFERENCES {} ({})",
        foreign_key.name,
        quoted_list(&foreign_key.columns),
        referenced_table_reference(
            schema,
            &foreign_key.referenced_schema,
            &foreign_key.referenced_table
        ),
        quoted_list(&foreign_key.referenced_columns)
    )
}

/// A foreign key inside the converged schema must resolve in the target database, so only a
/// genuinely cross-schema parent keeps its schema qualifier. A parent already expressed
/// relative to its endpoint carries the sentinel and is equally unqualified.
fn referenced_table_reference(
    schema: &str,
    referenced_schema: &str,
    referenced_table: &str,
) -> String {
    if referenced_schema == schema || referenced_schema == OWN_SCHEMA {
        format!("`{referenced_table}`")
    } else {
        format!("`{referenced_schema}`.`{referenced_table}`")
    }
}

fn quoted_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| format!("`{column}`"))
        .collect::<Vec<_>>()
        .join(",")
}

fn table_map(inventory: &SchemaInventory) -> BTreeMap<&str, &TableInventory> {
    inventory
        .tables
        .iter()
        .map(|table| (table.name.as_str(), table))
        .collect()
}

fn column_map(table: &TableInventory) -> BTreeMap<&str, &ColumnInventory> {
    table
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect()
}

fn indexes_for<'a>(inventory: &'a SchemaInventory, table: &str) -> Vec<&'a IndexInventory> {
    inventory
        .indexes
        .iter()
        .filter(|index| index.table == table)
        .collect()
}

fn fingerprint(value: &impl Serialize) -> Result<String, String> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| format!("schema fingerprint failed: {error}"))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

/// Fingerprint of the MySQL table the translated source schema produces.
fn expected_target_table_fingerprint(table: &TableInventory) -> Result<String, String> {
    semantic_table_fingerprint(table, expected_target_column)
}

/// Fingerprint of the MySQL table as the target reports it.
fn observed_target_table_fingerprint(table: &TableInventory) -> Result<String, String> {
    semantic_table_fingerprint(table, observed_target_column)
}

fn semantic_table_fingerprint(
    table: &TableInventory,
    canonical: fn(&ColumnInventory) -> ColumnInventory,
) -> Result<String, String> {
    let mut table = table.clone();
    table.columns = table.columns.iter().map(canonical).collect();
    table.collation = table.collation.as_deref().map(canonical_collation);
    fingerprint(&table)
}

fn dependency_order(source: &SchemaInventory, selected: &[String]) -> (Vec<String>, Vec<String>) {
    let selected = selected.iter().cloned().collect::<BTreeSet<_>>();
    let mut ordered = Vec::new();
    let mut pending = selected.clone();
    while !pending.is_empty() {
        let ready = pending
            .iter()
            .filter(|table| {
                source
                    .foreign_keys
                    .iter()
                    .filter(|foreign_key| {
                        foreign_key.table == **table
                            && foreign_key.referenced_table != foreign_key.table
                            && selected.contains(&foreign_key.referenced_table)
                    })
                    .all(|foreign_key| ordered.contains(&foreign_key.referenced_table))
            })
            .cloned()
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return (ordered, pending.into_iter().collect());
        }
        for table in ready {
            pending.remove(&table);
            ordered.push(table);
        }
    }
    (ordered, Vec::new())
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn required(values: &BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .get(flag)
        .cloned()
        .ok_or_else(|| format!("missing required option {flag}"))
}

fn optional_u16(
    values: &BTreeMap<String, String>,
    flag: &str,
    default: u16,
) -> Result<u16, String> {
    values.get(flag).map_or(Ok(default), |value| {
        value
            .parse::<u16>()
            .map_err(|_| format!("invalid {flag}: {value}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{
        ColumnInventory, ForeignKeyInventory, IndexColumnInventory, IndexInventory,
        SchemaInventory, TableInventory,
    };

    #[test]
    fn plans_exact_selected_table_convergence_in_dependency_phases() {
        let source = inventory(
            vec![table(
                "children",
                vec![
                    column("id", "bigint", false),
                    column("parent_id", "bigint", false),
                ],
                vec!["id"],
            )],
            vec![foreign_key("children", "parents")],
        );
        let target = inventory(
            vec![table(
                "children",
                vec![
                    column("id", "int", false),
                    column("obsolete", "varchar(20)", true),
                ],
                vec!["id"],
            )],
            vec![],
        );

        let plan = plan_schema_convergence(
            &source,
            &target,
            &["children".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");

        assert!(plan.tables[0].statements.iter().any(|statement| {
            statement.sql.contains("DROP COLUMN `obsolete`")
                && statement.phase == SchemaPhase::Columns
        }));
        assert!(plan.tables[0].statements.iter().any(|statement| {
            statement.sql.contains("MODIFY COLUMN `id` BIGINT")
                && statement.phase == SchemaPhase::Columns
        }));
        assert!(plan.tables[0].statements.iter().any(|statement| {
            statement.sql.contains("ADD CONSTRAINT") && statement.phase == SchemaPhase::Constraints
        }));
        let foreign_key = plan.tables[0]
            .statements
            .iter()
            .find(|statement| statement.sql.contains("ADD CONSTRAINT"))
            .expect("foreign key statement");
        assert!(
            foreign_key
                .prerequisites
                .contains(&"column:children.parent_id".to_string())
        );
    }

    #[test]
    fn failed_plans_preserve_available_semantic_table_fingerprints() {
        let source_table = table(
            "broken",
            vec![column("payload", "mystery", true)],
            vec!["payload"],
        );
        let source = inventory(vec![source_table.clone()], vec![]);

        let missing_target = plan_schema_convergence(
            &source,
            &inventory(vec![], vec![]),
            &["broken".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("missing-target failed plan");
        assert_eq!(missing_target.tables[0].status, TableSchemaStatus::Failed);
        assert_eq!(
            missing_target.tables[0].source_fingerprint,
            expected_target_table_fingerprint(&source_table).expect("source fingerprint")
        );
        assert!(missing_target.tables[0].target_fingerprint.is_empty());

        let target_table = table(
            "broken",
            vec![column("payload", "bigint", true)],
            vec!["payload"],
        );
        let existing_target = plan_schema_convergence(
            &source,
            &inventory(vec![target_table.clone()], vec![]),
            &["broken".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("existing-target failed plan");
        assert_eq!(existing_target.tables[0].status, TableSchemaStatus::Failed);
        assert_eq!(
            existing_target.tables[0].source_fingerprint,
            expected_target_table_fingerprint(&source_table).expect("source fingerprint")
        );
        assert_eq!(
            existing_target.tables[0].target_fingerprint,
            observed_target_table_fingerprint(&target_table).expect("target fingerprint")
        );
    }

    #[test]
    fn coercion_preflight_uses_target_values_and_reports_exact_blockers() {
        let source = inventory(
            vec![
                table(
                    "unsafe_values",
                    vec![
                        column("id", "bigint", false),
                        column("label", "varchar(4)", false),
                    ],
                    vec!["id"],
                ),
                table(
                    "safe_values",
                    vec![
                        column("id", "bigint", false),
                        column("label", "varchar(4)", false),
                    ],
                    vec!["id"],
                ),
            ],
            vec![],
        );
        let target = inventory(
            vec![
                table(
                    "unsafe_values",
                    vec![
                        column("id", "bigint", false),
                        column("label", "varchar(8)", false),
                    ],
                    vec!["id"],
                ),
                table(
                    "safe_values",
                    vec![
                        column("id", "bigint", false),
                        column("label", "varchar(8)", false),
                    ],
                    vec!["id"],
                ),
            ],
            vec![],
        );
        let preflight = FixtureCoercionPreflight::with_blockers(
            "unsafe_values",
            "label",
            2,
            vec![vec!["7".to_string()], vec!["9".to_string()]],
        );

        let plan = plan_schema_convergence(
            &source,
            &target,
            &["unsafe_values".to_string(), "safe_values".to_string()],
            &preflight,
        )
        .expect("schema plan");

        let unsafe_plan = plan
            .tables
            .iter()
            .find(|table| table.table == "unsafe_values")
            .expect("unsafe plan");
        let safe_plan = plan
            .tables
            .iter()
            .find(|table| table.table == "safe_values")
            .expect("safe plan");
        assert_eq!(unsafe_plan.status, TableSchemaStatus::Planned);
        assert_eq!(unsafe_plan.preflights[0].count, 2);
        assert_eq!(
            unsafe_plan.preflights[0].sample_primary_keys,
            vec![vec!["7".to_string()], vec!["9".to_string()]]
        );
        assert_eq!(unsafe_plan.preflights[0].status, PreflightStatus::Blocked);
        assert_eq!(safe_plan.status, TableSchemaStatus::Planned);
        assert!(
            safe_plan
                .statements
                .iter()
                .any(|statement| statement.sql.contains("MODIFY COLUMN `label` VARCHAR(4)"))
        );
    }

    #[test]
    fn blocks_data_rewriting_alter_for_nonempty_table_and_continues_independent_table() {
        let source = inventory(
            vec![
                table("blocked", vec![column("id", "bigint", false)], vec!["id"]),
                table(
                    "independent",
                    vec![column("id", "bigint", false)],
                    vec!["id"],
                ),
            ],
            vec![],
        );
        let target = inventory(
            vec![
                table(
                    "blocked",
                    vec![column("id", "varchar(8)", false)],
                    vec!["id"],
                ),
                table("independent", vec![column("id", "int", false)], vec!["id"]),
            ],
            vec![],
        );

        let preflight = FixtureCoercionPreflight::with_blockers(
            "blocked",
            "id",
            1,
            vec![vec!["1".to_string()]],
        );
        let plan = plan_schema_convergence(
            &source,
            &target,
            &["blocked".to_string(), "independent".to_string()],
            &preflight,
        )
        .expect("schema plan");

        assert_eq!(plan.tables[0].status, TableSchemaStatus::Planned);
        assert!(plan.tables[0].blockers[0].contains("coercion preflight"));
        assert_eq!(plan.tables[1].status, TableSchemaStatus::Planned);
    }

    #[test]
    fn blocks_signedness_change_and_decimal_narrowing_on_nonempty_tables() {
        let mut source_id = column("id", "bigint unsigned", false);
        source_id.data_type = "bigint".to_string();
        let mut target_id = column("id", "bigint", false);
        target_id.data_type = "bigint".to_string();
        let mut source_amount = column("amount", "decimal(8,2)", false);
        source_amount.data_type = "decimal".to_string();
        let mut target_amount = column("amount", "decimal(12,2)", false);
        target_amount.data_type = "decimal".to_string();
        let source = inventory(
            vec![table(
                "payments",
                vec![source_id, source_amount],
                vec!["id"],
            )],
            vec![],
        );
        let target = inventory(
            vec![table(
                "payments",
                vec![target_id, target_amount],
                vec!["id"],
            )],
            vec![],
        );

        let preflight = FixtureCoercionPreflight::with_blockers(
            "payments",
            "id",
            1,
            vec![vec!["1".to_string()]],
        )
        .and_blockers("payments", "amount", 1, vec![vec!["1".to_string()]]);
        let plan = plan_schema_convergence(&source, &target, &["payments".to_string()], &preflight)
            .expect("schema plan");

        assert_eq!(plan.tables[0].status, TableSchemaStatus::Planned);
        assert!(
            plan.tables[0]
                .blockers
                .iter()
                .any(|blocker| blocker.contains("amount"))
        );
        assert!(
            plan.tables[0]
                .blockers
                .iter()
                .any(|blocker| blocker.contains("id"))
        );
    }

    #[test]
    fn permits_safe_timestamp_mapping_and_varchar_widening_on_nonempty_tables() {
        let source = inventory(
            vec![table(
                "safe_changes",
                vec![
                    column("end_time", "timestamp", true),
                    column("label", "varchar(64)", true),
                ],
                vec!["end_time"],
            )],
            vec![],
        );
        let target = inventory(
            vec![table(
                "safe_changes",
                vec![
                    column("end_time", "datetime", true),
                    column("label", "varchar(16)", true),
                ],
                vec!["end_time"],
            )],
            vec![],
        );

        let plan = plan_schema_convergence(
            &source,
            &target,
            &["safe_changes".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("safe schema plan");

        assert_eq!(plan.tables[0].status, TableSchemaStatus::Planned);
        assert!(plan.tables[0].blockers.is_empty());
    }

    #[test]
    fn modeled_translation_and_sync_schema_are_identical_for_generated_create() {
        let source = inventory(
            vec![table(
                "tokens",
                vec![column("end_time", "timestamp", true)],
                vec!["end_time"],
            )],
            vec![],
        );
        let target = inventory(vec![], vec![]);
        let source_sql = render_create_table(&source, &source.tables[0], false);
        let translated = translate_modeled_ddl(&source_sql, &[])
            .unwrap_or_else(|error| panic!("modeled translator accepts {source_sql}: {error}"));
        let plan = plan_schema_convergence(
            &source,
            &target,
            &["tokens".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");

        assert_eq!(
            translated.target_sql.as_deref(),
            Some(plan.tables[0].statements[0].sql.as_str())
        );
    }

    #[test]
    fn modeled_translation_and_sync_schema_are_identical_for_generated_alter() {
        let source = inventory(
            vec![table(
                "accounts",
                vec![
                    column("id", "bigint", false),
                    column("label", "varchar(64)", true),
                ],
                vec!["id"],
            )],
            vec![],
        );
        let target = inventory(
            vec![table(
                "accounts",
                vec![column("id", "bigint", false)],
                vec!["id"],
            )],
            vec![],
        );
        let plan = plan_schema_convergence(
            &source,
            &target,
            &["accounts".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");
        let planned = plan.tables[0]
            .statements
            .iter()
            .find(|statement| statement.sql.contains("ADD COLUMN `label`"))
            .expect("planned label ALTER");
        let translated = translate_modeled_ddl(&planned.sql, &[])
            .expect("modeled translator accepts generated convergence ALTER");

        assert_eq!(translated.target_sql.as_deref(), Some(planned.sql.as_str()));
    }

    #[test]
    fn coercion_predicates_match_the_planned_conversion() {
        let varchar_source = column("label", "varchar(4)", false);
        let varchar_target = column("label", "varchar(8)", false);
        assert_eq!(
            coercion_blocker_predicate(&varchar_source, &varchar_target).unwrap(),
            "(CHAR_LENGTH(`label`) > 4)"
        );

        let mut unsigned_source = column("score", "tinyint unsigned", false);
        unsigned_source.data_type = "tinyint".to_string();
        let mut signed_target = column("score", "bigint", false);
        signed_target.data_type = "bigint".to_string();
        assert_eq!(
            coercion_blocker_predicate(&unsigned_source, &signed_target).unwrap(),
            "(`score` < 0 OR `score` > 255)"
        );
    }

    #[test]
    fn cross_width_signedness_changes_require_range_preflight() {
        let mut bigint_unsigned = column("score", "bigint unsigned", false);
        bigint_unsigned.data_type = "bigint".to_string();
        let mut int_signed = column("score", "int", false);
        int_signed.data_type = "int".to_string();

        assert!(column_change_requires_data_preflight(
            &bigint_unsigned,
            &int_signed
        ));
        assert_eq!(
            coercion_blocker_predicate(&bigint_unsigned, &int_signed).unwrap(),
            "(`score` < 0 OR `score` > 18446744073709551615)"
        );
        assert!(column_change_requires_data_preflight(
            &int_signed,
            &bigint_unsigned
        ));
        assert_eq!(
            coercion_blocker_predicate(&int_signed, &bigint_unsigned).unwrap(),
            "(`score` < -2147483648 OR `score` > 2147483647)"
        );
    }

    #[test]
    fn existing_table_engine_charset_and_collation_drift_plans_modeled_table_options() {
        let mut source_table = table("localized", vec![column("id", "bigint", false)], vec!["id"]);
        source_table.engine = Some("InnoDB".to_string());
        source_table.collation = Some("utf8mb4_unicode_ci".to_string());
        let mut target_table = source_table.clone();
        target_table.engine = Some("MyISAM".to_string());
        target_table.collation = Some("latin1_swedish_ci".to_string());

        let plan = plan_schema_convergence(
            &inventory(vec![source_table], vec![]),
            &inventory(vec![target_table], vec![]),
            &["localized".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");

        let statement = plan.tables[0]
            .statements
            .iter()
            .find(|statement| statement.objects == ["table:localized.options"])
            .expect("table options ALTER");
        assert_eq!(statement.phase, SchemaPhase::Columns);
        assert!(statement.sql.contains("ENGINE=InnoDB"), "{}", statement.sql);
        assert!(
            statement.sql.contains("DEFAULT CHARACTER SET=utf8mb4"),
            "{}",
            statement.sql
        );
        assert!(
            statement.sql.contains("COLLATE=utf8mb4_unicode_ci"),
            "{}",
            statement.sql
        );
    }

    #[test]
    fn column_comment_and_ordinal_drift_plan_modify_and_reorder() {
        let mut source_id = column("id", "bigint", false);
        source_id.ordinal_position = 1;
        let mut source_label = column("label", "varchar(32)", true);
        source_label.ordinal_position = 2;
        source_label.comment = "source label".to_string();
        let mut target_label = source_label.clone();
        target_label.ordinal_position = 1;
        target_label.comment = "target label".to_string();
        let mut target_id = source_id.clone();
        target_id.ordinal_position = 2;

        let source = inventory(
            vec![table("items", vec![source_id, source_label], vec!["id"])],
            vec![],
        );
        let target = inventory(
            vec![table("items", vec![target_label, target_id], vec!["id"])],
            vec![],
        );

        let plan = plan_schema_convergence(
            &source,
            &target,
            &["items".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");

        let label = plan.tables[0]
            .statements
            .iter()
            .find(|statement| statement.sql.contains("MODIFY COLUMN `label`"))
            .expect("comment/ordinal MODIFY");
        assert!(
            label.sql.contains("COMMENT 'source label'"),
            "{}",
            label.sql
        );
        assert!(label.sql.contains("AFTER `id`"), "{}", label.sql);
        assert_ne!(
            expected_target_table_fingerprint(&source.tables[0]).unwrap(),
            observed_target_table_fingerprint(&target.tables[0]).unwrap()
        );
    }

    #[test]
    fn unique_index_visibility_and_comment_converge_for_existing_and_missing_tables() {
        let mut source = inventory(
            vec![table(
                "items",
                vec![column("id", "bigint", false)],
                vec!["id"],
            )],
            vec![],
        );
        let source_index = IndexInventory {
            table: "items".to_string(),
            name: "uidx_items_id".to_string(),
            unique: true,
            index_type: "BTREE".to_string(),
            visible: false,
            comment: Some("source\\index".to_string()),
            columns: vec![IndexColumnInventory {
                name: "id".to_string(),
                sequence: 1,
                prefix_length: None,
                collation: None,
                order: "ASC".to_string(),
            }],
        };
        source.indexes.push(source_index.clone());

        let missing_plan = plan_schema_convergence(
            &source,
            &inventory(vec![], vec![]),
            &["items".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("missing-table schema plan");
        let create = &missing_plan.tables[0].statements[0].sql;
        assert!(create.contains("UNIQUE KEY `uidx_items_id`"), "{create}");
        assert!(
            create.contains("USING BTREE INVISIBLE COMMENT 'source\\\\index'"),
            "{create}"
        );

        let mut target = inventory(
            vec![table(
                "items",
                vec![column("id", "bigint", false)],
                vec!["id"],
            )],
            vec![],
        );
        let mut target_index = source_index;
        target_index.visible = true;
        target_index.comment = None;
        target.indexes.push(target_index);
        let existing_plan = plan_schema_convergence(
            &source,
            &target,
            &["items".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("existing-table schema plan");
        let sql = existing_plan.tables[0]
            .statements
            .iter()
            .map(|statement| statement.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(sql.contains("DROP INDEX `uidx_items_id`"), "{sql}");
        assert!(sql.contains("CREATE UNIQUE INDEX `uidx_items_id`"), "{sql}");
        assert!(
            sql.contains("USING BTREE INVISIBLE COMMENT 'source\\\\index'"),
            "{sql}"
        );
        let replacement = existing_plan.tables[0]
            .statements
            .iter()
            .find(|statement| statement.sql.contains("CREATE UNIQUE INDEX"))
            .expect("replacement create");
        assert!(
            replacement
                .prerequisites
                .contains(&"index:items.uidx_items_id".to_string())
        );
    }

    #[test]
    fn canonical_foreign_key_replacement_keeps_columns_indexes_and_parent_prerequisites() {
        let mut source = inventory(
            vec![
                table(
                    "children",
                    vec![
                        column("id", "bigint", false),
                        column("parent_id", "bigint", false),
                    ],
                    vec!["id"],
                ),
                table("parents", vec![column("id", "bigint", false)], vec!["id"]),
            ],
            vec![foreign_key("children", "parents")],
        );
        source.indexes.push(IndexInventory {
            table: "children".to_string(),
            name: "idx_parent".to_string(),
            unique: false,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: None,
            columns: vec![IndexColumnInventory {
                name: "parent_id".to_string(),
                sequence: 1,
                prefix_length: None,
                collation: None,
                order: "ASC".to_string(),
            }],
        });
        let mut plan = plan_schema_convergence(
            &source,
            &inventory(vec![], vec![]),
            &["parents".to_string(), "children".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");
        let canonical = CanonicalForeignKey {
            constraint_schema: "globalcomix".to_string(),
            constraint_name: "fk_children_parents".to_string(),
            child_schema: "globalcomix".to_string(),
            child_table: "children".to_string(),
            child_columns: vec!["parent_id".to_string()],
            parent_schema: "globalcomix".to_string(),
            parent_table: "parents".to_string(),
            parent_columns: vec!["id".to_string()],
            update_rule: "CASCADE".to_string(),
            delete_rule: "RESTRICT".to_string(),
            match_option: "NONE".to_string(),
            enforced: true,
        };

        let mut target_canonical = canonical.clone();
        target_canonical.update_rule = "RESTRICT".to_string();
        target_canonical.delete_rule = "CASCADE".to_string();

        append_canonical_foreign_key_plan(
            &mut plan,
            &source,
            &[canonical],
            &[target_canonical],
            "globalcomix",
        );

        let statement = plan
            .tables
            .iter()
            .find(|table| table.table == "children")
            .and_then(|table| {
                table.statements.iter().find(|statement| {
                    statement
                        .sql
                        .contains("ADD CONSTRAINT `fk_children_parents`")
                })
            })
            .expect("canonical foreign key statement");
        for prerequisite in [
            "column:children.parent_id",
            "index:children.idx_parent",
            "table:parents",
            "column:parents.id",
            "primary_key:parents",
        ] {
            assert!(
                statement
                    .prerequisites
                    .iter()
                    .any(|value| value == prerequisite),
                "missing prerequisite {prerequisite}: {:?}",
                statement.prerequisites
            );
        }
    }

    #[test]
    fn missing_table_create_renders_source_charset_and_collation() {
        let mut source_table = table("localized", vec![column("id", "bigint", false)], vec!["id"]);
        source_table.collation = Some("utf8mb4_unicode_ci".to_string());
        let source = inventory(vec![source_table], vec![]);

        let sql = render_create_table(&source, &source.tables[0], false);

        assert!(sql.contains("DEFAULT CHARACTER SET=utf8mb4"), "{sql}");
        assert!(sql.contains("COLLATE=utf8mb4_unicode_ci"), "{sql}");
    }

    /// Each case is a real `information_schema` disagreement measured between the prod MariaDB
    /// source and the do-managed MySQL target for a column that is already converged.
    #[test]
    fn already_converged_columns_compare_equal_across_both_engines() {
        let cases: &[(&str, ColumnInventory, ColumnInventory)] = &[
            (
                "integer display width",
                int_column("int(11) unsigned"),
                int_column("int unsigned"),
            ),
            (
                "tinyint display width",
                int_column("tinyint(1) unsigned"),
                int_column("tinyint unsigned"),
            ),
            (
                "mediumint display width",
                int_column("mediumint(8)"),
                int_column("mediumint"),
            ),
            (
                "literal NULL default",
                defaulted_column("int(11) unsigned", Some("NULL"), ""),
                defaulted_column("int unsigned", None, ""),
            ),
            (
                "quoted string default",
                defaulted_column("char(2)", Some("'en'"), ""),
                defaulted_column("char(2)", Some("en"), ""),
            ),
            (
                "quoted empty-string default",
                defaulted_column("varchar(32)", Some("''"), ""),
                defaulted_column("varchar(32)", Some(""), ""),
            ),
            (
                "current_timestamp default and DEFAULT_GENERATED marker",
                defaulted_column("datetime", Some("current_timestamp()"), ""),
                defaulted_column("datetime", Some("CURRENT_TIMESTAMP"), "DEFAULT_GENERATED"),
            ),
            (
                "on update current_timestamp extra",
                defaulted_column("datetime", None, "on update current_timestamp()"),
                defaulted_column(
                    "datetime",
                    None,
                    "DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
                ),
            ),
            (
                "fractional current_timestamp",
                defaulted_column(
                    "datetime(6)",
                    Some("current_timestamp(6)"),
                    "on update current_timestamp(6)",
                ),
                defaulted_column(
                    "datetime(6)",
                    Some("CURRENT_TIMESTAMP(6)"),
                    "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)",
                ),
            ),
            (
                "bit literal default",
                defaulted_column("bit(1)", Some("b'0'"), ""),
                defaulted_column("bit(1)", Some("b'0'"), ""),
            ),
            (
                "upper-case string default",
                defaulted_column("varchar(10)", Some("'POST'"), ""),
                defaulted_column("varchar(10)", Some("POST"), ""),
            ),
        ];
        for (label, source, target) in cases {
            assert!(columns_equal(source, target), "{label}");
        }

        let mut uca = defaulted_column("varchar(32)", None, "");
        uca.collation = Some("utf8mb4_uca1400_ai_ci".to_string());
        let mut mysql_collation = defaulted_column("varchar(32)", None, "");
        mysql_collation.collation = Some("utf8mb4_0900_ai_ci".to_string());
        assert!(columns_equal(&uca, &mysql_collation), "UCA-1400 collation");

        let mut source_generated = defaulted_column("tinyint(1)", None, "VIRTUAL GENERATED");
        source_generated.generated = Some(crate::inventory::GeneratedColumn {
            expression: "json_valid(`adaptations`) and json_length(`adaptations`) > 0".to_string(),
            generation_kind: "virtual".to_string(),
        });
        let mut target_generated = defaulted_column("tinyint", None, "VIRTUAL GENERATED");
        target_generated.generated = Some(crate::inventory::GeneratedColumn {
            expression: "(json_valid(`adaptations`) and (json_length(`adaptations`) > 0))"
                .to_string(),
            generation_kind: "VIRTUAL".to_string(),
        });
        assert!(
            columns_equal(&source_generated, &target_generated),
            "generated expression parentheses"
        );

        // A genuine difference still compares unequal.
        assert!(
            !columns_equal(
                &defaulted_column("char(2)", Some("'en'"), ""),
                &defaulted_column("char(2)", Some("us"), "")
            ),
            "different string defaults"
        );
        assert!(
            !columns_equal(
                &int_column("int(11) unsigned"),
                &int_column("bigint unsigned")
            ),
            "different integer widths"
        );
        assert!(
            !columns_equal(&int_column("int(11)"), &int_column("int unsigned")),
            "signedness"
        );
    }

    #[test]
    fn source_timestamp_converges_to_datetime_but_a_timestamp_target_does_not() {
        let mut source = defaulted_column("timestamp", None, "");
        source.data_type = "timestamp".to_string();
        let mut converged = defaulted_column("datetime", None, "");
        converged.data_type = "datetime".to_string();
        let mut unconverged = source.clone();
        unconverged.column_type = "timestamp".to_string();

        assert!(columns_equal(&source, &converged));
        assert!(!columns_equal(&source, &unconverged));
    }

    #[test]
    fn check_constraints_compare_through_the_clause_mysql_renders() {
        let source = CheckConstraint {
            table: "comics_enrichments".to_string(),
            name: "ck_comic_identity".to_string(),
            clause: "`comic_id` is not null or `comic_slug` is not null".to_string(),
        };
        let target = CheckConstraint {
            table: "comics_enrichments".to_string(),
            name: "ck_comic_identity".to_string(),
            clause: "((`comic_id` is not null) or (`comic_slug` is not null))".to_string(),
        };
        assert_eq!(source, target);

        let renamed = CheckConstraint {
            name: "comics_enrichments_chk_1".to_string(),
            ..target.clone()
        };
        assert_ne!(source, renamed);

        let other_column = CheckConstraint {
            clause: "((`comic_id` is not null) or (`comic_name` is not null))".to_string(),
            ..target
        };
        assert_ne!(source, other_column);
    }

    /// The relative form is a comparison device; it must never reach the target as SQL.
    #[test]
    fn a_relative_parent_schema_renders_unqualified() {
        let key = CanonicalForeignKey {
            constraint_schema: OWN_SCHEMA.to_string(),
            constraint_name: "fk_child_parent".to_string(),
            child_schema: OWN_SCHEMA.to_string(),
            child_table: "children".to_string(),
            child_columns: vec!["parent_id".to_string()],
            parent_schema: OWN_SCHEMA.to_string(),
            parent_table: "parents".to_string(),
            parent_columns: vec!["id".to_string()],
            update_rule: "RESTRICT".to_string(),
            delete_rule: "CASCADE".to_string(),
            match_option: "NONE".to_string(),
            enforced: true,
        };

        let sql = render_canonical_foreign_key(&key, "globalcomix");

        assert!(sql.contains("REFERENCES `parents` (`id`)"), "{sql}");
        assert!(!sql.contains(OWN_SCHEMA), "{sql}");
    }

    #[test]
    fn a_same_schema_parent_matches_a_differently_named_target_database() {
        let source_key = ForeignKeyInventory {
            table: "children".to_string(),
            name: "fk_parent".to_string(),
            columns: vec!["parent_id".to_string()],
            referenced_schema: "globalcomix".to_string(),
            referenced_table: "parents".to_string(),
            referenced_columns: vec!["id".to_string()],
        };
        let target_key = ForeignKeyInventory {
            referenced_schema: "globalcomix_rehearsal".to_string(),
            ..source_key.clone()
        };
        let cross_schema_key = ForeignKeyInventory {
            referenced_schema: "other".to_string(),
            ..source_key.clone()
        };

        assert!(foreign_keys_equal(
            &source_key,
            "globalcomix",
            &target_key,
            "globalcomix_rehearsal"
        ));
        assert!(!foreign_keys_equal(
            &source_key,
            "globalcomix",
            &cross_schema_key,
            "globalcomix_rehearsal"
        ));
        assert!(foreign_keys_equal(
            &cross_schema_key,
            "globalcomix",
            &cross_schema_key,
            "globalcomix_rehearsal"
        ));
    }

    #[test]
    fn check_names_reused_across_source_tables_are_qualified_for_mysql() {
        let source = vec![
            CheckConstraint {
                table: "income_reconciliations".to_string(),
                name: "config_json".to_string(),
                clause: "json_valid(`config_json`)".to_string(),
            },
            CheckConstraint {
                table: "income_reconciliations_history".to_string(),
                name: "config_json".to_string(),
                clause: "json_valid(`config_json`)".to_string(),
            },
            CheckConstraint {
                table: "comics_enrichments".to_string(),
                name: "ck_comic_identity".to_string(),
                clause: "`comic_id` is not null".to_string(),
            },
        ];

        let names = target_check_constraints(&source)
            .into_iter()
            .map(|check| (check.table, check.name))
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                (
                    "income_reconciliations".to_string(),
                    "income_reconciliations_config_json".to_string()
                ),
                (
                    "income_reconciliations_history".to_string(),
                    "income_reconciliations_history_config_json".to_string()
                ),
                (
                    "comics_enrichments".to_string(),
                    "ck_comic_identity".to_string()
                ),
            ]
        );
    }

    /// MariaDB permits one check name on several tables, so the source inventory must report the
    /// owning table rather than joining every same-named constraint to every table.
    #[test]
    fn check_constraint_inventory_is_scoped_per_endpoint() {
        assert!(
            check_constraint_query(InventoryEndpointRole::Source, None)
                .contains("FROM information_schema.CHECK_CONSTRAINTS")
        );
        assert!(!check_constraint_query(InventoryEndpointRole::Source, None).contains("JOIN"));
        assert!(check_constraint_query(InventoryEndpointRole::Target, None).contains("JOIN"));
    }

    #[test]
    fn enum_values_keep_their_case_while_the_type_name_is_recased() {
        let mut channel = defaulted_column("enum('dev','Prod')", Some("'dev'"), "");
        channel.is_nullable = false;

        assert_eq!(
            render_column(&channel),
            "`value` ENUM('dev','Prod') NOT NULL DEFAULT 'dev'"
        );
        assert_eq!(
            canonical_column_type("ENUM('dev','Prod')"),
            "enum('dev','Prod')"
        );
        assert!(!columns_equal(
            &defaulted_column("enum('dev','Prod')", None, ""),
            &defaulted_column("enum('dev','prod')", None, "")
        ));
    }

    #[test]
    fn literal_defaults_render_without_double_quoting() {
        assert_eq!(render_default("'en'"), "'en'");
        assert_eq!(render_default("''"), "''");
        assert_eq!(render_default("b'0'"), "b'0'");
        assert_eq!(render_default("0"), "'0'");
        assert_eq!(render_default("current_timestamp()"), "CURRENT_TIMESTAMP()");
        assert_eq!(render_default("it's"), "'it''s'");
    }

    fn int_column(column_type: &str) -> ColumnInventory {
        defaulted_column(column_type, None, "")
    }

    fn defaulted_column(column_type: &str, default: Option<&str>, extra: &str) -> ColumnInventory {
        let mut column = column("value", column_type, true);
        // Both engines report DATA_TYPE without the display width or the attributes.
        column.data_type = column_type
            .split(['(', ' '])
            .next()
            .expect("column type")
            .to_string();
        column.default_value = default.map(str::to_string);
        column.extra = extra.to_string();
        column
    }

    #[test]
    fn non_unique_parent_key_gains_a_synthesized_unique_index_the_target_keeps() {
        let mut source = inventory(
            vec![
                table(
                    "users",
                    vec![
                        column("id", "mediumint unsigned", false),
                        column("name", "varchar(255)", false),
                    ],
                    vec!["id"],
                ),
                table(
                    "users_replies",
                    vec![
                        column("id", "bigint", false),
                        column("user_id", "mediumint unsigned", false),
                        column("user_name", "varchar(255)", false),
                    ],
                    vec!["id"],
                ),
            ],
            vec![ForeignKeyInventory {
                table: "users_replies".to_string(),
                name: "users_replies_ibfk_1".to_string(),
                columns: vec!["user_id".to_string(), "user_name".to_string()],
                referenced_schema: "globalcomix".to_string(),
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string(), "name".to_string()],
            }],
        );
        // MariaDB only requires a non-unique parent index for this constraint.
        source.indexes.push(IndexInventory {
            table: "users".to_string(),
            name: "fk_user".to_string(),
            unique: false,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: None,
            columns: vec![
                IndexColumnInventory {
                    name: "id".to_string(),
                    sequence: 1,
                    prefix_length: None,
                    collation: Some("A".to_string()),
                    order: String::new(),
                },
                IndexColumnInventory {
                    name: "name".to_string(),
                    sequence: 2,
                    prefix_length: None,
                    collation: Some("A".to_string()),
                    order: String::new(),
                },
            ],
        });

        let expected = expected_target_inventory(&source);
        let synthesized = expected
            .indexes
            .iter()
            .find(|index| index.name == "uq_cdc_users_id_name")
            .expect("synthesized parent index");
        assert!(synthesized.unique);
        assert_eq!(synthesized.table, "users");
        assert_eq!(
            synthesized
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name"]
        );

        // A target that already holds the synthesized index keeps it, and the child foreign
        // key waits for it.
        let mut target = source.clone();
        target.indexes.push(synthesized.clone());
        target.foreign_keys.clear();
        let plan = plan_schema_convergence(
            &source,
            &target,
            &["users".to_string(), "users_replies".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");
        let sql = plan
            .tables
            .iter()
            .flat_map(|table| table.statements.iter())
            .map(|statement| statement.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!sql.contains("DROP INDEX `uq_cdc_users_id_name`"), "{sql}");

        let foreign_key = plan
            .tables
            .iter()
            .flat_map(|table| table.statements.iter())
            .find(|statement| {
                statement
                    .sql
                    .contains("ADD CONSTRAINT `users_replies_ibfk_1`")
            })
            .expect("foreign key statement");
        assert!(
            foreign_key
                .prerequisites
                .contains(&"index:users.uq_cdc_users_id_name".to_string()),
            "{:?}",
            foreign_key.prerequisites
        );

        // A target without it is told to create it.
        target
            .indexes
            .retain(|index| index.name != "uq_cdc_users_id_name");
        let create_plan = plan_schema_convergence(
            &source,
            &target,
            &["users".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");
        let create_sql = create_plan.tables[0]
            .statements
            .iter()
            .map(|statement| statement.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            create_sql
                .contains("CREATE UNIQUE INDEX `uq_cdc_users_id_name` ON `users` (`id`,`name`)"),
            "{create_sql}"
        );
    }

    #[test]
    fn unique_parent_key_needs_no_synthesized_index() {
        let mut source = inventory(
            vec![table(
                "guests",
                vec![
                    column("guest_id", "int unsigned", false),
                    column("guest_hash", "varchar(64)", false),
                ],
                vec!["guest_id"],
            )],
            vec![ForeignKeyInventory {
                table: "sessions".to_string(),
                name: "fk_sessions_guest".to_string(),
                columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
                referenced_schema: "globalcomix".to_string(),
                referenced_table: "guests".to_string(),
                referenced_columns: vec!["guest_id".to_string(), "guest_hash".to_string()],
            }],
        );
        source.indexes.push(IndexInventory {
            table: "guests".to_string(),
            name: "idx_guest_fk".to_string(),
            unique: true,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: None,
            columns: vec![
                IndexColumnInventory {
                    name: "guest_id".to_string(),
                    sequence: 1,
                    prefix_length: None,
                    collation: Some("A".to_string()),
                    order: String::new(),
                },
                IndexColumnInventory {
                    name: "guest_hash".to_string(),
                    sequence: 2,
                    prefix_length: None,
                    collation: Some("A".to_string()),
                    order: String::new(),
                },
            ],
        });

        assert_eq!(
            expected_target_inventory(&source).indexes,
            source.indexes,
            "a unique parent index already satisfies MySQL"
        );
    }

    #[test]
    fn column_character_set_and_collation_precede_nullability_default_and_generation() {
        let mut email = column("email", "varchar(255)", true);
        email.character_set = Some("utf8mb3".to_string());
        email.collation = Some("utf8mb3_bin".to_string());

        assert_eq!(
            render_column(&email),
            "`email` VARCHAR(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NULL DEFAULT NULL"
        );

        let mut slug = column("slug", "varchar(64)", false);
        slug.character_set = Some("ascii".to_string());
        slug.collation = Some("ascii_bin".to_string());
        slug.generated = Some(crate::inventory::GeneratedColumn {
            expression: "lower(`email`)".to_string(),
            generation_kind: "stored".to_string(),
        });

        assert_eq!(
            render_column(&slug),
            "`slug` VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin GENERATED ALWAYS AS (lower(`email`)) STORED"
        );
    }

    #[test]
    fn shared_translation_maps_extended_timestamp_and_is_used_by_schema_plan() {
        let source = inventory(
            vec![table(
                "tokens",
                vec![column("end_time", "timestamp", true)],
                vec!["end_time"],
            )],
            vec![],
        );
        let target = inventory(vec![], vec![]);

        let translated = crate::live::ddl_semantics::translate_modeled_ddl(
            "CREATE TABLE `tokens` (`end_time` TIMESTAMP NULL, PRIMARY KEY (`end_time`)) ENGINE=InnoDB",
            &[],
        )
        .expect("shared translation");
        let plan = plan_schema_convergence(
            &source,
            &target,
            &["tokens".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");

        assert!(translated.target_sql.unwrap().contains("DATETIME"));
        assert!(plan.tables[0].statements[0].sql.contains("DATETIME"));
    }

    #[test]
    fn statement_execution_continues_and_skips_only_explicit_prerequisites() {
        let plan = SchemaConvergencePlan {
            source_fingerprint: "source".to_string(),
            target_fingerprint: "target".to_string(),
            tables: vec![TableSchemaPlan {
                table: "accounts".to_string(),
                source_fingerprint: "source".to_string(),
                target_fingerprint: "target".to_string(),
                dependencies: Vec::new(),
                status: TableSchemaStatus::Planned,
                blockers: Vec::new(),
                preflights: Vec::new(),
                statements: vec![
                    planned_statement(
                        "ALTER TABLE `accounts` ADD COLUMN `broken` BIGINT",
                        vec!["column:accounts.broken"],
                        Vec::new(),
                    ),
                    planned_statement(
                        "ALTER TABLE `accounts` ADD COLUMN `independent` BIGINT",
                        vec!["column:accounts.independent"],
                        Vec::new(),
                    ),
                    planned_statement(
                        "ALTER TABLE `accounts` ADD KEY `broken_key` (`broken`)",
                        vec!["index:accounts.broken_key"],
                        vec!["column:accounts.broken"],
                    ),
                ],
            }],
        };
        let mut executor = RecordingExecutor::failing_sql("ADD COLUMN `broken`");

        let report = execute_schema_plan(plan, &mut executor, &|_| {
            vec!["schema remains divergent".to_string()]
        });

        assert_eq!(executor.executed.len(), 2);
        assert!(executor.executed[1].contains("independent"));
        assert_eq!(report.tables[0].executions[0].status, "failed");
        assert_eq!(report.tables[0].executions[1].status, "executed");
        assert_eq!(report.tables[0].executions[2].status, "skipped");
        assert!(
            report.tables[0].executions[2]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("column:accounts.broken"))
        );
    }

    #[test]
    fn execution_continues_after_failure_and_skips_failed_dependencies() {
        let plan = SchemaConvergencePlan {
            source_fingerprint: "source".to_string(),
            target_fingerprint: "target".to_string(),
            tables: vec![
                table_plan(
                    "parents",
                    vec!["ALTER TABLE `parents` ADD COLUMN `x` BIGINT"],
                ),
                dependent_table_plan("children", "parents"),
                table_plan(
                    "independent",
                    vec!["ALTER TABLE `independent` ADD COLUMN `x` BIGINT"],
                ),
            ],
        };
        let mut executor = RecordingExecutor::failing("parents");

        let report = execute_schema_plan(plan, &mut executor, &|_| Vec::new());

        assert_eq!(report.tables[0].status, TableSchemaStatus::Failed);
        assert_eq!(report.tables[1].status, TableSchemaStatus::Skipped);
        assert_eq!(report.tables[2].status, TableSchemaStatus::Converged);
        assert_eq!(report.overall_status, OverallSchemaStatus::Partial);
        assert_eq!(executor.executed.len(), 2);
    }

    #[test]
    fn orders_selected_parent_before_child_and_drops_target_only_foreign_key() {
        let source = inventory(
            vec![
                table(
                    "children",
                    vec![
                        column("id", "bigint", false),
                        column("parent_id", "bigint", false),
                    ],
                    vec!["id"],
                ),
                table("parents", vec![column("id", "bigint", false)], vec!["id"]),
            ],
            vec![foreign_key("children", "parents")],
        );
        let target = inventory(
            source.tables.clone(),
            vec![ForeignKeyInventory {
                table: "children".to_string(),
                name: "obsolete_fk".to_string(),
                columns: vec!["parent_id".to_string()],
                referenced_schema: "globalcomix".to_string(),
                referenced_table: "legacy_parents".to_string(),
                referenced_columns: vec!["id".to_string()],
            }],
        );

        let plan = plan_schema_convergence(
            &source,
            &target,
            &["children".to_string(), "parents".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("schema plan");

        assert_eq!(
            plan.tables
                .iter()
                .map(|table| table.table.as_str())
                .collect::<Vec<_>>(),
            vec!["parents", "children"]
        );
        assert!(
            plan.tables[1]
                .statements
                .iter()
                .any(|statement| statement.sql.contains("DROP FOREIGN KEY `obsolete_fk`"))
        );
    }

    #[test]
    fn semantic_fingerprint_treats_source_timestamp_as_target_datetime() {
        let source = table(
            "tokens",
            vec![column("end_time", "timestamp", true)],
            vec!["end_time"],
        );
        let target = table(
            "tokens",
            vec![column("end_time", "datetime", true)],
            vec!["end_time"],
        );

        assert_eq!(
            expected_target_table_fingerprint(&source).unwrap(),
            observed_target_table_fingerprint(&target).unwrap()
        );
    }

    #[test]
    fn check_addition_depends_on_referenced_selected_table_columns() {
        let mut plan = SchemaConvergencePlan {
            source_fingerprint: "source".to_string(),
            target_fingerprint: "target".to_string(),
            tables: vec![table_plan(
                "accounts",
                vec!["ALTER TABLE `accounts` MODIFY COLUMN `balance` BIGINT"],
            )],
        };
        plan.tables[0].statements[0].objects = vec!["column:accounts.balance".to_string()];
        let source_inventory = inventory(
            vec![table(
                "accounts",
                vec![
                    column("id", "bigint", false),
                    column("balance", "bigint", false),
                ],
                vec!["id"],
            )],
            vec![],
        );
        let checks = vec![CheckConstraint {
            table: "accounts".to_string(),
            name: "positive_balance".to_string(),
            clause: "(balance >= 0)".to_string(),
        }];

        append_check_constraint_plan(&mut plan, &source_inventory, &checks, &[]);

        let check = plan.tables[0]
            .statements
            .iter()
            .find(|statement| statement.sql.contains("ADD CONSTRAINT `positive_balance`"))
            .expect("CHECK addition");
        assert_eq!(check.prerequisites, vec!["column:accounts.balance"]);

        let mut executor = RecordingExecutor::failing_sql("MODIFY COLUMN `balance`");
        let report = execute_schema_plan(plan, &mut executor, &|_| {
            vec!["schema remains divergent".to_string()]
        });
        assert!(report.tables[0].executions.iter().any(|execution| {
            execution.sql.contains("ADD CONSTRAINT `positive_balance`")
                && execution.status == "skipped"
        }));
    }

    #[test]
    fn converges_check_constraints_by_dropping_target_only_and_adding_source() {
        let mut plan = SchemaConvergencePlan {
            source_fingerprint: "source".to_string(),
            target_fingerprint: "target".to_string(),
            tables: vec![table_plan("accounts", vec![])],
        };
        let source = vec![CheckConstraint {
            table: "accounts".to_string(),
            name: "positive_balance".to_string(),
            clause: "(`balance` >= 0)".to_string(),
        }];
        let target = vec![CheckConstraint {
            table: "accounts".to_string(),
            name: "legacy_balance".to_string(),
            clause: "(`balance` > -100)".to_string(),
        }];

        let source_inventory = inventory(
            vec![table(
                "accounts",
                vec![column("balance", "bigint", false)],
                vec!["balance"],
            )],
            vec![],
        );
        append_check_constraint_plan(&mut plan, &source_inventory, &source, &target);

        assert!(
            plan.tables[0].statements[0]
                .sql
                .contains("DROP CHECK `legacy_balance`")
        );
        assert!(
            plan.tables[0].statements[1]
                .sql
                .contains("ADD CONSTRAINT `positive_balance` CHECK")
        );
    }

    #[test]
    fn every_command_termination_is_structured_json() {
        let (failed_json, failed_exit) =
            render_sync_schema_termination(Err("parse failed".to_string()));
        let failed: serde_json::Value =
            serde_json::from_str(&failed_json).expect("failed JSON report");
        assert_eq!(failed_exit, 2);
        assert_eq!(failed["overall_status"], "failed");
        assert_eq!(failed["error"], "parse failed");

        let report = SchemaConvergenceReport {
            transformation_version: DDL_TRANSFORMATION_VERSION.to_string(),
            source_fingerprint: "source".to_string(),
            target_fingerprint: "target".to_string(),
            overall_status: OverallSchemaStatus::Partial,
            error: Some("one or more selected tables remain divergent".to_string()),
            tables: Vec::new(),
        };
        let (partial_json, partial_exit) = render_sync_schema_termination(Ok(report));
        let partial: serde_json::Value =
            serde_json::from_str(&partial_json).expect("partial JSON report");
        assert_eq!(partial_exit, 1);
        assert_eq!(partial["overall_status"], "partial");
        assert!(partial["error"].is_string());
    }

    #[test]
    fn catalog_and_repeated_tables_form_one_deduplicated_selection() {
        let catalog = r#"{"tables":[{"name":"alpha"},{"name":"beta"},{"name":"alpha"}]}"#;
        let selected = selected_tables(
            &["beta".to_string(), "gamma".to_string()],
            Some(catalog.as_bytes()),
        )
        .expect("selected tables");

        assert_eq!(selected, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn all_tables_selection_resolves_every_source_table_from_the_inventory() {
        let selection = schema_selection(&[], None, true).expect("all-tables selection");
        assert_eq!(selection, SchemaSelection::AllSourceTables);

        let source = inventory(
            vec![
                table("zeta", vec![column("id", "int", false)], vec!["id"]),
                table("alpha", vec![column("id", "int", false)], vec!["id"]),
            ],
            vec![],
        );
        let selected =
            resolve_schema_selection(&selection, &source).expect("resolved all-tables selection");

        assert_eq!(selected, vec!["alpha", "zeta"]);
    }

    #[test]
    fn all_tables_selection_rejects_named_tables_and_an_empty_source() {
        let combined_table = schema_selection(&["alpha".to_string()], None, true)
            .expect_err("--table with --all-tables");
        assert_eq!(
            combined_table,
            "sync-schema --all-tables cannot be combined with --table or --catalog"
        );

        let combined_catalog =
            schema_selection(&[], Some(br#"{"tables":[{"name":"alpha"}]}"#), true)
                .expect_err("--catalog with --all-tables");
        assert_eq!(
            combined_catalog,
            "sync-schema --all-tables cannot be combined with --table or --catalog"
        );

        let empty = resolve_schema_selection(
            &SchemaSelection::AllSourceTables,
            &inventory(vec![], vec![]),
        )
        .expect_err("empty source inventory");
        assert_eq!(
            empty,
            "sync-schema --all-tables found no source base tables"
        );
    }

    #[test]
    fn named_selection_requires_a_table_catalog_or_all_tables() {
        let error = schema_selection(&[], None, false).expect_err("empty selection");
        assert_eq!(
            error,
            "sync-schema requires at least one --table, --catalog, or --all-tables true"
        );
    }

    #[test]
    fn parses_all_tables_selection_from_the_command_line() {
        let config = parse_sync_schema_config(sync_schema_args(&["--all-tables", "true"]))
            .expect("all-tables config");
        assert!(config.all_tables);
        assert!(config.tables.is_empty());
        assert!(config.catalog.is_none());

        let default = parse_sync_schema_config(sync_schema_args(&["--table", "alpha"]))
            .expect("named config");
        assert!(!default.all_tables);

        let invalid = parse_sync_schema_config(sync_schema_args(&["--all-tables", "yes"]))
            .expect_err("invalid boolean");
        assert_eq!(invalid, "--all-tables must be true or false");
    }

    fn sync_schema_args(extra: &[&str]) -> Vec<String> {
        // SAFETY: single-threaded test setup for the password environment lookup.
        unsafe {
            std::env::set_var("SYNC_SCHEMA_TEST_PASSWORD", "secret");
        }
        let mut args = [
            "--source-host",
            "source.example",
            "--source-user",
            "reader",
            "--source-password-env",
            "SYNC_SCHEMA_TEST_PASSWORD",
            "--source-database",
            "globalcomix",
            "--target-host",
            "target.example",
            "--target-user",
            "writer",
            "--target-password-env",
            "SYNC_SCHEMA_TEST_PASSWORD",
            "--target-database",
            "globalcomix",
            "--target-tls-ca-file",
            "/etc/ca.pem",
        ]
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
        args.extend(extra.iter().map(|value| value.to_string()));
        args
    }

    fn inventory(
        tables: Vec<TableInventory>,
        foreign_keys: Vec<ForeignKeyInventory>,
    ) -> SchemaInventory {
        SchemaInventory {
            schema: "globalcomix".to_string(),
            tables,
            indexes: vec![],
            foreign_keys,
            views: vec![],
            triggers: vec![],
            routines: vec![],
            events: vec![],
        }
    }

    fn table(name: &str, columns: Vec<ColumnInventory>, primary_key: Vec<&str>) -> TableInventory {
        TableInventory {
            name: name.to_string(),
            table_type: "BASE TABLE".to_string(),
            engine: Some("InnoDB".to_string()),
            collation: Some("utf8mb4_0900_ai_ci".to_string()),
            primary_key: primary_key.into_iter().map(str::to_string).collect(),
            columns,
        }
    }

    #[test]
    fn all_schema_statement_families_are_translated_before_execution() {
        let statements = [
            "ALTER TABLE `items` DROP PRIMARY KEY",
            "ALTER TABLE `items` ADD PRIMARY KEY (`id`)",
            "CREATE INDEX `idx_label` ON `items` (`label`)",
            "DROP INDEX `idx_label` ON `items`",
            "ALTER TABLE `items` ADD CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES `parents` (`id`)",
            "ALTER TABLE `items` DROP FOREIGN KEY `fk_parent`",
            "ALTER TABLE `items` ADD CONSTRAINT `positive_id` CHECK (`id` > 0)",
            "ALTER TABLE `items` DROP CHECK `positive_id`",
            "ALTER TABLE `items` DROP COLUMN IF EXISTS `obsolete`",
        ];

        for sql in statements {
            let target_columns = if sql.contains("DROP COLUMN") {
                vec!["obsolete".to_string()]
            } else {
                Vec::new()
            };
            translate_statement(
                SchemaPhase::Constraints,
                sql.to_string(),
                vec![],
                &target_columns,
            )
            .unwrap_or_else(|error| {
                panic!("statement bypassed or failed translation: {sql}: {error}")
            });
        }
    }

    #[test]
    fn generated_change_without_predicate_does_not_block_unrelated_column_work() {
        let mut source_generated = column("computed", "bigint", true);
        source_generated.generated = Some(crate::inventory::GeneratedColumn {
            expression: "(`id` + 1)".to_string(),
            generation_kind: "stored".to_string(),
        });
        let mut target_generated = source_generated.clone();
        target_generated.generated = Some(crate::inventory::GeneratedColumn {
            expression: "(`id` + 2)".to_string(),
            generation_kind: "stored".to_string(),
        });
        let source = inventory(
            vec![table(
                "items",
                vec![
                    column("id", "bigint", false),
                    source_generated,
                    column("label", "varchar(64)", true),
                ],
                vec!["id"],
            )],
            vec![],
        );
        let target = inventory(
            vec![table(
                "items",
                vec![
                    column("id", "bigint", false),
                    target_generated,
                    column("label", "varchar(16)", true),
                ],
                vec!["id"],
            )],
            vec![],
        );
        let preflight = FailingGeneratedPreflight;

        let plan = plan_schema_convergence(&source, &target, &["items".to_string()], &preflight)
            .expect("planning produces a structured table plan");

        assert_eq!(plan.tables[0].status, TableSchemaStatus::Planned);
        assert!(
            plan.tables[0]
                .statements
                .iter()
                .any(|statement| statement.sql.contains("MODIFY COLUMN `label`"))
        );
    }

    #[test]
    fn dependency_cycle_is_structured_per_table_and_independent_table_continues() {
        let source = inventory(
            vec![
                table(
                    "cycle_a",
                    vec![
                        column("id", "bigint", false),
                        column("parent_id", "bigint", false),
                    ],
                    vec!["id"],
                ),
                table(
                    "cycle_b",
                    vec![
                        column("id", "bigint", false),
                        column("parent_id", "bigint", false),
                    ],
                    vec!["id"],
                ),
                table(
                    "independent",
                    vec![column("id", "bigint", false)],
                    vec!["id"],
                ),
            ],
            vec![
                foreign_key("cycle_a", "cycle_b"),
                foreign_key("cycle_b", "cycle_a"),
            ],
        );
        let target = inventory(vec![], vec![]);

        let plan = plan_schema_convergence(
            &source,
            &target,
            &[
                "cycle_a".to_string(),
                "cycle_b".to_string(),
                "independent".to_string(),
            ],
            &FixtureCoercionPreflight::default(),
        )
        .expect("dependency cycle must not abort independent planning");

        assert_eq!(
            plan.tables
                .iter()
                .find(|table| table.table == "independent")
                .unwrap()
                .status,
            TableSchemaStatus::Planned
        );
        for table in ["cycle_a", "cycle_b"] {
            let failed = plan.tables.iter().find(|plan| plan.table == table).unwrap();
            assert_eq!(failed.status, TableSchemaStatus::Failed);
            assert!(failed.blockers[0].contains("dependency cycle"));
        }
    }

    #[test]
    fn planning_failure_is_structured_per_table_and_independent_table_continues() {
        let source = inventory(
            vec![
                table(
                    "unsupported",
                    vec![column("kind", "set('a','b')", false)],
                    vec!["kind"],
                ),
                table(
                    "independent",
                    vec![column("id", "bigint", false)],
                    vec!["id"],
                ),
            ],
            vec![],
        );
        let target = inventory(vec![], vec![]);

        let plan = plan_schema_convergence(
            &source,
            &target,
            &["unsupported".to_string(), "independent".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("one table planning failure must not abort the command");

        let unsupported = plan
            .tables
            .iter()
            .find(|table| table.table == "unsupported")
            .unwrap();
        let independent = plan
            .tables
            .iter()
            .find(|table| table.table == "independent")
            .unwrap();
        assert_eq!(unsupported.status, TableSchemaStatus::Failed);
        assert!(!unsupported.blockers.is_empty());
        assert_eq!(independent.status, TableSchemaStatus::Planned);
    }

    #[test]
    fn blocked_and_dependency_skipped_tables_are_always_reinventoried() {
        let mut blocked = table_plan("blocked", vec![]);
        blocked.status = TableSchemaStatus::Failed;
        let skipped = dependent_table_plan("children", "blocked");
        let calls = std::cell::RefCell::new(Vec::new());
        let mut executor = RecordingExecutor::failing_sql("never");

        let report = execute_schema_plan(
            SchemaConvergencePlan {
                source_fingerprint: "source".to_string(),
                target_fingerprint: "target".to_string(),
                tables: vec![blocked, skipped],
            },
            &mut executor,
            &|table| {
                calls.borrow_mut().push(table.to_string());
                vec![format!("{table} remains divergent")]
            },
        );

        assert_eq!(&*calls.borrow(), &["blocked", "children"]);
        assert_eq!(
            report.tables[0].final_differences,
            vec!["blocked remains divergent"]
        );
        assert_eq!(
            report.tables[1].final_differences,
            vec!["children remains divergent"]
        );
    }

    #[test]
    fn preflight_sampling_uses_actual_target_primary_key_inventory() {
        let source = inventory(
            vec![table(
                "items",
                vec![
                    column("source_id", "bigint", false),
                    column("label", "varchar(4)", false),
                ],
                vec!["source_id"],
            )],
            vec![],
        );
        let target = inventory(
            vec![table(
                "items",
                vec![
                    column("target_id", "bigint", false),
                    column("label", "varchar(8)", false),
                ],
                vec!["target_id"],
            )],
            vec![],
        );
        let preflight = CapturingTargetPrimaryKeyPreflight::default();

        plan_schema_convergence(&source, &target, &["items".to_string()], &preflight)
            .expect("schema plan");

        assert_eq!(
            &*preflight.primary_keys.borrow(),
            &[vec!["target_id".to_string()]]
        );
    }

    #[test]
    fn generated_change_on_proven_empty_table_is_planned() {
        let mut source_generated = column("computed", "bigint", true);
        source_generated.generated = Some(crate::inventory::GeneratedColumn {
            expression: "(`id` + 1)".to_string(),
            generation_kind: "stored".to_string(),
        });
        let mut target_generated = source_generated.clone();
        target_generated.generated = Some(crate::inventory::GeneratedColumn {
            expression: "(`id` + 2)".to_string(),
            generation_kind: "stored".to_string(),
        });
        let source = inventory(
            vec![table(
                "items",
                vec![column("id", "bigint", false), source_generated],
                vec!["id"],
            )],
            vec![],
        );
        let target = inventory(
            vec![table(
                "items",
                vec![column("id", "bigint", false), target_generated],
                vec!["id"],
            )],
            vec![],
        );

        let plan = plan_schema_convergence(
            &source,
            &target,
            &["items".to_string()],
            &FixtureCoercionPreflight::default(),
        )
        .expect("empty table proof permits generated expression change");

        assert_eq!(plan.tables[0].preflights[0].count, 0);
        assert_eq!(plan.tables[0].preflights[0].status, PreflightStatus::Passed);
        assert!(
            plan.tables[0]
                .statements
                .iter()
                .any(|statement| statement.sql.contains("MODIFY COLUMN `computed`"))
        );
    }

    #[test]
    fn preflight_events_are_structured_and_include_zero_count_and_sample_failure() {
        let blockers = coercion_blockers(
            Some("CHAR_LENGTH(`label`) > 4".to_string()),
            2,
            Err("sample query failed".to_string()),
        );
        let event = preflight_event("label", blockers);
        let zero = CoercionPreflightEvent {
            column: "safe".to_string(),
            predicate: Some("`safe` IS NULL".to_string()),
            count: 0,
            sample_primary_keys: Vec::new(),
            status: PreflightStatus::Passed,
            error: None,
        };

        let json = serde_json::to_value(vec![event, zero]).expect("preflight JSON");
        assert_eq!(json[0]["count"], 2);
        assert_eq!(json[0]["status"], "blocked");
        assert_eq!(json[0]["error"], "sample query failed");
        assert_eq!(json[1]["count"], 0);
        assert_eq!(json[1]["status"], "passed");
    }

    #[derive(Default)]
    struct CapturingTargetPrimaryKeyPreflight {
        primary_keys: std::cell::RefCell<Vec<Vec<String>>>,
    }

    impl SchemaCoercionPreflight for CapturingTargetPrimaryKeyPreflight {
        fn inspect(
            &self,
            table: &TableInventory,
            _source: &ColumnInventory,
            _target: &ColumnInventory,
        ) -> Result<CoercionBlockers, String> {
            self.primary_keys
                .borrow_mut()
                .push(table.primary_key.clone());
            Ok(CoercionBlockers {
                predicate: Some("fixture predicate".to_string()),
                count: 0,
                sample_primary_keys: Vec::new(),
                sample_error: None,
            })
        }
    }

    struct FailingGeneratedPreflight;

    impl SchemaCoercionPreflight for FailingGeneratedPreflight {
        fn inspect(
            &self,
            _table: &TableInventory,
            source: &ColumnInventory,
            _target: &ColumnInventory,
        ) -> Result<CoercionBlockers, String> {
            if source.name == "computed" {
                Err("generated-column conversion has no safe target-data predicate".to_string())
            } else {
                Ok(CoercionBlockers {
                    predicate: Some("fixture predicate".to_string()),
                    count: 0,
                    sample_primary_keys: Vec::new(),
                    sample_error: None,
                })
            }
        }
    }

    fn column(name: &str, column_type: &str, nullable: bool) -> ColumnInventory {
        ColumnInventory {
            name: name.to_string(),
            ordinal_position: 1,
            column_type: column_type.to_string(),
            data_type: column_type.split('(').next().unwrap().to_string(),
            is_nullable: nullable,
            character_set: None,
            collation: None,
            default_value: None,
            extra: String::new(),
            comment: String::new(),
            generated: None,
        }
    }

    fn foreign_key(table: &str, parent: &str) -> ForeignKeyInventory {
        ForeignKeyInventory {
            table: table.to_string(),
            name: format!("fk_{table}_{parent}"),
            columns: vec!["parent_id".to_string()],
            referenced_schema: "globalcomix".to_string(),
            referenced_table: parent.to_string(),
            referenced_columns: vec!["id".to_string()],
        }
    }

    fn table_plan(name: &str, sql: Vec<&str>) -> TableSchemaPlan {
        TableSchemaPlan {
            table: name.to_string(),
            source_fingerprint: "source".to_string(),
            target_fingerprint: "target".to_string(),
            dependencies: vec![],
            status: TableSchemaStatus::Planned,
            blockers: vec![],
            preflights: Vec::new(),
            statements: sql
                .into_iter()
                .map(|sql| PlannedSchemaStatement {
                    phase: SchemaPhase::Columns,
                    sql: sql.to_string(),
                    objects: vec![],
                    prerequisites: vec![],
                })
                .collect(),
        }
    }

    fn planned_statement(
        sql: &str,
        objects: Vec<&str>,
        prerequisites: Vec<&str>,
    ) -> PlannedSchemaStatement {
        PlannedSchemaStatement {
            phase: SchemaPhase::Columns,
            sql: sql.to_string(),
            objects: objects.into_iter().map(str::to_string).collect(),
            prerequisites: prerequisites.into_iter().map(str::to_string).collect(),
        }
    }

    fn dependent_table_plan(name: &str, parent: &str) -> TableSchemaPlan {
        let mut plan = table_plan(
            name,
            vec![
                "ALTER TABLE `children` ADD CONSTRAINT `fk` FOREIGN KEY (`parent_id`) REFERENCES `parents` (`id`)",
            ],
        );
        plan.dependencies.push(parent.to_string());
        plan
    }

    #[derive(Default)]
    struct FixtureCoercionPreflight {
        blockers: BTreeMap<(String, String), CoercionBlockers>,
    }

    impl FixtureCoercionPreflight {
        fn with_blockers(
            table: &str,
            column: &str,
            count: u64,
            sample_primary_keys: Vec<Vec<String>>,
        ) -> Self {
            let mut fixture = Self::default();
            fixture.blockers.insert(
                (table.to_string(), column.to_string()),
                CoercionBlockers {
                    predicate: Some("fixture predicate".to_string()),
                    count,
                    sample_primary_keys,
                    sample_error: None,
                },
            );
            fixture
        }

        fn and_blockers(
            mut self,
            table: &str,
            column: &str,
            count: u64,
            sample_primary_keys: Vec<Vec<String>>,
        ) -> Self {
            self.blockers.insert(
                (table.to_string(), column.to_string()),
                CoercionBlockers {
                    predicate: Some("fixture predicate".to_string()),
                    count,
                    sample_primary_keys,
                    sample_error: None,
                },
            );
            self
        }
    }

    impl SchemaCoercionPreflight for FixtureCoercionPreflight {
        fn inspect(
            &self,
            table: &TableInventory,
            source: &ColumnInventory,
            _target: &ColumnInventory,
        ) -> Result<CoercionBlockers, String> {
            Ok(self
                .blockers
                .get(&(table.name.clone(), source.name.clone()))
                .cloned()
                .unwrap_or(CoercionBlockers {
                    predicate: Some("fixture predicate".to_string()),
                    count: 0,
                    sample_primary_keys: Vec::new(),
                    sample_error: None,
                }))
        }
    }

    struct RecordingExecutor {
        fail_table: Option<String>,
        fail_sql: Option<String>,
        executed: Vec<String>,
    }

    impl RecordingExecutor {
        fn failing(table: &str) -> Self {
            Self {
                fail_table: Some(table.to_string()),
                fail_sql: None,
                executed: vec![],
            }
        }

        fn failing_sql(sql: &str) -> Self {
            Self {
                fail_table: None,
                fail_sql: Some(sql.to_string()),
                executed: vec![],
            }
        }
    }

    impl SchemaStatementExecutor for RecordingExecutor {
        fn execute(&mut self, table: &str, sql: &str) -> Result<(), String> {
            self.executed.push(sql.to_string());
            let table_failed = self.fail_table.as_deref() == Some(table);
            let sql_failed = self
                .fail_sql
                .as_deref()
                .is_some_and(|fragment| sql.contains(fragment));
            if table_failed || sql_failed {
                Err("fixture failure".to_string())
            } else {
                Ok(())
            }
        }
    }
}
