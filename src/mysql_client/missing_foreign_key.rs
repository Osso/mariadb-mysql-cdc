use super::query::target_query_error;
use super::{PersistentMySqlSource, PersistentTargetExecutor};
use crate::mysql_support::quote_ident;
use crate::target::{
    SqlStatement, TargetExecuteError, TargetRowChange, TargetRowChangeKind,
    render_submitted_sql_statement,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Params, Row, Value};
use std::collections::{BTreeMap, BTreeSet};

mod duplicate_parent;
mod superseded_insert;

const MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH: usize = 8;

#[derive(Clone, Debug)]
pub(crate) enum MissingForeignKeyRepair {
    Parent(MissingForeignKeyParent),
    SupersededInsert(SupersededSourceInsert),
}

#[derive(Clone, Debug)]
pub(crate) struct MissingForeignKeyParent {
    pub(crate) change: TargetRowChange,
    constraint: String,
    repair_key: MissingForeignKeyRepairKey,
}

struct SourceParentRow {
    columns: Vec<String>,
    values: Vec<Value>,
}

#[derive(Clone, Debug)]
pub(crate) struct SupersededSourceInsert {
    current_change: Option<TargetRowChange>,
    constraint: String,
    repair_key: MissingForeignKeyRepairKey,
}

impl MissingForeignKeyRepair {
    fn constraint(&self) -> &str {
        match self {
            Self::Parent(parent) => &parent.constraint,
            Self::SupersededInsert(insert) => &insert.constraint,
        }
    }

    fn repair_key(&self) -> &MissingForeignKeyRepairKey {
        match self {
            Self::Parent(parent) => &parent.repair_key,
            Self::SupersededInsert(insert) => &insert.repair_key,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DuplicateParentReconciliation {
    pub(crate) owner_change: TargetRowChange,
    pub(crate) retry_parent_insert: bool,
    pub(crate) verification: SqlStatement,
    repair_key: DuplicateParentRepairKey,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DuplicateParentRepairKey {
    schema: String,
    table: String,
    index: String,
    values: Vec<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MissingForeignKeyRepairKey {
    child_schema: String,
    child_table: String,
    constraint: String,
    values: Vec<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ActiveRepairKey {
    DuplicateParent(DuplicateParentRepairKey),
    MissingForeignKey(MissingForeignKeyRepairKey),
}

pub(crate) struct ForeignKeyReference {
    constraint: String,
    parent_schema: String,
    parent_table: String,
    columns: Vec<(String, String)>,
}

pub(crate) trait MissingForeignKeyRepairExecutor {
    fn execute_row_change_statement(
        &mut self,
        change: &TargetRowChange,
    ) -> Result<(), TargetExecuteError>;

    fn load_missing_foreign_key_repair(
        &mut self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<MissingForeignKeyRepair, TargetExecuteError>;

    fn load_duplicate_parent_reconciliation(
        &mut self,
        _change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<DuplicateParentReconciliation, TargetExecuteError> {
        Err(error.clone())
    }

    fn verify_duplicate_parent_reconciliation(
        &mut self,
        _change: &TargetRowChange,
        _reconciliation: &DuplicateParentReconciliation,
    ) -> Result<(), TargetExecuteError> {
        Err(TargetExecuteError::new(
            "duplicate-parent verification is unavailable",
        ))
    }
}

#[derive(Clone, Copy)]
enum DuplicateInsertBehavior {
    Ignore,
    Reconcile,
    Reject,
}

pub(crate) fn execute_row_change_with_missing_foreign_key_repair<E>(
    executor: &mut E,
    change: &TargetRowChange,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    let mut active_repairs = BTreeSet::new();
    execute_row_change_with_active_repairs(
        executor,
        change,
        &mut active_repairs,
        DuplicateInsertBehavior::Ignore,
    )
}

fn execute_row_change_with_active_repairs<E>(
    executor: &mut E,
    change: &TargetRowChange,
    active_repairs: &mut BTreeSet<ActiveRepairKey>,
    duplicate_behavior: DuplicateInsertBehavior,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    let result = executor.execute_row_change_statement(change);
    let Err(error) = result else {
        return Ok(());
    };
    if error.mysql_code() == Some(1062) && change.kind == TargetRowChangeKind::Insert {
        return handle_duplicate_insert(
            executor,
            change,
            error,
            active_repairs,
            duplicate_behavior,
        );
    }
    if error.mysql_code() != Some(1452) || change.kind == TargetRowChangeKind::Delete {
        return Err(error);
    }
    repair_parent_and_retry(executor, change, &error, active_repairs, duplicate_behavior)
}

fn handle_duplicate_insert<E>(
    executor: &mut E,
    change: &TargetRowChange,
    error: TargetExecuteError,
    active_repairs: &mut BTreeSet<ActiveRepairKey>,
    duplicate_behavior: DuplicateInsertBehavior,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    match duplicate_behavior {
        DuplicateInsertBehavior::Ignore => Ok(()),
        DuplicateInsertBehavior::Reject => Err(error),
        DuplicateInsertBehavior::Reconcile => {
            reconcile_duplicate_parent(executor, change, &error, active_repairs)
        }
    }
}

fn reconcile_duplicate_parent<E>(
    executor: &mut E,
    parent_change: &TargetRowChange,
    error: &TargetExecuteError,
    active_repairs: &mut BTreeSet<ActiveRepairKey>,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    let reconciliation = executor.load_duplicate_parent_reconciliation(parent_change, error)?;
    ensure_duplicate_repair_can_start(parent_change, &reconciliation, active_repairs)?;
    let repair_key = ActiveRepairKey::DuplicateParent(reconciliation.repair_key.clone());
    active_repairs.insert(repair_key.clone());
    let result = apply_duplicate_parent_reconciliation(
        executor,
        parent_change,
        &reconciliation,
        active_repairs,
    );
    active_repairs.remove(&repair_key);
    result
}

fn apply_duplicate_parent_reconciliation<E>(
    executor: &mut E,
    parent_change: &TargetRowChange,
    reconciliation: &DuplicateParentReconciliation,
    active_repairs: &mut BTreeSet<ActiveRepairKey>,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    execute_row_change_with_active_repairs(
        executor,
        &reconciliation.owner_change,
        active_repairs,
        DuplicateInsertBehavior::Reconcile,
    )?;
    if reconciliation.retry_parent_insert {
        execute_row_change_with_active_repairs(
            executor,
            parent_change,
            active_repairs,
            DuplicateInsertBehavior::Reject,
        )?;
    }
    executor.verify_duplicate_parent_reconciliation(parent_change, reconciliation)
}

fn repair_parent_and_retry<E>(
    executor: &mut E,
    change: &TargetRowChange,
    error: &TargetExecuteError,
    active_repairs: &mut BTreeSet<ActiveRepairKey>,
    duplicate_behavior: DuplicateInsertBehavior,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    let repair = executor.load_missing_foreign_key_repair(change, error)?;
    ensure_missing_foreign_key_repair_can_start(change, &repair, active_repairs)?;
    let repair_key = ActiveRepairKey::MissingForeignKey(repair.repair_key().clone());
    active_repairs.insert(repair_key.clone());
    let result = apply_missing_foreign_key_repair(
        executor,
        change,
        &repair,
        active_repairs,
        duplicate_behavior,
    );
    active_repairs.remove(&repair_key);
    result
}

fn apply_missing_foreign_key_repair<E>(
    executor: &mut E,
    historical_change: &TargetRowChange,
    repair: &MissingForeignKeyRepair,
    active_repairs: &mut BTreeSet<ActiveRepairKey>,
    duplicate_behavior: DuplicateInsertBehavior,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    match repair {
        MissingForeignKeyRepair::Parent(parent) => insert_parent_and_retry(
            executor,
            historical_change,
            parent,
            active_repairs,
            duplicate_behavior,
        ),
        MissingForeignKeyRepair::SupersededInsert(insert) => {
            reconcile_superseded_insert(executor, historical_change, insert, active_repairs)
        }
    }
}

fn insert_parent_and_retry<E>(
    executor: &mut E,
    child: &TargetRowChange,
    parent: &MissingForeignKeyParent,
    active_repairs: &mut BTreeSet<ActiveRepairKey>,
    duplicate_behavior: DuplicateInsertBehavior,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    execute_row_change_with_active_repairs(
        executor,
        &parent.change,
        active_repairs,
        DuplicateInsertBehavior::Reconcile,
    )?;
    execute_row_change_with_active_repairs(executor, child, active_repairs, duplicate_behavior)?;
    eprintln!(
        "cdc_missing_fk_parent_inserted child={}.{} constraint={} parent={}.{}",
        child.schema, child.table, parent.constraint, parent.change.schema, parent.change.table
    );
    Ok(())
}

fn reconcile_superseded_insert<E>(
    executor: &mut E,
    historical_change: &TargetRowChange,
    insert: &SupersededSourceInsert,
    active_repairs: &mut BTreeSet<ActiveRepairKey>,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    let Some(current_change) = &insert.current_change else {
        eprintln!(
            "cdc_missing_fk_superseded_insert_skipped child={}.{} constraint={} source_row=absent",
            historical_change.schema, historical_change.table, insert.constraint
        );
        return Ok(());
    };
    execute_row_change_with_active_repairs(
        executor,
        current_change,
        active_repairs,
        DuplicateInsertBehavior::Reconcile,
    )?;
    eprintln!(
        "cdc_missing_fk_superseded_insert_reconciled child={}.{} constraint={}",
        historical_change.schema, historical_change.table, insert.constraint
    );
    Ok(())
}

fn ensure_missing_foreign_key_repair_can_start(
    change: &TargetRowChange,
    repair: &MissingForeignKeyRepair,
    active_repairs: &BTreeSet<ActiveRepairKey>,
) -> Result<(), TargetExecuteError> {
    let repair_key = ActiveRepairKey::MissingForeignKey(repair.repair_key().clone());
    if active_repairs.contains(&repair_key) {
        return Err(TargetExecuteError::new(format!(
            "missing-FK repair cycle detected for {}.{} constraint {}",
            change.schema,
            change.table,
            repair.constraint()
        )));
    }
    ensure_repair_depth(
        active_repairs,
        format!(
            "applying {}.{} constraint {}",
            change.schema,
            change.table,
            repair.constraint()
        ),
    )
}

fn ensure_duplicate_repair_can_start(
    change: &TargetRowChange,
    reconciliation: &DuplicateParentReconciliation,
    active_repairs: &BTreeSet<ActiveRepairKey>,
) -> Result<(), TargetExecuteError> {
    let repair_key = ActiveRepairKey::DuplicateParent(reconciliation.repair_key.clone());
    if active_repairs.contains(&repair_key) {
        return Err(TargetExecuteError::new(format!(
            "duplicate-parent repair cycle detected for {}.{} index {}",
            change.schema, change.table, reconciliation.repair_key.index
        )));
    }
    ensure_repair_depth(
        active_repairs,
        format!(
            "reconciling {}.{} index {}",
            change.schema, change.table, reconciliation.repair_key.index
        ),
    )
}

fn ensure_repair_depth(
    active_repairs: &BTreeSet<ActiveRepairKey>,
    context: String,
) -> Result<(), TargetExecuteError> {
    if active_repairs.len() >= MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH {
        return Err(TargetExecuteError::new(format!(
            "automatic repair exceeded maximum depth {} while {context}",
            MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH
        )));
    }
    Ok(())
}

impl PersistentTargetExecutor {
    pub(super) fn load_missing_foreign_key_repair(
        &self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<MissingForeignKeyRepair, TargetExecuteError> {
        let source = self.source.as_ref().ok_or_else(|| {
            TargetExecuteError::new("missing-FK repair source connection is unavailable")
        })?;
        let reference =
            self.with_connection(|target| query_foreign_key_reference(target, change, error))?;
        fetch_source_missing_foreign_key_repair(source, change, &reference)
    }

    pub(super) fn load_duplicate_parent_reconciliation(
        &self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<DuplicateParentReconciliation, TargetExecuteError> {
        let source = self.source.as_ref().ok_or_else(|| {
            TargetExecuteError::new("duplicate-parent repair source connection is unavailable")
        })?;
        self.with_connection(|target| {
            duplicate_parent::load_duplicate_parent_reconciliation(target, source, change, error)
        })
    }

    pub(super) fn verify_duplicate_parent_reconciliation(
        &self,
        change: &TargetRowChange,
        reconciliation: &DuplicateParentReconciliation,
    ) -> Result<(), TargetExecuteError> {
        self.with_connection(|target| {
            duplicate_parent::verify_duplicate_parent_reconciliation(target, change, reconciliation)
        })
    }
}

pub(crate) fn query_foreign_key_reference(
    target: &mut Conn,
    change: &TargetRowChange,
    error: &TargetExecuteError,
) -> Result<ForeignKeyReference, TargetExecuteError> {
    let constraint = foreign_key_constraint(&error.to_string()).ok_or_else(|| {
        TargetExecuteError::new(format!(
            "missing-FK target error did not identify a constraint: {error}"
        ))
    })?;
    let rows = target
        .exec::<(String, String, String, String), _, _>(
            "SELECT COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
             FROM information_schema.KEY_COLUMN_USAGE \
             WHERE CONSTRAINT_SCHEMA = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ? \
               AND CONSTRAINT_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL \
             ORDER BY ORDINAL_POSITION",
            (&change.schema, &change.schema, &change.table, &constraint),
        )
        .map_err(target_query_error)?;
    build_foreign_key_reference(change, constraint, rows)
}

pub(crate) fn fetch_source_missing_foreign_key_repair(
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
    reference: &ForeignKeyReference,
) -> Result<MissingForeignKeyRepair, TargetExecuteError> {
    let key_values = foreign_key_values(change, reference)?;
    let repair_key = missing_foreign_key_repair_key(change, reference, &key_values)?;
    let parent = fetch_source_parent_row(source, reference, key_values)?;
    if let Some(parent) = parent {
        return Ok(MissingForeignKeyRepair::Parent(MissingForeignKeyParent {
            change: build_parent_row_change(reference, parent.columns, parent.values),
            constraint: reference.constraint.clone(),
            repair_key,
        }));
    }
    if change.kind != TargetRowChangeKind::Insert {
        return Err(source_parent_count_error(reference, 0));
    }
    superseded_insert::load_superseded_source_insert(source, change, reference, repair_key)
        .map(MissingForeignKeyRepair::SupersededInsert)
}

fn foreign_key_constraint(error: &str) -> Option<String> {
    let marker = "CONSTRAINT `";
    let start = error.find(marker)? + marker.len();
    let remainder = &error[start..];
    let end = remainder.find('`')?;
    let constraint = &remainder[..end];
    (!constraint.is_empty()).then(|| constraint.to_string())
}

fn build_foreign_key_reference(
    change: &TargetRowChange,
    constraint: String,
    rows: Vec<(String, String, String, String)>,
) -> Result<ForeignKeyReference, TargetExecuteError> {
    let Some((_, parent_schema, parent_table, _)) = rows.first() else {
        return Err(TargetExecuteError::new(format!(
            "missing-FK constraint metadata is absent for {}.{} constraint {constraint}",
            change.schema, change.table
        )));
    };
    if parent_schema != &change.schema {
        return Err(TargetExecuteError::new(format!(
            "missing-FK repair does not support cross-schema constraint {constraint}: {} -> {parent_schema}.{parent_table}",
            change.schema
        )));
    }
    if rows
        .iter()
        .any(|(_, schema, table, _)| schema != parent_schema || table != parent_table)
    {
        return Err(TargetExecuteError::new(format!(
            "missing-FK constraint metadata is ambiguous for {}.{} constraint {constraint}",
            change.schema, change.table
        )));
    }
    Ok(ForeignKeyReference {
        constraint,
        parent_schema: parent_schema.clone(),
        parent_table: parent_table.clone(),
        columns: rows
            .into_iter()
            .map(|(child, _, _, parent)| (child, parent))
            .collect(),
    })
}

fn foreign_key_values(
    change: &TargetRowChange,
    reference: &ForeignKeyReference,
) -> Result<Vec<Value>, TargetExecuteError> {
    reference
        .columns
        .iter()
        .map(|(child_column, _)| {
            change.values.get(child_column).cloned().ok_or_else(|| {
                TargetExecuteError::new(format!(
                    "missing-FK child value is absent for {}.{} column {child_column}",
                    change.schema, change.table
                ))
            })
        })
        .collect()
}

fn missing_foreign_key_repair_key(
    change: &TargetRowChange,
    reference: &ForeignKeyReference,
    key_values: &[Value],
) -> Result<MissingForeignKeyRepairKey, TargetExecuteError> {
    let values = key_values
        .iter()
        .map(render_repair_key_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(MissingForeignKeyRepairKey {
        child_schema: change.schema.clone(),
        child_table: change.table.clone(),
        constraint: reference.constraint.clone(),
        values,
    })
}

fn render_repair_key_value(value: &Value) -> Result<String, TargetExecuteError> {
    render_submitted_sql_statement(&SqlStatement {
        sql: "?".to_string(),
        params: vec![value.clone()],
    })
}

fn fetch_source_parent_row(
    source: &PersistentMySqlSource,
    reference: &ForeignKeyReference,
    key_values: Vec<Value>,
) -> Result<Option<SourceParentRow>, TargetExecuteError> {
    let mut conn = source.conn.borrow_mut();
    let columns = query_source_parent_columns(&mut conn, reference)?;
    let sql = build_source_parent_select(reference, &columns);
    let rows = query_source_parent_rows(&mut conn, reference, sql, key_values)?;
    decode_source_parent_row(reference, rows)
        .map(|values| values.map(|values| SourceParentRow { columns, values }))
}

fn query_source_parent_columns(
    conn: &mut Conn,
    reference: &ForeignKeyReference,
) -> Result<Vec<String>, TargetExecuteError> {
    let columns = conn
        .exec::<String, _, _>(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA NOT LIKE '%GENERATED%' \
             ORDER BY ORDINAL_POSITION",
            (&reference.parent_schema, &reference.parent_table),
        )
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "missing-FK source parent column query failed: {error}"
            ))
        })?;
    if columns.is_empty() {
        return Err(TargetExecuteError::new(format!(
            "missing-FK source parent table has no writable columns: {}.{}",
            reference.parent_schema, reference.parent_table
        )));
    }
    Ok(columns)
}

fn build_source_parent_select(reference: &ForeignKeyReference, columns: &[String]) -> String {
    let select_columns = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let predicates = reference
        .columns
        .iter()
        .map(|(_, parent_column)| format!("{} <=> ?", quote_ident(parent_column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "SELECT {select_columns} FROM {}.{} WHERE {predicates} LIMIT 2",
        quote_ident(&reference.parent_schema),
        quote_ident(&reference.parent_table)
    )
}

fn query_source_parent_rows(
    conn: &mut Conn,
    reference: &ForeignKeyReference,
    sql: String,
    key_values: Vec<Value>,
) -> Result<Vec<Row>, TargetExecuteError> {
    conn.exec(sql, Params::Positional(key_values))
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "missing-FK source parent query failed for {}.{}: {error}",
                reference.parent_schema, reference.parent_table
            ))
        })
}

fn decode_source_parent_row(
    reference: &ForeignKeyReference,
    rows: Vec<Row>,
) -> Result<Option<Vec<Value>>, TargetExecuteError> {
    match rows.len() {
        0 => Ok(None),
        1 => Ok(Some(
            rows.into_iter()
                .next()
                .expect("one source parent row")
                .unwrap(),
        )),
        count => Err(source_parent_count_error(reference, count)),
    }
}

fn source_parent_count_error(reference: &ForeignKeyReference, count: usize) -> TargetExecuteError {
    TargetExecuteError::new(format!(
        "missing-FK source parent query returned {count} rows for {}.{} constraint {}",
        reference.parent_schema, reference.parent_table, reference.constraint
    ))
}

fn build_parent_row_change(
    reference: &ForeignKeyReference,
    columns: Vec<String>,
    values: Vec<Value>,
) -> TargetRowChange {
    let row_values = columns
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .collect::<BTreeMap<_, _>>();
    TargetRowChange {
        statement: build_parent_insert_statement(reference, &columns, values),
        kind: TargetRowChangeKind::Insert,
        schema: reference.parent_schema.clone(),
        table: reference.parent_table.clone(),
        values: row_values,
    }
}

fn build_parent_insert_statement(
    reference: &ForeignKeyReference,
    columns: &[String],
    values: Vec<Value>,
) -> SqlStatement {
    let column_sql = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; columns.len()].join(", ");
    SqlStatement {
        sql: format!(
            "INSERT INTO {}.{} ({column_sql}) VALUES ({placeholders})",
            quote_ident(&reference.parent_schema),
            quote_ident(&reference.parent_table)
        ),
        params: values,
    }
}

#[cfg(test)]
#[path = "missing_foreign_key/tests.rs"]
mod tests;
