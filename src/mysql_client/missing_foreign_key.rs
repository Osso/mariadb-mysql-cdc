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

const MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH: usize = 8;

#[derive(Clone, Debug)]
pub(crate) struct MissingForeignKeyParent {
    pub(crate) change: TargetRowChange,
    constraint: String,
    repair_key: MissingForeignKeyRepairKey,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MissingForeignKeyRepairKey {
    child_schema: String,
    child_table: String,
    constraint: String,
    values: Vec<String>,
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

    fn load_missing_foreign_key_parent(
        &mut self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<MissingForeignKeyParent, TargetExecuteError>;
}

pub(crate) fn execute_row_change_with_missing_foreign_key_repair<E>(
    executor: &mut E,
    change: &TargetRowChange,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    let mut active_repairs = BTreeSet::new();
    execute_row_change_with_active_repairs(executor, change, &mut active_repairs)
}

fn execute_row_change_with_active_repairs<E>(
    executor: &mut E,
    change: &TargetRowChange,
    active_repairs: &mut BTreeSet<MissingForeignKeyRepairKey>,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    let result = executor.execute_row_change_statement(change);
    let Err(error) = result else {
        return Ok(());
    };
    if error.mysql_code() == Some(1062) && change.kind == TargetRowChangeKind::Insert {
        return Ok(());
    }
    if error.mysql_code() != Some(1452) || change.kind == TargetRowChangeKind::Delete {
        return Err(error);
    }
    repair_parent_and_retry(executor, change, &error, active_repairs)
}

fn repair_parent_and_retry<E>(
    executor: &mut E,
    change: &TargetRowChange,
    error: &TargetExecuteError,
    active_repairs: &mut BTreeSet<MissingForeignKeyRepairKey>,
) -> Result<(), TargetExecuteError>
where
    E: MissingForeignKeyRepairExecutor,
{
    let parent = executor.load_missing_foreign_key_parent(change, error)?;
    ensure_repair_can_start(change, &parent, active_repairs)?;
    let repair_key = parent.repair_key.clone();
    active_repairs.insert(repair_key.clone());

    let parent_result =
        execute_row_change_with_active_repairs(executor, &parent.change, active_repairs);
    let result = match parent_result {
        Ok(()) => execute_row_change_with_active_repairs(executor, change, active_repairs),
        Err(error) => Err(error),
    };
    if result.is_ok() {
        eprintln!(
            "cdc_missing_fk_parent_inserted child={}.{} constraint={} parent={}.{}",
            change.schema,
            change.table,
            parent.constraint,
            parent.change.schema,
            parent.change.table
        );
    }
    active_repairs.remove(&repair_key);
    result
}

fn ensure_repair_can_start(
    change: &TargetRowChange,
    parent: &MissingForeignKeyParent,
    active_repairs: &BTreeSet<MissingForeignKeyRepairKey>,
) -> Result<(), TargetExecuteError> {
    if active_repairs.contains(&parent.repair_key) {
        return Err(TargetExecuteError::new(format!(
            "missing-FK repair cycle detected for {}.{} constraint {}",
            change.schema, change.table, parent.constraint
        )));
    }
    if active_repairs.len() >= MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH {
        return Err(TargetExecuteError::new(format!(
            "missing-FK repair exceeded maximum depth {} while applying {}.{} constraint {}",
            MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH, change.schema, change.table, parent.constraint
        )));
    }
    Ok(())
}

impl PersistentTargetExecutor {
    pub(super) fn load_missing_foreign_key_parent(
        &self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<MissingForeignKeyParent, TargetExecuteError> {
        let source = self.source.as_ref().ok_or_else(|| {
            TargetExecuteError::new("missing-FK repair source connection is unavailable")
        })?;
        let reference =
            self.with_connection(|target| query_foreign_key_reference(target, change, error))?;
        fetch_source_missing_foreign_key_parent(source, change, &reference)
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

pub(crate) fn fetch_source_missing_foreign_key_parent(
    source: &PersistentMySqlSource,
    change: &TargetRowChange,
    reference: &ForeignKeyReference,
) -> Result<MissingForeignKeyParent, TargetExecuteError> {
    let key_values = foreign_key_values(change, reference)?;
    let repair_key = missing_foreign_key_repair_key(change, reference, &key_values)?;
    let (columns, values) = fetch_source_parent_row(source, reference, key_values)?;
    Ok(MissingForeignKeyParent {
        change: build_parent_row_change(reference, columns, values),
        constraint: reference.constraint.clone(),
        repair_key,
    })
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
) -> Result<(Vec<String>, Vec<Value>), TargetExecuteError> {
    let mut conn = source.conn.borrow_mut();
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
    let sql = format!(
        "SELECT {select_columns} FROM {}.{} WHERE {predicates} LIMIT 2",
        quote_ident(&reference.parent_schema),
        quote_ident(&reference.parent_table)
    );
    let rows = conn
        .exec::<Row, _, _>(sql, Params::Positional(key_values))
        .map_err(|error| {
            TargetExecuteError::new(format!(
                "missing-FK source parent query failed for {}.{}: {error}",
                reference.parent_schema, reference.parent_table
            ))
        })?;
    if rows.len() != 1 {
        return Err(TargetExecuteError::new(format!(
            "missing-FK source parent query returned {} rows for {}.{} constraint {}",
            rows.len(),
            reference.parent_schema,
            reference.parent_table,
            reference.constraint
        )));
    }
    let values = rows
        .into_iter()
        .next()
        .expect("one source parent row")
        .unwrap();
    Ok((columns, values))
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
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct FakeRepairExecutor {
        outcomes: BTreeMap<String, VecDeque<Result<(), TargetExecuteError>>>,
        parents: BTreeMap<String, MissingForeignKeyParent>,
        executed: Vec<String>,
    }

    impl MissingForeignKeyRepairExecutor for FakeRepairExecutor {
        fn execute_row_change_statement(
            &mut self,
            change: &TargetRowChange,
        ) -> Result<(), TargetExecuteError> {
            self.executed.push(change.table.clone());
            self.outcomes
                .get_mut(&change.table)
                .and_then(VecDeque::pop_front)
                .unwrap_or(Ok(()))
        }

        fn load_missing_foreign_key_parent(
            &mut self,
            change: &TargetRowChange,
            error: &TargetExecuteError,
        ) -> Result<MissingForeignKeyParent, TargetExecuteError> {
            assert_eq!(error.mysql_code(), Some(1452));
            self.parents.get(&change.table).cloned().ok_or_else(|| {
                TargetExecuteError::new(format!(
                    "missing fake parent for {}.{}",
                    change.schema, change.table
                ))
            })
        }
    }

    #[test]
    fn recursively_repairs_nested_parents_before_retrying_child() {
        let mut executor = FakeRepairExecutor {
            outcomes: BTreeMap::from([
                ("sessions".to_string(), outcomes([missing_fk(), Ok(())])),
                ("guests".to_string(), outcomes([missing_fk(), Ok(())])),
                ("utms".to_string(), outcomes([Ok(())])),
            ]),
            parents: BTreeMap::from([
                (
                    "sessions".to_string(),
                    fake_parent("sessions", "sessions_guest", "guests"),
                ),
                (
                    "guests".to_string(),
                    fake_parent("guests", "guests_utm", "utms"),
                ),
            ]),
            executed: Vec::new(),
        };

        execute_row_change_with_missing_foreign_key_repair(&mut executor, &row_change("sessions"))
            .expect("repair nested parents");

        assert_eq!(
            executor.executed,
            ["sessions", "guests", "utms", "guests", "sessions"]
        );
    }

    #[test]
    fn rejects_repeated_repair_key_as_a_cycle() {
        let mut executor = FakeRepairExecutor {
            outcomes: BTreeMap::from([
                ("alpha".to_string(), outcomes([missing_fk(), missing_fk()])),
                ("beta".to_string(), outcomes([missing_fk()])),
            ]),
            parents: BTreeMap::from([
                (
                    "alpha".to_string(),
                    fake_parent("alpha", "alpha_beta", "beta"),
                ),
                (
                    "beta".to_string(),
                    fake_parent("beta", "beta_alpha", "alpha"),
                ),
            ]),
            executed: Vec::new(),
        };

        let error =
            execute_row_change_with_missing_foreign_key_repair(&mut executor, &row_change("alpha"))
                .expect_err("cycle must fail closed");

        assert!(error.to_string().contains("repair cycle detected"));
        assert_eq!(executor.executed, ["alpha", "beta", "alpha"]);
    }

    #[test]
    fn rejects_parent_chain_beyond_bounded_depth() {
        let mut outcomes_by_table = BTreeMap::new();
        let mut parents = BTreeMap::new();
        for depth in 0..=MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH {
            let child = format!("table_{depth}");
            let parent = format!("table_{}", depth + 1);
            outcomes_by_table.insert(child.clone(), outcomes([missing_fk()]));
            parents.insert(
                child.clone(),
                fake_parent(&child, &format!("constraint_{depth}"), &parent),
            );
        }
        let mut executor = FakeRepairExecutor {
            outcomes: outcomes_by_table,
            parents,
            executed: Vec::new(),
        };

        let error = execute_row_change_with_missing_foreign_key_repair(
            &mut executor,
            &row_change("table_0"),
        )
        .expect_err("over-depth repair must fail closed");

        assert!(error.to_string().contains("exceeded maximum depth"));
        assert_eq!(
            executor.executed.len(),
            MAX_MISSING_FOREIGN_KEY_REPAIR_DEPTH + 1
        );
    }

    fn outcomes<const N: usize>(
        values: [Result<(), TargetExecuteError>; N],
    ) -> VecDeque<Result<(), TargetExecuteError>> {
        VecDeque::from(values)
    }

    fn missing_fk() -> Result<(), TargetExecuteError> {
        Err(TargetExecuteError::from_mysql(1452, "missing parent"))
    }

    fn fake_parent(
        child_table: &str,
        constraint: &str,
        parent_table: &str,
    ) -> MissingForeignKeyParent {
        MissingForeignKeyParent {
            change: row_change(parent_table),
            constraint: constraint.to_string(),
            repair_key: MissingForeignKeyRepairKey {
                child_schema: "globalcomix".to_string(),
                child_table: child_table.to_string(),
                constraint: constraint.to_string(),
                values: vec![parent_table.to_string()],
            },
        }
    }

    fn row_change(table: &str) -> TargetRowChange {
        TargetRowChange {
            statement: SqlStatement {
                sql: format!("INSERT INTO `{table}` (`id`) VALUES (?)"),
                params: vec![Value::UInt(1)],
            },
            kind: TargetRowChangeKind::Insert,
            schema: "globalcomix".to_string(),
            table: table.to_string(),
            values: BTreeMap::from([("id".to_string(), Value::UInt(1))]),
        }
    }
}
