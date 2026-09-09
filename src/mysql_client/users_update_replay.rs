use super::PersistentTargetExecutor;
use crate::mysql_support::quote_ident;
use crate::target::{
    TargetExecuteError, TargetRowChange, TargetRowChangeKind, duplicate_index_from_error,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Row, Value};

type Column = (String, String, String);

fn numeric(value: &Value) -> Option<u64> {
    match value {
        Value::UInt(value) => Some(*value),
        Value::Int(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn same_row(left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| match (numeric(left), numeric(right)) {
                (Some(left), Some(right)) => left == right,
                _ => left == right,
            })
}

fn candidate_id(change: &TargetRowChange, error: &TargetExecuteError) -> Option<u64> {
    if change.kind != TargetRowChangeKind::Update
        || change.table != "users"
        || error.mysql_code() != Some(1062)
    {
        return None;
    }
    let index = duplicate_index_from_error(&error.to_string())?;
    if index != "name" && index != "users.name" {
        return None;
    }
    let (_, predicate) = change.statement.sql.rsplit_once(" WHERE ")?;
    if predicate != "`id` = ?" || !change.values.contains_key("name") {
        return None;
    }
    let before = numeric(change.statement.params.last()?)?;
    let after = numeric(change.values.get("id")?)?;
    (before == after).then_some(after)
}

fn proof_error(error: mysql::Error) -> TargetExecuteError {
    // Server error text may contain names, emails, or other row values.
    TargetExecuteError::new(format!(
        "users UPDATE replay proof query failed (mysql_code={:?})",
        match error {
            mysql::Error::MySqlError(error) => Some(error.code),
            _ => None,
        }
    ))
}

fn read_columns(
    conn: &mut Conn,
    change: &TargetRowChange,
) -> Result<Vec<Column>, TargetExecuteError> {
    conn.exec("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND EXTRA NOT LIKE '%VIRTUAL GENERATED%' AND EXTRA NOT LIKE '%STORED GENERATED%' ORDER BY ORDINAL_POSITION", (&change.schema, &change.table)).map_err(proof_error)
}

fn normalized_type(kind: &str, definition: &str) -> String {
    if matches!(
        kind,
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
    ) && let Some((base, width)) = definition.split_once('(')
        && let Some((_, suffix)) = width.split_once(')')
    {
        return format!("{base}{suffix}");
    }
    definition.to_owned()
}

fn same_columns(source: &[Column], target: &[Column]) -> bool {
    !source.is_empty()
        && source.len() == target.len()
        && source.iter().zip(target).all(|(source, target)| {
            source.0 == target.0
                && source.1 == target.1
                && normalized_type(&source.1, &source.2) == normalized_type(&target.1, &target.2)
        })
}

fn has_id_primary_key(
    conn: &mut Conn,
    change: &TargetRowChange,
) -> Result<bool, TargetExecuteError> {
    let keys: Vec<String> = conn.exec("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION", (&change.schema, &change.table)).map_err(proof_error)?;
    Ok(keys == ["id"])
}

fn has_name_unique_index(
    conn: &mut Conn,
    change: &TargetRowChange,
) -> Result<bool, TargetExecuteError> {
    let indexes: Vec<(Option<String>, u64, Option<u64>)> = conn.exec("SELECT COLUMN_NAME, NON_UNIQUE, SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='name' ORDER BY SEQ_IN_INDEX", (&change.schema, &change.table)).map_err(proof_error)?;
    Ok(indexes == [(Some("name".to_owned()), 0, None)])
}

fn read_one(
    conn: &mut Conn,
    sql: &str,
    value: Value,
) -> Result<Option<Vec<Value>>, TargetExecuteError> {
    let mut rows: Vec<Row> = conn.exec(sql, (value,)).map_err(proof_error)?;
    if rows.len() != 1 {
        return Ok(None);
    }
    Ok(rows.pop().map(Row::unwrap))
}

fn prove_rows(
    target: &mut Conn,
    source: &mut Conn,
    change: &TargetRowChange,
    columns: &[Column],
    id: u64,
) -> Result<bool, TargetExecuteError> {
    let projection = columns
        .iter()
        .map(|column| quote_ident(&column.0))
        .collect::<Vec<_>>()
        .join(",");
    let table = format!(
        "{}.{}",
        quote_ident(&change.schema),
        quote_ident(&change.table)
    );
    let event_sql = format!("SELECT {projection} FROM {table} WHERE `id`=? LIMIT 2");
    let Some(target_row) = read_one(target, &format!("{event_sql} FOR UPDATE"), Value::UInt(id))?
    else {
        return Ok(false);
    };
    let name = change.values["name"].clone();
    let owner_sql = format!("SELECT `id` FROM {table} WHERE `name` <=> ? LIMIT 2");
    let Some(target_owner) = read_one(target, &format!("{owner_sql} FOR UPDATE"), name.clone())?
    else {
        return Ok(false);
    };
    let Some(owner_id) = target_owner.first().and_then(numeric) else {
        return Ok(false);
    };
    if owner_id == id {
        return Ok(false);
    }
    let Some(source_row) = read_one(source, &event_sql, Value::UInt(id))? else {
        return Ok(false);
    };
    let Some(source_owner) = read_one(source, &owner_sql, name)? else {
        return Ok(false);
    };
    Ok(same_row(&target_row, &source_row) && same_row(&target_owner, &source_owner))
}

impl PersistentTargetExecutor {
    pub(super) fn prove_current_users_update_replay(
        &self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<bool, TargetExecuteError> {
        if !self.users_update_replay_enabled {
            return Ok(false);
        }
        let Some(id) = candidate_id(change, error) else {
            return Ok(false);
        };
        let Some(source) = self.source.as_ref() else {
            return Ok(false);
        };
        self.with_connection(|target| {
            let mut source = source.conn.borrow_mut();
            let source_columns = read_columns(&mut source, change)?;
            let target_columns = read_columns(target, change)?;
            if !same_columns(&source_columns, &target_columns)
                || !has_id_primary_key(&mut source, change)?
                || !has_id_primary_key(target, change)?
                || !has_name_unique_index(target, change)?
                || !has_name_unique_index(&mut source, change)?
            {
                return Ok(false);
            }
            let verified = prove_rows(target, &mut source, change, &source_columns, id)?;
            eprintln!("cdc_users_update_replay id={id} verified={verified}");
            Ok(verified)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::SqlStatement;

    #[test]
    fn current_rows_compare_exactly_except_nonnegative_integer_representation() {
        assert!(same_row(
            &[Value::Int(7), Value::NULL, Value::from("new")],
            &[Value::UInt(7), Value::NULL, Value::from("new")]
        ));
        assert!(!same_row(&[Value::Int(-1)], &[Value::UInt(u64::MAX)]));
        assert!(!same_row(&[Value::from("New")], &[Value::from("new")]));
        assert!(!same_row(&[Value::NULL], &[Value::from("")]));
        assert!(!same_row(&[Value::Int(1)], &[]));
    }

    #[test]
    fn candidate_rejects_changed_keys_and_other_unique_indexes() {
        let mut change = TargetRowChange {
            statement: SqlStatement {
                sql: "UPDATE `users` SET `name` = ? WHERE `id` = ?".into(),
                params: vec![Value::from("historical"), Value::Int(42)],
            },
            kind: TargetRowChangeKind::Update,
            schema: "test".into(),
            table: "users".into(),
            values: [
                ("id".into(), Value::UInt(42)),
                ("name".into(), Value::from("historical")),
            ]
            .into(),
        };
        let error =
            TargetExecuteError::from_mysql(1062, "Duplicate entry 'private' for key 'users.name'");
        assert_eq!(candidate_id(&change, &error), Some(42));
        assert_eq!(
            candidate_id(
                &change,
                &TargetExecuteError::from_mysql(
                    1062,
                    "Duplicate entry 'private' for key 'users.email'"
                )
            ),
            None
        );
        change.values.insert("id".into(), Value::UInt(43));
        assert_eq!(candidate_id(&change, &error), None);
    }

    #[test]
    fn column_mapping_preserves_signedness_and_enum_definition() {
        let source = vec![("id".into(), "int".into(), "int(11) unsigned".into())];
        assert!(same_columns(
            &source,
            &[("id".into(), "int".into(), "int unsigned".into())]
        ));
        assert!(!same_columns(
            &source,
            &[("id".into(), "int".into(), "int".into())]
        ));
        assert!(!same_columns(
            &[("e".into(), "enum".into(), "enum('a','b')".into())],
            &[("e".into(), "enum".into(), "enum('b','a')".into())]
        ));
    }
}
