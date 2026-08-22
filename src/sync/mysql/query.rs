use super::super::model::SyncTable;
use super::decode_sync_rows;
use crate::database_row::DatabaseRow;
use crate::mysql_client::value_to_string;
use crate::target::SqlStatement;
use mysql::prelude::Queryable;
use mysql::{Conn, Params};

pub(super) fn decode_optional_exact_row(
    table: &SyncTable,
    rows: Vec<Vec<Option<String>>>,
    endpoint: &str,
) -> Result<Option<DatabaseRow>, String> {
    let mut decoded = decode_sync_rows(table, rows)?;
    match decoded.len() {
        0 => Ok(None),
        1 => Ok(decoded.pop()),
        count => Err(format!(
            "{endpoint} exact-row query returned {count} rows for `{}`",
            table.name
        )),
    }
}

pub(super) fn query_statement_rows_as_strings(
    conn: &mut Conn,
    statement: &SqlStatement,
    operation: &str,
) -> Result<Vec<Vec<Option<String>>>, String> {
    let rows = conn
        .exec::<mysql::Row, _, _>(&statement.sql, Params::Positional(statement.params.clone()))
        .map_err(|error| format!("{operation} mysql query failed: {error}"))?;
    Ok(mysql_rows_to_strings(rows))
}

pub(super) fn mysql_rows_to_strings(rows: Vec<mysql::Row>) -> Vec<Vec<Option<String>>> {
    rows.into_iter()
        .map(|row| row.unwrap().into_iter().map(value_to_string).collect())
        .collect()
}
