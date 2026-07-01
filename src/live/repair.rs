use crate::probe::BinlogCoordinate;
use crate::statement::{StatementApplyError, StatementEvent};

use super::ApplyBinlogError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StatementRepairRequest {
    pub coordinate: BinlogCoordinate,
    pub default_database: Option<String>,
    pub table: String,
    pub sql: String,
    pub error: String,
}

pub(super) trait FailedStatementRepairer {
    fn repair(&self, request: &StatementRepairRequest) -> Result<(), ApplyBinlogError>;
}

pub(super) fn repair_failed_statement(
    repairer: &impl FailedStatementRepairer,
    event: &StatementEvent,
    error: &StatementApplyError,
) -> Result<bool, ApplyBinlogError> {
    let StatementApplyError::Target { sql, source, .. } = error else {
        return Ok(false);
    };
    let Some(table) = repairable_table_name(sql) else {
        println!("{}", format_statement_repair_skipped(event, sql));
        return Ok(false);
    };
    let request = StatementRepairRequest {
        coordinate: event.coordinate.clone(),
        default_database: event.default_database.clone(),
        table,
        sql: sql.clone(),
        error: source.to_string(),
    };

    repairer.repair(&request)?;
    Ok(true)
}

pub(super) fn repair_table_name(sql: &str) -> Option<String> {
    let sql = sql.trim();
    let upper = sql.to_ascii_uppercase();
    if upper.starts_with("INSERT INTO ") {
        return table_after_keyword(sql, "INTO");
    }
    if upper.starts_with("INSERT IGNORE INTO ") {
        return table_after_keyword(sql, "INTO");
    }
    if upper.starts_with("REPLACE INTO ") {
        return table_after_keyword(sql, "INTO");
    }
    if upper.starts_with("UPDATE ") {
        return first_identifier_after(sql, "UPDATE");
    }
    if upper.starts_with("DELETE FROM ") {
        return table_after_keyword(sql, "FROM");
    }

    None
}

pub(super) fn repairable_table_name(sql: &str) -> Option<String> {
    let sql = sql.trim();
    let upper = sql.to_ascii_uppercase();
    if upper.starts_with("DELETE FROM ") {
        return None;
    }

    repair_table_name(sql)
}

fn table_after_keyword(sql: &str, keyword: &str) -> Option<String> {
    let (_, rest) = sql.split_once(keyword)?;
    read_table_identifier(rest.trim())
}

fn first_identifier_after(sql: &str, keyword: &str) -> Option<String> {
    let rest = sql.get(keyword.len()..)?;
    read_table_identifier(rest.trim())
}

fn read_table_identifier(input: &str) -> Option<String> {
    let token = input.split_whitespace().next()?;
    let table = token.trim_matches('`').trim_end_matches(',');
    let table = table.rsplit('.').next()?.trim_matches('`');

    if table.is_empty() {
        None
    } else {
        Some(table.to_string())
    }
}

fn format_statement_repair_skipped(event: &StatementEvent, sql: &str) -> String {
    format!(
        "cdc_statement_repair_skipped file={} position={} reason=no_table sql={}",
        event.coordinate.file,
        event.coordinate.position,
        shell_word(sql)
    )
}

fn shell_word(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
