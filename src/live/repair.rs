use crate::inventory::{InventoryConfig, MariaDbInventoryReader, build_inventory};
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::probe::BinlogCoordinate;
use crate::snapshot::SnapshotTable;
use crate::statement::{StatementApplyError, StatementEvent};
use crate::table_sync::{SyncMode, SyncTable, SyncTableConfig, run_sync_table};

use super::{ApplyBinlogConfig, ApplyBinlogError};

const REPAIR_CHUNK_SIZE: usize = 1000;

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

pub(super) struct TableSyncStatementRepairer {
    config: ApplyBinlogConfig,
}

impl TableSyncStatementRepairer {
    pub fn new(config: ApplyBinlogConfig) -> Self {
        Self { config }
    }
}

impl FailedStatementRepairer for TableSyncStatementRepairer {
    fn repair(&self, request: &StatementRepairRequest) -> Result<(), ApplyBinlogError> {
        println!("{}", format_statement_repair_start(request));
        let sync_config = self.sync_config(request)?;
        let report = run_sync_table(&sync_config).map_err(|error| {
            ApplyBinlogError::Statement(format!(
                "failed to repair {} after stream apply failure: {error}",
                request.table
            ))
        })?;
        println!("{}", format_statement_repair_complete(request, &report));
        Ok(())
    }
}

impl TableSyncStatementRepairer {
    fn sync_config(
        &self,
        request: &StatementRepairRequest,
    ) -> Result<SyncTableConfig, ApplyBinlogError> {
        let database = repair_database(&self.config, request)?;
        Ok(SyncTableConfig {
            source: self.source_config(&database),
            target: self.config.target.clone(),
            mariadb: self.config.mariadb.clone(),
            table: read_sync_table(&self.config, &database, &request.table)?,
            chunk_size: REPAIR_CHUNK_SIZE,
            mode: SyncMode::Apply,
            progress_table: "cdc.table_sync_progress".to_string(),
        })
    }

    fn source_config(&self, database: &str) -> MySqlConnectionConfig {
        MySqlConnectionConfig {
            host: self.config.source.host.clone(),
            port: self.config.source.port,
            user: self.config.source.user.clone(),
            password: self.config.source.password.clone(),
            database: database.to_string(),
            mariadb: self.config.mariadb.clone(),
        }
    }
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

fn repair_database(
    config: &ApplyBinlogConfig,
    request: &StatementRepairRequest,
) -> Result<String, ApplyBinlogError> {
    request
        .default_database
        .clone()
        .or_else(|| config.source.database.clone())
        .ok_or_else(|| {
            ApplyBinlogError::Statement(format!(
                "failed to repair {}: no source database was recorded",
                request.table
            ))
        })
}

fn read_sync_table(
    config: &ApplyBinlogConfig,
    database: &str,
    table: &str,
) -> Result<SyncTable, ApplyBinlogError> {
    let inventory_config = InventoryConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        mariadb: config.mariadb.clone(),
    };
    let reader = MariaDbInventoryReader::new(inventory_config);
    let inventory = build_inventory(database, &reader).map_err(|error| {
        ApplyBinlogError::Statement(format!("repair inventory failed: {error}"))
    })?;
    let table = inventory
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .ok_or_else(|| ApplyBinlogError::Statement(format!("repair table not found: {table}")))?;
    let snapshot_table = SnapshotTable::from(table);

    Ok(SyncTable {
        name: snapshot_table.name,
        primary_key: snapshot_table.primary_key,
        columns: snapshot_table.columns,
    })
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

fn format_statement_repair_start(request: &StatementRepairRequest) -> String {
    format!(
        "cdc_statement_repair_start file={} position={} database={} table={} error={}",
        request.coordinate.file,
        request.coordinate.position,
        request.default_database.as_deref().unwrap_or("-"),
        request.table,
        shell_word(&request.error)
    )
}

fn format_statement_repair_complete(
    request: &StatementRepairRequest,
    report: &crate::table_sync::SyncTableReport,
) -> String {
    format!(
        "cdc_statement_repair_complete file={} position={} table={} chunks={} rows_scanned={} inserts={} updates={} extra_target_rows={}",
        request.coordinate.file,
        request.coordinate.position,
        request.table,
        report.chunks,
        report.rows_scanned,
        report.inserts,
        report.updates,
        report.extra_target_rows
    )
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
