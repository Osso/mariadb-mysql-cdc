use crate::checkpoint::Checkpoint;
use crate::live::{
    InsertConflictPolicy, TargetMySqlConfig, should_ignore_duplicate_insert,
    should_ignore_duplicate_row_change,
};
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::{
    TARGET_TLS_CA_FILE, quote_ident, quote_identifier_path, quote_sql_literal, ssl_opts_from_ca,
};
use crate::snapshot::{
    ChunkRequest, SnapshotError, SnapshotProgress, SnapshotRow, SnapshotSource,
    TableSnapshotProgress,
};
use crate::table_sync::progress::{
    build_add_total_rows_column_sql, build_create_progress_schema_sql,
    build_create_progress_table_sql, build_progress_upsert_sql,
};
use crate::table_sync::{SyncTableProgress, TableSyncError};
use crate::target::{
    DuplicateConflict, SqlStatement, TargetExecuteError, TargetExecutionOutcome, TargetExecutor,
    TransactionalTargetExecutor, render_sql_statement,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Params, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;

pub struct PersistentMySqlSource {
    conn: RefCell<Conn>,
}

pub struct PersistentTargetExecutor {
    conn: RefCell<Conn>,
    insert_conflict_policy: InsertConflictPolicy,
}

pub struct PersistentProgressWriter {
    conn: RefCell<Conn>,
    default_database: String,
    progress_table: String,
}

impl PersistentMySqlSource {
    pub fn new(config: &MySqlConnectionConfig) -> Result<Self, SnapshotError> {
        let opts = base_opts(
            &config.host,
            config.port,
            &config.user,
            &config.password,
            &config.database,
            config.tls_ca_file.as_deref(),
        );
        let conn = open_conn(opts).map_err(snapshot_connect_error)?;
        Ok(Self {
            conn: RefCell::new(conn),
        })
    }

    pub fn count_rows(&self, table: &str) -> Result<u64, SnapshotError> {
        let sql = format!("SELECT COUNT(*) FROM {}", quote_ident(table));
        self.conn
            .borrow_mut()
            .query_first::<u64, _>(sql)
            .map_err(snapshot_query_error)?
            .ok_or_else(|| SnapshotError::InvalidTable(format!("{table} row count was empty")))
    }

    pub fn read_range_boundaries(
        &self,
        table: &crate::snapshot::SnapshotTable,
        workers: usize,
        total_rows: u64,
    ) -> Result<Vec<Vec<String>>, SnapshotError> {
        snapshot_boundary_offsets(total_rows, workers)
            .into_iter()
            .map(|offset| self.read_range_boundary(table, offset))
            .collect()
    }

    fn read_range_boundary(
        &self,
        table: &crate::snapshot::SnapshotTable,
        offset: u64,
    ) -> Result<Vec<String>, SnapshotError> {
        let sql = build_snapshot_boundary_select_sql(table, offset);
        let row = self
            .conn
            .borrow_mut()
            .query_first::<mysql::Row, _>(sql)
            .map_err(snapshot_query_error)?
            .ok_or_else(|| {
                SnapshotError::InvalidTable(format!("{} boundary was empty", table.name))
            })?;
        row.unwrap()
            .into_iter()
            .map(value_to_string)
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| {
                    SnapshotError::InvalidTable(format!(
                        "primary-key column {} was NULL",
                        table.primary_key[index]
                    ))
                })
            })
            .collect()
    }

    pub fn read_create_table(&self, table: &str) -> Result<String, SnapshotError> {
        let sql = format!("SHOW CREATE TABLE {}", quote_ident(table));
        self.conn
            .borrow_mut()
            .query_first::<(String, String), _>(sql)
            .map_err(snapshot_query_error)?
            .map(|(_, ddl)| ddl)
            .ok_or_else(|| SnapshotError::InvalidTable(format!("{table} DDL was empty")))
    }

    pub(crate) fn query_rows_as_strings(
        &self,
        sql: &str,
    ) -> Result<Vec<Vec<Option<String>>>, SnapshotError> {
        let rows = self
            .conn
            .borrow_mut()
            .query::<mysql::Row, _>(sql)
            .map_err(snapshot_query_error)?;
        Ok(rows.into_iter().map(row_to_strings).collect())
    }
}

impl SnapshotSource for PersistentMySqlSource {
    fn read_chunk(&self, request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError> {
        let sql = crate::mysql_snapshot::build_select_chunk_sql(request);
        let rows = self
            .conn
            .borrow_mut()
            .query::<mysql::Row, _>(sql)
            .map_err(snapshot_query_error)?;

        rows.into_iter()
            .map(|row| snapshot_row_from_mysql_row(request, row))
            .collect()
    }
}

impl PersistentTargetExecutor {
    pub fn new(config: &TargetMySqlConfig) -> Result<Self, TargetExecuteError> {
        let opts = base_opts(
            &config.host,
            config.port,
            &config.user,
            &config.password,
            &config.database,
            Some(TARGET_TLS_CA_FILE),
        );
        let mut conn = open_conn(opts).map_err(target_connect_error)?;
        conn.query_drop(crate::live::target_session_init_command())
            .map_err(target_query_error)?;
        Ok(Self {
            conn: RefCell::new(conn),
            insert_conflict_policy: config.insert_conflict_policy,
        })
    }

    pub fn read_column_names(&self, table: &str) -> Result<Vec<String>, TargetExecuteError> {
        self.conn
            .borrow_mut()
            .query(build_target_column_select_sql(table))
            .map_err(target_query_error)
    }
}

impl TargetExecutor for PersistentTargetExecutor {
    fn execute(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        let result = self.execute_statement(statement);
        match result {
            Ok(()) => Ok(()),
            Err(error) => self.retry_or_return_error(statement, error),
        }
    }

    fn execute_row_change(
        &self,
        statement: &SqlStatement,
    ) -> Result<TargetExecutionOutcome, TargetExecuteError> {
        match self.execute_statement(statement) {
            Ok(()) => Ok(TargetExecutionOutcome::Applied),
            Err(error)
                if should_ignore_duplicate_row_change(
                    self.insert_conflict_policy,
                    &statement.sql,
                    &error.to_string(),
                ) =>
            {
                Ok(TargetExecutionOutcome::DuplicateIgnored(DuplicateConflict {
                    error_code: 1062,
                    error_text: error.to_string(),
                    duplicate_index: crate::target::duplicate_index_from_error(&error.to_string()),
                }))
            }
            Err(error) => {
                self.retry_or_return_error(statement, error)?;
                Ok(TargetExecutionOutcome::Applied)
            }
        }
    }
}

impl TransactionalTargetExecutor for PersistentTargetExecutor {
    fn acquire_stream_lease(&self, lease_name: &str) -> Result<(), TargetExecuteError> {
        let sql = build_stream_lease_sql(lease_name);
        let acquired = self
            .conn
            .borrow_mut()
            .query_first::<u8, _>(sql)
            .map_err(target_query_error)?
            .unwrap_or(0);
        if acquired == 1 {
            return Ok(());
        }
        Err(TargetExecuteError::new(format!(
            "stream lease `{lease_name}` is already held"
        )))
    }

    fn begin_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("BEGIN")
    }

    fn load_transaction_checkpoint_for_update(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<Option<Checkpoint>, TargetExecuteError> {
        let sql = crate::stream_checkpoint::build_checkpoint_select_for_update_sql(
            checkpoint_table,
            checkpoint_name,
        );
        let checkpoint_json = self
            .conn
            .borrow_mut()
            .query_first::<String, _>(sql)
            .map_err(target_query_error)?;
        checkpoint_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|error| {
                    TargetExecuteError::new(format!(
                        "invalid locked stream checkpoint JSON: {error}"
                    ))
                })
            })
            .transpose()
    }

    fn save_transaction_checkpoint(
        &self,
        checkpoint_table: &str,
        checkpoint_name: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), TargetExecuteError> {
        let sql = crate::stream_checkpoint::build_checkpoint_upsert_sql_for_checkpoint(
            checkpoint_table,
            checkpoint_name,
            checkpoint,
        )
        .map_err(TargetExecuteError::new)?;
        self.conn
            .borrow_mut()
            .query_drop(sql)
            .map_err(target_query_error)
    }

    fn commit_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("COMMIT")
    }

    fn rollback_transaction(&self) -> Result<(), TargetExecuteError> {
        self.execute_transaction_control("ROLLBACK")
    }
}

impl PersistentTargetExecutor {
    fn execute_transaction_control(&self, sql: &str) -> Result<(), TargetExecuteError> {
        self.conn
            .borrow_mut()
            .query_drop(sql)
            .map_err(target_query_error)
    }

    fn execute_statement(&self, statement: &SqlStatement) -> Result<(), TargetExecuteError> {
        let params = statement.params.clone();
        self.conn
            .borrow_mut()
            .exec_drop(&statement.sql, Params::Positional(params))
            .map_err(target_query_error)
    }

    fn retry_or_return_error(
        &self,
        statement: &SqlStatement,
        error: TargetExecuteError,
    ) -> Result<(), TargetExecuteError> {
        if self.can_ignore_duplicate_insert(&statement.sql, &error.to_string()) {
            return Ok(());
        }
        let Some(retry_sql) = generated_column_retry_sql(statement, &error.to_string()) else {
            return Err(error);
        };
        self.conn
            .borrow_mut()
            .query_drop(retry_sql)
            .map_err(target_query_error)
    }

    fn can_ignore_duplicate_insert(&self, sql: &str, error: &str) -> bool {
        should_ignore_duplicate_insert(self.insert_conflict_policy, sql, error)
    }
}

impl PersistentProgressWriter {
    pub fn new(config: &TargetMySqlConfig, progress_table: String) -> Result<Self, TableSyncError> {
        let opts = base_opts(
            &config.host,
            config.port,
            &config.user,
            &config.password,
            &config.database,
            Some(TARGET_TLS_CA_FILE),
        );
        let mut conn = open_conn(opts).map_err(progress_connect_error)?;
        conn.query_drop(crate::live::target_session_init_command())
            .map_err(progress_query_error)?;
        Ok(Self {
            conn: RefCell::new(conn),
            default_database: config.database.clone(),
            progress_table,
        })
    }

    pub fn ensure(&self) -> Result<(), TableSyncError> {
        if let Some(sql) = build_create_progress_schema_sql(&self.progress_table) {
            self.execute_progress_sql(sql)?;
        }
        self.execute_progress_sql(build_create_progress_table_sql(&self.progress_table))?;
        self.execute_progress_sql(build_add_total_rows_column_sql(
            &self.default_database,
            &self.progress_table,
        ))
    }

    pub fn save(&self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.execute_progress_sql(build_progress_upsert_sql(&self.progress_table, progress))
    }

    pub fn save_error_message(&self, table: &str, error: &str) -> Result<(), TableSyncError> {
        self.execute_progress_sql(build_progress_error_message_sql(
            &self.progress_table,
            table,
            error,
        ))
    }

    pub fn load_snapshot_progress(&self) -> Result<SnapshotProgress, TableSyncError> {
        let sql = build_snapshot_progress_select_sql(&self.progress_table);
        let rows = self
            .conn
            .borrow_mut()
            .query::<SnapshotProgressRow, _>(sql)
            .map_err(progress_query_error)?;
        snapshot_progress_from_rows(rows)
    }

    pub fn execute_table_sync_progress_sql(&self, sql: String) -> Result<(), TableSyncError> {
        self.execute_progress_sql(sql)
    }

    pub fn query_table_sync_progress_tsv(&self, sql: String) -> Result<String, TableSyncError> {
        let rows = self
            .conn
            .borrow_mut()
            .query::<mysql::Row, _>(sql)
            .map_err(progress_query_error)?;
        Ok(rows_to_tsv(rows))
    }

    fn execute_progress_sql(&self, sql: String) -> Result<(), TableSyncError> {
        self.conn
            .borrow_mut()
            .query_drop(sql)
            .map_err(progress_query_error)
    }
}

fn rows_to_tsv(rows: Vec<mysql::Row>) -> String {
    rows.into_iter()
        .map(row_to_tsv)
        .collect::<Vec<_>>()
        .join("\n")
}

fn row_to_tsv(row: mysql::Row) -> String {
    row.unwrap()
        .into_iter()
        .map(value_to_string)
        .map(|value| value.unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\t")
}

type SnapshotProgressRow = (String, String, u64, String);

fn build_snapshot_progress_select_sql(progress_table: &str) -> String {
    format!(
        "SELECT table_name, COALESCE(last_primary_key_json, ''), rows_scanned, status FROM {}",
        quote_identifier_path(progress_table)
    )
}

fn build_progress_error_message_sql(progress_table: &str, table: &str, error: &str) -> String {
    format!(
        "INSERT INTO {} (table_name,mode,status,last_error) VALUES ({},'apply','error',{}) ON DUPLICATE KEY UPDATE status='error',last_error=VALUES(last_error)",
        quote_identifier_path(progress_table),
        quote_sql_literal(table),
        quote_sql_literal(error)
    )
}

fn snapshot_progress_from_rows(
    rows: Vec<SnapshotProgressRow>,
) -> Result<SnapshotProgress, TableSyncError> {
    let tables = rows
        .into_iter()
        .map(snapshot_table_progress_from_row)
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(SnapshotProgress { tables })
}

fn snapshot_table_progress_from_row(
    row: SnapshotProgressRow,
) -> Result<(String, TableSnapshotProgress), TableSyncError> {
    let (table, primary_key_json, rows_copied, status) = row;
    let progress = TableSnapshotProgress {
        last_primary_key: parse_progress_primary_key(&primary_key_json)?,
        rows_copied,
        complete: status == "complete",
    };
    Ok((table, progress))
}

fn parse_progress_primary_key(value: &str) -> Result<Option<Vec<String>>, TableSyncError> {
    if value.is_empty() {
        return Ok(None);
    }

    serde_json::from_str(value)
        .map(Some)
        .map_err(|error| TableSyncError::Progress(format!("invalid primary key json: {error}")))
}

fn base_opts(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
    tls_ca_file: Option<&str>,
) -> Opts {
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password))
        .db_name(Some(database))
        .prefer_socket(false);
    if let Some(ca_file) = tls_ca_file {
        builder = builder.ssl_opts(ssl_opts_from_ca(Some(ca_file)));
    }
    Opts::from(builder)
}

fn open_conn(opts: Opts) -> mysql::Result<Conn> {
    Conn::new(opts)
}

fn snapshot_row_from_mysql_row(
    request: &ChunkRequest,
    row: mysql::Row,
) -> Result<SnapshotRow, SnapshotError> {
    let values = row
        .unwrap()
        .into_iter()
        .map(value_to_string)
        .collect::<Vec<_>>();
    let values_by_column = request
        .selected_columns
        .iter()
        .cloned()
        .zip(values)
        .collect::<BTreeMap<_, _>>();
    let primary_key = request
        .primary_key
        .iter()
        .map(|column| {
            let value = values_by_column.get(column).cloned().ok_or_else(|| {
                SnapshotError::InvalidTable(format!(
                    "primary-key column `{column}` was not selected"
                ))
            })?;
            value.ok_or_else(|| {
                SnapshotError::InvalidTable(format!("primary-key column `{column}` was NULL"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SnapshotRow {
        primary_key,
        values: values_by_column,
    })
}

fn row_to_strings(row: mysql::Row) -> Vec<Option<String>> {
    row.unwrap().into_iter().map(value_to_string).collect()
}

pub(crate) fn value_to_string(value: Value) -> Option<String> {
    match value {
        Value::NULL => None,
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).to_string()),
        Value::Int(value) => Some(value.to_string()),
        Value::UInt(value) => Some(value.to_string()),
        Value::Float(value) => Some(value.to_string()),
        Value::Double(value) => Some(value.to_string()),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            Some(format_date(year, month, day, hour, minute, second, micros))
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            Some(format_time(negative, days, hours, minutes, seconds, micros))
        }
    }
}

fn snapshot_boundary_offsets(total_rows: u64, workers: usize) -> Vec<u64> {
    if total_rows == 0 || workers <= 1 {
        return Vec::new();
    }

    let mut offsets = (1..workers)
        .map(|worker| snapshot_boundary_offset(total_rows, workers, worker))
        .filter(|offset| *offset < total_rows)
        .collect::<Vec<_>>();
    offsets.dedup();
    offsets
}

fn snapshot_boundary_offset(total_rows: u64, workers: usize, worker: usize) -> u64 {
    let numerator = total_rows * worker as u64;
    numerator.div_ceil(workers as u64).saturating_sub(1)
}

fn build_snapshot_boundary_select_sql(
    table: &crate::snapshot::SnapshotTable,
    offset: u64,
) -> String {
    let primary_key = quote_column_list(&table.primary_key);
    format!(
        "SELECT {primary_key} FROM {} ORDER BY {primary_key} LIMIT 1 OFFSET {offset}",
        quote_ident(&table.name)
    )
}

fn build_target_column_select_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
        quote_sql_literal(table)
    )
}

fn quote_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_date(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> String {
    let base = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
    if micros == 0 {
        base
    } else {
        format!("{base}.{micros:06}")
    }
}

fn format_time(
    negative: bool,
    days: u32,
    hours: u8,
    minutes: u8,
    seconds: u8,
    micros: u32,
) -> String {
    let sign = if negative { "-" } else { "" };
    let total_hours = days * 24 + u32::from(hours);
    let base = format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}");
    if micros == 0 {
        base
    } else {
        format!("{base}.{micros:06}")
    }
}

fn generated_column_retry_sql(statement: &SqlStatement, error: &str) -> Option<String> {
    let generated_column = generated_column_from_error(error)?;
    let rendered = render_sql_statement(statement).ok()?;
    strip_insert_column(&rendered, &generated_column)
}

fn generated_column_from_error(error: &str) -> Option<String> {
    let marker = "generated column '";
    let start = error.find(marker)? + marker.len();
    let rest = &error[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

fn strip_insert_column(sql: &str, generated_column: &str) -> Option<String> {
    crate::live::strip_insert_column_for_retry(sql, generated_column)
}

fn snapshot_connect_error(error: mysql::Error) -> SnapshotError {
    SnapshotError::InvalidTable(format!("failed to connect to source mysql: {error}"))
}

fn snapshot_query_error(error: mysql::Error) -> SnapshotError {
    SnapshotError::InvalidTable(format!("source mysql query failed: {error}"))
}

fn build_stream_lease_sql(lease_name: &str) -> String {
    format!(
        "SELECT GET_LOCK(SHA2({},256),0)",
        quote_sql_literal(lease_name)
    )
}

fn target_connect_error(error: mysql::Error) -> TargetExecuteError {
    TargetExecuteError::new(format!("failed to connect to target mysql: {error}"))
}

fn target_query_error(error: mysql::Error) -> TargetExecuteError {
    TargetExecuteError::new(format!("target mysql query failed: {error}"))
}

fn progress_connect_error(error: mysql::Error) -> TableSyncError {
    TableSyncError::Progress(format!("failed to connect to target mysql: {error}"))
}

fn progress_query_error(error: mysql::Error) -> TableSyncError {
    TableSyncError::Progress(format!("target progress query failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_mysql_values_like_snapshot_text_rows() {
        assert_eq!(value_to_string(Value::NULL), None);
        assert_eq!(
            value_to_string(Value::Bytes(b"NULL".to_vec())),
            Some("NULL".to_string())
        );
        assert_eq!(value_to_string(Value::Int(-5)), Some("-5".to_string()));
        assert_eq!(value_to_string(Value::UInt(5)), Some("5".to_string()));
        assert_eq!(
            value_to_string(Value::Date(2026, 6, 22, 12, 3, 4, 0)),
            Some("2026-06-22 12:03:04".to_string())
        );
        assert_eq!(
            value_to_string(Value::Time(false, 1, 2, 3, 4, 0)),
            Some("26:03:04".to_string())
        );
    }

    #[test]
    fn target_opts_require_authenticated_tls() {
        let target = TargetMySqlConfig {
            host: "target".to_string(),
            port: 25060,
            user: "target_user".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            tls_ca_file: crate::mysql_support::TARGET_TLS_CA_FILE.to_string(),
            insert_conflict_policy: InsertConflictPolicy::IgnoreDuplicate,
        };

        let opts = base_opts(
            &target.host,
            target.port,
            &target.user,
            &target.password,
            &target.database,
            Some(TARGET_TLS_CA_FILE),
        );

        assert!(opts.get_ssl_opts().is_some());
    }

    #[test]
    fn connection_opts_use_explicit_ca_for_tls() {
        let ca_path = std::env::temp_dir().join(format!(
            "mariadb-mysql-cdc-target-reader-ca-{}",
            std::process::id()
        ));
        std::fs::write(&ca_path, b"test ca").expect("write CA fixture");

        let opts = base_opts(
            "target",
            25060,
            "target_user",
            "secret",
            "globalcomix",
            ca_path.to_str(),
        );
        let ssl = opts.get_ssl_opts().expect("TLS opts");

        assert_eq!(ssl.root_cert_path(), Some(ca_path.as_path()));
        assert!(ssl.skip_domain_validation());

        std::fs::remove_file(ca_path).expect("remove CA fixture");
    }

    #[test]
    fn stream_lease_uses_nonblocking_hashed_mysql_lock() {
        assert_eq!(
            build_stream_lease_sql("cdc-stream:globalcomix"),
            "SELECT GET_LOCK(SHA2('cdc-stream:globalcomix',256),0)"
        );
    }

    #[test]
    fn builds_snapshot_progress_select_sql_for_cdc_table() {
        let sql = build_snapshot_progress_select_sql("cdc.table_sync_progress");

        assert_eq!(
            sql,
            "SELECT table_name, COALESCE(last_primary_key_json, ''), rows_scanned, status FROM `cdc`.`table_sync_progress`"
        );
    }

    #[test]
    fn builds_progress_error_sql_with_table_and_message() {
        let sql =
            build_progress_error_message_sql("cdc.table_sync_progress", "releases", "can't copy");

        assert_eq!(
            sql,
            "INSERT INTO `cdc`.`table_sync_progress` (table_name,mode,status,last_error) VALUES ('releases','apply','error','can''t copy') ON DUPLICATE KEY UPDATE status='error',last_error=VALUES(last_error)"
        );
    }

    #[test]
    fn converts_mysql_progress_rows_to_snapshot_progress() {
        let rows = vec![
            (
                "accounts".to_string(),
                "[\"42\"]".to_string(),
                42,
                "running".to_string(),
            ),
            (
                "releases".to_string(),
                String::new(),
                100,
                "complete".to_string(),
            ),
        ];

        let progress = snapshot_progress_from_rows(rows).expect("progress");

        let accounts = progress.table("accounts").expect("accounts");
        assert_eq!(accounts.last_primary_key, Some(vec!["42".to_string()]));
        assert_eq!(accounts.rows_copied, 42);
        assert!(!accounts.complete);

        let releases = progress.table("releases").expect("releases");
        assert_eq!(releases.last_primary_key, None);
        assert_eq!(releases.rows_copied, 100);
        assert!(releases.complete);
    }

    #[test]
    fn plans_snapshot_boundary_offsets_for_four_workers() {
        assert_eq!(snapshot_boundary_offsets(10, 4), vec![2, 4, 7]);
    }

    #[test]
    fn skips_snapshot_boundary_offsets_when_rows_are_too_sparse() {
        assert_eq!(snapshot_boundary_offsets(2, 4), vec![0, 1]);
    }

    #[test]
    fn builds_snapshot_boundary_select_sql() {
        let table = crate::snapshot::SnapshotTable {
            name: "accounts".to_string(),
            primary_key: vec!["tenant_id".to_string(), "id".to_string()],
            columns: Vec::new(),
        };

        let sql = build_snapshot_boundary_select_sql(&table, 99);

        assert_eq!(
            sql,
            "SELECT `tenant_id`, `id` FROM `accounts` ORDER BY `tenant_id`, `id` LIMIT 1 OFFSET 99"
        );
    }

    #[test]
    fn builds_target_column_select_sql() {
        let sql = build_target_column_select_sql("accounts");

        assert_eq!(
            sql,
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' ORDER BY ORDINAL_POSITION"
        );
    }
}
