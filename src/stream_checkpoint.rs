use crate::checkpoint::Checkpoint;
use crate::live::TargetMySqlConfig;
use crate::mysql_support::{quote_ident, quote_identifier_path, quote_sql_literal};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, SslOpts};
use std::cell::{Cell, RefCell};

const DEFAULT_CHECKPOINT_NAME: &str = "stream-binlog";

#[derive(Debug)]
pub struct MySqlStreamCheckpointStore {
    target: TargetMySqlConfig,
    table: String,
    conn: RefCell<Option<Conn>>,
    ensured: Cell<bool>,
    last_checkpoint: RefCell<Option<Checkpoint>>,
}

impl MySqlStreamCheckpointStore {
    pub fn new(target: TargetMySqlConfig, table: String) -> Self {
        Self {
            target,
            table,
            conn: RefCell::new(None),
            ensured: Cell::new(false),
            last_checkpoint: RefCell::new(None),
        }
    }

    pub fn ensure(&self) -> Result<(), String> {
        if self.ensured.get() {
            return Ok(());
        }
        if let Some(schema_sql) = build_create_checkpoint_schema_sql(&self.table) {
            self.execute(&schema_sql)?;
        }
        self.execute(&build_create_checkpoint_table_sql(&self.table))?;
        self.ensured.set(true);
        Ok(())
    }

    pub fn load(&self) -> Result<Option<Checkpoint>, String> {
        self.ensure()?;
        let row = self.query_checkpoint_json(&build_checkpoint_select_sql(&self.table))?;
        let Some(value) = row else {
            self.last_checkpoint.replace(None);
            return Ok(None);
        };
        let checkpoint: Checkpoint = serde_json::from_str(&value)
            .map_err(|error| format!("invalid stream checkpoint JSON: {error}"))?;
        self.last_checkpoint.replace(Some(checkpoint.clone()));
        Ok(Some(checkpoint))
    }

    pub fn checkpoint_for_skip(&self) -> Result<Option<Checkpoint>, String> {
        if let Some(checkpoint) = self.last_checkpoint.borrow().clone() {
            return Ok(Some(checkpoint));
        }
        self.load()
    }

    pub fn save(&self, checkpoint: &Checkpoint) -> Result<(), String> {
        self.ensure()?;
        let json = serde_json::to_string(checkpoint)
            .map_err(|error| format!("failed to encode stream checkpoint: {error}"))?;
        self.execute(&build_checkpoint_upsert_sql(&self.table, &json))?;
        self.last_checkpoint.replace(Some(checkpoint.clone()));
        Ok(())
    }

    fn execute(&self, sql: &str) -> Result<(), String> {
        self.with_conn(|conn| conn.query_drop(sql).map_err(mysql_error))
    }

    fn query_checkpoint_json(&self, sql: &str) -> Result<Option<String>, String> {
        self.with_conn(|conn| conn.query_first(sql).map_err(mysql_error))
    }

    fn with_conn<T>(
        &self,
        query: impl FnOnce(&mut Conn) -> Result<T, String>,
    ) -> Result<T, String> {
        if self.conn.borrow().is_none() {
            let conn = Conn::new(target_opts(&self.target)).map_err(mysql_error)?;
            self.conn.replace(Some(conn));
        }
        let mut conn_ref = self.conn.borrow_mut();
        let conn = conn_ref
            .as_mut()
            .expect("checkpoint mysql connection exists after initialization");
        query(conn)
    }
}

pub fn default_stream_checkpoint_table() -> String {
    "cdc.stream_checkpoint".to_string()
}

fn build_create_checkpoint_schema_sql(table: &str) -> Option<String> {
    let schema = table.split('.').next()?;
    if schema == table {
        return None;
    }
    Some(format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        quote_ident(schema)
    ))
}

fn build_create_checkpoint_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\
         checkpoint_name VARCHAR(64) PRIMARY KEY,\
         checkpoint_json LONGTEXT NOT NULL,\
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\
         )",
        quote_identifier_path(table)
    )
}

fn build_checkpoint_select_sql(table: &str) -> String {
    format!(
        "SELECT checkpoint_json FROM {} WHERE checkpoint_name = {} LIMIT 1",
        quote_identifier_path(table),
        quote_sql_literal(DEFAULT_CHECKPOINT_NAME)
    )
}

fn build_checkpoint_upsert_sql(table: &str, checkpoint_json: &str) -> String {
    format!(
        "INSERT INTO {} (checkpoint_name, checkpoint_json) VALUES ({}, {}) \
         ON DUPLICATE KEY UPDATE checkpoint_json=VALUES(checkpoint_json)",
        quote_identifier_path(table),
        quote_sql_literal(DEFAULT_CHECKPOINT_NAME),
        quote_sql_literal(checkpoint_json)
    )
}

fn target_opts(target: &TargetMySqlConfig) -> Opts {
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(&target.host))
        .tcp_port(target.port)
        .user(Some(&target.user))
        .pass(Some(&target.password))
        .db_name(Some(&target.database))
        .prefer_socket(false)
        .ssl_opts(insecure_ssl_opts());
    Opts::from(builder)
}

fn insecure_ssl_opts() -> SslOpts {
    SslOpts::default()
        .with_danger_skip_domain_validation(true)
        .with_danger_accept_invalid_certs(true)
}

fn mysql_error(error: mysql::Error) -> String {
    format!("checkpoint mysql query failed: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_checkpoint_table_in_cdc_schema_by_default() {
        assert_eq!(default_stream_checkpoint_table(), "cdc.stream_checkpoint");

        let schema_sql =
            build_create_checkpoint_schema_sql("cdc.stream_checkpoint").expect("schema create sql");
        let table_sql = build_create_checkpoint_table_sql("cdc.stream_checkpoint");

        assert_eq!(schema_sql, "CREATE DATABASE IF NOT EXISTS `cdc`");
        assert!(table_sql.starts_with("CREATE TABLE IF NOT EXISTS `cdc`.`stream_checkpoint`"));
        assert!(table_sql.contains("checkpoint_json LONGTEXT NOT NULL"));
    }

    #[test]
    fn upsert_checkpoint_sql_stores_single_named_checkpoint() {
        let sql = build_checkpoint_upsert_sql("cdc.stream_checkpoint", "{\"source_file\":\"bin\"}");

        assert!(sql.contains("`cdc`.`stream_checkpoint`"));
        assert!(sql.contains("'stream-binlog'"));
        assert!(sql.contains("'{\"source_file\":\"bin\"}'"));
    }
}
