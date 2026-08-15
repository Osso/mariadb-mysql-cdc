use crate::checkpoint::Checkpoint;
use crate::live::TargetMySqlConfig;
#[cfg(test)]
use crate::mysql_support::quote_ident;
use crate::mysql_support::{quote_identifier_path, quote_sql_literal, target_mysql_opts};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts};
use std::cell::{Cell, RefCell};

const STREAM_CHECKPOINT_PREFIX: &str = "stream-binlog:";

#[derive(Debug)]
pub struct MySqlStreamCheckpointStore {
    target: TargetMySqlConfig,
    table: String,
    checkpoint_name: String,
    conn: RefCell<Option<Conn>>,
    ensured: Cell<bool>,
    last_checkpoint: RefCell<Option<Checkpoint>>,
}

impl MySqlStreamCheckpointStore {
    pub fn new(target: TargetMySqlConfig, table: String, source_identity: &str) -> Self {
        Self {
            target,
            table,
            checkpoint_name: stream_checkpoint_name(source_identity),
            conn: RefCell::new(None),
            ensured: Cell::new(false),
            last_checkpoint: RefCell::new(None),
        }
    }

    pub fn ensure(&self) -> Result<(), String> {
        if self.ensured.get() {
            return Ok(());
        }
        self.validate_schema()?;
        let checkpoint = self.query_checkpoint_json(&build_checkpoint_select_sql(
            &self.table,
            &self.checkpoint_name,
        ))?;
        if checkpoint.is_none() {
            return Err(format!(
                "source-scoped stream checkpoint `{}` is missing from `{}`; bootstrap it explicitly before starting CDC",
                self.checkpoint_name, self.table
            ));
        }
        self.ensured.set(true);
        Ok(())
    }

    fn validate_schema(&self) -> Result<(), String> {
        let (schema, table) = self
            .table
            .split_once('.')
            .ok_or_else(|| "stream checkpoint table must be schema-qualified".to_string())?;
        let columns_sql = format!(
            "SELECT column_name,LOWER(column_type),is_nullable,LOWER(COALESCE(CAST(column_default AS CHAR),'<null>')),LOWER(extra) FROM information_schema.columns WHERE table_schema={} AND table_name={} ORDER BY ordinal_position",
            quote_sql_literal(schema),
            quote_sql_literal(table),
        );
        let columns = self.with_conn(|conn| {
            conn.query::<(String, String, String, String, String), _>(columns_sql)
                .map_err(mysql_error)
        })?;
        validate_stream_checkpoint_columns(&columns)?;

        let constraints_sql = format!(
            "SELECT constraint_type,enforced FROM information_schema.table_constraints WHERE table_schema={} AND table_name={} ORDER BY constraint_type,constraint_name",
            quote_sql_literal(schema),
            quote_sql_literal(table),
        );
        let constraints = self.with_conn(|conn| {
            conn.query::<(String, String), _>(constraints_sql)
                .map_err(mysql_error)
        })?;
        validate_stream_checkpoint_constraints(&constraints)
    }

    pub fn load(&self) -> Result<Option<Checkpoint>, String> {
        self.ensure()?;
        let row = self.query_checkpoint_json(&build_checkpoint_select_sql(
            &self.table,
            &self.checkpoint_name,
        ))?;
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
        let sql = build_checkpoint_upsert_sql_for_checkpoint(
            &self.table,
            &self.checkpoint_name,
            checkpoint,
        )?;
        self.execute(&sql)?;
        self.last_checkpoint.replace(Some(checkpoint.clone()));
        Ok(())
    }

    pub fn bootstrap(&self, checkpoint: &Checkpoint) -> Result<(), String> {
        self.validate_schema()?;
        let existing = self.query_checkpoint_json(&build_checkpoint_select_sql(
            &self.table,
            &self.checkpoint_name,
        ))?;
        if let Some(value) = existing {
            let existing_checkpoint: Checkpoint = serde_json::from_str(&value)
                .map_err(|error| format!("invalid stream checkpoint JSON: {error}"))?;
            self.last_checkpoint.replace(Some(existing_checkpoint));
            self.ensured.set(true);
            return Ok(());
        }
        let sql = build_checkpoint_insert_sql_for_checkpoint(
            &self.table,
            &self.checkpoint_name,
            checkpoint,
        )?;
        self.execute(&sql)?;
        self.last_checkpoint.replace(Some(checkpoint.clone()));
        self.ensured.set(true);
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
            let conn = Conn::new(target_opts(&self.target)?).map_err(mysql_error)?;
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

pub fn stream_checkpoint_name(source_identity: &str) -> String {
    format!("{STREAM_CHECKPOINT_PREFIX}{source_identity}")
}

#[cfg(test)]
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

#[cfg(test)]
fn build_create_checkpoint_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\
         checkpoint_name VARCHAR(512) PRIMARY KEY,\
         checkpoint_json LONGTEXT NOT NULL,\
         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\
         )",
        quote_identifier_path(table)
    )
}

fn build_checkpoint_select_sql(table: &str, checkpoint_name: &str) -> String {
    build_checkpoint_select_sql_with_suffix(table, checkpoint_name, "")
}

pub(crate) fn build_checkpoint_select_for_update_sql(table: &str, checkpoint_name: &str) -> String {
    build_checkpoint_select_sql_with_suffix(table, checkpoint_name, " FOR UPDATE")
}

fn build_checkpoint_select_sql_with_suffix(
    table: &str,
    checkpoint_name: &str,
    suffix: &str,
) -> String {
    format!(
        "SELECT checkpoint_json FROM {} WHERE checkpoint_name = {} LIMIT 1{}",
        quote_identifier_path(table),
        quote_sql_literal(checkpoint_name),
        suffix
    )
}

pub(crate) fn build_checkpoint_upsert_sql_for_checkpoint(
    table: &str,
    checkpoint_name: &str,
    checkpoint: &Checkpoint,
) -> Result<String, String> {
    let json = serde_json::to_string(checkpoint)
        .map_err(|error| format!("failed to encode stream checkpoint: {error}"))?;
    Ok(build_checkpoint_upsert_sql(table, checkpoint_name, &json))
}

fn build_checkpoint_insert_sql_for_checkpoint(
    table: &str,
    checkpoint_name: &str,
    checkpoint: &Checkpoint,
) -> Result<String, String> {
    let json = serde_json::to_string(checkpoint)
        .map_err(|error| format!("failed to encode stream checkpoint: {error}"))?;
    Ok(format!(
        "INSERT INTO {} (checkpoint_name, checkpoint_json) VALUES ({}, {})",
        quote_identifier_path(table),
        quote_sql_literal(checkpoint_name),
        quote_sql_literal(&json)
    ))
}

fn build_checkpoint_upsert_sql(
    table: &str,
    checkpoint_name: &str,
    checkpoint_json: &str,
) -> String {
    format!(
        "INSERT INTO {} (checkpoint_name, checkpoint_json) VALUES ({}, {}) \
         ON DUPLICATE KEY UPDATE checkpoint_json=VALUES(checkpoint_json)",
        quote_identifier_path(table),
        quote_sql_literal(checkpoint_name),
        quote_sql_literal(checkpoint_json)
    )
}

fn expected_stream_checkpoint_columns() -> Vec<(String, String, String, String, String)> {
    [
        ("checkpoint_name", "varchar(512)", "NO", "<null>", ""),
        ("checkpoint_json", "longtext", "NO", "<null>", ""),
        (
            "updated_at",
            "timestamp",
            "NO",
            "current_timestamp",
            "default_generated on update current_timestamp",
        ),
    ]
    .into_iter()
    .map(|(name, column_type, nullable, default_value, extra)| {
        (
            name.to_string(),
            column_type.to_string(),
            nullable.to_string(),
            default_value.to_string(),
            extra.to_string(),
        )
    })
    .collect()
}

fn validate_stream_checkpoint_columns(
    columns: &[(String, String, String, String, String)],
) -> Result<(), String> {
    let expected = expected_stream_checkpoint_columns();
    if columns == expected {
        return Ok(());
    }
    Err(format!(
        "stream checkpoint column schema mismatch: expected {expected:?}, found {columns:?}"
    ))
}

fn validate_stream_checkpoint_constraints(constraints: &[(String, String)]) -> Result<(), String> {
    let expected = [("PRIMARY KEY".to_string(), "YES".to_string())];
    if constraints == expected {
        return Ok(());
    }
    Err(format!(
        "stream checkpoint constraint inventory mismatch: expected {expected:?}, found {constraints:?}"
    ))
}

fn target_opts(target: &TargetMySqlConfig) -> Result<Opts, String> {
    target_mysql_opts(target)
}

fn mysql_error(error: mysql::Error) -> String {
    format!("checkpoint mysql query failed: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_target_connection_uses_configured_ca() {
        let target = TargetMySqlConfig {
            host: "target-db".to_string(),
            port: 3306,
            user: "cdc".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
            insert_conflict_policy: crate::live::InsertConflictPolicy::Error,
        };

        let opts = target_opts(&target).expect("checkpoint target options");

        assert_eq!(
            opts.get_ssl_opts().and_then(|ssl| ssl.root_cert_path()),
            Some(std::path::Path::new(&target.tls_ca_file))
        );
    }

    #[test]
    fn checkpoint_name_is_scoped_to_source_identity() {
        assert_eq!(
            stream_checkpoint_name("production-source"),
            "stream-binlog:production-source"
        );
    }

    #[test]
    fn creates_checkpoint_table_in_cdc_schema_by_default() {
        assert_eq!(default_stream_checkpoint_table(), "cdc.stream_checkpoint");

        let schema_sql =
            build_create_checkpoint_schema_sql("cdc.stream_checkpoint").expect("schema create sql");
        let table_sql = build_create_checkpoint_table_sql("cdc.stream_checkpoint");

        assert_eq!(schema_sql, "CREATE DATABASE IF NOT EXISTS `cdc`");
        assert!(table_sql.starts_with("CREATE TABLE IF NOT EXISTS `cdc`.`stream_checkpoint`"));
        assert!(table_sql.contains("checkpoint_json LONGTEXT NOT NULL"));
        assert!(
            build_checkpoint_select_for_update_sql(
                "cdc.stream_checkpoint",
                "stream-binlog:test-source"
            )
            .ends_with("LIMIT 1 FOR UPDATE")
        );
    }

    #[test]
    fn validates_preprovisioned_checkpoint_schema() {
        let columns = expected_stream_checkpoint_columns();
        assert!(validate_stream_checkpoint_columns(&columns).is_ok());
        let mut wrong = columns;
        wrong[0].1 = "varchar(64)".to_string();
        assert!(validate_stream_checkpoint_columns(&wrong).is_err());
        assert!(
            validate_stream_checkpoint_constraints(&[(
                "PRIMARY KEY".to_string(),
                "YES".to_string(),
            )])
            .is_ok()
        );
    }

    #[test]
    fn upsert_checkpoint_sql_stores_single_named_checkpoint() {
        let sql = build_checkpoint_upsert_sql(
            "cdc.stream_checkpoint",
            "stream-binlog:test-source",
            "{\"source_file\":\"bin\"}",
        );

        assert!(sql.contains("`cdc`.`stream_checkpoint`"));
        assert!(sql.contains("'stream-binlog:test-source'"));
        assert!(sql.contains("'{\"source_file\":\"bin\"}'"));
    }
}
