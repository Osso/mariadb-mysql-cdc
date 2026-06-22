use crate::checkpoint::Checkpoint;
use crate::live::TargetMySqlConfig;
use crate::mysql_support::{
    quote_ident, quote_identifier_path, quote_sql_literal, target_mysql_args,
};
use std::process::Command;

const DEFAULT_CHECKPOINT_NAME: &str = "stream-binlog";

#[derive(Clone, Debug)]
pub struct MySqlStreamCheckpointStore {
    mariadb: String,
    target: TargetMySqlConfig,
    table: String,
}

impl MySqlStreamCheckpointStore {
    pub fn new(mariadb: String, target: TargetMySqlConfig, table: String) -> Self {
        Self {
            mariadb,
            target,
            table,
        }
    }

    pub fn ensure(&self) -> Result<(), String> {
        if let Some(schema_sql) = build_create_checkpoint_schema_sql(&self.table) {
            self.execute(&schema_sql)?;
        }
        self.execute(&build_create_checkpoint_table_sql(&self.table))
    }

    pub fn load(&self) -> Result<Option<Checkpoint>, String> {
        self.ensure()?;
        let output = self.query(&build_checkpoint_select_sql(&self.table))?;
        let Some(line) = output.lines().find(|line| !line.trim().is_empty()) else {
            return Ok(None);
        };
        serde_json::from_str(line)
            .map(Some)
            .map_err(|error| format!("invalid stream checkpoint JSON: {error}"))
    }

    pub fn save(&self, checkpoint: &Checkpoint) -> Result<(), String> {
        self.ensure()?;
        let json = serde_json::to_string(checkpoint)
            .map_err(|error| format!("failed to encode stream checkpoint: {error}"))?;
        self.execute(&build_checkpoint_upsert_sql(&self.table, &json))
    }

    fn execute(&self, sql: &str) -> Result<(), String> {
        let output = self.command(sql).output().map_err(command_spawn_error)?;
        if output.status.success() {
            return Ok(());
        }
        Err(command_stderr(output))
    }

    fn query(&self, sql: &str) -> Result<String, String> {
        let output = self.command(sql).output().map_err(command_spawn_error)?;
        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).to_string());
        }
        Err(command_stderr(output))
    }

    fn command(&self, sql: &str) -> Command {
        let mut command = Command::new(&self.mariadb);
        command
            .args(target_mysql_args(&self.target))
            .arg("--batch")
            .arg("--raw")
            .arg("--skip-column-names")
            .arg("--execute")
            .arg(sql);
        command
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

fn command_spawn_error(error: std::io::Error) -> String {
    format!("failed to run mariadb: {error}")
}

fn command_stderr(output: std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim().to_string()
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
