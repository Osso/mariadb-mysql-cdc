use crate::checkpoint::FileCheckpointStore;
use crate::stream_checkpoint::{MySqlStreamCheckpointStore, default_stream_checkpoint_table};
use crate::{live, mysql_snapshot};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

pub fn run_sync_progress_command(args: Vec<String>, usage: &str) {
    let config = match parse_sync_progress_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };

    match read_sync_progress(&config) {
        Ok(report) => print!("{report}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

#[derive(Clone, Debug)]
struct SyncProgressConfig {
    target: live::TargetMySqlConfig,
    source: mysql_snapshot::MySqlConnectionConfig,
    mariadb: String,
    progress_table: String,
    checkpoint_table: String,
    table: Option<String>,
    checkpoint_file: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SyncProgressRow {
    table: String,
    rows_scanned: u64,
    inserts: u64,
    updates: u64,
    extra_target_rows: u64,
    status: String,
    last_primary_key: String,
    elapsed_seconds: u64,
    last_error: String,
}

fn parse_sync_progress_config(args: Vec<String>) -> Result<SyncProgressConfig, String> {
    let mut config = default_sync_progress_config();
    load_file_config(&mut config)?;
    let mut index = 0;

    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;
        sync_progress_option(&mut config, flag, value)?;
        index += 2;
    }

    validate_sync_progress_config(&config)?;
    Ok(config)
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    sync_progress: Option<FileSyncProgressConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileSyncProgressConfig {
    target_host: Option<String>,
    target_port: Option<u16>,
    target_user: Option<String>,
    target_password_env: Option<String>,
    target_database: Option<String>,
    mariadb: Option<String>,
    progress_table: Option<String>,
    checkpoint_table: Option<String>,
}

fn default_sync_progress_config() -> SyncProgressConfig {
    SyncProgressConfig {
        target: live::TargetMySqlConfig::default(),
        source: mysql_snapshot::MySqlConnectionConfig::default(),
        mariadb: "mariadb".to_string(),
        progress_table: "cdc.table_sync_progress".to_string(),
        checkpoint_table: default_stream_checkpoint_table(),
        table: None,
        checkpoint_file: None,
    }
}

fn load_file_config(config: &mut SyncProgressConfig) -> Result<(), String> {
    let Some(path) = config_path() else {
        return Ok(());
    };
    let contents = fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let file_config = serde_json::from_str::<FileConfig>(&contents)
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))?;
    apply_file_config(config, file_config)
}

fn config_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("MARIADB_MYSQL_CDC_CONFIG") {
        if path.is_empty() {
            return None;
        }
        return Some(PathBuf::from(path));
    }

    let home = std::env::var_os("HOME")?;
    let path = PathBuf::from(home)
        .join(".config")
        .join("mariadb-mysql-cdc")
        .join("config.json");
    path.exists().then_some(path)
}

fn apply_file_config(
    config: &mut SyncProgressConfig,
    file_config: FileConfig,
) -> Result<(), String> {
    let Some(sync_progress) = file_config.sync_progress else {
        return Ok(());
    };
    apply_optional_string(&mut config.target.host, sync_progress.target_host);
    apply_optional_u16(&mut config.target.port, sync_progress.target_port);
    apply_optional_string(&mut config.target.user, sync_progress.target_user);
    apply_optional_password_env(
        &mut config.target.password,
        sync_progress.target_password_env,
    )?;
    apply_optional_string(&mut config.target.database, sync_progress.target_database);
    apply_optional_string(&mut config.mariadb, sync_progress.mariadb);
    apply_optional_string(&mut config.progress_table, sync_progress.progress_table);
    apply_optional_string(&mut config.checkpoint_table, sync_progress.checkpoint_table);
    Ok(())
}

fn apply_optional_string(target: &mut String, value: Option<String>) {
    if let Some(value) = value {
        *target = value;
    }
}

fn apply_optional_u16(target: &mut u16, value: Option<u16>) {
    if let Some(value) = value {
        *target = value;
    }
}

fn apply_optional_password_env(target: &mut String, value: Option<String>) -> Result<(), String> {
    let Some(env_name) = value else {
        return Ok(());
    };
    *target = crate::read_env_password(&env_name)?;
    Ok(())
}

fn sync_progress_option(
    config: &mut SyncProgressConfig,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    if target_option(&mut config.target, flag, value)? {
        return Ok(());
    }
    if source_option(&mut config.source, flag, value)? {
        return Ok(());
    }

    match flag {
        "--progress-table" => config.progress_table = value.to_string(),
        "--checkpoint-table" => config.checkpoint_table = value.to_string(),
        "--checkpoint-file" => config.checkpoint_file = Some(PathBuf::from(value)),
        "--table" => config.table = Some(value.to_string()),
        "--mariadb" => {
            config.mariadb = value.to_string();
            config.source.mariadb = value.to_string();
        }
        other => return Err(format!("unknown sync-progress option: {other}")),
    }

    Ok(())
}

fn target_option(
    target: &mut live::TargetMySqlConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--target-host" => target.host = value.to_string(),
        "--target-port" => target.port = crate::parse_u16(flag, value)?,
        "--target-user" => target.user = value.to_string(),
        "--target-password-env" => target.password = crate::read_env_password(value)?,
        "--target-database" => target.database = value.to_string(),
        _ => return Ok(false),
    }

    Ok(true)
}

fn source_option(
    source: &mut mysql_snapshot::MySqlConnectionConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--source-host" => source.host = value.to_string(),
        "--source-port" => source.port = crate::parse_u16(flag, value)?,
        "--source-user" => source.user = value.to_string(),
        "--source-password-env" => source.password = crate::read_env_password(value)?,
        "--source-database" => source.database = value.to_string(),
        _ => return Ok(false),
    }

    Ok(true)
}

fn validate_sync_progress_config(config: &SyncProgressConfig) -> Result<(), String> {
    validate_required_target(config)?;
    validate_optional_source(config)?;
    if config.progress_table.is_empty() {
        return Err("progress table is required".to_string());
    }
    Ok(())
}

fn validate_required_target(config: &SyncProgressConfig) -> Result<(), String> {
    if config.target.host.is_empty() {
        return Err("target host is required".to_string());
    }
    if config.target.user.is_empty() {
        return Err("target user is required".to_string());
    }
    if config.target.password.is_empty() {
        return Err("target password is required".to_string());
    }
    if config.target.database.is_empty() {
        return Err("target database is required".to_string());
    }
    Ok(())
}

fn validate_optional_source(config: &SyncProgressConfig) -> Result<(), String> {
    if !has_source_count_config(config) {
        return Ok(());
    }
    if config.source.host.is_empty() {
        return Err("source host is required when source count options are used".to_string());
    }
    if config.source.user.is_empty() {
        return Err("source user is required when source count options are used".to_string());
    }
    if config.source.password.is_empty() {
        return Err("source password is required when source count options are used".to_string());
    }
    if config.source.database.is_empty() {
        return Err("source database is required when source count options are used".to_string());
    }
    Ok(())
}

fn has_source_count_config(config: &SyncProgressConfig) -> bool {
    !config.source.host.is_empty()
        || !config.source.user.is_empty()
        || !config.source.password.is_empty()
        || !config.source.database.is_empty()
}

fn read_sync_progress(config: &SyncProgressConfig) -> Result<String, String> {
    let rows = query_progress_rows(config)?;
    let checkpoint = read_checkpoint_line(config)?;
    let mut lines = Vec::new();
    if let Some(checkpoint) = checkpoint {
        lines.push(checkpoint);
    }
    match rows {
        Some(rows) => lines.extend(rows.iter().map(|row| format_progress_row(config, row))),
        None => lines.push(format!(
            "sync_progress_table table={} status=missing",
            config.progress_table
        )),
    }
    Ok(format!("{}\n", lines.join("\n")))
}

fn query_progress_rows(
    config: &SyncProgressConfig,
) -> Result<Option<Vec<SyncProgressRow>>, String> {
    if !progress_table_exists(config)? {
        return Ok(None);
    }
    let sql = build_progress_query(&config.progress_table, config.table.as_deref());
    let output = run_mysql_query(&config.mariadb, &config.target, &sql)?;
    parse_progress_rows(&output).map(Some)
}

fn progress_table_exists(config: &SyncProgressConfig) -> Result<bool, String> {
    let sql = build_progress_table_exists_query(&config.target.database, &config.progress_table);
    let output = run_mysql_query(&config.mariadb, &config.target, &sql)?;
    Ok(output.trim() == "1")
}

fn read_checkpoint_line(config: &SyncProgressConfig) -> Result<Option<String>, String> {
    let checkpoint = read_stream_checkpoint(config)?;
    Ok(checkpoint.map(|checkpoint| {
        format!(
            "stream_checkpoint file={} position={} event_type={}",
            checkpoint.source_file, checkpoint.source_position, checkpoint.last_event.event_type
        )
    }))
}

fn read_stream_checkpoint(
    config: &SyncProgressConfig,
) -> Result<Option<crate::checkpoint::Checkpoint>, String> {
    if let Some(path) = &config.checkpoint_file {
        return FileCheckpointStore::new(path)
            .load()
            .map_err(|error| error.to_string());
    }
    MySqlStreamCheckpointStore::new(
        config.mariadb.clone(),
        config.target.clone(),
        config.checkpoint_table.clone(),
    )
    .load()
}

fn format_progress_row(config: &SyncProgressConfig, row: &SyncProgressRow) -> String {
    let rows_per_second = rate(row.rows_scanned, row.elapsed_seconds);
    let inserts_per_second = rate(row.inserts, row.elapsed_seconds);
    let total_rows = source_count(config, &row.table).ok().flatten();
    let remaining = total_rows.map(|total| total.saturating_sub(row.rows_scanned));
    let eta_seconds = remaining.and_then(|remaining| eta(remaining, rows_per_second));

    format!(
        "table={} status={} rows_scanned={} total_rows={} progress={} rows_per_second={:.2} inserts_per_second={:.2} eta={} last_pk={} inserts={} updates={} extras={} error={}",
        row.table,
        row.status,
        row.rows_scanned,
        display_optional_u64(total_rows),
        display_percent(row.rows_scanned, total_rows),
        rows_per_second,
        inserts_per_second,
        display_duration(eta_seconds),
        display_last_primary_key(&row.last_primary_key),
        row.inserts,
        row.updates,
        row.extra_target_rows,
        display_error(&row.last_error)
    )
}

fn build_progress_query(progress_table: &str, table: Option<&str>) -> String {
    let table_filter = table
        .map(|table| format!(" WHERE table_name = {}", quote_sql_literal(table)))
        .unwrap_or_default();
    format!(
        "SELECT table_name, rows_scanned, inserts_applied, updates_applied, extra_target_rows, status, COALESCE(last_primary_key_json,''), GREATEST(1,TIMESTAMPDIFF(SECOND,created_at,IF(status='running',NOW(),updated_at))), COALESCE(last_error,'') FROM {}{} ORDER BY FIELD(status,'running','error','complete'), updated_at DESC, table_name",
        quote_identifier_path(progress_table),
        table_filter
    )
}

fn build_progress_table_exists_query(default_schema: &str, progress_table: &str) -> String {
    let (schema, table) = progress_table_parts(default_schema, progress_table);
    format!(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = {} AND table_name = {}",
        quote_sql_literal(&schema),
        quote_sql_literal(&table)
    )
}

fn progress_table_parts(default_schema: &str, progress_table: &str) -> (String, String) {
    let parts = progress_table.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [schema, table] => (schema.to_string(), table.to_string()),
        [table] => (default_schema.to_string(), table.to_string()),
        _ => (default_schema.to_string(), progress_table.to_string()),
    }
}

fn parse_progress_rows(output: &str) -> Result<Vec<SyncProgressRow>, String> {
    output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(parse_progress_row)
        .collect()
}

fn parse_progress_row(line: &str) -> Result<SyncProgressRow, String> {
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() != 9 {
        return Err(format!(
            "progress row has {} fields, expected 9",
            fields.len()
        ));
    }

    Ok(SyncProgressRow {
        table: fields[0].to_string(),
        rows_scanned: parse_u64_field("rows_scanned", fields[1])?,
        inserts: parse_u64_field("inserts_applied", fields[2])?,
        updates: parse_u64_field("updates_applied", fields[3])?,
        extra_target_rows: parse_u64_field("extra_target_rows", fields[4])?,
        status: fields[5].to_string(),
        last_primary_key: fields[6].to_string(),
        elapsed_seconds: parse_u64_field("elapsed_seconds", fields[7])?,
        last_error: fields[8].to_string(),
    })
}

fn source_count(config: &SyncProgressConfig, table: &str) -> Result<Option<u64>, String> {
    if config.source.host.is_empty() {
        return Ok(None);
    }
    let sql = format!("SELECT COUNT(*) FROM {}", quote_ident(table));
    let output = run_source_query(&config.mariadb, &config.source, &sql)?;
    output
        .trim()
        .parse()
        .map(Some)
        .map_err(|_| format!("source count for {table} was not an integer"))
}

fn run_mysql_query(
    mariadb: &str,
    target: &live::TargetMySqlConfig,
    sql: &str,
) -> Result<String, String> {
    run_mariadb_query(mariadb, target_mysql_args(target), sql)
}

fn run_source_query(
    mariadb: &str,
    source: &mysql_snapshot::MySqlConnectionConfig,
    sql: &str,
) -> Result<String, String> {
    run_mariadb_query(mariadb, source_mysql_args(source), sql)
}

fn run_mariadb_query(mariadb: &str, args: Vec<String>, sql: &str) -> Result<String, String> {
    let output = Command::new(mariadb)
        .args(args)
        .arg("--batch")
        .arg("--skip-column-names")
        .arg("--execute")
        .arg(sql)
        .output()
        .map_err(|error| format!("failed to run mariadb: {error}"))?;
    command_output(output)
}

fn command_output(output: std::process::Output) -> Result<String, String> {
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).to_string());
    }

    Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
}

fn target_mysql_args(target: &live::TargetMySqlConfig) -> Vec<String> {
    vec![
        "--host".to_string(),
        target.host.clone(),
        "--port".to_string(),
        target.port.to_string(),
        "--user".to_string(),
        target.user.clone(),
        format!("--password={}", target.password),
        "--database".to_string(),
        target.database.clone(),
    ]
}

fn source_mysql_args(source: &mysql_snapshot::MySqlConnectionConfig) -> Vec<String> {
    vec![
        "--host".to_string(),
        source.host.clone(),
        "--port".to_string(),
        source.port.to_string(),
        "--user".to_string(),
        source.user.clone(),
        format!("--password={}", source.password),
        "--database".to_string(),
        source.database.clone(),
    ]
}

fn rate(count: u64, seconds: u64) -> f64 {
    count as f64 / seconds.max(1) as f64
}

fn eta(remaining: u64, rows_per_second: f64) -> Option<u64> {
    if rows_per_second <= 0.0 {
        None
    } else {
        Some((remaining as f64 / rows_per_second).ceil() as u64)
    }
}

fn display_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn display_percent(done: u64, total: Option<u64>) -> String {
    match total {
        Some(0) => "100.00%".to_string(),
        Some(total) => format!("{:.2}%", (done as f64 / total as f64) * 100.0),
        None => "-".to_string(),
    }
}

fn display_duration(seconds: Option<u64>) -> String {
    let Some(seconds) = seconds else {
        return "-".to_string();
    };
    let minutes = seconds / 60;
    let seconds = seconds % 60;
    format!("{minutes}m{seconds:02}s")
}

fn display_last_primary_key(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn display_error(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn parse_u64_field(field: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{field} must be an integer"))
}

fn quote_identifier_path(identifier: &str) -> String {
    identifier
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

#[cfg(test)]
mod tests;
