use crate::mysql_support::{SOURCE_TLS_CA_FILE, qualified_table_parts};
use crate::stream_checkpoint::default_stream_checkpoint_table;
use crate::{live, mysql_snapshot};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

mod cache;
mod output;
mod progress_format;

const SYNC_PROGRESS_DB_TIMEOUT: Duration = Duration::from_secs(2);
pub fn run_sync_progress_command(args: Vec<String>, usage: &str) {
    let config = match parse_sync_progress_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };

    match read_sync_progress(&config) {
        Ok(report) => output::write_report_or_exit(&report),
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
    progress_table: String,
    checkpoint_table: String,
    source_identity: Option<String>,
    table: Option<String>,
    run_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SyncProgressRow {
    run_id: String,
    table: String,
    rows_scanned: u64,
    total_rows: Option<u64>,
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
    target_password: Option<String>,
    target_password_env: Option<String>,
    target_database: Option<String>,
    target_tls_ca_file: Option<String>,
    progress_table: Option<String>,
    checkpoint_table: Option<String>,
    source_identity: Option<String>,
}

fn default_sync_progress_config() -> SyncProgressConfig {
    SyncProgressConfig {
        target: live::TargetMySqlConfig::default(),
        source: mysql_snapshot::MySqlConnectionConfig::default(),
        progress_table: "cdc.table_sync_progress".to_string(),
        checkpoint_table: default_stream_checkpoint_table(),
        source_identity: None,
        table: None,
        run_id: None,
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
    apply_optional_string(&mut config.target.password, sync_progress.target_password);
    if config.target.password.is_empty() {
        apply_optional_password_env(
            &mut config.target.password,
            sync_progress.target_password_env,
        )?;
    }
    apply_optional_string(&mut config.target.database, sync_progress.target_database);
    apply_optional_string(
        &mut config.target.tls_ca_file,
        sync_progress.target_tls_ca_file,
    );
    apply_optional_string(&mut config.progress_table, sync_progress.progress_table);
    apply_optional_string(&mut config.checkpoint_table, sync_progress.checkpoint_table);
    config.source_identity = sync_progress.source_identity;
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
        "--source-identity" => config.source_identity = Some(value.to_string()),
        "--table" => config.table = Some(value.to_string()),
        "--run-id" => config.run_id = Some(value.to_string()),
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
        "--target-tls-ca-file" => target.tls_ca_file = value.to_string(),
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
    if config.target.tls_ca_file.is_empty() {
        return Err("target TLS CA file is required".to_string());
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
    let (sender, receiver) = mpsc::channel();
    let cache_key = sync_progress_cache_key(config);
    let live_config = config.clone();
    thread::spawn(move || {
        let _ = sender.send(read_live_sync_progress(&live_config));
    });

    match receiver.recv_timeout(cache::sync_progress_cache_timeout()) {
        Ok(Ok(report)) => {
            cache::write_sync_progress_cache(&cache_key, &report);
            Ok(report)
        }
        Ok(Err(error)) if is_authoritative_checkpoint_error(&error) => Err(error),
        Ok(Err(error)) => cache::read_sync_progress_cache(&cache_key)
            .map(|cached| cache::format_cached_sync_progress(&cached, &error))
            .ok_or(error),
        Err(mpsc::RecvTimeoutError::Timeout) => cache::read_sync_progress_cache(&cache_key)
            .map(|cached| cache::format_cached_sync_progress(&cached, "live read exceeded 1500ms"))
            .ok_or_else(|| {
                "sync-progress live read exceeded 1500ms and no cache exists".to_string()
            }),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("sync-progress worker exited without a result".to_string())
        }
    }
}

fn sync_progress_cache_key(config: &SyncProgressConfig) -> String {
    let identity = format!(
        "target={}:{}:{};progress={};checkpoint={}:{};table={};run={}",
        config.target.host,
        config.target.port,
        config.target.database,
        config.progress_table,
        config.checkpoint_table,
        config.source_identity.as_deref().unwrap_or(""),
        config.table.as_deref().unwrap_or(""),
        config.run_id.as_deref().unwrap_or(""),
    );
    format!("v3-{:016x}", fnv1a64(identity.as_bytes()))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn is_authoritative_checkpoint_error(error: &str) -> bool {
    error.starts_with("authoritative_checkpoint_error")
}

fn read_live_sync_progress(config: &SyncProgressConfig) -> Result<String, String> {
    let mut reader = TargetProgressReader::new(&config.target)?;
    let rows = query_progress_rows(config, &mut reader)?;
    let checkpoint = read_checkpoint_line(config, &mut reader)?;
    let mut lines = Vec::new();
    if let Some(checkpoint) = checkpoint {
        lines.push(checkpoint);
    }
    match rows {
        Some(rows) if rows.is_empty() => lines.push(format_progress_table_status(
            &config.progress_table,
            "empty",
        )),
        Some(rows) => lines.extend(progress_format::format_progress_rows(config, &rows)),
        None => lines.push(format_progress_table_status(
            &config.progress_table,
            "missing",
        )),
    }
    Ok(format!("{}\n", lines.join("\n")))
}

fn format_progress_table_status(table: &str, status: &str) -> String {
    format!("sync_progress_table table={table} status={status}")
}

fn query_progress_rows(
    config: &SyncProgressConfig,
    reader: &mut TargetProgressReader,
) -> Result<Option<Vec<SyncProgressRow>>, String> {
    if !reader.progress_table_exists(&config.progress_table)? {
        return Ok(None);
    }
    let has_total_rows = reader.progress_table_has_total_rows(&config.progress_table)?;
    let has_run_id = reader.progress_table_has_run_id(&config.progress_table)?;
    if config.run_id.is_some() && !has_run_id {
        return Err(format!(
            "progress table `{}` has no run_id column; select a run-scoped table such as `cdc.table_sync_runs`",
            config.progress_table
        ));
    }
    let sql = build_progress_query(
        &config.progress_table,
        config.table.as_deref(),
        config.run_id.as_deref(),
        has_total_rows,
        has_run_id,
    );
    reader.query_progress_rows(&sql).map(Some)
}

fn read_checkpoint_line(
    config: &SyncProgressConfig,
    reader: &mut TargetProgressReader,
) -> Result<Option<String>, String> {
    let checkpoint = read_stream_checkpoint(config, reader)?;
    Ok(checkpoint.map(|checkpoint| {
        format!(
            "stream_checkpoint file={} position={} event_type={}",
            checkpoint.source_file, checkpoint.source_position, checkpoint.last_event.event_type
        )
    }))
}

fn read_stream_checkpoint(
    config: &SyncProgressConfig,
    reader: &mut TargetProgressReader,
) -> Result<Option<crate::checkpoint::Checkpoint>, String> {
    let Some(source_identity) = &config.source_identity else {
        return Ok(None);
    };
    let checkpoint_name = crate::stream_checkpoint::stream_checkpoint_name(source_identity);
    reader
        .read_stream_checkpoint(&config.checkpoint_table, &checkpoint_name)
        .map_err(|error| format!("authoritative_checkpoint_error {error}"))?
        .ok_or_else(|| {
            format!(
                "authoritative_checkpoint_error missing table={} checkpoint_name={checkpoint_name}",
                config.checkpoint_table
            )
        })
        .map(Some)
}

fn build_progress_query(
    progress_table: &str,
    table: Option<&str>,
    run_id: Option<&str>,
    has_total_rows: bool,
    has_run_id: bool,
) -> String {
    let mut filters = Vec::new();
    if let Some(table) = table {
        filters.push(format!("table_name = {}", quote_sql_literal(table)));
    }
    if let Some(run_id) = run_id {
        filters.push(format!("run_id = {}", quote_sql_literal(run_id)));
    }
    let progress_filter = if filters.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", filters.join(" AND "))
    };
    let total_rows_expression = if has_total_rows {
        "COALESCE(total_rows, '')"
    } else {
        "''"
    };
    let run_id_expression = if has_run_id { "run_id" } else { "''" };
    format!(
        "SELECT {run_id_expression}, table_name, rows_scanned, {total_rows_expression}, inserts_applied, updates_applied, extra_target_rows, status, COALESCE(last_primary_key_json,''), GREATEST(1,TIMESTAMPDIFF(SECOND,created_at,IF(status='running',NOW(),updated_at))), COALESCE(last_error,'') FROM {}{} ORDER BY FIELD(status,'running','error','complete'), updated_at DESC, table_name",
        quote_identifier_path(progress_table),
        progress_filter
    )
}

fn build_progress_run_id_exists_query(default_schema: &str, progress_table: &str) -> String {
    build_progress_column_exists_query(default_schema, progress_table, "run_id")
}

fn build_progress_total_rows_exists_query(default_schema: &str, progress_table: &str) -> String {
    build_progress_column_exists_query(default_schema, progress_table, "total_rows")
}

fn build_progress_column_exists_query(
    default_schema: &str,
    progress_table: &str,
    column: &str,
) -> String {
    let (schema, table) = qualified_table_parts(default_schema, progress_table);
    format!(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = {} AND table_name = {} AND column_name = {}",
        quote_sql_literal(&schema),
        quote_sql_literal(&table),
        quote_sql_literal(column)
    )
}

fn build_progress_table_exists_query(default_schema: &str, progress_table: &str) -> String {
    let (schema, table) = qualified_table_parts(default_schema, progress_table);
    format!(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = {} AND table_name = {}",
        quote_sql_literal(&schema),
        quote_sql_literal(&table)
    )
}

#[cfg(test)]
fn parse_progress_rows(output: &str) -> Result<Vec<SyncProgressRow>, String> {
    output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(parse_progress_row)
        .collect()
}

#[cfg(test)]
fn parse_progress_row(line: &str) -> Result<SyncProgressRow, String> {
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() != 11 {
        return Err(format!(
            "progress row has {} fields, expected 11",
            fields.len()
        ));
    }

    Ok(SyncProgressRow {
        run_id: fields[0].to_string(),
        table: fields[1].to_string(),
        rows_scanned: parse_u64_field("rows_scanned", fields[2])?,
        total_rows: parse_optional_u64_field("total_rows", fields[3])?,
        inserts: parse_u64_field("inserts_applied", fields[4])?,
        updates: parse_u64_field("updates_applied", fields[5])?,
        extra_target_rows: parse_u64_field("extra_target_rows", fields[6])?,
        status: fields[7].to_string(),
        last_primary_key: fields[8].to_string(),
        elapsed_seconds: parse_u64_field("elapsed_seconds", fields[9])?,
        last_error: fields[10].to_string(),
    })
}

type ProgressDbRow = (
    String,
    String,
    u64,
    String,
    u64,
    u64,
    u64,
    String,
    String,
    u64,
    String,
);

struct TargetProgressReader {
    conn: Conn,
    default_database: String,
}

impl TargetProgressReader {
    fn new(target: &live::TargetMySqlConfig) -> Result<Self, String> {
        let opts = target_opts(target)?;
        let mut conn = Conn::new(opts).map_err(mysql_error)?;
        conn.query_drop(live::target_session_init_command())
            .map_err(mysql_error)?;
        Ok(Self {
            conn,
            default_database: target.database.clone(),
        })
    }

    fn progress_table_exists(&mut self, progress_table: &str) -> Result<bool, String> {
        let sql = build_progress_table_exists_query(&self.default_database, progress_table);
        self.query_count(&sql).map(|count| count == 1)
    }

    fn progress_table_has_total_rows(&mut self, progress_table: &str) -> Result<bool, String> {
        let sql = build_progress_total_rows_exists_query(&self.default_database, progress_table);
        self.query_count(&sql).map(|count| count == 1)
    }

    fn progress_table_has_run_id(&mut self, progress_table: &str) -> Result<bool, String> {
        let sql = build_progress_run_id_exists_query(&self.default_database, progress_table);
        self.query_count(&sql).map(|count| count == 1)
    }

    fn query_progress_rows(&mut self, sql: &str) -> Result<Vec<SyncProgressRow>, String> {
        let rows = self
            .conn
            .query::<ProgressDbRow, _>(sql)
            .map_err(mysql_error)?;
        rows.into_iter()
            .map(sync_progress_row_from_db)
            .collect::<Result<Vec<_>, _>>()
    }

    fn read_stream_checkpoint(
        &mut self,
        checkpoint_table: &str,
        checkpoint_name: &str,
    ) -> Result<Option<crate::checkpoint::Checkpoint>, String> {
        let sql = build_checkpoint_json_select_sql(checkpoint_table, checkpoint_name);
        let json = match self.conn.query_first::<String, _>(sql) {
            Ok(json) => json,
            Err(error) if is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(mysql_error(error)),
        };
        json.map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(|error| format!("invalid stream checkpoint JSON: {error}"))
    }

    fn query_count(&mut self, sql: &str) -> Result<u64, String> {
        self.conn
            .query_first::<u64, _>(sql)
            .map_err(mysql_error)?
            .ok_or_else(|| "count query returned no rows".to_string())
    }
}

fn sync_progress_row_from_db(row: ProgressDbRow) -> Result<SyncProgressRow, String> {
    Ok(SyncProgressRow {
        run_id: row.0,
        table: row.1,
        rows_scanned: row.2,
        total_rows: parse_optional_u64_field("total_rows", &row.3)?,
        inserts: row.4,
        updates: row.5,
        extra_target_rows: row.6,
        status: row.7,
        last_primary_key: row.8,
        elapsed_seconds: row.9,
        last_error: row.10,
    })
}

fn build_checkpoint_json_select_sql(table: &str, checkpoint_name: &str) -> String {
    format!(
        "SELECT checkpoint_json FROM {} WHERE checkpoint_name = {} LIMIT 1",
        quote_identifier_path(table),
        crate::mysql_support::quote_sql_literal(checkpoint_name),
    )
}

fn parse_optional_u64_field(field: &str, value: &str) -> Result<Option<u64>, String> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_u64_field(field, value).map(Some)
}

fn source_count(config: &SyncProgressConfig, table: &str) -> Result<Option<u64>, String> {
    if config.source.host.is_empty() {
        return Ok(None);
    }
    let sql = format!("SELECT COUNT(*) FROM {}", quote_ident(table));
    let mut conn = Conn::new(source_opts(&config.source)?).map_err(mysql_error)?;
    conn.query_first::<u64, _>(sql)
        .map_err(mysql_error)?
        .map(Some)
        .ok_or_else(|| format!("source count for {table} returned no rows"))
}

fn target_opts(target: &live::TargetMySqlConfig) -> Result<Opts, String> {
    mysql_opts(
        &target.host,
        target.port,
        &target.user,
        &target.password,
        &target.database,
        &target.tls_ca_file,
        &format!("target `{}`:{}", target.host, target.port),
    )
}

fn source_opts(source: &mysql_snapshot::MySqlConnectionConfig) -> Result<Opts, String> {
    let endpoint = format!("source `{}`:{}", source.host, source.port);
    mysql_opts(
        &source.host,
        source.port,
        &source.user,
        &source.password,
        &source.database,
        SOURCE_TLS_CA_FILE,
        &endpoint,
    )
}

fn mysql_opts(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
    tls_ca_file: &str,
    endpoint: &str,
) -> Result<Opts, String> {
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password))
        .db_name(Some(database))
        .prefer_socket(false)
        .tcp_connect_timeout(Some(SYNC_PROGRESS_DB_TIMEOUT))
        .read_timeout(Some(SYNC_PROGRESS_DB_TIMEOUT))
        .write_timeout(Some(SYNC_PROGRESS_DB_TIMEOUT))
        .ssl_opts(crate::mysql_support::ssl_opts_from_ca(
            endpoint,
            host,
            tls_ca_file,
        )?);
    Ok(Opts::from(builder))
}

fn mysql_error(error: mysql::Error) -> String {
    error.to_string()
}

fn is_missing_table_error(error: &mysql::Error) -> bool {
    matches!(error, mysql::Error::MySqlError(inner) if inner.code == 1146)
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
