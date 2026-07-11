use super::progress_format::{
    display_duration, display_percent, eta, format_progress_row, format_progress_rows, rate,
};
use super::*;
use crate::mysql_support::qualified_table_parts;
use std::env;
use std::fs;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn parses_progress_config_with_source_scoped_checkpoint_and_counts() {
    let _guard = env_lock();
    disable_file_config();
    set_env("SYNC_PROGRESS_TARGET_PASSWORD", "target-pass");
    set_env("SYNC_PROGRESS_SOURCE_PASSWORD", "source-pass");

    let config = parse_sync_progress_config(args([
        "--target-host",
        "target-db",
        "--target-user",
        "target-user",
        "--target-password-env",
        "SYNC_PROGRESS_TARGET_PASSWORD",
        "--target-database",
        "globalcomix",
        "--source-host",
        "source-db",
        "--source-user",
        "source-user",
        "--source-password-env",
        "SYNC_PROGRESS_SOURCE_PASSWORD",
        "--source-database",
        "globalcomix",
        "--source-identity",
        "production-source",
        "--run-id",
        "repair-20260710-01",
    ]))
    .expect("progress config");

    assert_eq!(config.target.host, "target-db");
    assert_eq!(config.target.password, "target-pass");
    assert_eq!(config.source.host, "source-db");
    assert_eq!(config.source.password, "source-pass");
    assert_eq!(config.progress_table, "cdc.table_sync_progress");
    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");
    assert_eq!(config.run_id.as_deref(), Some("repair-20260710-01"));
    assert_eq!(
        config.source_identity.as_deref(),
        Some("production-source")
    );
}

#[test]
fn parses_progress_config_without_source_or_checkpoint_file() {
    let _guard = env_lock();
    disable_file_config();
    set_env("SYNC_PROGRESS_TARGET_PASSWORD_ONLY", "target-pass");

    let config = parse_sync_progress_config(args([
        "--target-host",
        "target-db",
        "--target-user",
        "target-user",
        "--target-password-env",
        "SYNC_PROGRESS_TARGET_PASSWORD_ONLY",
        "--target-database",
        "globalcomix",
        "--source-identity",
        "production-source",
    ]))
    .expect("progress config");

    assert_eq!(config.progress_table, "cdc.table_sync_progress");
    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");
    assert_eq!(
        config.source_identity.as_deref(),
        Some("production-source")
    );
    assert_eq!(config.source.host, "");
}

#[test]
fn cache_key_changes_with_source_and_target_identity() {
    let mut left = default_sync_progress_config();
    left.target.host = "target-a".to_string();
    left.target.database = "globalcomix".to_string();
    left.source_identity = Some("source-a".to_string());
    let mut right = left.clone();
    right.source_identity = Some("source-b".to_string());
    assert_ne!(
        sync_progress_cache_key(&left),
        sync_progress_cache_key(&right)
    );

    right = left.clone();
    right.target.host = "target-b".to_string();
    assert_ne!(
        sync_progress_cache_key(&left),
        sync_progress_cache_key(&right)
    );
}

#[test]
fn loads_sync_progress_defaults_from_config_file() {
    let _guard = env_lock();
    let path = unique_path("config.json");
    fs::write(
        &path,
        r#"{
          "sync_progress": {
            "target_host": "target-from-config",
            "target_port": 25060,
            "target_user": "target_user",
            "target_password_env": "SYNC_PROGRESS_CONFIG_PASSWORD",
            "target_database": "globalcomix",
            "progress_table": "cdc.table_sync_progress",
            "checkpoint_table": "cdc.stream_checkpoint"
          }
        }"#,
    )
    .expect("write config");
    set_env("MARIADB_MYSQL_CDC_CONFIG", path.to_string_lossy().as_ref());
    set_env("SYNC_PROGRESS_CONFIG_PASSWORD", "target-pass");

    let config = parse_sync_progress_config(Vec::new()).expect("progress config");

    assert_eq!(config.target.host, "target-from-config");
    assert_eq!(config.target.port, 25060);
    assert_eq!(config.target.user, "target_user");
    assert_eq!(config.target.password, "target-pass");
    assert_eq!(config.target.database, "globalcomix");
    assert_eq!(config.progress_table, "cdc.table_sync_progress");
    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");

    let _ = fs::remove_file(path);
}

#[test]
fn loads_direct_target_password_from_config_file() {
    let _guard = env_lock();
    let path = unique_path("config-password.json");
    fs::write(
        &path,
        r#"{
          "sync_progress": {
            "target_host": "target-from-config",
            "target_user": "target_user",
            "target_password": "target-pass",
            "target_database": "globalcomix"
          }
        }"#,
    )
    .expect("write config");
    set_env("MARIADB_MYSQL_CDC_CONFIG", path.to_string_lossy().as_ref());

    let config = parse_sync_progress_config(Vec::new()).expect("progress config");

    assert_eq!(config.target.host, "target-from-config");
    assert_eq!(config.target.password, "target-pass");
    assert_eq!(config.target.database, "globalcomix");

    let _ = fs::remove_file(path);
}

#[test]
fn command_line_overrides_config_file_defaults() {
    let _guard = env_lock();
    let path = unique_path("config-override.json");
    fs::write(
        &path,
        r#"{
          "sync_progress": {
            "target_host": "target-from-config",
            "target_user": "config-user",
            "target_password_env": "SYNC_PROGRESS_CONFIG_PASSWORD_OVERRIDE",
            "target_database": "globalcomix"
          }
        }"#,
    )
    .expect("write config");
    set_env("MARIADB_MYSQL_CDC_CONFIG", path.to_string_lossy().as_ref());
    set_env("SYNC_PROGRESS_CONFIG_PASSWORD_OVERRIDE", "config-pass");
    set_env("SYNC_PROGRESS_ARG_PASSWORD", "arg-pass");

    let config = parse_sync_progress_config(args([
        "--target-host",
        "target-from-args",
        "--target-user",
        "arg-user",
        "--target-password-env",
        "SYNC_PROGRESS_ARG_PASSWORD",
    ]))
    .expect("progress config");

    assert_eq!(config.target.host, "target-from-args");
    assert_eq!(config.target.user, "arg-user");
    assert_eq!(config.target.password, "arg-pass");
    assert_eq!(config.target.database, "globalcomix");

    let _ = fs::remove_file(path);
}

#[test]
fn parses_progress_rows() {
    let row = "repair-20260710-01\treleases\t200\t1000\t10\t3\t1\trunning\t[\"42\"]\t20\t";

    let rows = parse_progress_rows(row).expect("progress rows");

    assert_eq!(
        rows,
        vec![SyncProgressRow {
            run_id: "repair-20260710-01".to_string(),
            table: "releases".to_string(),
            rows_scanned: 200,
            total_rows: Some(1000),
            inserts: 10,
            updates: 3,
            extra_target_rows: 1,
            status: "running".to_string(),
            last_primary_key: "[\"42\"]".to_string(),
            elapsed_seconds: 20,
            last_error: String::new(),
        }]
    );
}

#[test]
fn empty_progress_rows_are_reported_explicitly() {
    assert_eq!(
        format_progress_table_status("cdc.table_sync_progress", "empty"),
        "sync_progress_table table=cdc.table_sync_progress status=empty"
    );
}

#[test]
fn formats_rate_and_eta_when_total_rows_are_known() {
    let eta = eta(50, rate(100, 10));

    assert_eq!(eta, Some(5));
    assert_eq!(display_percent(25, Some(100)), "25.00%");
    assert_eq!(display_duration(Some(125)), "2m05s");
}

#[test]
fn formats_progress_from_stored_total_rows_without_source_config() {
    let row = SyncProgressRow {
        run_id: "repair-01".to_string(),
        table: "access_tokens".to_string(),
        rows_scanned: 21_000,
        total_rows: Some(42_000),
        inserts: 21_000,
        updates: 0,
        extra_target_rows: 0,
        status: "running".to_string(),
        last_primary_key: "[\"21001\"]".to_string(),
        elapsed_seconds: 100,
        last_error: String::new(),
    };
    let config = default_sync_progress_config();

    let line = format_progress_row(&config, &row);

    assert!(line.contains("total_rows=42000"));
    assert!(line.contains("progress=50.00%"));
    assert!(line.contains("eta=1m40s"));
}

#[test]
fn formats_running_progress_at_bottom_under_header() {
    let config = default_sync_progress_config();
    let rows = vec![
        progress_row("running_table", "running"),
        progress_row("complete_table", "complete"),
        progress_row("error_table", "error"),
    ];

    let lines = format_progress_rows(&config, &rows);

    assert_eq!(lines[0], format_progress_row(&config, &rows[1]));
    assert_eq!(lines[1], format_progress_row(&config, &rows[2]));
    assert_eq!(lines[2], "sync_progress_section name=in_progress");
    assert_eq!(lines[3], format_progress_row(&config, &rows[0]));
}

#[test]
fn aggregates_running_range_progress_by_parent_table() {
    let config = default_sync_progress_config();
    let rows = vec![
        range_progress_row("comics_releases_fragments_views#range0", 40, 100),
        range_progress_row("comics_releases_fragments_views#range1", 30, 100),
        progress_row("unrelated", "running"),
    ];

    let lines = format_progress_rows(&config, &rows);

    assert_eq!(lines[0], "sync_progress_section name=in_progress");
    assert!(lines[1].contains("table=comics_releases_fragments_views status=running"));
    assert!(lines[1].contains("rows_scanned=70"));
    assert!(lines[1].contains("total_rows=200"));
    assert!(lines[1].contains("progress=35.00%"));
    assert!(!lines.iter().any(|line| line.contains("#range")));
    assert!(
        !lines
            .iter()
            .any(|line| line.contains("sync_progress_section name=range_details"))
    );
}

#[test]
fn builds_run_scoped_progress_query_with_run_id_filter() {
    let sql = build_progress_query(
        "cdc.table_sync_runs",
        Some("releases"),
        Some("repair-01"),
        true,
        true,
    );

    assert!(sql.starts_with("SELECT run_id, table_name"));
    assert!(sql.contains("WHERE table_name = 'releases' AND run_id = 'repair-01'"));
}

#[test]
fn builds_progress_table_lookup_for_qualified_and_default_schema_tables() {
    assert_eq!(
        qualified_table_parts("globalcomix", "cdc.table_sync_progress"),
        ("cdc".to_string(), "table_sync_progress".to_string())
    );
    assert_eq!(
        qualified_table_parts("globalcomix", "table_sync_progress"),
        ("globalcomix".to_string(), "table_sync_progress".to_string())
    );

    let sql = build_progress_table_exists_query("globalcomix", "cdc.table_sync_progress");

    assert!(sql.contains("table_schema = 'cdc'"));
    assert!(sql.contains("table_name = 'table_sync_progress'"));
}

#[test]
fn builds_total_rows_column_lookup_for_progress_table() {
    let sql = build_progress_total_rows_exists_query("globalcomix", "cdc.table_sync_progress");

    assert!(sql.contains("information_schema.columns"));
    assert!(sql.contains("table_schema = 'cdc'"));
    assert!(sql.contains("table_name = 'table_sync_progress'"));
    assert!(sql.contains("column_name = 'total_rows'"));
}

#[test]
fn target_progress_connection_has_short_io_timeouts() {
    let target = live::TargetMySqlConfig {
        host: "target-db".to_string(),
        port: 3306,
        user: "cdc".to_string(),
        password: "secret".to_string(),
        database: "globalcomix".to_string(),
        insert_conflict_policy: live::InsertConflictPolicy::Error,
    };

    let opts = target_opts(&target);

    assert_eq!(opts.get_tcp_connect_timeout(), Some(Duration::from_secs(2)));
    assert_eq!(opts.get_read_timeout(), Some(&Duration::from_secs(2)));
    assert_eq!(opts.get_write_timeout(), Some(&Duration::from_secs(2)));
}

#[test]
fn sync_progress_cache_timeout_defaults_and_accepts_override() {
    let _guard = env_lock();
    remove_env("MARIADB_MYSQL_CDC_SYNC_PROGRESS_TIMEOUT_MS");
    assert_eq!(
        cache::sync_progress_cache_timeout(),
        Duration::from_millis(1500)
    );

    set_env("MARIADB_MYSQL_CDC_SYNC_PROGRESS_TIMEOUT_MS", "9000");

    assert_eq!(cache::sync_progress_cache_timeout(), Duration::from_secs(9));
}

#[test]
fn formats_stale_sync_progress_cache_with_age_and_reason() {
    let cache = cache::CachedSyncProgress {
        report: "sync_progress_table table=cdc.table_sync_progress status=empty\n".to_string(),
        modified: SystemTime::now() - Duration::from_secs(42),
    };

    let output = cache::format_cached_sync_progress(&cache, "live read exceeded 1500ms");

    assert!(output.starts_with(
        "sync_progress_cache status=stale age_seconds=42 reason=live_read_exceeded_1500ms\n"
    ));
    assert!(output.contains("sync_progress_table table=cdc.table_sync_progress status=empty"));
}

fn progress_row(table: &str, status: &str) -> SyncProgressRow {
    SyncProgressRow {
        run_id: String::new(),
        table: table.to_string(),
        rows_scanned: 10,
        total_rows: Some(100),
        inserts: 10,
        updates: 0,
        extra_target_rows: 0,
        status: status.to_string(),
        last_primary_key: String::new(),
        elapsed_seconds: 5,
        last_error: String::new(),
    }
}

fn range_progress_row(table: &str, rows_scanned: u64, total_rows: u64) -> SyncProgressRow {
    SyncProgressRow {
        run_id: String::new(),
        table: table.to_string(),
        rows_scanned,
        total_rows: Some(total_rows),
        inserts: rows_scanned,
        updates: 0,
        extra_target_rows: 0,
        status: "running".to_string(),
        last_primary_key: String::new(),
        elapsed_seconds: 10,
        last_error: String::new(),
    }
}

fn args<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}

fn set_env(name: &str, value: &str) {
    unsafe {
        env::set_var(name, value);
    }
}

fn remove_env(name: &str) {
    unsafe {
        env::remove_var(name);
    }
}

fn disable_file_config() {
    set_env("MARIADB_MYSQL_CDC_CONFIG", "");
}

fn unique_path(file_name: &str) -> PathBuf {
    let mut path = env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    path.push(format!("mariadb-mysql-cdc-{nanos}-{file_name}"));
    path
}

fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().expect("env lock")
}
