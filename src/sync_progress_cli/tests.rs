use super::*;
use std::env;
use std::fs;
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn parses_progress_config_with_checkpoint_and_source_counts() {
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
        "--checkpoint-file",
        "/var/lib/cdc/stream-checkpoint.json",
    ]))
    .expect("progress config");

    assert_eq!(config.target.host, "target-db");
    assert_eq!(config.target.password, "target-pass");
    assert_eq!(config.source.host, "source-db");
    assert_eq!(config.source.password, "source-pass");
    assert_eq!(config.progress_table, "cdc.table_sync_progress");
    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");
    assert_eq!(
        config.checkpoint_file,
        Some(PathBuf::from("/var/lib/cdc/stream-checkpoint.json"))
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
    ]))
    .expect("progress config");

    assert_eq!(config.progress_table, "cdc.table_sync_progress");
    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");
    assert_eq!(config.checkpoint_file, None);
    assert_eq!(config.source.host, "");
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
            "mariadb": "/tmp/mariadb-noverify",
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
    assert_eq!(config.mariadb, "/tmp/mariadb-noverify");
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
    let row = "releases\t200\t10\t3\t1\trunning\t[\"42\"]\t20\t";

    let rows = parse_progress_rows(row).expect("progress rows");

    assert_eq!(
        rows,
        vec![SyncProgressRow {
            table: "releases".to_string(),
            rows_scanned: 200,
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
fn formats_rate_and_eta_when_total_rows_are_known() {
    let eta = eta(50, rate(100, 10));

    assert_eq!(eta, Some(5));
    assert_eq!(display_percent(25, Some(100)), "25.00%");
    assert_eq!(display_duration(Some(125)), "2m05s");
}

#[test]
fn builds_progress_table_lookup_for_qualified_and_default_schema_tables() {
    assert_eq!(
        progress_table_parts("globalcomix", "cdc.table_sync_progress"),
        ("cdc".to_string(), "table_sync_progress".to_string())
    );
    assert_eq!(
        progress_table_parts("globalcomix", "table_sync_progress"),
        ("globalcomix".to_string(), "table_sync_progress".to_string())
    );

    let sql = build_progress_table_exists_query("globalcomix", "cdc.table_sync_progress");

    assert!(sql.contains("table_schema = 'cdc'"));
    assert!(sql.contains("table_name = 'table_sync_progress'"));
}

fn args<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}

fn set_env(name: &str, value: &str) {
    unsafe {
        env::set_var(name, value);
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
