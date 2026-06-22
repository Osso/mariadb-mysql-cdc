use super::*;

mod checkpoint_config {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/main/tests/checkpoint_config.rs"
    ));
}

#[test]
fn parses_apply_binlog_config_with_all_source_and_target_options() {
    set_env("SRC_PASSWORD", "source-secret");
    set_env("TARGET_PASSWORD", "target-secret");

    let config = parse_apply_binlog_config(args([
        "--source-host",
        "10.0.0.2",
        "--source-port",
        "3307",
        "--source-user",
        "cdc",
        "--source-password-env",
        "SRC_PASSWORD",
        "--source-database",
        "app",
        "--binlog-file",
        "mysqld-bin.000777",
        "--start-position",
        "12345",
        "--stop-position",
        "45678",
        "--target-host",
        "target.db",
        "--target-port",
        "25060",
        "--target-user",
        "writer",
        "--target-password-env",
        "TARGET_PASSWORD",
        "--target-database",
        "app_target",
        "--insert-conflict-policy",
        "ignore-duplicate",
        "--mariadb",
        "/usr/bin/mariadb",
        "--mariadb-binlog",
        "/usr/bin/mariadb-binlog",
        "--checkpoint-file",
        "/var/lib/mariadb-mysql-cdc/stream-checkpoint.json",
        "--checkpoint-table",
        "cdc.stream_checkpoint",
        "--max-reconnects",
        "3",
    ]))
    .expect("apply config");

    assert_eq!(config.source.host, "10.0.0.2");
    assert_eq!(config.source.port, 3307);
    assert_eq!(config.source.password, "source-secret");
    assert_eq!(config.source.database.as_deref(), Some("app"));
    assert_eq!(config.source.binlog_file, "mysqld-bin.000777");
    assert_eq!(config.source.start_position, 12345);
    assert_eq!(config.source.stop_position, Some(45678));
    assert_eq!(config.target.host, "target.db");
    assert_eq!(config.target.port, 25060);
    assert_eq!(config.target.password, "target-secret");
    assert_eq!(config.target.database, "app_target");
    assert_eq!(
        config.target.insert_conflict_policy,
        live::InsertConflictPolicy::IgnoreDuplicate
    );
    assert_eq!(config.mariadb, "/usr/bin/mariadb");
    assert_eq!(config.mariadb_binlog, "/usr/bin/mariadb-binlog");
    assert_eq!(
        config.checkpoint_file,
        Some(PathBuf::from(
            "/var/lib/mariadb-mysql-cdc/stream-checkpoint.json"
        ))
    );
    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");
    assert_eq!(config.max_reconnects, 3);
}

#[test]
fn parses_probe_config_with_optional_coordinates_and_tools() {
    set_env("PROBE_PASSWORD", "probe-secret");

    let config = parse_probe_config(args([
        "--host",
        "10.0.0.2",
        "--port",
        "3307",
        "--user",
        "cdc",
        "--password-env",
        "PROBE_PASSWORD",
        "--binlog-file",
        "mysqld-bin.000777",
        "--start-position",
        "12345",
        "--stop-position",
        "45678",
        "--mariadb",
        "/usr/bin/mariadb",
        "--mariadb-binlog",
        "/usr/bin/mariadb-binlog",
    ]))
    .expect("probe config");

    assert_eq!(config.host, "10.0.0.2");
    assert_eq!(config.port, 3307);
    assert_eq!(config.user, "cdc");
    assert_eq!(config.password, "probe-secret");
    assert_eq!(config.binlog_file.as_deref(), Some("mysqld-bin.000777"));
    assert_eq!(config.start_position, Some(12345));
    assert_eq!(config.stop_position, Some(45678));
    assert_eq!(config.mariadb, "/usr/bin/mariadb");
    assert_eq!(config.mariadb_binlog, "/usr/bin/mariadb-binlog");
}

#[test]
fn parses_catchup_snapshot_config() {
    set_env("SRC_PASSWORD", "source-secret");
    set_env("TARGET_PASSWORD", "target-secret");

    let config = parse_catchup_snapshot_config(args([
        "--source-host",
        "10.0.0.2",
        "--source-port",
        "3307",
        "--source-user",
        "cdc",
        "--source-password-env",
        "SRC_PASSWORD",
        "--source-database",
        "globalcomix",
        "--target-host",
        "target.db",
        "--target-port",
        "25060",
        "--target-user",
        "target_user",
        "--target-password-env",
        "TARGET_PASSWORD",
        "--target-database",
        "globalcomix",
        "--progress-file",
        "/var/lib/cdc/snapshot-progress.json",
        "--chunk-size",
        "5000",
        "--throttle-ms",
        "250",
        "--table",
        "activity_tracking",
        "--mariadb",
        "/usr/bin/mariadb",
    ]))
    .expect("catchup config");

    assert_eq!(config.source.host, "10.0.0.2");
    assert_eq!(config.source.port, 3307);
    assert_eq!(config.source.password, "source-secret");
    assert_eq!(config.source.database, "globalcomix");
    assert_eq!(config.target.host, "target.db");
    assert_eq!(config.target.port, 25060);
    assert_eq!(config.target.password, "target-secret");
    assert_eq!(config.target.database, "globalcomix");
    assert_eq!(
        config.progress_file,
        PathBuf::from("/var/lib/cdc/snapshot-progress.json")
    );
    assert_eq!(config.progress_table, "cdc.table_sync_progress");
    assert_eq!(config.chunk_size, 5000);
    assert_eq!(config.throttle, Duration::from_millis(250));
    assert_eq!(config.table.as_deref(), Some("activity_tracking"));
    assert_eq!(config.source.mariadb, "/usr/bin/mariadb");
}

#[test]
fn rejects_apply_binlog_options_without_values() {
    let error = parse_apply_binlog_config(args(["--source-host"])).expect_err("missing value");

    assert_eq!(error, "--source-host needs a value");
}

#[test]
fn rejects_probe_options_without_values() {
    let error = parse_probe_config(args(["--host"])).expect_err("missing value");

    assert_eq!(error, "--host needs a value");
}

#[test]
fn rejects_unknown_probe_option() {
    let error = parse_probe_config(args(["--bogus", "x"])).expect_err("unknown option");

    assert_eq!(error, "unknown probe option: --bogus");
}

#[test]
fn rejects_unknown_apply_binlog_option() {
    let error = apply_binlog_option(&mut live::ApplyBinlogConfig::default(), "--bogus", "x")
        .expect_err("unknown option");

    assert_eq!(error, "unknown apply-binlog option: --bogus");
}

#[test]
fn rejects_unknown_catchup_snapshot_option() {
    let error = catchup_snapshot_option(
        &mut mysql_snapshot::CatchupSnapshotConfig {
            source: mysql_snapshot::MySqlConnectionConfig::default(),
            target: live::TargetMySqlConfig::default(),
            progress_file: PathBuf::new(),
            progress_table: "cdc.table_sync_progress".to_string(),
            chunk_size: 10_000,
            throttle: Duration::ZERO,
            table: None,
        },
        "--bogus",
        "x",
    )
    .expect_err("unknown option");

    assert_eq!(error, "unknown catchup-snapshot option: --bogus");
}

#[test]
fn parses_catchup_progress_file() {
    let progress_file = parse_progress_file(args([
        "--progress-file",
        "/var/lib/cdc/snapshot-progress.json",
    ]))
    .expect("progress file");

    assert_eq!(
        progress_file,
        PathBuf::from("/var/lib/cdc/snapshot-progress.json")
    );
}

#[test]
fn rejects_unknown_catchup_progress_option() {
    let error = parse_progress_file(args(["--bogus", "/tmp/progress.json"])).expect_err("unknown");

    assert_eq!(error, "unknown catchup-progress option: --bogus");
}

#[test]
fn rejects_invalid_numeric_options() {
    assert_eq!(
        parse_u16("--source-port", "not-a-port").expect_err("invalid port"),
        "--source-port must be an integer"
    );
    assert_eq!(
        parse_u64("--start-position", "not-a-position").expect_err("invalid position"),
        "--start-position must be an integer"
    );
    assert_eq!(
        parse_usize("--chunk-size", "not-a-size").expect_err("invalid size"),
        "--chunk-size must be an integer"
    );
    assert_eq!(
        parse_u32("--max-reconnects", "not-a-count").expect_err("invalid count"),
        "--max-reconnects must be an integer"
    );
}

#[test]
fn rejects_unknown_insert_conflict_policy() {
    let error = parse_insert_policy("replace").expect_err("unknown policy");

    assert_eq!(error, "unknown insert conflict policy: replace");
}

#[test]
fn reports_missing_password_env() {
    let missing_name = "MARIADB_MYSQL_CDC_TEST_MISSING_PASSWORD";
    remove_env(missing_name);

    let error = read_env_password(missing_name).expect_err("missing password");

    assert_eq!(error, format!("{missing_name} is not set"));
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
