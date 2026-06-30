use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn drift_check_cli_exits_three_when_drift_is_reported() {
    let mariadb = write_fake_mariadb(
        "drift",
        r#"#!/usr/bin/env bash
if [[ "$*" == *"target_db"* ]]; then
  echo 9
else
  echo 10
fi
"#,
    );

    let output = run_drift_check(&mariadb, &["--table", "accounts"]);

    assert_eq!(output.status.code(), Some(3));
    assert!(stderr(&output).is_empty());
    assert!(stdout(&output).contains("drift_check tables=1 mismatches=1"));
    assert!(stdout(&output).contains("status=drift"));
}

#[test]
fn drift_check_cli_exits_three_when_table_is_missing() {
    let mariadb = write_fake_mariadb(
        "target_missing",
        r#"#!/usr/bin/env bash
if [[ "$*" == *"target_db"* ]]; then
  echo "ERROR 1146 (42S02) at line 1: Table 'target_db.accounts' doesn't exist" >&2
  exit 1
fi
echo 10
"#,
    );

    let output = run_drift_check(&mariadb, &["--table", "accounts"]);

    assert_eq!(output.status.code(), Some(3));
    assert!(stderr(&output).is_empty());
    assert!(stdout(&output).contains("drift_check tables=1 mismatches=1"));
    assert!(stdout(&output).contains("status=target_missing"));
}

#[test]
fn drift_check_cli_exits_zero_when_report_is_clean() {
    let mariadb = write_fake_mariadb(
        "clean",
        r#"#!/usr/bin/env bash
echo 10
"#,
    );

    let output = run_drift_check(&mariadb, &["--table", "accounts"]);

    assert_eq!(output.status.code(), Some(0));
    assert!(stderr(&output).is_empty());
    assert!(stdout(&output).contains("drift_check tables=1 mismatches=0"));
    assert!(stdout(&output).contains("status=ok"));
}

#[test]
fn drift_check_cli_keeps_query_errors_as_exit_one() {
    let mariadb = write_fake_mariadb(
        "query_error",
        r#"#!/usr/bin/env bash
echo "ERROR 1045 (28000): Access denied for user" >&2
exit 1
"#,
    );

    let output = run_drift_check(&mariadb, &["--table", "accounts"]);

    assert_eq!(output.status.code(), Some(1));
    assert!(stdout(&output).is_empty());
    assert!(stderr(&output).contains("ERROR 1045"));
}

#[test]
fn drift_check_cli_keeps_config_errors_as_exit_two() {
    let output = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"))
        .args([
            "drift-check",
            "--source-host",
            "source-host",
            "--source-user",
            "source-user",
            "--source-password-env",
            "MARIADB_MYSQL_CDC_TEST_MISSING_PASSWORD",
        ])
        .output()
        .expect("run drift-check config error");

    assert_eq!(output.status.code(), Some(2));
    assert!(stdout(&output).is_empty());
    assert!(stderr(&output).contains("MARIADB_MYSQL_CDC_TEST_MISSING_PASSWORD is not set"));
}

fn run_drift_check(mariadb: &str, extra_args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"));
    command
        .env("DRIFT_SOURCE_PASSWORD", "source-secret")
        .env("DRIFT_TARGET_PASSWORD", "target-secret")
        .args([
            "drift-check",
            "--source-host",
            "source-host",
            "--source-user",
            "source-user",
            "--source-password-env",
            "DRIFT_SOURCE_PASSWORD",
            "--source-database",
            "source_db",
            "--target-host",
            "target-host",
            "--target-user",
            "target-user",
            "--target-password-env",
            "DRIFT_TARGET_PASSWORD",
            "--target-database",
            "target_db",
            "--mariadb",
            mariadb,
        ])
        .args(extra_args);
    command.output().expect("run drift-check")
}

fn write_fake_mariadb(name: &str, script: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "mariadb-mysql-cdc-{name}-{}.sh",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos()
    ));
    fs::write(&path, script).expect("write fake mariadb");
    let mut permissions = fs::metadata(&path)
        .expect("fake mariadb metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("mark fake mariadb executable");
    path.to_string_lossy().into_owned()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
