use std::process::{Command, Output};

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

#[test]
fn repair_drift_cli_keeps_config_errors_as_exit_two() {
    let output = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"))
        .args([
            "repair-drift",
            "--source-host",
            "source-host",
            "--source-user",
            "source-user",
            "--source-password-env",
            "MARIADB_MYSQL_CDC_TEST_MISSING_PASSWORD",
        ])
        .output()
        .expect("run repair-drift config error");

    assert_eq!(output.status.code(), Some(2));
    assert!(stdout(&output).is_empty());
    assert!(stderr(&output).contains("MARIADB_MYSQL_CDC_TEST_MISSING_PASSWORD is not set"));
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
