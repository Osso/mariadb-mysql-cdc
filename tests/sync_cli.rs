use std::process::{Command, Output};

const BINARY: &str = env!("CARGO_BIN_EXE_mariadb-mysql-cdc");
const MISSING_SOURCE_PASSWORD_ENV: &str = "MARIADB_MYSQL_CDC_TEST_MISSING_SYNC_PASSWORD";

#[test]
fn help_documents_one_unified_schema_and_table_sync_command() {
    let output = run(&["--help"]);

    assert!(output.status.success());
    assert!(stderr(&output).is_empty());
    let help = stdout(&output);
    let sync_usage = help
        .lines()
        .filter(|line| line.trim_start().starts_with("mariadb-mysql-cdc sync "))
        .collect::<Vec<_>>();
    assert_eq!(sync_usage.len(), 1, "unexpected sync usage:\n{help}");
    assert!(help.contains("Synchronize target schemas and table rows from source."));
    for obsolete in [
        "catchup-snapshot",
        "catchup-progress",
        "sync-table",
        "sync-progress",
        "sync-schema",
        "drift-check",
        "repair-drift",
    ] {
        assert!(
            !help.contains(obsolete),
            "obsolete command remains documented: {obsolete}\n{help}"
        );
    }
}

#[test]
fn obsolete_sync_command_names_are_rejected_without_dispatch() {
    for obsolete in [
        "catchup-snapshot",
        "catchup-progress",
        "sync-table",
        "sync-progress",
        "sync-schema",
        "drift-check",
        "repair-drift",
    ] {
        let output = run(&[obsolete]);

        assert_eq!(output.status.code(), Some(2));
        assert!(stdout(&output).is_empty());
        assert!(
            stderr(&output).starts_with(&format!("unknown command: {obsolete}\n")),
            "obsolete command still dispatched: {obsolete}\n{}",
            stderr(&output)
        );
    }
}

#[test]
fn sync_accepts_unified_scope_progress_and_parallelism_options() {
    for run_identity in [
        ["--run-id", "sync-run-1"],
        ["--run-id-prefix", "scheduled-sync"],
    ] {
        let mut arguments = vec![
            "sync",
            "--source-host",
            "source-host",
            "--source-user",
            "source-user",
            "--source-database",
            "source-database",
            "--target-host",
            "target-host",
            "--target-user",
            "target-user",
            "--target-database",
            "target-database",
            "--target-tls-ca-file",
            "/tmp/test-target-ca.pem",
            "--table",
            "parents",
            "--table",
            "children",
            "--chunk-size",
            "500",
            "--parallelism",
            "4",
            "--progress-table",
            "cdc.sync_runs",
        ];
        arguments.extend(run_identity);
        arguments.extend([
            "--source-password-env",
            MISSING_SOURCE_PASSWORD_ENV,
            "--target-password-env",
            "MARIADB_MYSQL_CDC_TEST_MISSING_SYNC_TARGET_PASSWORD",
        ]);

        let output = run(&arguments);

        assert_eq!(output.status.code(), Some(2));
        assert!(stdout(&output).is_empty());
        assert!(
            stderr(&output).contains(&format!("{MISSING_SOURCE_PASSWORD_ENV} is not set")),
            "sync rejected a unified option before environment validation:\n{}",
            stderr(&output)
        );
        assert!(!stderr(&output).contains("unknown command"));
        assert!(!stderr(&output).contains("unknown sync option"));
    }
}

#[test]
fn sync_rejects_obsolete_partial_behavior_flags() {
    for flag in ["--phase", "--mode", "--copy", "--insert-conflict-policy"] {
        let output = run(&["sync", flag, "value"]);

        assert_eq!(output.status.code(), Some(2));
        assert!(stdout(&output).is_empty());
        assert!(
            stderr(&output).contains(&format!("unknown sync option: {flag}")),
            "obsolete sync behavior flag was not rejected explicitly: {flag}\n{}",
            stderr(&output)
        );
    }
}

fn run(arguments: &[&str]) -> Output {
    Command::new(BINARY)
        .args(arguments)
        .output()
        .expect("run mariadb-mysql-cdc")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
