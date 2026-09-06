use std::process::Command;

#[test]
fn help_documents_explicit_prepared_recovery_resume() {
    let output = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"))
        .arg("--help")
        .output()
        .expect("run CLI help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(help.contains(
        "recover-lost-binlog --authorization-file PATH --source-host HOST --source-user USER --source-password-env ENV --source-database DB --source-identity ID --target-host HOST --target-user USER --target-password-env ENV --target-database DB [--parallelism WORKERS]"
    ));
    assert!(help.contains(
        "resume-lost-binlog --authorization-file PATH --source-host HOST --source-user USER --source-password-env ENV --source-database DB --source-identity ID --target-host HOST --target-user USER --target-password-env ENV --target-database DB [--parallelism WORKERS]"
    ));
}

#[test]
fn lost_binlog_commands_reject_invalid_parallelism_before_authorization_or_connections() {
    for command in ["recover-lost-binlog", "resume-lost-binlog"] {
        for value in ["0", "not-a-number"] {
            let output = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"))
                .args([command, "--parallelism", value])
                .output()
                .expect("run lost-binlog command with invalid parallelism");

            assert_eq!(output.status.code(), Some(2), "{command} {value}");
            let error = String::from_utf8(output.stderr).expect("UTF-8 error");
            assert!(
                error.contains("--parallelism"),
                "{command} {value}: {error}"
            );
            assert!(
                !error.contains("--authorization-file is required"),
                "{command} {value}: {error}"
            );
        }
    }
}

#[test]
fn prepared_resume_requires_authorization() {
    let output = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"))
        .arg("resume-lost-binlog")
        .output()
        .expect("run resume without authorization");
    assert_eq!(output.status.code(), Some(2));
    let error = String::from_utf8(output.stderr).expect("UTF-8 error");
    assert!(error.contains("--authorization-file"));
    assert!(!error.contains("unknown command"));
}

#[test]
fn prepared_resume_reports_unreadable_authorization_before_connecting() {
    let path = std::env::temp_dir()
        .join(format!("cdc-resume-missing-auth-{}", std::process::id()))
        .join("authorization.json");
    assert!(!path.exists());
    let output = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"))
        .args([
            "resume-lost-binlog",
            "--authorization-file",
            path.to_str().expect("UTF-8 temporary path"),
            "--source-host",
            "127.0.0.1",
            "--source-user",
            "source",
            "--source-password-env",
            "CDC_RESUME_TEST_PASSWORD",
            "--source-database",
            "source",
            "--source-identity",
            "resume-cli-fixture",
            "--target-host",
            "127.0.0.1",
            "--target-user",
            "target",
            "--target-password-env",
            "CDC_RESUME_TEST_PASSWORD",
            "--target-database",
            "target",
        ])
        .env("CDC_RESUME_TEST_PASSWORD", "unused-fixture-password")
        .output()
        .expect("run resume with missing authorization file");
    assert_eq!(output.status.code(), Some(1));
    let error = String::from_utf8(output.stderr).expect("UTF-8 error");
    assert!(error.contains("read recovery authorization"), "{error}");
    assert!(error.contains(path.to_str().expect("UTF-8 temporary path")));
}
