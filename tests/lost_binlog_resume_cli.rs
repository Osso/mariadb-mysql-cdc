use std::process::Command;

#[test]
fn help_documents_explicit_prepared_recovery_resume() {
    let output = Command::new(env!("CARGO_BIN_EXE_mariadb-mysql-cdc"))
        .arg("--help")
        .output()
        .expect("run CLI help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(help.contains("resume-lost-binlog --authorization-file PATH"));
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
