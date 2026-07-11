use super::*;

#[test]
fn rejects_non_atomic_checkpoint_file_option() {
    set_env("SRC_PASSWORD", "source-secret");
    set_env("TARGET_PASSWORD", "target-secret");

    let error = parse_apply_binlog_config(args([
        "--source-host",
        "10.0.0.2",
        "--source-user",
        "cdc",
        "--source-password-env",
        "SRC_PASSWORD",
        "--source-identity",
        "test-source-incarnation",
        "--target-host",
        "target.db",
        "--target-user",
        "writer",
        "--target-password-env",
        "TARGET_PASSWORD",
        "--target-database",
        "app_target",
        "--checkpoint-file",
        "/var/lib/mariadb-mysql-cdc/stream-checkpoint.json",
    ]))
    .expect_err("checkpoint files must not bypass target transaction atomicity");

    assert!(error.contains("unknown apply-binlog option: --checkpoint-file"));
}

#[test]
fn parses_stream_config_with_default_cdc_checkpoint_table() {
    set_env("SRC_PASSWORD_DEFAULT", "source-secret");
    set_env("TARGET_PASSWORD_DEFAULT", "target-secret");

    let config = parse_apply_binlog_config(args([
        "--source-host",
        "10.0.0.2",
        "--source-user",
        "cdc",
        "--source-password-env",
        "SRC_PASSWORD_DEFAULT",
        "--source-database",
        "app",
        "--source-identity",
        "test-source-incarnation",
        "--target-host",
        "target.db",
        "--target-user",
        "writer",
        "--target-password-env",
        "TARGET_PASSWORD_DEFAULT",
        "--target-database",
        "app_target",
    ]))
    .expect("checkpoint config");

    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");
}
