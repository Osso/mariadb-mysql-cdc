use super::*;

#[test]
fn parses_stream_config_with_checkpoint_as_coordinate_source() {
    set_env("SRC_PASSWORD", "source-secret");
    set_env("TARGET_PASSWORD", "target-secret");

    let config = parse_apply_binlog_config(args([
        "--source-host",
        "10.0.0.2",
        "--source-user",
        "cdc",
        "--source-password-env",
        "SRC_PASSWORD",
        "--source-database",
        "app",
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
    .expect("checkpoint config");

    assert_eq!(config.source.binlog_file, "");
    assert_eq!(config.source.start_position, 0);
    assert_eq!(
        config.checkpoint_file,
        Some(PathBuf::from(
            "/var/lib/mariadb-mysql-cdc/stream-checkpoint.json"
        ))
    );
    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");
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

    assert_eq!(config.checkpoint_file, None);
    assert_eq!(config.checkpoint_table, "cdc.stream_checkpoint");
}
