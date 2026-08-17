use super::set_env;
use crate::sync_cli::parse_sync_config;

#[test]
fn unified_sync_parser_maps_endpoints_scope_defaults_and_exact_run_id() {
    set_env("CDC_UNIFIED_SYNC_SOURCE_PASSWORD", "source-password");
    set_env("CDC_UNIFIED_SYNC_TARGET_PASSWORD", "target-password");

    let config = parse_sync_config(args([
        "--source-host",
        "source-db",
        "--source-port",
        "3307",
        "--source-user",
        "source-user",
        "--source-password-env",
        "CDC_UNIFIED_SYNC_SOURCE_PASSWORD",
        "--source-database",
        "source-schema",
        "--target-host",
        "target-db",
        "--target-port",
        "25060",
        "--target-user",
        "target-user",
        "--target-password-env",
        "CDC_UNIFIED_SYNC_TARGET_PASSWORD",
        "--target-database",
        "target-schema",
        "--target-tls-ca-file",
        "/tmp/target-ca.pem",
        "--table",
        "parents",
        "--table",
        "children",
        "--run-id",
        "sync-run-7",
    ]))
    .expect("unified sync config");

    assert_eq!(config.source.host, "source-db");
    assert_eq!(config.source.port, 3307);
    assert_eq!(config.source.user, "source-user");
    assert_eq!(config.source.password, "source-password");
    assert_eq!(config.source.database, "source-schema");
    assert_eq!(config.target.host, "target-db");
    assert_eq!(config.target.port, 25060);
    assert_eq!(config.target.user, "target-user");
    assert_eq!(config.target.password, "target-password");
    assert_eq!(config.target.database, "target-schema");
    assert_eq!(config.target.tls_ca_file, "/tmp/target-ca.pem");
    assert_eq!(config.tables, ["parents", "children"]);
    assert_eq!(config.chunk_size, 1000);
    assert_eq!(config.parallelism, 1);
    assert_eq!(config.progress_table, "cdc.sync_runs");
    assert_eq!(config.run_id.as_deref(), Some("sync-run-7"));
    assert_eq!(config.run_id_prefix, None);
}

#[test]
fn unified_sync_parser_maps_runtime_options_and_requires_one_run_identity() {
    set_env("CDC_UNIFIED_SYNC_SOURCE_PASSWORD_2", "source-password");
    set_env("CDC_UNIFIED_SYNC_TARGET_PASSWORD_2", "target-password");
    let required = [
        "--source-host",
        "source-db",
        "--source-user",
        "source-user",
        "--source-password-env",
        "CDC_UNIFIED_SYNC_SOURCE_PASSWORD_2",
        "--source-database",
        "source-schema",
        "--target-host",
        "target-db",
        "--target-user",
        "target-user",
        "--target-password-env",
        "CDC_UNIFIED_SYNC_TARGET_PASSWORD_2",
        "--target-database",
        "target-schema",
        "--target-tls-ca-file",
        "/tmp/target-ca.pem",
        "--table",
        "items",
    ];
    let mut values = args(required);
    values.extend(args([
        "--chunk-size",
        "500",
        "--parallelism",
        "4",
        "--progress-table",
        "control.sync_runs",
        "--run-id-prefix",
        "scheduled",
    ]));

    let config = parse_sync_config(values).expect("prefixed unified sync config");

    assert_eq!(config.chunk_size, 500);
    assert_eq!(config.parallelism, 4);
    assert_eq!(config.progress_table, "control.sync_runs");
    assert_eq!(config.run_id, None);
    assert_eq!(config.run_id_prefix.as_deref(), Some("scheduled"));

    let mut both = args(required);
    both.extend(args([
        "--run-id",
        "exact",
        "--run-id-prefix",
        "scheduled",
    ]));
    assert_eq!(
        parse_sync_config(both).expect_err("two run identities"),
        "exactly one of run_id or run_id_prefix is required"
    );
}

fn args<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}
