use crate::{live, mysql_snapshot, table_sync};

pub fn run_sync_table_command(args: Vec<String>, usage: &str) {
    let config = match parse_sync_table_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };

    match table_sync::run_sync_table(&config) {
        Ok(report) => println!("{}", format_sync_table_report(&report)),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn format_sync_table_report(report: &table_sync::SyncTableReport) -> String {
    format!(
        "sync_table table={} chunks={} rows_scanned={} inserts={} updates={} extra_target_rows={}",
        report.table,
        report.chunks,
        report.rows_scanned,
        report.inserts,
        report.updates,
        report.extra_target_rows
    )
}

fn parse_sync_table_config(args: Vec<String>) -> Result<table_sync::SyncTableConfig, String> {
    let mut config = default_sync_table_config();
    let mut index = 0;

    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;

        sync_table_option(&mut config, flag, value)?;
        index += 2;
    }

    validate_sync_table_config(&config)?;
    Ok(config)
}

fn default_sync_table_config() -> table_sync::SyncTableConfig {
    table_sync::SyncTableConfig {
        source: mysql_snapshot::MySqlConnectionConfig::default(),
        target: live::TargetMySqlConfig::default(),
        table: table_sync::SyncTable {
            name: String::new(),
            primary_key: Vec::new(),
            primary_key_ordering: Vec::new(),
            columns: Vec::new(),
        },
        chunk_size: 1000,
        mode: table_sync::SyncMode::DryRun,
        progress_table: "cdc.table_sync_runs".to_string(),
        run_id: String::new(),
        start_after: None,
        end_at: None,
        updated_since: None,
        plan_hash: None,
    }
}

fn sync_table_option(
    config: &mut table_sync::SyncTableConfig,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    if source_option(&mut config.source, flag, value)? {
        return Ok(());
    }
    if target_option(&mut config.target, flag, value)? {
        return Ok(());
    }
    if apply_sync_table_identity_option(config, flag, value)? {
        return Ok(());
    }
    if apply_sync_table_window_option(config, flag, value)? {
        return Ok(());
    }
    Err(format!("unknown sync-table option: {flag}"))
}

fn apply_sync_table_identity_option(
    config: &mut table_sync::SyncTableConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--table" => config.table.name = value.to_string(),
        "--primary-key" => config.table.primary_key = parse_csv_columns(value),
        "--columns" => config.table.columns = parse_csv_columns(value),
        "--chunk-size" => config.chunk_size = crate::parse_usize(flag, value)?,
        "--mode" => config.mode = parse_sync_mode(value)?,
        "--progress-table" => config.progress_table = value.to_string(),
        "--run-id" => config.run_id = value.to_string(),
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_sync_table_window_option(
    config: &mut table_sync::SyncTableConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--start-after" => config.start_after = Some(parse_csv_columns(value)),
        "--end-at" => config.end_at = Some(parse_csv_columns(value)),
        "--start-after-json" => config.start_after = Some(parse_json_columns(flag, value)?),
        "--end-at-json" => config.end_at = Some(parse_json_columns(flag, value)?),
        "--updated-at-column" => set_updated_since_column(config, value),
        "--updated-since" => set_updated_since_value(config, value),
        _ => return Ok(false),
    }
    Ok(true)
}

fn source_option(
    source: &mut mysql_snapshot::MySqlConnectionConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--source-host" => source.host = value.to_string(),
        "--source-port" => source.port = crate::parse_u16(flag, value)?,
        "--source-user" => source.user = value.to_string(),
        "--source-password-env" => source.password = crate::read_env_password(value)?,
        "--source-database" => source.database = value.to_string(),
        _ => return Ok(false),
    }

    Ok(true)
}

fn target_option(
    target: &mut live::TargetMySqlConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--target-host" => target.host = value.to_string(),
        "--target-port" => target.port = crate::parse_u16(flag, value)?,
        "--target-user" => target.user = value.to_string(),
        "--target-password-env" => target.password = crate::read_env_password(value)?,
        "--target-database" => target.database = value.to_string(),
        "--target-tls-ca-file" => target.tls_ca_file = value.to_string(),
        "--insert-conflict-policy" => {
            target.insert_conflict_policy = crate::parse_insert_policy(value)?
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn set_updated_since_column(config: &mut table_sync::SyncTableConfig, value: &str) {
    config
        .updated_since
        .get_or_insert_with(empty_updated_since)
        .column = value.to_string();
}

fn set_updated_since_value(config: &mut table_sync::SyncTableConfig, value: &str) {
    config
        .updated_since
        .get_or_insert_with(empty_updated_since)
        .value = value.to_string();
}

fn empty_updated_since() -> table_sync::UpdatedSince {
    table_sync::UpdatedSince {
        column: String::new(),
        value: String::new(),
    }
}

fn validate_sync_table_config(config: &table_sync::SyncTableConfig) -> Result<(), String> {
    validate_source_connection(&config.source)?;
    validate_target_connection(&config.target)?;
    if config.table.name.is_empty() {
        return Err("table is required".to_string());
    }
    if config.table.primary_key.is_empty() {
        return Err("primary key is required".to_string());
    }
    if config.table.columns.is_empty() {
        return Err("columns are required".to_string());
    }
    if config.chunk_size == 0 {
        return Err("chunk size must be greater than zero".to_string());
    }
    if config.progress_table.is_empty() {
        return Err("progress table is required".to_string());
    }
    if config.run_id.is_empty() {
        return Err("run id is required".to_string());
    }
    validate_bound_arity(
        &config.table.primary_key,
        config.start_after.as_ref(),
        "start-after",
    )?;
    validate_bound_arity(&config.table.primary_key, config.end_at.as_ref(), "end-at")?;
    validate_updated_since(config)?;
    Ok(())
}

fn validate_updated_since(config: &table_sync::SyncTableConfig) -> Result<(), String> {
    let Some(updated_since) = &config.updated_since else {
        return Ok(());
    };
    if config.mode == table_sync::SyncMode::MissingPrimaryKeys {
        return Err("missing-primary-keys mode cannot use updated-since".to_string());
    }
    if updated_since.column.is_empty() {
        return Err("updated-at column is required when updated-since is set".to_string());
    }
    if updated_since.value.is_empty() {
        return Err("updated-since value is required when updated-at column is set".to_string());
    }
    if config.start_after.is_some() || config.end_at.is_some() {
        return Err("updated-since cannot be combined with start-after or end-at".to_string());
    }
    if !config.table.columns.contains(&updated_since.column) {
        return Err(format!(
            "updated-at column `{}` must be included in columns",
            updated_since.column
        ));
    }
    Ok(())
}

fn validate_bound_arity(
    primary_key: &[String],
    values: Option<&Vec<String>>,
    label: &str,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.len() != primary_key.len() {
        return Err(format!(
            "{label} has {} values for {} primary-key columns",
            values.len(),
            primary_key.len()
        ));
    }
    Ok(())
}

fn validate_source_connection(
    config: &mysql_snapshot::MySqlConnectionConfig,
) -> Result<(), String> {
    if config.host.is_empty() {
        return Err("source host is required".to_string());
    }
    if config.user.is_empty() {
        return Err("source user is required".to_string());
    }
    if config.password.is_empty() {
        return Err("source password is required".to_string());
    }
    if config.database.is_empty() {
        return Err("source database is required".to_string());
    }
    Ok(())
}

fn validate_target_connection(target: &live::TargetMySqlConfig) -> Result<(), String> {
    if target.host.is_empty() {
        return Err("target host is required".to_string());
    }
    if target.user.is_empty() {
        return Err("target user is required".to_string());
    }
    if target.password.is_empty() {
        return Err("target password is required".to_string());
    }
    if target.database.is_empty() {
        return Err("target database is required".to_string());
    }
    if target.tls_ca_file.is_empty() {
        return Err("target TLS CA file is required".to_string());
    }
    Ok(())
}

fn parse_json_columns(flag: &str, value: &str) -> Result<Vec<String>, String> {
    serde_json::from_str::<Vec<String>>(value)
        .map_err(|error| format!("{flag} must be a JSON string array: {error}"))
}

fn parse_csv_columns(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_sync_mode(value: &str) -> Result<table_sync::SyncMode, String> {
    match value {
        "dry-run" => Ok(table_sync::SyncMode::DryRun),
        "apply" => Ok(table_sync::SyncMode::Apply),
        "missing-primary-keys" => Ok(table_sync::SyncMode::MissingPrimaryKeys),
        other => Err(format!("unknown sync mode: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn parses_required_sync_table_options() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let config = parse_sync_table_config(args([
            "--source-host",
            "source-db",
            "--source-user",
            "source-user",
            "--source-password-env",
            "CDC_SYNC_SOURCE_PASSWORD",
            "--source-database",
            "globalcomix",
            "--target-host",
            "target-db",
            "--target-user",
            "target-user",
            "--target-password-env",
            "CDC_SYNC_TARGET_PASSWORD",
            "--target-database",
            "globalcomix",
            "--table",
            "releases",
            "--primary-key",
            "id",
            "--columns",
            "id, slug, title",
            "--run-id",
            "repair-20260710-01",
        ]))
        .expect("sync-table config");

        assert_eq!(config.source.host, "source-db");
        assert_eq!(config.source.password, "source-pass");
        assert_eq!(config.target.host, "target-db");
        assert_eq!(config.target.password, "target-pass");
        assert_eq!(config.table.name, "releases");
        assert_eq!(config.table.primary_key, vec!["id"]);
        assert_eq!(config.table.columns, vec!["id", "slug", "title"]);
        assert_eq!(config.chunk_size, 1000);
        assert_eq!(config.mode, table_sync::SyncMode::DryRun);
        assert_eq!(config.progress_table, "cdc.table_sync_runs");
        assert_eq!(config.run_id, "repair-20260710-01");
    }

    #[test]
    fn parses_source_and_target_options_directly() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let mut config = default_sync_table_config();
        for (flag, value) in [
            ("--source-host", "source-db"),
            ("--source-port", "3310"),
            ("--source-user", "source-user"),
            ("--source-password-env", "CDC_SYNC_SOURCE_PASSWORD"),
            ("--source-database", "source_database"),
            ("--target-host", "target-db"),
            ("--target-port", "3311"),
            ("--target-user", "target-user"),
            ("--target-password-env", "CDC_SYNC_TARGET_PASSWORD"),
            ("--target-database", "target_database"),
            ("--target-tls-ca-file", "/tmp/target-ca.pem"),
        ] {
            sync_table_option(&mut config, flag, value).expect("connection option");
        }

        assert_eq!(config.source.host, "source-db");
        assert_eq!(config.source.port, 3310);
        assert_eq!(config.source.user, "source-user");
        assert_eq!(config.source.password, "source-pass");
        assert_eq!(config.source.database, "source_database");
        assert_eq!(config.target.host, "target-db");
        assert_eq!(config.target.port, 3311);
        assert_eq!(config.target.user, "target-user");
        assert_eq!(config.target.password, "target-pass");
        assert_eq!(config.target.database, "target_database");
        assert_eq!(config.target.tls_ca_file, "/tmp/target-ca.pem");
    }

    #[test]
    fn parses_identity_and_window_options_directly() {
        let mut config = default_sync_table_config();
        for (flag, value) in [
            ("--table", "releases"),
            ("--primary-key", "tenant_id,id"),
            ("--columns", "tenant_id,id,updated_at"),
            ("--chunk-size", "250"),
            ("--mode", "apply"),
            ("--progress-table", "cdc.table_sync_progress"),
            ("--run-id", "repair-20260716-01"),
            ("--start-after-json", "[\"tenant,1\",\"10\"]"),
            ("--end-at-json", "[\"tenant,1\",\"20\"]"),
            ("--updated-at-column", "updated_at"),
            ("--updated-since", "2026-07-16 00:00:00"),
        ] {
            sync_table_option(&mut config, flag, value).expect("sync-table option");
        }

        assert_eq!(config.table.name, "releases");
        assert_eq!(config.table.primary_key, vec!["tenant_id", "id"]);
        assert_eq!(config.table.columns, vec!["tenant_id", "id", "updated_at"]);
        assert_eq!(config.chunk_size, 250);
        assert_eq!(config.mode, table_sync::SyncMode::Apply);
        assert_eq!(config.progress_table, "cdc.table_sync_progress");
        assert_eq!(config.run_id, "repair-20260716-01");
        assert_eq!(
            config.start_after,
            Some(vec!["tenant,1".to_string(), "10".to_string()])
        );
        assert_eq!(
            config.end_at,
            Some(vec!["tenant,1".to_string(), "20".to_string()])
        );
        assert_eq!(
            config.updated_since,
            Some(table_sync::UpdatedSince {
                column: "updated_at".to_string(),
                value: "2026-07-16 00:00:00".to_string(),
            })
        );
    }

    #[test]
    fn preserves_direct_option_parser_errors() {
        let mut config = default_sync_table_config();

        assert_eq!(
            sync_table_option(&mut config, "--chunk-size", "invalid").expect_err("chunk size"),
            "--chunk-size must be an integer"
        );
        assert_eq!(
            sync_table_option(&mut config, "--start-after-json", "invalid")
                .expect_err("JSON bound"),
            "--start-after-json must be a JSON string array: expected value at line 1 column 1"
        );
        assert_eq!(
            sync_table_option(&mut config, "--bogus", "value").expect_err("unknown option"),
            "unknown sync-table option: --bogus"
        );
    }

    #[test]
    fn rejects_source_tls_ca_file_option() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let error = parse_sync_table_config({
            let mut values = required_args([]);
            values.extend(args(["--source-tls-ca-file", "/tmp/source-ca.pem"]));
            values
        })
        .expect_err("source CA option");

        assert_eq!(error, "unknown sync-table option: --source-tls-ca-file");
    }

    #[test]
    fn rejects_missing_target_tls_ca_file() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let mut values = required_args([]);
        values.extend(args(["--target-tls-ca-file", ""]));

        let error = parse_sync_table_config(values).expect_err("missing target TLS CA");

        assert_eq!(error, "target TLS CA file is required");
    }

    #[test]
    fn target_cli_config_keeps_dns_hostname_verification() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let mut config = parse_sync_table_config(required_args([])).expect("sync config");
        config.target.tls_ca_file =
            concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string();
        let opts = crate::mysql_support::target_mysql_opts(&config.target).expect("target TLS");
        let ssl = opts.get_ssl_opts().expect("target TLS configured");

        assert!(!ssl.skip_domain_validation());
        assert!(!ssl.accept_invalid_certs());
    }

    #[test]
    fn rejects_missing_run_id() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let error = parse_sync_table_config(args([
            "--source-host",
            "source-db",
            "--source-user",
            "source-user",
            "--source-password-env",
            "CDC_SYNC_SOURCE_PASSWORD",
            "--source-database",
            "globalcomix",
            "--target-host",
            "target-db",
            "--target-user",
            "target-user",
            "--target-password-env",
            "CDC_SYNC_TARGET_PASSWORD",
            "--target-database",
            "globalcomix",
            "--table",
            "releases",
            "--primary-key",
            "id",
            "--columns",
            "id,slug,title",
        ]))
        .expect_err("missing run id");

        assert_eq!(error, "run id is required");
    }

    #[test]
    fn parses_apply_mode_custom_chunk_size_and_progress_table() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let config = parse_sync_table_config(required_args([
            "--chunk-size",
            "250",
            "--mode",
            "apply",
            "--progress-table",
            "cdc.table_sync_progress",
        ]))
        .expect("sync-table config");

        assert_eq!(config.chunk_size, 250);
        assert_eq!(config.mode, table_sync::SyncMode::Apply);
        assert_eq!(config.progress_table, "cdc.table_sync_progress");
    }

    #[test]
    fn parses_missing_primary_keys_mode() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let config = parse_sync_table_config(required_args(["--mode", "missing-primary-keys"]))
            .expect("missing primary-key config");

        assert_eq!(config.mode, table_sync::SyncMode::MissingPrimaryKeys);
    }

    #[test]
    fn parses_range_bounds_for_targeted_repair() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let config =
            parse_sync_table_config(required_args(["--start-after", "10", "--end-at", "20"]))
                .expect("sync-table config");

        assert_eq!(config.start_after, Some(vec!["10".to_string()]));
        assert_eq!(config.end_at, Some(vec!["20".to_string()]));
    }

    #[test]
    fn parses_updated_since_accelerator_options() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let config = parse_sync_table_config(required_args([
            "--columns",
            "id,slug,updated_at",
            "--updated-at-column",
            "updated_at",
            "--updated-since",
            "2026-06-01 00:00:00",
        ]))
        .expect("sync-table config");

        assert_eq!(
            config.updated_since,
            Some(table_sync::UpdatedSince {
                column: "updated_at".to_string(),
                value: "2026-06-01 00:00:00".to_string(),
            })
        );
    }

    #[test]
    fn rejects_updated_since_with_missing_primary_keys_mode() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let error = parse_sync_table_config(required_args([
            "--mode",
            "missing-primary-keys",
            "--columns",
            "id,name,updated_at",
            "--updated-at-column",
            "updated_at",
            "--updated-since",
            "2026-06-01 00:00:00",
        ]))
        .expect_err("incompatible updated-since");

        assert_eq!(error, "missing-primary-keys mode cannot use updated-since");
    }

    #[test]
    fn rejects_updated_since_column_missing_from_selected_columns() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let error = parse_sync_table_config(required_args([
            "--updated-at-column",
            "updated_at",
            "--updated-since",
            "2026-06-01 00:00:00",
        ]))
        .expect_err("missing column");

        assert_eq!(
            error,
            "updated-at column `updated_at` must be included in columns"
        );
    }

    #[test]
    fn parses_json_range_bounds_for_values_containing_commas() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let config = parse_sync_table_config(required_args([
            "--start-after-json",
            "[\"tenant,1\",\"10\"]",
            "--end-at-json",
            "[\"tenant,1\",\"20\"]",
            "--primary-key",
            "tenant_id,id",
        ]))
        .expect("sync-table config");

        assert_eq!(
            config.start_after,
            Some(vec!["tenant,1".to_string(), "10".to_string()])
        );
        assert_eq!(
            config.end_at,
            Some(vec!["tenant,1".to_string(), "20".to_string()])
        );
    }

    #[test]
    fn rejects_updated_since_combined_with_primary_key_bounds() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let error = parse_sync_table_config(required_args([
            "--columns",
            "id,slug,updated_at",
            "--updated-at-column",
            "updated_at",
            "--updated-since",
            "2026-06-01 00:00:00",
            "--start-after",
            "10",
        ]))
        .expect_err("conflicting bounds");

        assert_eq!(
            error,
            "updated-since cannot be combined with start-after or end-at"
        );
    }

    #[test]
    fn rejects_range_bounds_with_wrong_composite_primary_key_arity() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let error = parse_sync_table_config(required_args([
            "--primary-key",
            "tenant_id,id",
            "--start-after",
            "10",
        ]))
        .expect_err("bad arity");

        assert_eq!(error, "start-after has 1 values for 2 primary-key columns");
    }

    #[test]
    fn rejects_unknown_sync_mode() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let error =
            parse_sync_table_config(required_args(["--mode", "repair"])).expect_err("invalid mode");

        assert_eq!(error, "unknown sync mode: repair");
    }

    #[test]
    fn rejects_unknown_sync_table_option() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let error = parse_sync_table_config(required_args(["--bogus", "value"]))
            .expect_err("unknown option");

        assert_eq!(error, "unknown sync-table option: --bogus");
    }

    fn required_args<const N: usize>(extra: [&str; N]) -> Vec<String> {
        let mut values = args([
            "--source-host",
            "source-db",
            "--source-user",
            "source-user",
            "--source-password-env",
            "CDC_SYNC_SOURCE_PASSWORD",
            "--source-database",
            "globalcomix",
            "--target-host",
            "target-db",
            "--target-user",
            "target-user",
            "--target-password-env",
            "CDC_SYNC_TARGET_PASSWORD",
            "--target-database",
            "globalcomix",
            "--table",
            "releases",
            "--primary-key",
            "id",
            "--columns",
            "id,slug,title",
            "--run-id",
            "test-run",
        ]);
        values.extend(args(extra));
        values
    }

    fn args<const N: usize>(values: [&str; N]) -> Vec<String> {
        values.into_iter().map(str::to_string).collect()
    }

    fn set_env(name: &str, value: &str) {
        unsafe {
            env::set_var(name, value);
        }
    }
}
