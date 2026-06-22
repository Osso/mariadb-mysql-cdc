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
        mariadb: "mariadb".to_string(),
        table: table_sync::SyncTable {
            name: String::new(),
            primary_key: Vec::new(),
            columns: Vec::new(),
        },
        chunk_size: 1000,
        mode: table_sync::SyncMode::DryRun,
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

    match flag {
        "--table" => config.table.name = value.to_string(),
        "--primary-key" => config.table.primary_key = parse_csv_columns(value),
        "--columns" => config.table.columns = parse_csv_columns(value),
        "--chunk-size" => config.chunk_size = crate::parse_usize(flag, value)?,
        "--mode" => config.mode = parse_sync_mode(value)?,
        "--mariadb" => {
            config.mariadb = value.to_string();
            config.source.mariadb = value.to_string();
        }
        other => return Err(format!("unknown sync-table option: {other}")),
    }

    Ok(())
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
        "--insert-conflict-policy" => {
            target.insert_conflict_policy = crate::parse_insert_policy(value)?
        }
        _ => return Ok(false),
    }

    Ok(true)
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
    Ok(())
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
    }

    #[test]
    fn parses_apply_mode_and_custom_chunk_size() {
        set_env("CDC_SYNC_SOURCE_PASSWORD", "source-pass");
        set_env("CDC_SYNC_TARGET_PASSWORD", "target-pass");

        let config =
            parse_sync_table_config(required_args(["--chunk-size", "250", "--mode", "apply"]))
                .expect("sync-table config");

        assert_eq!(config.chunk_size, 250);
        assert_eq!(config.mode, table_sync::SyncMode::Apply);
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
