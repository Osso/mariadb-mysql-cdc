use crate::live;
use crate::sync::{DEFAULT_SYNC_PROGRESS_TABLE, SyncConfig, run_mysql_sync, validate_sync_config};

pub(crate) fn run_sync_command(args: Vec<String>, usage: &str) {
    let config = match parse_sync_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };

    match run_mysql_sync(config) {
        Ok(rows) => println!("{}", format_sync_report(&rows)),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

pub(crate) fn parse_sync_config(args: Vec<String>) -> Result<SyncConfig, String> {
    let mut config = default_sync_config();
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;
        apply_sync_option(&mut config, flag, value)?;
        index += 2;
    }
    validate_sync_config(&config)?;
    Ok(config)
}

fn default_sync_config() -> SyncConfig {
    SyncConfig {
        source: crate::mysql_config::MySqlConnectionConfig::default(),
        target: live::TargetMySqlConfig::default(),
        tables: Vec::new(),
        chunk_size: 1000,
        parallelism: 1,
        progress_table: DEFAULT_SYNC_PROGRESS_TABLE.to_string(),
        run_id: None,
        run_id_prefix: None,
    }
}

fn apply_sync_option(config: &mut SyncConfig, flag: &str, value: &str) -> Result<(), String> {
    if apply_source_option(&mut config.source, flag, value)? {
        return Ok(());
    }
    if apply_target_option(&mut config.target, flag, value)? {
        return Ok(());
    }
    match flag {
        "--table" => config.tables.push(value.to_string()),
        "--chunk-size" => config.chunk_size = crate::parse_usize(flag, value)?,
        "--parallelism" => config.parallelism = crate::parse_usize(flag, value)?,
        "--progress-table" => config.progress_table = value.to_string(),
        "--run-id" => config.run_id = Some(value.to_string()),
        "--run-id-prefix" => config.run_id_prefix = Some(value.to_string()),
        _ => return Err(format!("unknown sync option: {flag}")),
    }
    Ok(())
}

fn apply_source_option(
    source: &mut crate::mysql_config::MySqlConnectionConfig,
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

fn apply_target_option(
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
        _ => return Ok(false),
    }
    Ok(true)
}

fn format_sync_report(rows: &[crate::sync::SyncChunkProgress]) -> String {
    let chunks = rows.iter().map(|row| row.chunks).sum::<u64>();
    let rows_scanned = rows.iter().map(|row| row.rows_scanned).sum::<u64>();
    let inserts = rows.iter().map(|row| row.inserts).sum::<u64>();
    let updates = rows.iter().map(|row| row.updates).sum::<u64>();
    let deletes = rows.iter().map(|row| row.deletes).sum::<u64>();
    format!(
        "sync tables={} chunks={chunks} rows_scanned={rows_scanned} inserts={inserts} updates={updates} deletes={deletes}",
        rows.len()
    )
}
