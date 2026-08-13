use super::RepairDriftConfig;
use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::table_sync::SyncMode;

pub(crate) fn parse_repair_drift_config(args: Vec<String>) -> Result<RepairDriftConfig, String> {
    let mut config = default_repair_drift_config();
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} needs a value"))?;
        repair_drift_option(&mut config, flag, value)?;
        index += 2;
    }
    validate_repair_drift_config(&config)?;
    Ok(config)
}

pub(crate) fn default_repair_drift_config() -> RepairDriftConfig {
    RepairDriftConfig {
        source: MySqlConnectionConfig::default(),
        source_identity: String::new(),
        target: TargetMySqlConfig::default(),
        tables: Vec::new(),
        parent_first: Vec::new(),
        start_after: None,
        end_at: None,
        content_check: true,
        mode: SyncMode::DryRun,
        chunk_size: 1000,
        parallelism: 1,
        conflict_reconcile_limit: 0,
        progress_table: "cdc.table_sync_runs".to_string(),
        run_id: None,
        run_id_prefix: "repair-drift".to_string(),
        #[cfg(feature = "integration-failpoints")]
        integration_failpoint: None,
    }
}

type RepairOptionGroup = fn(&mut RepairDriftConfig, &str, &str) -> Result<bool, String>;

const REPAIR_OPTION_GROUPS: &[RepairOptionGroup] = &[
    apply_repair_source_option,
    apply_repair_target_option,
    apply_repair_table_option,
    apply_repair_window_option,
    apply_repair_execution_option,
    apply_repair_run_option,
    #[cfg(feature = "integration-failpoints")]
    apply_repair_failpoint_option,
];

pub(crate) fn repair_drift_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<(), String> {
    for apply_option_group in REPAIR_OPTION_GROUPS {
        if apply_option_group(config, flag, value)? {
            return Ok(());
        }
    }
    Err(format!("unknown repair-drift option: {flag}"))
}

fn apply_repair_source_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--source-host" => config.source.host = value.to_string(),
        "--source-port" => config.source.port = crate::parse_u16(flag, value)?,
        "--source-user" => config.source.user = value.to_string(),
        "--source-password-env" => config.source.password = crate::read_env_password(value)?,
        "--source-database" => config.source.database = value.to_string(),
        "--source-identity" => config.source_identity = value.to_string(),
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_repair_target_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--target-host" => config.target.host = value.to_string(),
        "--target-port" => config.target.port = crate::parse_u16(flag, value)?,
        "--target-user" => config.target.user = value.to_string(),
        "--target-password-env" => config.target.password = crate::read_env_password(value)?,
        "--target-database" => config.target.database = value.to_string(),
        "--target-tls-ca-file" => config.target.tls_ca_file = value.to_string(),
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_repair_table_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--table" => config.tables.push(value.to_string()),
        "--parent-first" => config.parent_first.extend(parse_csv(value)),
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_repair_window_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--start-after" => config.start_after = Some(parse_csv(value)),
        "--end-at" => config.end_at = Some(parse_csv(value)),
        "--start-after-json" => config.start_after = Some(parse_json_primary_key(flag, value)?),
        "--end-at-json" => config.end_at = Some(parse_json_primary_key(flag, value)?),
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_repair_execution_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--content-check" => config.content_check = crate::parse_bool(flag, value)?,
        "--mode" => config.mode = parse_sync_mode(value)?,
        "--chunk-size" => config.chunk_size = crate::parse_usize(flag, value)?,
        "--parallelism" => config.parallelism = crate::parse_nonzero_usize(flag, value)?,
        "--conflict-reconcile-limit" => {
            config.conflict_reconcile_limit = crate::parse_usize(flag, value)?
        }
        "--progress-table" => config.progress_table = value.to_string(),
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_repair_run_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    match flag {
        "--run-id" => config.run_id = Some(value.to_string()),
        "--run-id-prefix" => config.run_id_prefix = value.to_string(),
        _ => return Ok(false),
    }
    Ok(true)
}

#[cfg(feature = "integration-failpoints")]
fn apply_repair_failpoint_option(
    config: &mut RepairDriftConfig,
    flag: &str,
    value: &str,
) -> Result<bool, String> {
    if flag != "--integration-failpoint" {
        return Ok(false);
    }
    config.integration_failpoint = Some(crate::live::IntegrationFailpoint::parse(value)?);
    Ok(true)
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_json_primary_key(flag: &str, value: &str) -> Result<Vec<String>, String> {
    let values = serde_json::from_str::<Vec<String>>(value)
        .map_err(|error| format!("{flag} expects a JSON string array: {error}"))?;
    if values.is_empty() {
        return Err(format!(
            "{flag} must contain at least one primary-key value"
        ));
    }
    Ok(values)
}

fn parse_sync_mode(value: &str) -> Result<SyncMode, String> {
    match value {
        "dry-run" => Ok(SyncMode::DryRun),
        "apply" => Ok(SyncMode::Apply),
        _ => Err(format!("unknown mode: {value}; expected dry-run or apply")),
    }
}

pub(crate) fn validate_repair_drift_config(config: &RepairDriftConfig) -> Result<(), String> {
    validate_source_config(config)?;
    validate_target_config(config)?;
    validate_repair_options(config)?;
    validate_apply_config(config)
}

fn validate_source_config(config: &RepairDriftConfig) -> Result<(), String> {
    if config.source.host.is_empty() {
        return Err("source host is required".to_string());
    }
    if config.source.user.is_empty() {
        return Err("source user is required".to_string());
    }
    if config.source.password.is_empty() {
        return Err("source password is required".to_string());
    }
    if config.source.database.is_empty() {
        return Err("source database is required".to_string());
    }
    Ok(())
}

fn validate_target_config(config: &RepairDriftConfig) -> Result<(), String> {
    if config.target.host.is_empty() {
        return Err("target host is required".to_string());
    }
    if config.target.user.is_empty() {
        return Err("target user is required".to_string());
    }
    if config.target.password.is_empty() {
        return Err("target password is required".to_string());
    }
    if config.target.database.is_empty() {
        return Err("target database is required".to_string());
    }
    if config.target.tls_ca_file.is_empty() {
        return Err("target TLS CA file is required".to_string());
    }
    Ok(())
}

fn validate_repair_options(config: &RepairDriftConfig) -> Result<(), String> {
    if config.chunk_size == 0 {
        return Err("chunk size must be greater than zero".to_string());
    }
    if config.parallelism == 0 {
        return Err("parallelism must be greater than zero".to_string());
    }
    if config.progress_table.is_empty() {
        return Err("progress table is required".to_string());
    }
    if config.run_id_prefix.is_empty() {
        return Err("run id prefix is required".to_string());
    }
    Ok(())
}

fn validate_apply_config(config: &RepairDriftConfig) -> Result<(), String> {
    if config.conflict_reconcile_limit > 0 && config.mode != SyncMode::Apply {
        return Err("conflict reconciliation requires apply mode".to_string());
    }
    if config.mode != SyncMode::Apply {
        return Ok(());
    }
    if config.source_identity.is_empty() {
        return Err("source identity is required in apply mode".to_string());
    }
    Ok(())
}
