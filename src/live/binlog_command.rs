use super::{ApplyBinlogConfig, ApplyBinlogError, SourceBinlogConfig};
use std::process::Command;

pub(super) fn read_remote_binlog(config: &ApplyBinlogConfig) -> Result<String, ApplyBinlogError> {
    let args = binlog_args(&config.source);
    let output = Command::new(&config.mariadb_binlog)
        .args(args)
        .output()
        .map_err(|error| {
            ApplyBinlogError::SourceCommand(format!("failed to run mariadb-binlog: {error}"))
        })?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(ApplyBinlogError::SourceCommand(format!(
            "mariadb-binlog exited with {}: {}",
            output.status,
            stderr.trim()
        )))
    }
}

fn binlog_args(source: &SourceBinlogConfig) -> Vec<String> {
    let mut args = vec![
        "--read-from-remote-server".to_string(),
        "--verbose".to_string(),
        "--base64-output=decode-rows".to_string(),
        "--host".to_string(),
        source.host.clone(),
        "--port".to_string(),
        source.port.to_string(),
        "--user".to_string(),
        source.user.clone(),
        format!("--password={}", source.password),
        "--start-position".to_string(),
        source.start_position.to_string(),
    ];

    if let Some(database) = &source.database {
        args.push("--database".to_string());
        args.push(database.clone());
    }

    if let Some(stop_position) = source.stop_position {
        args.push("--stop-position".to_string());
        args.push(stop_position.to_string());
    }

    args.push(source.binlog_file.clone());
    args
}

pub(super) fn stop_never_args(source: &SourceBinlogConfig) -> Vec<String> {
    let mut args = binlog_args(source);
    let binlog_file_index = args.len().saturating_sub(1);
    args.insert(binlog_file_index, "--stop-never".to_string());
    args
}
