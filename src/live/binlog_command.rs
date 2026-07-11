#[cfg(test)]
use super::SourceBinlogConfig;
use super::{ApplyBinlogConfig, ApplyBinlogError};

#[cfg(test)]
const FNV_OFFSET_BASIS: u32 = 2_166_136_261;
#[cfg(test)]
const FNV_PRIME: u32 = 16_777_619;

pub(super) fn read_remote_binlog(_config: &ApplyBinlogConfig) -> Result<String, ApplyBinlogError> {
    Err(ApplyBinlogError::SourceCommand(
        "apply-binlog text mode was removed; use stream-binlog native replication".to_string(),
    ))
}

#[cfg(test)]
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

#[cfg(test)]
pub(super) fn stop_never_args(source: &SourceBinlogConfig) -> Vec<String> {
    let mut args = binlog_args(source);
    let binlog_file_index = args.len().saturating_sub(1);
    args.insert(binlog_file_index, "--stop-never".to_string());
    args.insert(
        binlog_file_index + 1,
        format!(
            "--stop-never-slave-server-id={}",
            stop_never_slave_server_id(source)
        ),
    );
    args
}

#[cfg(test)]
fn stop_never_slave_server_id(source: &SourceBinlogConfig) -> u32 {
    source
        .stop_never_slave_server_id
        .unwrap_or_else(generate_stop_never_slave_server_id)
}

#[cfg(test)]
fn generate_stop_never_slave_server_id() -> u32 {
    let hostname = std::env::var("HOSTNAME").unwrap_or_default();
    let process_id = std::process::id();
    let hash = fnv1a(hostname.as_bytes(), FNV_OFFSET_BASIS);
    let hash = fnv1a(&process_id.to_le_bytes(), hash);

    if hash == 0 { 1 } else { hash }
}

#[cfg(test)]
fn fnv1a(bytes: &[u8], seed: u32) -> u32 {
    bytes.iter().fold(seed, |hash, byte| {
        let mixed = hash ^ u32::from(*byte);
        mixed.wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_remote_binlog_args_with_database_and_stop_position() {
        let args = binlog_args(&SourceBinlogConfig {
            host: "10.0.0.2".to_string(),
            port: 3307,
            user: "cdc".to_string(),
            password: "secret".to_string(),
            database: Some("app".to_string()),
            tls_ca_file: "/etc/mariadb-mysql-cdc/source-ca.pem".to_string(),
            binlog_file: "mysqld-bin.000777".to_string(),
            start_position: 12345,
            stop_position: Some(45678),
            stop_never_slave_server_id: None,
        });

        assert_eq!(
            args,
            vec![
                "--read-from-remote-server",
                "--verbose",
                "--base64-output=decode-rows",
                "--host",
                "10.0.0.2",
                "--port",
                "3307",
                "--user",
                "cdc",
                "--password=secret",
                "--start-position",
                "12345",
                "--database",
                "app",
                "--stop-position",
                "45678",
                "mysqld-bin.000777",
            ]
        );
    }

    #[test]
    fn stop_never_args_include_slave_server_id_before_binlog_file() {
        let args = stop_never_args(&SourceBinlogConfig {
            host: "10.0.0.2".to_string(),
            user: "cdc".to_string(),
            password: "secret".to_string(),
            binlog_file: "mysqld-bin.000777".to_string(),
            start_position: 12345,
            stop_never_slave_server_id: Some(4242),
            ..SourceBinlogConfig::default()
        });

        assert_eq!(
            &args[args.len() - 3..],
            [
                "--stop-never",
                "--stop-never-slave-server-id=4242",
                "mysqld-bin.000777",
            ]
        );
        assert_eq!(args.last(), Some(&"mysqld-bin.000777".to_string()));
    }

    #[test]
    fn stop_never_args_generate_nonzero_slave_server_id_when_not_configured() {
        let args = stop_never_args(&SourceBinlogConfig {
            host: "10.0.0.2".to_string(),
            user: "cdc".to_string(),
            password: "secret".to_string(),
            binlog_file: "mysqld-bin.000777".to_string(),
            start_position: 12345,
            stop_never_slave_server_id: None,
            ..SourceBinlogConfig::default()
        });

        let generated_arg = args
            .iter()
            .find(|arg| arg.starts_with("--stop-never-slave-server-id="))
            .expect("generated stop-never slave server id arg");
        let generated_id = generated_arg
            .trim_start_matches("--stop-never-slave-server-id=")
            .parse::<u32>()
            .expect("numeric generated id");

        assert_ne!(generated_id, 0);
        assert_eq!(args.last(), Some(&"mysqld-bin.000777".to_string()));
    }
}
