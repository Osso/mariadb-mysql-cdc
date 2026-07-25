use crate::live::TargetMySqlConfig;
use mysql::{Opts, OptsBuilder, SslOpts};
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const TARGET_TLS_CA_FILE: &str = "/etc/mariadb-mysql-cdc/do-ca.pem";
pub(crate) const DEFAULT_MYSQL_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_MYSQL_READ_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_MYSQL_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const MYSQL_TCP_KEEPALIVE_TIME_MS: u32 = 10_000;
#[cfg(target_os = "linux")]
const MYSQL_TCP_KEEPALIVE_INTERVAL_SECS: u32 = 5;
#[cfg(target_os = "linux")]
const MYSQL_TCP_KEEPALIVE_PROBE_COUNT: u32 = 3;
#[cfg(target_os = "linux")]
const MYSQL_TCP_USER_TIMEOUT_MS: u32 = 30_000;

pub fn target_mysql_opts(target: &TargetMySqlConfig) -> Result<Opts, String> {
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(target.host.clone()))
        .tcp_port(target.port)
        .user(Some(target.user.clone()))
        .pass(Some(target.password.clone()))
        .db_name(Some(target.database.clone()))
        .prefer_socket(false)
        .ssl_opts(Some(target_ssl_opts(target)?));
    Ok(Opts::from(apply_mysql_connection_liveness(builder)))
}

pub(crate) fn apply_mysql_connection_liveness(builder: OptsBuilder) -> OptsBuilder {
    apply_mysql_tcp_liveness(builder.tcp_connect_timeout(Some(DEFAULT_MYSQL_CONNECT_TIMEOUT)))
}

pub(crate) fn apply_default_mysql_network_bounds(builder: OptsBuilder) -> OptsBuilder {
    apply_mysql_tcp_liveness(
        builder
            .tcp_connect_timeout(Some(DEFAULT_MYSQL_CONNECT_TIMEOUT))
            .read_timeout(Some(DEFAULT_MYSQL_READ_TIMEOUT))
            .write_timeout(Some(DEFAULT_MYSQL_WRITE_TIMEOUT)),
    )
}

pub(crate) fn apply_mysql_tcp_liveness(builder: OptsBuilder) -> OptsBuilder {
    let builder = builder.tcp_keepalive_time_ms(Some(MYSQL_TCP_KEEPALIVE_TIME_MS));
    #[cfg(target_os = "linux")]
    let builder = builder
        .tcp_keepalive_probe_interval_secs(Some(MYSQL_TCP_KEEPALIVE_INTERVAL_SECS))
        .tcp_keepalive_probe_count(Some(MYSQL_TCP_KEEPALIVE_PROBE_COUNT))
        .tcp_user_timeout_ms(Some(MYSQL_TCP_USER_TIMEOUT_MS));
    builder
}

pub fn validate_target_tls_ca_file(target: &TargetMySqlConfig) -> Result<(), String> {
    target_ssl_opts(target).map(|_| ())
}

pub fn target_ssl_opts(target: &TargetMySqlConfig) -> Result<SslOpts, String> {
    ssl_opts_from_ca(
        &format!(
            "target TLS CA file endpoint `{}`:{}",
            target.host, target.port
        ),
        &target.host,
        &target.tls_ca_file,
    )
}

pub fn ssl_opts_from_ca(endpoint: &str, host: &str, ca_file: &str) -> Result<SslOpts, String> {
    if ca_file.is_empty() {
        return Err(format!("{endpoint} TLS CA file is required"));
    }

    let path = Path::new(ca_file);
    let contents = fs::read(path)
        .map_err(|error| format!("{endpoint} TLS CA file `{ca_file}` is unreadable: {error}"))?;
    if contents.is_empty() {
        return Err(format!("{endpoint} TLS CA file `{ca_file}` is empty"));
    }

    validate_ca_certificate(endpoint, ca_file, &contents)?;

    Ok(SslOpts::default()
        .with_root_cert_path(Some(PathBuf::from(ca_file)))
        .with_danger_skip_domain_validation(host.parse::<IpAddr>().is_ok()))
}

fn validate_ca_certificate(endpoint: &str, ca_file: &str, contents: &[u8]) -> Result<(), String> {
    let is_pem = contents
        .windows(b"-----BEGIN CERTIFICATE-----".len())
        .any(|window| window == b"-----BEGIN CERTIFICATE-----");
    if is_pem {
        let certificates = native_tls::Certificate::stack_from_pem(contents)
            .map_err(|error| format!("{endpoint} TLS CA file `{ca_file}` is invalid: {error}"))?;
        if certificates.is_empty() {
            return Err(format!(
                "{endpoint} TLS CA file `{ca_file}` is invalid: contains no certificates"
            ));
        }
        return Ok(());
    }

    native_tls::Certificate::from_der(contents)
        .map(|_| ())
        .map_err(|error| format!("{endpoint} TLS CA file `{ca_file}` is invalid: {error}"))
}

pub fn target_mysql_args(target: &TargetMySqlConfig) -> Vec<String> {
    let mut args = vec![
        "--host".to_string(),
        target.host.clone(),
        "--port".to_string(),
        target.port.to_string(),
        "--user".to_string(),
        target.user.clone(),
        format!("--password={}", target.password),
        "--ssl".to_string(),
        format!("--ssl-ca={}", target.tls_ca_file),
        "--database".to_string(),
        target.database.clone(),
        "--default-character-set=utf8mb4".to_string(),
    ];
    if target.host.parse::<IpAddr>().is_err() {
        args.push("--ssl-verify-server-cert".to_string());
    }
    args
}

pub fn quote_identifier_path(identifier: &str) -> String {
    identifier
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

pub fn qualified_table_parts(default_schema: &str, table_path: &str) -> (String, String) {
    let parts = table_path.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [schema, table] => (schema.to_string(), table.to_string()),
        [table] => (default_schema.to_string(), table.to_string()),
        _ => (default_schema.to_string(), table_path.to_string()),
    }
}

pub fn quote_ident(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

pub fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

/// Extracts the MySQL 8 `Create Procedure` column from admin-only SHOW CREATE
/// PROCEDURE evidence without tuple conversion or panics on NULL/short rows.
#[cfg(test)]
pub(crate) fn parse_show_create_procedure_values(
    values: &[Option<String>],
) -> Result<String, String> {
    values
        .get(2)
        .ok_or_else(|| {
            format!(
                "SHOW CREATE PROCEDURE returned {} columns; missing Create Procedure column",
                values.len()
            )
        })?
        .clone()
        .ok_or_else(|| "SHOW CREATE PROCEDURE returned NULL Create Procedure metadata".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssl_opts_from_invalid_ca_is_rejected() {
        let ca_path =
            std::env::temp_dir().join(format!("mariadb-mysql-cdc-test-ca-{}", std::process::id()));
        std::fs::write(&ca_path, b"test ca").unwrap();

        let error = ssl_opts_from_ca("target `db`:25060", "db", ca_path.to_str().unwrap())
            .expect_err("invalid CA");

        assert!(error.contains("target `db`:25060 TLS CA file"));
        assert!(error.contains("invalid"));
        std::fs::remove_file(ca_path).unwrap();
    }

    #[test]
    fn target_default_uses_reviewed_ca_path() {
        assert_eq!(TargetMySqlConfig::default().tls_ca_file, TARGET_TLS_CA_FILE);
    }

    #[test]
    fn target_mysql_opts_uses_configured_ca_without_driver_default_fallback() {
        let target = TargetMySqlConfig {
            host: "target".to_string(),
            tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
            ..TargetMySqlConfig::default()
        };

        let opts = target_mysql_opts(&target).expect("configured target CA");
        let ssl = opts.get_ssl_opts().expect("target TLS options");

        assert_eq!(
            ssl.root_cert_path(),
            Some(std::path::Path::new(&target.tls_ca_file))
        );
        assert!(!ssl.accept_invalid_certs());
        assert!(!ssl.skip_domain_validation());
        assert_eq!(
            opts.get_tcp_connect_timeout(),
            Some(std::time::Duration::from_secs(10))
        );
        assert_eq!(opts.get_read_timeout(), None);
        assert_eq!(opts.get_write_timeout(), None);
        assert_eq!(opts.get_tcp_keepalive_time_ms(), Some(10_000));
        #[cfg(target_os = "linux")]
        {
            assert_eq!(opts.get_tcp_keepalive_probe_interval_secs(), Some(5));
            assert_eq!(opts.get_tcp_keepalive_probe_count(), Some(3));
            assert_eq!(opts.get_tcp_user_timeout_ms(), Some(30_000));
        }
    }

    #[test]
    fn target_ip_ssl_opts_skip_only_domain_validation() {
        let target = TargetMySqlConfig {
            host: "192.0.2.10".to_string(),
            tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
            ..TargetMySqlConfig::default()
        };

        let ssl = target_ssl_opts(&target).expect("configured target CA");

        assert!(ssl.skip_domain_validation());
        assert!(!ssl.accept_invalid_certs());
    }

    #[test]
    fn target_ssl_opts_uses_configured_ca_without_driver_default_fallback() {
        let target = TargetMySqlConfig {
            host: "target".to_string(),
            tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
            ..TargetMySqlConfig::default()
        };

        let ssl = target_ssl_opts(&target).expect("configured target CA");

        assert_eq!(
            ssl.root_cert_path(),
            Some(std::path::Path::new(&target.tls_ca_file))
        );
        assert!(!ssl.accept_invalid_certs());
        assert!(!ssl.skip_domain_validation());
    }

    #[test]
    fn target_tls_validation_rejects_missing_ca_before_connection() {
        let target = TargetMySqlConfig {
            host: "target-db".to_string(),
            tls_ca_file: "/tmp/mariadb-mysql-cdc-no-such-target-ca.pem".to_string(),
            ..TargetMySqlConfig::default()
        };

        let error = validate_target_tls_ca_file(&target).expect_err("missing target CA");

        assert!(error.contains("target TLS CA file"));
        assert!(error.contains("target-db"));
        assert!(error.contains(&target.tls_ca_file));
    }

    #[test]
    fn target_ssl_opts_rejects_missing_ca_with_endpoint_specific_diagnostic() {
        let target = TargetMySqlConfig {
            host: "target-db".to_string(),
            tls_ca_file: "/tmp/mariadb-mysql-cdc-no-such-target-ca.pem".to_string(),
            ..TargetMySqlConfig::default()
        };

        let error = target_ssl_opts(&target).expect_err("missing target CA");

        assert!(error.contains("target TLS CA file"));
        assert!(error.contains("target-db"));
        assert!(error.contains(&target.tls_ca_file));
    }

    #[test]
    fn target_mysql_args_use_configured_ca_and_server_verification() {
        let target = TargetMySqlConfig {
            host: "target".to_string(),
            port: 25060,
            user: "target_user".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            tls_ca_file: "/tmp/custom-do-ca.pem".to_string(),
            ..TargetMySqlConfig::default()
        };

        let args = target_mysql_args(&target);

        assert!(args.contains(&"--ssl".to_string()));
        assert!(args.contains(&format!("--ssl-ca={}", target.tls_ca_file)));
        assert!(args.contains(&"--ssl-verify-server-cert".to_string()));
        assert!(!args.contains(&"--ssl-verify-server-cert=0".to_string()));
    }

    #[test]
    fn quotes_identifier_paths_and_sql_literals() {
        assert_eq!(
            quote_identifier_path("cdc.table`name"),
            "`cdc`.`table``name`"
        );
        assert_eq!(quote_sql_literal("can't"), "'can''t'");
    }

    #[test]
    fn splits_qualified_table_paths() {
        assert_eq!(
            qualified_table_parts("globalcomix", "cdc.table_sync_progress"),
            ("cdc".to_string(), "table_sync_progress".to_string())
        );
        assert_eq!(
            qualified_table_parts("globalcomix", "table_sync_progress"),
            ("globalcomix".to_string(), "table_sync_progress".to_string())
        );
    }

    #[test]
    fn parses_mysql8_show_create_procedure_rows_without_panicking() {
        let row = vec![
            Some("row_conflicts_trigger_inventory".to_string()),
            Some("".to_string()),
            Some("CREATE PROCEDURE ...".to_string()),
            Some("utf8mb4".to_string()),
            Some("utf8mb4_0900_ai_ci".to_string()),
            Some("utf8mb4_0900_ai_ci".to_string()),
        ];
        assert_eq!(
            parse_show_create_procedure_values(&row).unwrap(),
            "CREATE PROCEDURE ..."
        );

        let null_definition = vec![
            Some("row_conflicts_trigger_inventory".to_string()),
            Some("".to_string()),
            None,
            Some("utf8mb4".to_string()),
            Some("utf8mb4_0900_ai_ci".to_string()),
            Some("utf8mb4_0900_ai_ci".to_string()),
        ];
        assert!(parse_show_create_procedure_values(&null_definition).is_err());
        assert!(parse_show_create_procedure_values(&row[..2]).is_err());
    }
}
