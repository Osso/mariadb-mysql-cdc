use crate::live::TargetMySqlConfig;
use mysql::{Opts, OptsBuilder, SslOpts};
use std::path::PathBuf;

pub const SOURCE_TLS_CA_FILE: &str = "/etc/mariadb-mysql-cdc/source-ca.pem";
pub const TARGET_TLS_CA_FILE: &str = "/etc/mariadb-mysql-cdc/do-ca.pem";

pub fn target_mysql_opts(target: &TargetMySqlConfig) -> Result<Opts, String> {
    Ok(Opts::from(
        OptsBuilder::default()
            .ip_or_hostname(Some(target.host.clone()))
            .tcp_port(target.port)
            .user(Some(target.user.clone()))
            .pass(Some(target.password.clone()))
            .db_name(Some(target.database.clone()))
            .prefer_socket(false)
            .ssl_opts(Some(ssl_opts_from_ca(Some(&target.tls_ca_file)))),
    ))
}

pub fn target_ssl_opts() -> SslOpts {
    ssl_opts_from_ca(Some(TARGET_TLS_CA_FILE))
}

pub fn ssl_opts_from_ca(ca_file: Option<&str>) -> SslOpts {
    let mut ssl = SslOpts::default();
    if let Some(ca_file) = ca_file
        && std::path::Path::new(ca_file).exists()
    {
        ssl = ssl
            .with_root_cert_path(Some(PathBuf::from(ca_file)))
            .with_danger_skip_domain_validation(true);
    }
    ssl
}

pub fn target_mysql_args(target: &TargetMySqlConfig) -> Vec<String> {
    vec![
        "--host".to_string(),
        target.host.clone(),
        "--port".to_string(),
        target.port.to_string(),
        "--user".to_string(),
        target.user.clone(),
        format!("--password={}", target.password),
        "--ssl".to_string(),
        "--ssl-verify-server-cert=0".to_string(),
        "--database".to_string(),
        target.database.clone(),
        "--default-character-set=utf8mb4".to_string(),
    ]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssl_opts_from_existing_ca_skips_hostname_validation_but_keeps_ca_path() {
        let ca_path =
            std::env::temp_dir().join(format!("mariadb-mysql-cdc-test-ca-{}", std::process::id()));
        std::fs::write(&ca_path, b"test ca").unwrap();

        let ssl = ssl_opts_from_ca(ca_path.to_str());

        assert_eq!(ssl.root_cert_path(), Some(ca_path.as_path()));
        assert!(ssl.skip_domain_validation());
        assert!(!ssl.accept_invalid_certs());

        std::fs::remove_file(ca_path).unwrap();
    }

    #[test]
    fn target_mysql_opts_uses_configured_ca() {
        let target = TargetMySqlConfig {
            host: "target".to_string(),
            tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
            ..TargetMySqlConfig::default()
        };

        let opts = target_mysql_opts(&target).expect("target options");

        assert_eq!(
            opts.get_ssl_opts().and_then(|ssl| ssl.root_cert_path()),
            Some(std::path::Path::new(&target.tls_ca_file))
        );
    }

    #[test]
    fn target_mysql_args_disable_server_cert_verification_for_do_mysql() {
        let target = TargetMySqlConfig {
            host: "target".to_string(),
            port: 25060,
            user: "target_user".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            tls_ca_file: TARGET_TLS_CA_FILE.to_string(),
            insert_conflict_policy: crate::live::InsertConflictPolicy::IgnoreDuplicate,
        };

        let args = target_mysql_args(&target);

        assert!(args.contains(&"--ssl".to_string()));
        assert!(args.contains(&"--ssl-verify-server-cert=0".to_string()));
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
}
