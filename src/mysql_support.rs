use crate::live::TargetMySqlConfig;

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
    fn target_mysql_args_disable_server_cert_verification_for_do_mysql() {
        let target = TargetMySqlConfig {
            host: "target".to_string(),
            port: 25060,
            user: "target_user".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
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
}
