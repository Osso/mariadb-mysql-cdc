use super::*;
use mysql::Value;

#[test]
fn formats_mysql_values_like_snapshot_text_rows() {
    assert_eq!(value_to_string(Value::NULL), None);
    assert_eq!(
        value_to_string(Value::Bytes(b"NULL".to_vec())),
        Some("NULL".to_string())
    );
    assert_eq!(value_to_string(Value::Int(-5)), Some("-5".to_string()));
    assert_eq!(value_to_string(Value::UInt(5)), Some("5".to_string()));
    assert_eq!(
        value_to_string(Value::Date(2026, 6, 22, 12, 3, 4, 0)),
        Some("2026-06-22 12:03:04".to_string())
    );
    assert_eq!(
        value_to_string(Value::Time(false, 1, 2, 3, 4, 0)),
        Some("26:03:04".to_string())
    );
}

#[test]
fn shared_source_opts_accept_plaintext_without_tls_ca() {
    let opts = base_opts(
        "source-db",
        3306,
        "reader",
        "secret",
        "globalcomix",
        None,
        "source `source-db`:3306",
    )
    .expect("plaintext source options");

    assert!(opts.get_ssl_opts().is_none());
}

#[test]
fn target_opts_require_authenticated_tls() {
    let target = TargetMySqlConfig {
        host: "target".to_string(),
        port: 25060,
        user: "target_user".to_string(),
        password: "secret".to_string(),
        database: "globalcomix".to_string(),
        tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
        insert_conflict_policy: InsertConflictPolicy::IgnoreDuplicate,
    };

    let opts = base_opts(
        &target.host,
        target.port,
        &target.user,
        &target.password,
        &target.database,
        Some(&target.tls_ca_file),
        "target `target`:25060",
    )
    .expect("target TLS options");

    let ssl = opts.get_ssl_opts().expect("target TLS options");

    assert!(!ssl.skip_domain_validation());
    assert!(!ssl.accept_invalid_certs());
}

#[test]
fn connection_opts_use_explicit_ca_for_tls() {
    let ca_path = std::env::temp_dir().join(format!(
        "mariadb-mysql-cdc-target-reader-ca-{}",
        std::process::id()
    ));
    std::fs::write(&ca_path, b"test ca").expect("write CA fixture");

    std::fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem"),
        &ca_path,
    )
    .expect("write CA fixture");

    let opts = base_opts(
        "target",
        25060,
        "target_user",
        "secret",
        "globalcomix",
        ca_path.to_str(),
        "target `target`:25060",
    )
    .expect("target options");
    let ssl = opts.get_ssl_opts().expect("TLS opts");

    assert_eq!(ssl.root_cert_path(), Some(ca_path.as_path()));
    assert!(!ssl.skip_domain_validation());

    std::fs::remove_file(ca_path).expect("remove CA fixture");
}

#[test]
fn stream_lease_uses_nonblocking_hashed_mysql_lock() {
    assert_eq!(
        build_stream_lease_sql("cdc-stream:globalcomix"),
        "SELECT GET_LOCK(SHA2('cdc-stream:globalcomix',256),0)"
    );
    ensure_stream_lease_acquired("cdc-stream:globalcomix", Some(1)).expect("acquired lease");
}

#[test]
fn stream_lease_rejects_missing_or_unacquired_lock() {
    assert!(ensure_stream_lease_acquired("cdc-stream:globalcomix", None).is_err());
    assert!(ensure_stream_lease_acquired("cdc-stream:globalcomix", Some(0)).is_err());
}

#[test]
fn classifies_supported_mysql_constraint_errors_for_durable_evidence() {
    for code in [1048, 1451, 1452, 3819, 4025] {
        let error = TargetExecuteError::from_mysql(code, format!("constraint failure {code}"));
        let conflict = constraint_conflict_from_error(&error).expect("constraint conflict");
        assert_eq!(conflict.error_code, code);
        assert_eq!(conflict.duplicate_index, None);
    }
}

#[test]
fn rejects_non_constraint_mysql_errors_as_conflict_evidence() {
    let error = TargetExecuteError::from_mysql(1142, "permission denied");

    assert_eq!(constraint_conflict_from_error(&error), None);
}

#[test]
fn builds_snapshot_progress_select_sql_for_cdc_table() {
    let sql = build_snapshot_progress_select_sql("cdc.table_sync_progress");

    assert_eq!(
        sql,
        "SELECT table_name, COALESCE(last_primary_key_json, ''), rows_scanned, status FROM `cdc`.`table_sync_progress`"
    );
}

#[test]
fn builds_progress_error_sql_with_table_and_message() {
    let sql = build_progress_error_message_sql("cdc.table_sync_progress", "releases", "can't copy");

    assert_eq!(
        sql,
        "INSERT INTO `cdc`.`table_sync_progress` (table_name,mode,status,last_error) VALUES ('releases','apply','error','can''t copy') ON DUPLICATE KEY UPDATE status='error',last_error=VALUES(last_error)"
    );
}

#[test]
fn converts_mysql_progress_rows_to_snapshot_progress() {
    let rows = vec![
        (
            "accounts".to_string(),
            "[\"42\"]".to_string(),
            42,
            "running".to_string(),
        ),
        (
            "releases".to_string(),
            String::new(),
            100,
            "complete".to_string(),
        ),
    ];

    let progress = snapshot_progress_from_rows(rows).expect("progress");

    let accounts = progress.table("accounts").expect("accounts");
    assert_eq!(accounts.last_primary_key, Some(vec!["42".to_string()]));
    assert_eq!(accounts.rows_copied, 42);
    assert!(!accounts.complete);

    let releases = progress.table("releases").expect("releases");
    assert_eq!(releases.last_primary_key, None);
    assert_eq!(releases.rows_copied, 100);
    assert!(releases.complete);
}

#[test]
fn plans_snapshot_boundary_offsets_for_four_workers() {
    assert_eq!(snapshot_boundary_offsets(10, 4), vec![2, 4, 7]);
}

#[test]
fn skips_snapshot_boundary_offsets_when_rows_are_too_sparse() {
    assert_eq!(snapshot_boundary_offsets(2, 4), vec![0, 1]);
}

#[test]
fn builds_snapshot_boundary_select_sql() {
    let table = crate::snapshot::SnapshotTable {
        name: "accounts".to_string(),
        primary_key: vec!["tenant_id".to_string(), "id".to_string()],
        columns: Vec::new(),
    };

    let sql = build_snapshot_boundary_select_sql(&table, 99);

    assert_eq!(
        sql,
        "SELECT `tenant_id`, `id` FROM `accounts` ORDER BY `tenant_id`, `id` LIMIT 1 OFFSET 99"
    );
}

#[test]
fn builds_target_column_select_sql() {
    let sql = build_target_column_select_sql("accounts");

    assert_eq!(
        sql,
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' ORDER BY ORDINAL_POSITION"
    );
}
