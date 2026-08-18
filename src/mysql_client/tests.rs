use super::*;
use mysql::{Opts, OptsBuilder, Value};
use std::net::TcpListener;
use std::time::{Duration, Instant};

#[test]
fn formats_mysql_values_as_source_text_rows() {
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
fn binlog_coordinate_uses_exact_mariadb_master_status_query() {
    assert_eq!(binlog_coordinate_query(), "SHOW MASTER STATUS");
}

#[test]
fn parses_mariadb_master_status_row_shape() {
    let checkpoint = parse_binlog_coordinate_checkpoint(vec![vec![
        Some("mysqld-bin.000123".to_string()),
        Some("456".to_string()),
        Some(String::new()),
        Some(String::new()),
    ]])
    .expect("valid MariaDB SHOW MASTER STATUS row");

    assert_eq!(checkpoint.source_file, "mysqld-bin.000123");
    assert_eq!(checkpoint.source_position, 456);
    assert_eq!(
        checkpoint.last_event.event_type,
        "LostBinlogRecoveryCoordinate"
    );
}

#[test]
fn rejects_invalid_mariadb_master_status_shapes() {
    let cases = [
        (Vec::new(), "MariaDB binlog coordinate is missing"),
        (
            vec![vec![None, Some("456".to_string()), None, None]],
            "MariaDB binlog coordinate file is missing",
        ),
        (
            vec![vec![
                Some("mysqld-bin.000123".to_string()),
                None,
                None,
                None,
            ]],
            "MariaDB binlog coordinate position is missing",
        ),
        (
            vec![vec![
                Some("mysqld-bin.000123".to_string()),
                Some("not-a-number".to_string()),
                None,
                None,
            ]],
            "invalid MariaDB binlog coordinate position",
        ),
    ];

    for (rows, expected_message) in cases {
        let error = parse_binlog_coordinate_checkpoint(rows)
            .expect_err("invalid MariaDB SHOW MASTER STATUS row must fail closed");
        assert!(
            error.to_string().contains(expected_message),
            "expected {expected_message:?}, got {error}"
        );
    }
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
fn shared_connection_opts_have_bounded_network_timeouts() {
    let opts = base_opts(
        "source-db",
        3306,
        "reader",
        "secret",
        "globalcomix",
        None,
        "source `source-db`:3306",
    )
    .expect("source options");

    assert_eq!(
        opts.get_tcp_connect_timeout(),
        Some(Duration::from_secs(10))
    );
    assert_eq!(opts.get_read_timeout(), Some(&Duration::from_secs(30)));
    assert_eq!(opts.get_write_timeout(), Some(&Duration::from_secs(30)));
    assert_eq!(opts.get_tcp_keepalive_time_ms(), Some(10_000));
    #[cfg(target_os = "linux")]
    {
        assert_eq!(opts.get_tcp_keepalive_probe_interval_secs(), Some(5));
        assert_eq!(opts.get_tcp_keepalive_probe_count(), Some(3));
        assert_eq!(opts.get_tcp_user_timeout_ms(), Some(30_000));
    }
}

#[test]
fn stalled_mysql_handshake_returns_within_read_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stalled server");
    let port = listener.local_addr().expect("listener address").port();
    let server = std::thread::spawn(move || {
        let (_connection, _) = listener.accept().expect("accept client");
        std::thread::sleep(Duration::from_secs(2));
    });
    let builder = OptsBuilder::from_opts(
        base_opts(
            "127.0.0.1",
            port,
            "reader",
            "secret",
            "globalcomix",
            None,
            "stalled source",
        )
        .expect("stalled source options"),
    );
    let opts = Opts::from(apply_network_timeouts(
        builder,
        NetworkTimeouts {
            connect: Duration::from_millis(100),
            read: Duration::from_millis(100),
            write: Duration::from_millis(100),
        },
    ));

    let started = Instant::now();
    let error = open_conn(opts).expect_err("stalled handshake must time out");

    assert!(started.elapsed() < Duration::from_secs(1));
    let message = error.to_string().to_ascii_lowercase();
    assert!(
        message.contains("timed out") || message.contains("resource temporarily unavailable"),
        "unexpected timeout error: {message}"
    );
    server.join().expect("stalled server");
}

#[test]
fn sync_target_writer_opts_have_bounded_operation_timeouts() {
    let target = TargetMySqlConfig {
        host: "target-db.example".to_string(),
        tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
        ..TargetMySqlConfig::default()
    };

    let opts = sync_target_opts(&target).expect("sync target options");

    assert_eq!(opts.get_read_timeout(), Some(&Duration::from_secs(30)));
    assert_eq!(opts.get_write_timeout(), Some(&Duration::from_secs(30)));
    assert_eq!(opts.get_tcp_keepalive_time_ms(), Some(10_000));
    #[cfg(target_os = "linux")]
    assert_eq!(opts.get_tcp_user_timeout_ms(), Some(30_000));
}

#[test]
fn persistent_target_reader_connection_uses_configured_ca() {
    let config = crate::mysql_config::MySqlConnectionConfig {
        host: "target-db.example".to_string(),
        port: 1,
        user: "reader".to_string(),
        password: "secret".to_string(),
        database: "globalcomix".to_string(),
    };
    let ca_file = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem");

    let error = match PersistentMySqlSource::new_with_tls_ca(&config, Some(ca_file)) {
        Ok(_) => panic!("test target reader connection should fail at port 1"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("failed to connect to source mysql")
    );
    assert!(!error.to_string().contains("TLS CA file"));
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
fn builds_target_column_select_sql() {
    let sql = build_target_column_select_sql("accounts");

    assert_eq!(
        sql,
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' ORDER BY ORDINAL_POSITION"
    );
}
