use crate::inventory::reader::{
    InventoryConnectionFactory, InventoryQueryConnection, InventoryQueryFailure,
    InventoryQueryStage, inventory_opts,
};
use crate::inventory::retry::format_inventory_reset_log;
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, InventoryReader, MariaDbInventoryReader,
};
use mysql::{DriverError, MySqlError};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

#[test]
fn inventory_reader_does_not_shell_out_to_mariadb_cli() {
    let source = include_str!("../reader.rs");

    assert!(source.contains("InventoryConnectionState"));
    assert!(source.contains("Conn::new"));
    assert!(!source.contains("std::process::Command"));
    assert!(!source.contains("Command::new"));
}

#[test]
fn inventory_options_enable_tls_when_configured() {
    let config = InventoryConfig {
        host: "target-db".to_string(),
        use_tls: true,
        tls_ca_file: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string()),
        endpoint_role: InventoryEndpointRole::Target,
        ..InventoryConfig::default()
    };

    let opts = inventory_opts(&config).expect("inventory TLS options");
    let ssl = opts.get_ssl_opts().expect("target inventory TLS options");

    assert_eq!(
        ssl.root_cert_path(),
        Some(std::path::Path::new(config.tls_ca_file.as_deref().unwrap()))
    );
    assert!(!ssl.accept_invalid_certs());
    assert!(!ssl.skip_domain_validation());
    assert_eq!(
        opts.get_tcp_connect_timeout(),
        Some(Duration::from_secs(10))
    );
    assert_eq!(opts.get_tcp_keepalive_time_ms(), Some(10_000));
    assert_eq!(opts.get_read_timeout(), None);
    assert_eq!(opts.get_write_timeout(), None);
    #[cfg(target_os = "linux")]
    assert_eq!(opts.get_tcp_user_timeout_ms(), Some(30_000));
}

#[test]
fn inventory_reset_log_identifies_retry_attempt_and_reset() {
    let config = target_config();
    let failure = InventoryQueryFailure {
        error: mysql::Error::DriverError(DriverError::PacketOutOfSync).to_string(),
        retryable: true,
        connection_age: Some(Duration::from_millis(42)),
    };

    let message = format_inventory_reset_log(
        InventoryQueryStage::Tables,
        "globalcomix",
        &config,
        &failure,
    );

    assert!(message.contains("attempt=1/2"));
    assert!(message.contains("reset=true"));
    assert!(message.contains("connection_age_ms=42"));
}

#[test]
fn inventory_query_reconnects_once_after_packet_desynchronization() {
    let factory = Rc::new(ScriptedConnectionFactory::new(vec![
        vec![Err(mysql::Error::DriverError(DriverError::PacketOutOfSync))],
        vec![Ok(vec![table_fields("accounts")])],
    ]));
    let reader = MariaDbInventoryReader::with_factory(target_config(), factory.clone());

    let tables = reader.read_tables("globalcomix").expect("retried tables");

    assert_eq!(tables[0].table_name, "accounts");
    assert_eq!(factory.opens.get(), 2);
}

#[test]
fn inventory_query_retries_initial_connection_failure_once() {
    let factory = Rc::new(FailFirstConnectionFactory {
        attempts: Cell::new(0),
        connection: RefCell::new(Some(ScriptedInventoryConnection {
            results: vec![Ok(vec![table_fields("accounts")])].into(),
        })),
    });
    let reader = MariaDbInventoryReader::with_factory(target_config(), factory.clone());

    let tables = reader
        .read_tables("globalcomix")
        .expect("initial connection failure retried");

    assert_eq!(tables[0].table_name, "accounts");
    assert_eq!(factory.attempts.get(), 2);
}

#[test]
fn inventory_query_does_not_retry_server_sql_errors() {
    let factory = Rc::new(ScriptedConnectionFactory::new(vec![vec![Err(
        mysql::Error::MySqlError(MySqlError {
            state: "42000".to_string(),
            message: "permission denied".to_string(),
            code: 1142,
        }),
    )]]));
    let reader = MariaDbInventoryReader::with_factory(target_config(), factory.clone());

    let error = reader
        .read_tables("globalcomix")
        .expect_err("server error must fail");

    assert_eq!(factory.opens.get(), 1);
    assert!(error.to_string().contains("role=target"));
    assert!(error.to_string().contains("stage=tables"));
    assert!(error.to_string().contains("schema=globalcomix"));
    assert!(error.to_string().contains("attempt=1/2"));
    assert!(error.to_string().contains("reset=false"));
}

#[test]
fn inventory_query_replaces_expired_connection_before_reuse() {
    let factory = Rc::new(ScriptedConnectionFactory::new(vec![
        vec![Ok(vec![table_fields("first")])],
        vec![Ok(vec![table_fields("second")])],
    ]));
    let config = InventoryConfig {
        max_connection_age: Duration::ZERO,
        ..target_config()
    };
    let reader = MariaDbInventoryReader::with_factory(config, factory.clone());

    let first = reader.read_tables("globalcomix").expect("first tables");
    let second = reader.read_tables("globalcomix").expect("second tables");

    assert_eq!(first[0].table_name, "first");
    assert_eq!(second[0].table_name, "second");
    assert_eq!(factory.opens.get(), 2);
}

type ScriptedQueryResult = Result<Vec<Vec<String>>, mysql::Error>;

struct ScriptedInventoryConnection {
    results: VecDeque<ScriptedQueryResult>,
}

impl InventoryQueryConnection for ScriptedInventoryConnection {
    fn query_rows(&mut self, _query: &str) -> Result<Vec<Vec<String>>, mysql::Error> {
        self.results
            .pop_front()
            .expect("scripted inventory query result")
    }

    fn query_result_sets(&mut self, query: &str) -> Result<Vec<Vec<Vec<String>>>, mysql::Error> {
        let statements = query.matches(';').count() + 1;
        (0..statements).map(|_| self.query_rows(query)).collect()
    }
}

struct FailFirstConnectionFactory {
    attempts: Cell<usize>,
    connection: RefCell<Option<ScriptedInventoryConnection>>,
}

impl InventoryConnectionFactory for FailFirstConnectionFactory {
    fn connect(
        &self,
        _config: &InventoryConfig,
    ) -> Result<Box<dyn InventoryQueryConnection>, InventoryQueryFailure> {
        let attempt = self.attempts.get() + 1;
        self.attempts.set(attempt);
        if attempt == 1 {
            return Err(InventoryQueryFailure {
                error: "target inventory connection failed: connection refused".to_string(),
                retryable: true,
                connection_age: None,
            });
        }
        self.connection
            .borrow_mut()
            .take()
            .map(|connection| Box::new(connection) as Box<dyn InventoryQueryConnection>)
            .ok_or_else(|| InventoryQueryFailure {
                error: "scripted inventory connection exhausted".to_string(),
                retryable: false,
                connection_age: None,
            })
    }
}

struct ScriptedConnectionFactory {
    connections: RefCell<VecDeque<ScriptedInventoryConnection>>,
    opens: Cell<usize>,
}

impl ScriptedConnectionFactory {
    fn new(connection_results: Vec<Vec<ScriptedQueryResult>>) -> Self {
        Self {
            connections: RefCell::new(
                connection_results
                    .into_iter()
                    .map(|results| ScriptedInventoryConnection {
                        results: results.into(),
                    })
                    .collect(),
            ),
            opens: Cell::new(0),
        }
    }
}

impl InventoryConnectionFactory for ScriptedConnectionFactory {
    fn connect(
        &self,
        _config: &InventoryConfig,
    ) -> Result<Box<dyn InventoryQueryConnection>, InventoryQueryFailure> {
        self.opens.set(self.opens.get() + 1);
        self.connections
            .borrow_mut()
            .pop_front()
            .map(|connection| Box::new(connection) as Box<dyn InventoryQueryConnection>)
            .ok_or_else(|| InventoryQueryFailure {
                error: "scripted inventory connection exhausted".to_string(),
                retryable: false,
                connection_age: None,
            })
    }
}

fn target_config() -> InventoryConfig {
    InventoryConfig {
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        ..InventoryConfig::default()
    }
}

fn table_fields(name: &str) -> Vec<String> {
    vec![
        name.to_string(),
        "BASE TABLE".to_string(),
        "InnoDB".to_string(),
        "utf8mb4_unicode_ci".to_string(),
    ]
}
