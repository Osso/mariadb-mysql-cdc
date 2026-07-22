use crate::mysql_support::{
    DEFAULT_MYSQL_CONNECT_TIMEOUT, DEFAULT_MYSQL_READ_TIMEOUT, DEFAULT_MYSQL_WRITE_TIMEOUT,
    apply_mysql_tcp_liveness, ssl_opts_from_ca,
};
use crate::snapshot::SnapshotError;
use crate::table_sync::TableSyncError;
use crate::target::TargetExecuteError;
use mysql::{Conn, Opts, OptsBuilder};
use std::time::Duration;

const DEFAULT_NETWORK_TIMEOUTS: NetworkTimeouts = NetworkTimeouts {
    connect: DEFAULT_MYSQL_CONNECT_TIMEOUT,
    read: DEFAULT_MYSQL_READ_TIMEOUT,
    write: DEFAULT_MYSQL_WRITE_TIMEOUT,
};

#[derive(Clone, Copy)]
pub(crate) struct NetworkTimeouts {
    pub(crate) connect: Duration,
    pub(crate) read: Duration,
    pub(crate) write: Duration,
}

pub(crate) fn apply_network_timeouts(
    builder: OptsBuilder,
    timeouts: NetworkTimeouts,
) -> OptsBuilder {
    apply_mysql_tcp_liveness(
        builder
            .tcp_connect_timeout(Some(timeouts.connect))
            .read_timeout(Some(timeouts.read))
            .write_timeout(Some(timeouts.write)),
    )
}

pub(crate) fn base_opts(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
    tls_ca_file: Option<&str>,
    endpoint: &str,
) -> Result<Opts, String> {
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password))
        .db_name(Some(database))
        .prefer_socket(false);
    let mut builder = apply_network_timeouts(builder, DEFAULT_NETWORK_TIMEOUTS);
    if let Some(ca_file) = tls_ca_file {
        builder = builder.ssl_opts(ssl_opts_from_ca(endpoint, host, ca_file)?);
    }
    Ok(Opts::from(builder))
}

pub(crate) fn open_conn(opts: Opts) -> mysql::Result<Conn> {
    Conn::new(opts)
}

pub(crate) fn snapshot_connect_error(error: mysql::Error) -> SnapshotError {
    SnapshotError::InvalidTable(format!("failed to connect to source mysql: {error}"))
}

pub(crate) fn target_connect_error(error: mysql::Error) -> TargetExecuteError {
    TargetExecuteError::new(format!("failed to connect to target mysql: {error}"))
}

pub(crate) fn progress_connect_error(error: mysql::Error) -> TableSyncError {
    TableSyncError::Progress(format!("failed to connect to target mysql: {error}"))
}
