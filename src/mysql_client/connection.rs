use crate::mysql_support::ssl_opts_from_ca;
use crate::snapshot::SnapshotError;
use crate::table_sync::TableSyncError;
use crate::target::TargetExecuteError;
use mysql::{Conn, Opts, OptsBuilder};

pub(crate) fn base_opts(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
    tls_ca_file: Option<&str>,
    endpoint: &str,
) -> Result<Opts, String> {
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(Some(host))
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password))
        .db_name(Some(database))
        .prefer_socket(false);
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
