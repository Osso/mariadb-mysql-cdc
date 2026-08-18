use super::parallel_target::{SubmittedQueryConnection, SubmittedQueryConnectionFactory};
use super::{ApplyBinlogConfig, TargetMySqlConfig, target_session_init_command};
use crate::mysql_client::missing_foreign_key::{
    DuplicateParentReconciliation, MissingForeignKeyParent,
    fetch_source_missing_foreign_key_parent, finish_duplicate_parent_probe,
    prepare_duplicate_parent_probe, query_foreign_key_reference, verify_parent_query_rows,
};
use crate::mysql_client::{
    PersistentMySqlSource, open_initialized_target_connection, open_stream_source,
};
use crate::mysql_support::{
    DEFAULT_MYSQL_CONNECT_TIMEOUT, DEFAULT_MYSQL_READ_TIMEOUT, DEFAULT_MYSQL_WRITE_TIMEOUT,
    target_mysql_opts,
};
use crate::target::{
    SqlStatement, TargetExecuteError, TargetRowChange, render_submitted_sql_statement,
};
use mysql::{Conn, Value};
use mysqlclient_sys::mysql_option::{
    MYSQL_OPT_CONNECT_TIMEOUT, MYSQL_OPT_READ_TIMEOUT, MYSQL_OPT_SSL_CA, MYSQL_OPT_SSL_ENFORCE,
    MYSQL_OPT_SSL_VERIFY_SERVER_CERT, MYSQL_OPT_WRITE_TIMEOUT,
};
use std::ffi::{CStr, CString, c_void};
use std::os::raw::{c_char, c_ulong};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::OnceLock;

const CLIENT_MULTI_STATEMENTS: c_ulong = 1 << 16;
const CLIENT_MULTI_RESULTS: c_ulong = 1 << 17;

static CONNECTOR_LIBRARY_INIT: OnceLock<Result<(), String>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct MariaDbSubmittedQueryFactory {
    config: ApplyBinlogConfig,
}

impl MariaDbSubmittedQueryFactory {
    pub(crate) fn new(config: &ApplyBinlogConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }
}

impl SubmittedQueryConnectionFactory for MariaDbSubmittedQueryFactory {
    type Connection = MariaDbSubmittedQueryConnection;

    fn open(&self) -> Result<Self::Connection, TargetExecuteError> {
        MariaDbSubmittedQueryConnection::open(&self.config)
    }
}

pub(crate) struct MariaDbSubmittedQueryConnection {
    raw: NonNull<mysqlclient_sys::MYSQL>,
    source: PersistentMySqlSource,
    target_config: TargetMySqlConfig,
    target_metadata: Option<Conn>,
}

struct StoredResult(NonNull<mysqlclient_sys::MYSQL_RES>);

impl Drop for StoredResult {
    fn drop(&mut self) {
        unsafe { mysqlclient_sys::mysql_free_result(self.0.as_ptr()) };
    }
}

struct MariaDbConnectionParameters {
    host: CString,
    user: CString,
    password: CString,
    database: CString,
    ca_file: CString,
    port: u16,
}

impl MariaDbConnectionParameters {
    fn from_config(config: &TargetMySqlConfig) -> Result<Self, TargetExecuteError> {
        Ok(Self {
            host: c_string("target host", &config.host)?,
            user: c_string("target user", &config.user)?,
            password: c_string("target password", &config.password)?,
            database: c_string("target database", &config.database)?,
            ca_file: c_string("target TLS CA file", &config.tls_ca_file)?,
            port: config.port,
        })
    }
}

impl MariaDbSubmittedQueryConnection {
    fn open(config: &ApplyBinlogConfig) -> Result<Self, TargetExecuteError> {
        initialize_connector_library()?;
        let source = open_stream_source(config)?;
        let parameters = MariaDbConnectionParameters::from_config(&config.target)?;
        let mut connection = Self::initialize(source, config.target.clone())?;
        connection.configure_network_and_tls(&parameters.ca_file)?;
        connection.connect(&parameters)?;
        connection.require_tls()?;
        connection.set_character_set()?;
        connection.execute_initialization_query(target_session_init_command())?;
        Ok(connection)
    }

    fn initialize(
        source: PersistentMySqlSource,
        target_config: TargetMySqlConfig,
    ) -> Result<Self, TargetExecuteError> {
        let raw = NonNull::new(unsafe { mysqlclient_sys::mysql_init(ptr::null_mut()) });
        let Some(raw) = raw else {
            unsafe { mysqlclient_sys::mysql_thread_end() };
            return Err(TargetExecuteError::new(
                "failed to initialize target MariaDB client",
            ));
        };
        Ok(Self {
            raw,
            source,
            target_config,
            target_metadata: None,
        })
    }

    fn configure_network_and_tls(&mut self, ca_file: &CString) -> Result<(), TargetExecuteError> {
        self.configure_timeout(
            TimeoutOption::Connect,
            duration_seconds(DEFAULT_MYSQL_CONNECT_TIMEOUT),
        )?;
        self.configure_timeout(
            TimeoutOption::Read,
            duration_seconds(DEFAULT_MYSQL_READ_TIMEOUT),
        )?;
        self.configure_timeout(
            TimeoutOption::Write,
            duration_seconds(DEFAULT_MYSQL_WRITE_TIMEOUT),
        )?;
        self.configure_tls(ca_file)
    }

    fn connect(
        &mut self,
        parameters: &MariaDbConnectionParameters,
    ) -> Result<(), TargetExecuteError> {
        let connected = unsafe {
            mysqlclient_sys::mysql_real_connect(
                self.raw.as_ptr(),
                parameters.host.as_ptr(),
                parameters.user.as_ptr(),
                parameters.password.as_ptr(),
                parameters.database.as_ptr(),
                u32::from(parameters.port),
                ptr::null(),
                CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS,
            )
        };
        if connected.is_null() {
            return Err(self.error("connect"));
        }
        Ok(())
    }

    fn configure_timeout(
        &mut self,
        option: TimeoutOption,
        seconds: u32,
    ) -> Result<(), TargetExecuteError> {
        let mysql_option = match option {
            TimeoutOption::Connect => MYSQL_OPT_CONNECT_TIMEOUT,
            TimeoutOption::Read => MYSQL_OPT_READ_TIMEOUT,
            TimeoutOption::Write => MYSQL_OPT_WRITE_TIMEOUT,
        };
        let result = unsafe {
            mysqlclient_sys::mysql_optionsv(
                self.raw.as_ptr(),
                mysql_option,
                (&seconds as *const u32).cast::<c_void>(),
            )
        };
        if result == 0 {
            return Ok(());
        }
        Err(self.error(option.error_label()))
    }

    fn configure_tls(&mut self, ca_file: &CString) -> Result<(), TargetExecuteError> {
        let enabled = mysqlclient_sys::TRUE;
        let enforce_result = unsafe {
            mysqlclient_sys::mysql_optionsv(
                self.raw.as_ptr(),
                MYSQL_OPT_SSL_ENFORCE,
                (&enabled as *const mysqlclient_sys::my_bool).cast::<c_void>(),
            )
        };
        if enforce_result != 0 {
            return Err(self.error("require TLS"));
        }
        let verify_result = unsafe {
            mysqlclient_sys::mysql_optionsv(
                self.raw.as_ptr(),
                MYSQL_OPT_SSL_VERIFY_SERVER_CERT,
                (&enabled as *const mysqlclient_sys::my_bool).cast::<c_void>(),
            )
        };
        if verify_result != 0 {
            return Err(self.error("enable TLS server certificate verification"));
        }
        let ca_result = unsafe {
            mysqlclient_sys::mysql_optionsv(
                self.raw.as_ptr(),
                MYSQL_OPT_SSL_CA,
                ca_file.as_ptr().cast::<c_void>(),
            )
        };
        if ca_result == 0 {
            return Ok(());
        }
        Err(self.error("configure target TLS CA"))
    }

    fn require_tls(&self) -> Result<(), TargetExecuteError> {
        let cipher = unsafe { mysqlclient_sys::mysql_get_ssl_cipher(self.raw.as_ptr()) };
        if cipher.is_null() {
            return Err(TargetExecuteError::new(
                "target MariaDB client connected without TLS",
            ));
        }
        Ok(())
    }

    fn set_character_set(&mut self) -> Result<(), TargetExecuteError> {
        let character_set = c_string("target character set", "utf8mb4")?;
        let result = unsafe {
            mysqlclient_sys::mysql_set_character_set(self.raw.as_ptr(), character_set.as_ptr())
        };
        if result == 0 {
            return Ok(());
        }
        Err(self.error("set target character set"))
    }

    fn execute_initialization_query(&mut self, sql: &str) -> Result<(), TargetExecuteError> {
        self.send_query(sql)?;
        self.read_query_result()
    }

    fn open_or_reuse_target_metadata_connection(
        &mut self,
    ) -> Result<&mut Conn, TargetExecuteError> {
        if self.target_metadata.is_none() {
            let opts = target_mysql_opts(&self.target_config).map_err(TargetExecuteError::new)?;
            let connection = open_initialized_target_connection(opts)?;
            self.target_metadata = Some(connection);
        }
        self.target_metadata.as_mut().ok_or_else(|| {
            TargetExecuteError::new("parallel target metadata connection is unavailable")
        })
    }

    fn query_transaction_rows(
        &mut self,
        statement: &SqlStatement,
    ) -> Result<Vec<Vec<Value>>, TargetExecuteError> {
        let sql = render_submitted_sql_statement(statement)?;
        self.send_query(&sql)?;
        self.read_transaction_rows()
    }

    fn read_transaction_rows(&mut self) -> Result<Vec<Vec<Value>>, TargetExecuteError> {
        self.accept_row_query_result()?;
        let result = self.store_row_query_result()?;
        let rows = self.read_stored_query_rows(&result)?;
        self.reject_additional_row_query_results()?;
        Ok(rows)
    }

    fn accept_row_query_result(&self) -> Result<(), TargetExecuteError> {
        let failed = unsafe { mysqlclient_sys::mysql_read_query_result(self.raw.as_ptr()) }
            != mysqlclient_sys::FALSE;
        if failed {
            return Err(self.error("read row query result"));
        }
        Ok(())
    }

    fn store_row_query_result(&self) -> Result<StoredResult, TargetExecuteError> {
        let result =
            NonNull::new(unsafe { mysqlclient_sys::mysql_store_result(self.raw.as_ptr()) });
        if let Some(result) = result {
            return Ok(StoredResult(result));
        }
        if unsafe { mysqlclient_sys::mysql_errno(self.raw.as_ptr()) } != 0 {
            return Err(self.error("store row query result"));
        }
        Err(TargetExecuteError::new(
            "submitted duplicate-parent query returned no result set",
        ))
    }

    fn read_stored_query_rows(
        &self,
        result: &StoredResult,
    ) -> Result<Vec<Vec<Value>>, TargetExecuteError> {
        let field_count =
            usize::try_from(unsafe { mysqlclient_sys::mysql_num_fields(result.0.as_ptr()) })
                .map_err(|_| TargetExecuteError::new("submitted row query field count overflow"))?;
        let mut rows = Vec::new();
        loop {
            let row = unsafe { mysqlclient_sys::mysql_fetch_row(result.0.as_ptr()) };
            if row.is_null() {
                if unsafe { mysqlclient_sys::mysql_errno(self.raw.as_ptr()) } != 0 {
                    return Err(self.error("fetch row query result"));
                }
                return Ok(rows);
            }
            let lengths = unsafe { mysqlclient_sys::mysql_fetch_lengths(result.0.as_ptr()) };
            if lengths.is_null() {
                return Err(self.error("fetch row query lengths"));
            }
            rows.push(copy_result_row(row, lengths, field_count)?);
        }
    }

    fn reject_additional_row_query_results(&self) -> Result<(), TargetExecuteError> {
        let has_more = unsafe { mysqlclient_sys::mysql_more_results(self.raw.as_ptr()) }
            != mysqlclient_sys::FALSE;
        if has_more {
            return Err(TargetExecuteError::new(
                "submitted duplicate-parent query returned additional result sets",
            ));
        }
        Ok(())
    }

    fn consume_current_result(&mut self) -> Result<(), TargetExecuteError> {
        let result = unsafe { mysqlclient_sys::mysql_store_result(self.raw.as_ptr()) };
        if !result.is_null() {
            unsafe { mysqlclient_sys::mysql_free_result(result) };
            return Ok(());
        }
        if unsafe { mysqlclient_sys::mysql_field_count(self.raw.as_ptr()) } == 0 {
            return Ok(());
        }
        Err(self.error("store query result"))
    }

    fn error(&self, operation: &str) -> TargetExecuteError {
        let code = unsafe { mysqlclient_sys::mysql_errno(self.raw.as_ptr()) };
        let message = c_error_text(unsafe { mysqlclient_sys::mysql_error(self.raw.as_ptr()) });
        let sql_state = c_error_text(unsafe { mysqlclient_sys::mysql_sqlstate(self.raw.as_ptr()) });
        TargetExecuteError::from_mysql(
            u16::try_from(code).unwrap_or(u16::MAX),
            format!(
                "parallel target MariaDB {operation} failed: ERROR {code} ({sql_state}): {message}"
            ),
        )
    }
}

fn copy_result_row(
    row: mysqlclient_sys::MYSQL_ROW,
    lengths: *mut c_ulong,
    field_count: usize,
) -> Result<Vec<Value>, TargetExecuteError> {
    (0..field_count)
        .map(|index| {
            let value = unsafe { *row.add(index) };
            if value.is_null() {
                return Ok(Value::NULL);
            }
            let length = usize::try_from(unsafe { *lengths.add(index) }).map_err(|_| {
                TargetExecuteError::new("submitted row query value length overflow")
            })?;
            let bytes = unsafe { slice::from_raw_parts(value.cast::<u8>(), length) };
            Ok(Value::Bytes(bytes.to_vec()))
        })
        .collect()
}

impl SubmittedQueryConnection for MariaDbSubmittedQueryConnection {
    fn send_query(&mut self, sql: &str) -> Result<(), TargetExecuteError> {
        let length = c_ulong::try_from(sql.len())
            .map_err(|_| TargetExecuteError::new("parallel target SQL exceeds client length"))?;
        let result = unsafe {
            mysqlclient_sys::mysql_send_query(
                self.raw.as_ptr(),
                sql.as_ptr().cast::<c_char>(),
                length,
            )
        };
        if result == 0 {
            return Ok(());
        }
        Err(self.error("send query"))
    }

    fn read_query_result(&mut self) -> Result<(), TargetExecuteError> {
        if unsafe { mysqlclient_sys::mysql_read_query_result(self.raw.as_ptr()) }
            != mysqlclient_sys::FALSE
        {
            return Err(self.error("read query result"));
        }
        loop {
            self.consume_current_result()?;
            match unsafe { mysqlclient_sys::mysql_next_result(self.raw.as_ptr()) } {
                -1 => return Ok(()),
                0 => {}
                _ => return Err(self.error("read next query result")),
            }
        }
    }

    fn load_missing_foreign_key_parent(
        &mut self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<MissingForeignKeyParent, TargetExecuteError> {
        let reference = {
            let target = self.open_or_reuse_target_metadata_connection()?;
            query_foreign_key_reference(target, change, error)?
        };
        fetch_source_missing_foreign_key_parent(&self.source, change, &reference)
    }

    fn load_duplicate_parent_reconciliation(
        &mut self,
        change: &TargetRowChange,
        error: &TargetExecuteError,
    ) -> Result<DuplicateParentReconciliation, TargetExecuteError> {
        let probe = {
            let target = self.open_or_reuse_target_metadata_connection()?;
            prepare_duplicate_parent_probe(target, change, error)?
        };
        let owner_statement = probe.owner_statement.clone();
        let owner_rows = self.query_transaction_rows(&owner_statement)?;
        finish_duplicate_parent_probe(&self.source, change, probe, owner_rows)
    }

    fn verify_duplicate_parent_reconciliation(
        &mut self,
        change: &TargetRowChange,
        reconciliation: &DuplicateParentReconciliation,
    ) -> Result<(), TargetExecuteError> {
        let verification = reconciliation.verification.clone();
        let rows = self.query_transaction_rows(&verification)?;
        verify_parent_query_rows(change, rows)
    }
}

impl Drop for MariaDbSubmittedQueryConnection {
    fn drop(&mut self) {
        unsafe {
            mysqlclient_sys::mysql_close(self.raw.as_ptr());
            mysqlclient_sys::mysql_thread_end();
        }
    }
}

#[derive(Clone, Copy)]
enum TimeoutOption {
    Connect,
    Read,
    Write,
}

impl TimeoutOption {
    fn error_label(self) -> &'static str {
        match self {
            Self::Connect => "configure connect timeout",
            Self::Read => "configure read timeout",
            Self::Write => "configure write timeout",
        }
    }
}

fn initialize_connector_library() -> Result<(), TargetExecuteError> {
    let result = CONNECTOR_LIBRARY_INIT.get_or_init(|| {
        let code =
            unsafe { mysqlclient_sys::mysql_server_init(0, ptr::null_mut(), ptr::null_mut()) };
        if code != 0 {
            return Err(format!(
                "failed to initialize target MariaDB client library: code {code}"
            ));
        }
        if unsafe { mysqlclient_sys::mysql_thread_safe() } == 0 {
            return Err("target MariaDB client library is not thread-safe".to_string());
        }
        Ok(())
    });
    result
        .as_ref()
        .map_err(|message| TargetExecuteError::new(message.clone()))
        .copied()
}

fn c_string(label: &str, value: &str) -> Result<CString, TargetExecuteError> {
    CString::new(value).map_err(|_| TargetExecuteError::new(format!("{label} contains a NUL byte")))
}

fn c_error_text(value: *const c_char) -> String {
    if value.is_null() {
        return "unknown client error".to_string();
    }
    unsafe { CStr::from_ptr(value) }
        .to_string_lossy()
        .into_owned()
}

fn duration_seconds(duration: std::time::Duration) -> u32 {
    u32::try_from(duration.as_secs()).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::{c_error_text, c_string, duration_seconds};
    use std::ffi::CString;
    use std::time::Duration;

    #[test]
    fn rejects_embedded_nul_without_exposing_the_value() {
        let error = c_string("target password", "secret\0suffix").expect_err("reject embedded NUL");

        assert_eq!(error.to_string(), "target password contains a NUL byte");
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn reads_connector_error_text_lossily() {
        let value = CString::new([b'e', b'r', b'r', 0xff]).expect("create C string");

        assert_eq!(c_error_text(value.as_ptr()), "err�");
    }

    #[test]
    fn bounds_connector_timeout_seconds() {
        assert_eq!(duration_seconds(Duration::from_secs(30)), 30);
        assert_eq!(duration_seconds(Duration::from_secs(u64::MAX)), u32::MAX);
    }
}
