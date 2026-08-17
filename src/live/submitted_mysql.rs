use super::parallel_target::{SubmittedQueryConnection, SubmittedQueryConnectionFactory};
use super::{TargetMySqlConfig, target_session_init_command};
use crate::mysql_support::{
    DEFAULT_MYSQL_CONNECT_TIMEOUT, DEFAULT_MYSQL_READ_TIMEOUT, DEFAULT_MYSQL_WRITE_TIMEOUT,
};
use crate::target::TargetExecuteError;
use mysqlclient_sys::mysql_option::{
    MYSQL_OPT_CONNECT_TIMEOUT, MYSQL_OPT_READ_TIMEOUT, MYSQL_OPT_SSL_CA, MYSQL_OPT_SSL_ENFORCE,
    MYSQL_OPT_SSL_VERIFY_SERVER_CERT, MYSQL_OPT_WRITE_TIMEOUT,
};
use std::ffi::{CStr, CString, c_void};
use std::os::raw::{c_char, c_ulong};
use std::ptr::{self, NonNull};
use std::sync::OnceLock;

const CLIENT_MULTI_STATEMENTS: c_ulong = 1 << 16;
const CLIENT_MULTI_RESULTS: c_ulong = 1 << 17;

static CONNECTOR_LIBRARY_INIT: OnceLock<Result<(), String>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct MariaDbSubmittedQueryFactory {
    config: TargetMySqlConfig,
}

impl MariaDbSubmittedQueryFactory {
    pub(crate) fn new(config: &TargetMySqlConfig) -> Self {
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
}

impl MariaDbSubmittedQueryConnection {
    fn open(config: &TargetMySqlConfig) -> Result<Self, TargetExecuteError> {
        initialize_connector_library()?;
        let host = c_string("target host", &config.host)?;
        let user = c_string("target user", &config.user)?;
        let password = c_string("target password", &config.password)?;
        let database = c_string("target database", &config.database)?;
        let ca_file = c_string("target TLS CA file", &config.tls_ca_file)?;
        let raw = NonNull::new(unsafe { mysqlclient_sys::mysql_init(ptr::null_mut()) });
        let Some(raw) = raw else {
            unsafe { mysqlclient_sys::mysql_thread_end() };
            return Err(TargetExecuteError::new(
                "failed to initialize target MariaDB client",
            ));
        };
        let mut connection = Self { raw };

        connection.configure_timeout(
            TimeoutOption::Connect,
            duration_seconds(DEFAULT_MYSQL_CONNECT_TIMEOUT),
        )?;
        connection.configure_timeout(
            TimeoutOption::Read,
            duration_seconds(DEFAULT_MYSQL_READ_TIMEOUT),
        )?;
        connection.configure_timeout(
            TimeoutOption::Write,
            duration_seconds(DEFAULT_MYSQL_WRITE_TIMEOUT),
        )?;
        connection.configure_tls(&ca_file)?;

        let connected = unsafe {
            mysqlclient_sys::mysql_real_connect(
                connection.raw.as_ptr(),
                host.as_ptr(),
                user.as_ptr(),
                password.as_ptr(),
                database.as_ptr(),
                u32::from(config.port),
                ptr::null(),
                CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS,
            )
        };
        if connected.is_null() {
            return Err(connection.error("connect"));
        }
        connection.require_tls()?;
        connection.set_character_set()?;
        connection.execute_initialization_query(target_session_init_command())?;
        Ok(connection)
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
