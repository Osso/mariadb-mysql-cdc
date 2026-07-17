use super::{DriftCheckError, MySqlConnectionConfig, TargetMySqlConfig};
use crate::checksum::{ChecksumColumn, ChecksumRequest, build_chunk_checksum_sql};
use crate::mysql_support::{SOURCE_TLS_CA_FILE, quote_ident, quote_sql_literal, ssl_opts_from_ca};
use mysql::prelude::{FromRow, Queryable};
use mysql::{Conn, Opts, OptsBuilder, Row, Value};
use std::cell::RefCell;

pub fn build_count_sql(table: &str) -> String {
    format!("SELECT COUNT(*) FROM {}", quote_ident(table))
}

pub fn build_list_tables_sql() -> String {
    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME".to_string()
}

pub fn build_primary_key_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
        quote_sql_literal(table)
    )
}

pub fn build_checksum_columns_sql(table: &str) -> String {
    format!(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {} AND COALESCE(GENERATION_EXPRESSION, '') = '' ORDER BY ORDINAL_POSITION",
        quote_sql_literal(table)
    )
}

pub fn build_json_check_clauses_sql(table: &str) -> String {
    format!(
        "SELECT cc.CHECK_CLAUSE FROM information_schema.TABLE_CONSTRAINTS tc JOIN information_schema.CHECK_CONSTRAINTS cc ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = {} AND tc.CONSTRAINT_TYPE = 'CHECK' AND cc.CHECK_CLAUSE LIKE '%json_valid%'",
        quote_sql_literal(table)
    )
}

pub fn build_primary_key_endpoints_sql(
    table: &str,
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    limit: usize,
) -> Result<String, DriftCheckError> {
    build_primary_key_endpoints_range_sql(table, primary_key, start_after, None, limit)
}

pub fn build_primary_key_endpoints_range_sql(
    table: &str,
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    limit: usize,
) -> Result<String, DriftCheckError> {
    validate_bound_arity(primary_key, start_after.as_ref(), "start_after")?;
    validate_bound_arity(primary_key, end_at.as_ref(), "end_at")?;
    let columns = primary_key
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let order_by = columns.clone();
    let bounds = endpoint_bounds(primary_key, start_after, end_at);
    Ok(format!(
        "SELECT {columns} FROM {}{bounds} ORDER BY {order_by} LIMIT {limit}",
        quote_ident(table)
    ))
}

fn endpoint_bounds(
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> String {
    let mut predicates = Vec::new();
    if let Some(start) = start_after {
        predicates.push(primary_key_bound_predicate(primary_key, &start, ">"));
    }
    if let Some(end) = end_at {
        predicates.push(format!(
            "NOT ({})",
            primary_key_bound_predicate(primary_key, &end, ">")
        ));
    }
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
}

pub(crate) fn query_count(
    config: &QueryConnectionConfig,
    table: &str,
    sql: &str,
) -> Result<Option<u64>, DriftCheckError> {
    match query_scalar::<u64>(config, sql) {
        Ok(Some(count)) => Ok(Some(count)),
        Ok(None) => Err(DriftCheckError::Query(format!(
            "{table} count query returned no rows"
        ))),
        Err(DriftCheckError::Query(message)) if is_missing_table_error(&message) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn is_missing_table_error(message: &str) -> bool {
    (message.contains("ERROR 1146") || message.contains("error 1146"))
        && message.contains("doesn't exist")
}

pub(crate) fn query_primary_key(
    conn: &mut Conn,
    table: &str,
) -> Result<Vec<String>, DriftCheckError> {
    conn.query::<String, _>(build_primary_key_sql(table))
        .map_err(query_error)
}

pub(crate) fn query_checksum_columns(
    conn: &mut Conn,
    table: &str,
) -> Result<Vec<ChecksumColumn>, DriftCheckError> {
    let mut columns = conn
        .query_map(
            build_checksum_columns_sql(table),
            |(name, data_type, column_type): (String, String, String)| ChecksumColumn {
                name,
                data_type,
                column_type,
            },
        )
        .map_err(query_error)?;
    let json_checks = query_json_check_clauses(conn, table)?;
    mark_json_alias_columns(&mut columns, &json_checks);
    Ok(columns)
}

fn query_json_check_clauses(conn: &mut Conn, table: &str) -> Result<Vec<String>, DriftCheckError> {
    conn.query::<String, _>(build_json_check_clauses_sql(table))
        .map_err(query_error)
}

pub(crate) fn mark_json_alias_columns(columns: &mut [ChecksumColumn], check_clauses: &[String]) {
    for column in columns {
        if check_clauses
            .iter()
            .any(|clause| check_clause_references_json_column(clause, &column.name))
        {
            column.data_type = "json".to_string();
        }
    }
}

fn check_clause_references_json_column(clause: &str, column: &str) -> bool {
    let lower_clause = clause.to_ascii_lowercase();
    let lower_column = quote_ident(column).to_ascii_lowercase();
    lower_clause.contains("json_valid") && lower_clause.contains(&lower_column)
}

pub(crate) fn query_primary_key_endpoints(
    conn: &RefCell<Conn>,
    table: &str,
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    limit: usize,
) -> Result<Vec<Vec<String>>, DriftCheckError> {
    query_primary_key_endpoints_in_range(conn, table, primary_key, start_after, None, limit)
}

pub(crate) fn query_primary_key_endpoints_in_range(
    conn: &RefCell<Conn>,
    table: &str,
    primary_key: &[String],
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
    limit: usize,
) -> Result<Vec<Vec<String>>, DriftCheckError> {
    let sql =
        build_primary_key_endpoints_range_sql(table, primary_key, start_after, end_at, limit)?;
    let rows = conn
        .borrow_mut()
        .query::<Row, _>(sql)
        .map_err(query_error)?;
    Ok(rows.into_iter().map(row_to_strings).collect())
}

pub(crate) fn query_chunk_checksum(
    conn: &RefCell<Conn>,
    table: &str,
    primary_key: &[String],
    columns: &[ChecksumColumn],
    start_after: Option<Vec<String>>,
    end_at: Option<Vec<String>>,
) -> Result<(u64, u64), DriftCheckError> {
    let sql = build_chunk_checksum_sql(&ChecksumRequest {
        table: table.to_string(),
        primary_key: primary_key.to_vec(),
        columns: columns.to_vec(),
        start_after,
        end_at,
    })
    .map_err(DriftCheckError::Config)?;
    conn.borrow_mut()
        .query_first::<(u64, u64), _>(sql)
        .map_err(query_error)?
        .ok_or_else(|| DriftCheckError::Query("checksum query returned no rows".to_string()))
}

pub(crate) fn query_list(
    config: &QueryConnectionConfig,
    sql: &str,
) -> Result<Vec<String>, DriftCheckError> {
    let mut conn = open_connection(config)?;
    conn.query::<String, _>(sql).map_err(query_error)
}

fn query_scalar<T>(config: &QueryConnectionConfig, sql: &str) -> Result<Option<T>, DriftCheckError>
where
    T: FromRow,
{
    let mut conn = open_connection(config)?;
    conn.query_first::<T, _>(sql).map_err(query_error)
}

fn row_to_strings(row: Row) -> Vec<String> {
    row.unwrap().into_iter().map(value_to_string).collect()
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::NULL => String::new(),
        Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            format_date(year, month, day, hour, minute, second, micros)
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            format_time(negative, days, hours, minutes, seconds, micros)
        }
    }
}

pub(crate) fn open_connection(config: &QueryConnectionConfig) -> Result<Conn, DriftCheckError> {
    let opts = connection_opts(config).map_err(DriftCheckError::Config)?;
    Conn::new(opts).map_err(query_error)
}

fn query_error(error: mysql::Error) -> DriftCheckError {
    DriftCheckError::Query(error.to_string())
}

pub(crate) fn connection_opts(config: &QueryConnectionConfig) -> Result<Opts, String> {
    let endpoint = format!("{} `{}`:{}", config.endpoint_role, config.host, config.port);
    let builder = OptsBuilder::default()
        .ip_or_hostname(Some(&config.host))
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .db_name(Some(&config.database))
        .prefer_socket(false)
        .ssl_opts(ssl_opts_from_ca(
            &endpoint,
            &config.host,
            &config.tls_ca_file,
        )?);
    Ok(Opts::from(builder))
}

#[derive(Clone, Debug)]
pub(crate) struct QueryConnectionConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) tls_ca_file: String,
    pub(crate) endpoint_role: &'static str,
}

pub(crate) fn source_query_config(config: &MySqlConnectionConfig) -> QueryConnectionConfig {
    QueryConnectionConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.user.clone(),
        password: config.password.clone(),
        database: config.database.clone(),
        tls_ca_file: config
            .tls_ca_file
            .clone()
            .unwrap_or_else(|| SOURCE_TLS_CA_FILE.to_string()),
        endpoint_role: "source",
    }
}

pub(crate) fn target_query_config(target: &TargetMySqlConfig) -> QueryConnectionConfig {
    QueryConnectionConfig {
        host: target.host.clone(),
        port: target.port,
        user: target.user.clone(),
        password: target.password.clone(),
        database: target.database.clone(),
        tls_ca_file: target.tls_ca_file.clone(),
        endpoint_role: "target",
    }
}

fn validate_bound_arity(
    primary_key: &[String],
    values: Option<&Vec<String>>,
    label: &str,
) -> Result<(), DriftCheckError> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.len() != primary_key.len() {
        return Err(DriftCheckError::Config(format!(
            "{label} has {} values for {} primary-key columns",
            values.len(),
            primary_key.len()
        )));
    }
    Ok(())
}

fn primary_key_bound_predicate(columns: &[String], values: &[String], operator: &str) -> String {
    columns
        .iter()
        .enumerate()
        .map(|(index, _column)| primary_key_bound_branch(columns, values, index, operator))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn primary_key_bound_branch(
    columns: &[String],
    values: &[String],
    index: usize,
    operator: &str,
) -> String {
    let mut parts = Vec::new();
    for equal_index in 0..index {
        parts.push(format!(
            "{} = {}",
            quote_ident(&columns[equal_index]),
            quote_sql_literal(&values[equal_index])
        ));
    }
    parts.push(format!(
        "{} {operator} {}",
        quote_ident(&columns[index]),
        quote_sql_literal(&values[index])
    ));
    format!("({})", parts.join(" AND "))
}

fn format_date(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> String {
    if hour == 0 && minute == 0 && second == 0 && micros == 0 {
        format!("{year:04}-{month:02}-{day:02}")
    } else if micros == 0 {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}")
    }
}

fn format_time(
    negative: bool,
    days: u32,
    hours: u8,
    minutes: u8,
    seconds: u8,
    micros: u32,
) -> String {
    let sign = if negative { "-" } else { "" };
    let total_hours = days * 24 + u32::from(hours);
    if micros == 0 {
        format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
    }
}
