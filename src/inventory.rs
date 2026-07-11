use mysql::prelude::Queryable;
use mysql::{Conn, DriverError, Opts, OptsBuilder, Row, SslOpts, Value};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

const BASE_TABLE_TYPE: &str = "BASE TABLE";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SchemaInventory {
    pub schema: String,
    pub tables: Vec<TableInventory>,
    pub views: Vec<ViewInventory>,
    pub triggers: Vec<TriggerInventory>,
    pub routines: Vec<RoutineInventory>,
    pub events: Vec<EventInventory>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TableInventory {
    pub name: String,
    pub table_type: String,
    pub engine: Option<String>,
    pub collation: Option<String>,
    pub primary_key: Vec<String>,
    pub columns: Vec<ColumnInventory>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ColumnInventory {
    pub name: String,
    pub ordinal_position: u32,
    pub column_type: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub default_value: Option<String>,
    pub extra: String,
    pub generated: Option<GeneratedColumn>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GeneratedColumn {
    pub expression: String,
    pub generation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ViewInventory {
    pub name: String,
    pub definition: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TriggerInventory {
    pub name: String,
    pub table: String,
    pub timing: String,
    pub event: String,
    pub statement: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutineInventory {
    pub name: String,
    pub routine_type: String,
    pub definition: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventInventory {
    pub name: String,
    pub status: String,
    pub definition: String,
}

pub trait InventoryReader {
    fn read_tables(&self, schema: &str) -> Result<Vec<TableRow>, InventoryError>;
    fn read_columns(&self, schema: &str) -> Result<Vec<ColumnRow>, InventoryError>;
    fn read_primary_keys(&self, schema: &str) -> Result<Vec<PrimaryKeyRow>, InventoryError>;
    fn read_views(&self, schema: &str) -> Result<Vec<ViewRow>, InventoryError>;
    fn read_triggers(&self, schema: &str) -> Result<Vec<TriggerRow>, InventoryError>;
    fn read_routines(&self, schema: &str) -> Result<Vec<RoutineRow>, InventoryError>;
    fn read_events(&self, schema: &str) -> Result<Vec<EventRow>, InventoryError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryEndpointRole {
    Source,
    Target,
}

impl InventoryEndpointRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Target => "target",
        }
    }
}

#[derive(Clone, Debug)]
pub struct InventoryConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub endpoint_role: InventoryEndpointRole,
    pub use_tls: bool,
    pub tls_ca_file: Option<String>,
    pub max_connection_age: Duration,
}

impl Default for InventoryConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            endpoint_role: InventoryEndpointRole::Source,
            use_tls: false,
            tls_ca_file: None,
            max_connection_age: Duration::from_secs(300),
        }
    }
}

trait InventoryQueryConnection {
    fn query_rows(&mut self, query: &str) -> Result<Vec<Vec<String>>, mysql::Error>;
}

trait InventoryConnectionFactory {
    fn connect(
        &self,
        config: &InventoryConfig,
    ) -> Result<Box<dyn InventoryQueryConnection>, mysql::Error>;
}

struct MySqlInventoryConnection(Conn);

impl InventoryQueryConnection for MySqlInventoryConnection {
    fn query_rows(&mut self, query: &str) -> Result<Vec<Vec<String>>, mysql::Error> {
        let rows = self.0.query::<Row, _>(query)?;
        Ok(rows.into_iter().map(row_to_inventory_fields).collect())
    }
}

struct MySqlInventoryConnectionFactory;

impl InventoryConnectionFactory for MySqlInventoryConnectionFactory {
    fn connect(
        &self,
        config: &InventoryConfig,
    ) -> Result<Box<dyn InventoryQueryConnection>, mysql::Error> {
        Conn::new(inventory_opts(config)).map(|conn| {
            Box::new(MySqlInventoryConnection(conn)) as Box<dyn InventoryQueryConnection>
        })
    }
}

struct InventoryConnectionState {
    connection: Box<dyn InventoryQueryConnection>,
    connected_at: Instant,
}

struct InventoryQueryFailure {
    error: mysql::Error,
    connection_age: Option<Duration>,
}

#[derive(Clone, Copy)]
enum InventoryQueryStage {
    Tables,
    Columns,
    PrimaryKeys,
    Views,
    Triggers,
    Routines,
    Events,
    BinlogSettings,
}

impl InventoryQueryStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tables => "tables",
            Self::Columns => "columns",
            Self::PrimaryKeys => "primary_keys",
            Self::Views => "views",
            Self::Triggers => "triggers",
            Self::Routines => "routines",
            Self::Events => "events",
            Self::BinlogSettings => "binlog_settings",
        }
    }
}

pub struct MariaDbInventoryReader {
    config: InventoryConfig,
    conn: RefCell<Option<InventoryConnectionState>>,
    factory: Rc<dyn InventoryConnectionFactory>,
}

impl MariaDbInventoryReader {
    pub fn new(config: InventoryConfig) -> Self {
        Self::with_factory(config, Rc::new(MySqlInventoryConnectionFactory))
    }

    fn with_factory(config: InventoryConfig, factory: Rc<dyn InventoryConnectionFactory>) -> Self {
        Self {
            config,
            conn: RefCell::new(None),
            factory,
        }
    }

    fn query_rows(
        &self,
        stage: InventoryQueryStage,
        schema: &str,
        query: &str,
    ) -> Result<Vec<Vec<String>>, InventoryError> {
        match self.query_once(query) {
            Ok(rows) => Ok(rows),
            Err(first_failure) if is_retryable_inventory_error(&first_failure.error) => {
                self.conn.replace(None);
                log_inventory_connection_reset(stage, schema, &self.config, &first_failure);
                self.query_once(query).map_err(|retry_failure| {
                    inventory_retry_error(stage, schema, &self.config, first_failure, retry_failure)
                })
            }
            Err(failure) => Err(inventory_attempt_error(
                stage,
                schema,
                &self.config,
                failure,
            )),
        }
    }

    fn query_once(&self, query: &str) -> Result<Vec<Vec<String>>, InventoryQueryFailure> {
        self.expire_connection();
        self.ensure_connection()?;
        let mut connection = self.conn.borrow_mut();
        let state = connection
            .as_mut()
            .expect("inventory connection initialized");
        let connection_age = state.connected_at.elapsed();
        state
            .connection
            .query_rows(query)
            .map_err(|error| InventoryQueryFailure {
                error,
                connection_age: Some(connection_age),
            })
    }

    fn expire_connection(&self) {
        let expired =
            self.conn.borrow().as_ref().is_some_and(|state| {
                state.connected_at.elapsed() >= self.config.max_connection_age
            });
        if expired {
            self.conn.replace(None);
        }
    }

    pub fn read_source_binlog_settings(&self) -> Result<SourceBinlogSettings, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::BinlogSettings,
            "global",
            "SELECT @@global.binlog_format, @@global.binlog_row_image",
        )?;
        let [row] = rows.as_slice() else {
            return Err(InventoryError::new(format!(
                "expected one source binlog settings row, found {}",
                rows.len()
            )));
        };
        let [format, row_image] = row.as_slice() else {
            return Err(InventoryError::new(format!(
                "expected source binlog format and row image, found {} columns",
                row.len()
            )));
        };
        Ok(SourceBinlogSettings {
            format: format.clone(),
            row_image: row_image.clone(),
        })
    }

    fn ensure_connection(&self) -> Result<(), InventoryQueryFailure> {
        if self.conn.borrow().is_some() {
            return Ok(());
        }
        let connection =
            self.factory
                .connect(&self.config)
                .map_err(|error| InventoryQueryFailure {
                    error,
                    connection_age: None,
                })?;
        self.conn.replace(Some(InventoryConnectionState {
            connection,
            connected_at: Instant::now(),
        }));
        Ok(())
    }
}

impl InventoryReader for MariaDbInventoryReader {
    fn read_tables(&self, schema: &str) -> Result<Vec<TableRow>, InventoryError> {
        let rows = self.query_rows(InventoryQueryStage::Tables, schema, &tables_query(schema))?;
        rows.iter().map(|row| parse_table_row(row)).collect()
    }

    fn read_columns(&self, schema: &str) -> Result<Vec<ColumnRow>, InventoryError> {
        let rows = self.query_rows(InventoryQueryStage::Columns, schema, &columns_query(schema))?;
        rows.iter().map(|row| parse_column_row(row)).collect()
    }

    fn read_primary_keys(&self, schema: &str) -> Result<Vec<PrimaryKeyRow>, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::PrimaryKeys,
            schema,
            &primary_keys_query(schema),
        )?;
        rows.iter().map(|row| parse_primary_key_row(row)).collect()
    }

    fn read_views(&self, schema: &str) -> Result<Vec<ViewRow>, InventoryError> {
        let rows = self.query_rows(InventoryQueryStage::Views, schema, &views_query(schema))?;
        rows.iter().map(|row| parse_view_row(row)).collect()
    }

    fn read_triggers(&self, schema: &str) -> Result<Vec<TriggerRow>, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::Triggers,
            schema,
            &triggers_query(schema),
        )?;
        rows.iter().map(|row| parse_trigger_row(row)).collect()
    }

    fn read_routines(&self, schema: &str) -> Result<Vec<RoutineRow>, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::Routines,
            schema,
            &routines_query(schema),
        )?;
        rows.iter().map(|row| parse_routine_row(row)).collect()
    }

    fn read_events(&self, schema: &str) -> Result<Vec<EventRow>, InventoryError> {
        let rows = self.query_rows(InventoryQueryStage::Events, schema, &events_query(schema))?;
        rows.iter().map(|row| parse_event_row(row)).collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceBinlogSettings {
    pub format: String,
    pub row_image: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableRow {
    pub table_name: String,
    pub table_type: String,
    pub engine: Option<String>,
    pub table_collation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnRow {
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: u32,
    pub column_type: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
    pub extra: String,
    pub generation_expression: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryKeyRow {
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewRow {
    pub table_name: String,
    pub view_definition: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TriggerRow {
    pub trigger_name: String,
    pub event_manipulation: String,
    pub action_timing: String,
    pub event_object_table: String,
    pub action_statement: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutineRow {
    pub routine_name: String,
    pub routine_type: String,
    pub routine_definition: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventRow {
    pub event_name: String,
    pub status: String,
    pub event_definition: String,
}

#[derive(Debug)]
pub struct InventoryError {
    message: String,
}

impl InventoryError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for InventoryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for InventoryError {}

pub fn build_inventory(
    schema: &str,
    reader: &impl InventoryReader,
) -> Result<SchemaInventory, InventoryError> {
    let tables = reader.read_tables(schema)?;
    let columns = group_columns(reader.read_columns(schema)?);
    let primary_keys = group_primary_keys(reader.read_primary_keys(schema)?);

    Ok(SchemaInventory {
        schema: schema.to_string(),
        tables: build_tables(tables, columns, primary_keys),
        views: build_views(reader.read_views(schema)?),
        triggers: build_triggers(reader.read_triggers(schema)?),
        routines: build_routines(reader.read_routines(schema)?),
        events: build_events(reader.read_events(schema)?),
    })
}

fn inventory_opts(config: &InventoryConfig) -> Opts {
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(Some(&config.host))
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .prefer_socket(false);
    if config.use_tls {
        let mut ssl = SslOpts::default();
        if let Some(ca_file) = &config.tls_ca_file {
            ssl = ssl
                .with_root_cert_path(Some(PathBuf::from(ca_file)))
                .with_danger_skip_domain_validation(true);
        }
        builder = builder.ssl_opts(ssl);
    }
    Opts::from(builder)
}

fn row_to_inventory_fields(row: Row) -> Vec<String> {
    row.unwrap()
        .into_iter()
        .map(inventory_value_to_string)
        .collect()
}

fn inventory_value_to_string(value: Value) -> String {
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

fn is_retryable_inventory_error(error: &mysql::Error) -> bool {
    match error {
        mysql::Error::IoError(_) | mysql::Error::CodecError(_) | mysql::Error::TlsError(_) => true,
        mysql::Error::DriverError(driver_error) => matches!(
            driver_error,
            DriverError::ConnectTimeout
                | DriverError::CouldNotConnect(_)
                | DriverError::PacketOutOfSync
                | DriverError::UnexpectedPacket
                | DriverError::SetupError
                | DriverError::Timeout
        ),
        _ => false,
    }
}

fn log_inventory_connection_reset(
    stage: InventoryQueryStage,
    schema: &str,
    config: &InventoryConfig,
    failure: &InventoryQueryFailure,
) {
    eprintln!(
        "{}",
        format_inventory_reset_log(stage, schema, config, failure)
    );
}

fn format_inventory_reset_log(
    stage: InventoryQueryStage,
    schema: &str,
    config: &InventoryConfig,
    failure: &InventoryQueryFailure,
) -> String {
    format!(
        "cdc_inventory_connection_reset role={} stage={} schema={} attempt=1/2 tls={} reset=true connection_age_ms={} error={}",
        config.endpoint_role.as_str(),
        stage.as_str(),
        schema,
        config.use_tls,
        format_connection_age(failure.connection_age),
        failure.error,
    )
}

fn inventory_attempt_error(
    stage: InventoryQueryStage,
    schema: &str,
    config: &InventoryConfig,
    failure: InventoryQueryFailure,
) -> InventoryError {
    InventoryError::new(format!(
        "inventory query failed role={} stage={} schema={} attempt=1/2 tls={} reset=false connection_age_ms={} error={}",
        config.endpoint_role.as_str(),
        stage.as_str(),
        schema,
        config.use_tls,
        format_connection_age(failure.connection_age),
        failure.error,
    ))
}

fn inventory_retry_error(
    stage: InventoryQueryStage,
    schema: &str,
    config: &InventoryConfig,
    first_failure: InventoryQueryFailure,
    retry_failure: InventoryQueryFailure,
) -> InventoryError {
    InventoryError::new(format!(
        "inventory query failed role={} stage={} schema={} attempt=2/2 tls={} reset=true connection_age_ms={} original_error={} retry_error={}",
        config.endpoint_role.as_str(),
        stage.as_str(),
        schema,
        config.use_tls,
        format_connection_age(retry_failure.connection_age),
        first_failure.error,
        retry_failure.error,
    ))
}

fn format_connection_age(age: Option<Duration>) -> String {
    age.map(|duration| duration.as_millis().to_string())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn parse_table_row(fields: &[String]) -> Result<TableRow, InventoryError> {
    require_len(fields, 4, "table")?;

    Ok(TableRow {
        table_name: fields[0].clone(),
        table_type: fields[1].clone(),
        engine: optional_string(&fields[2]),
        table_collation: optional_string(&fields[3]),
    })
}

fn parse_column_row(fields: &[String]) -> Result<ColumnRow, InventoryError> {
    require_len(fields, 9, "column")?;

    Ok(ColumnRow {
        table_name: fields[0].clone(),
        column_name: fields[1].clone(),
        ordinal_position: parse_u32(&fields[2], "column ordinal")?,
        column_type: fields[3].clone(),
        data_type: fields[4].clone(),
        is_nullable: fields[5] == "YES",
        column_default: optional_string(&fields[6]),
        extra: fields[7].clone(),
        generation_expression: optional_string(&fields[8]),
    })
}

fn parse_primary_key_row(fields: &[String]) -> Result<PrimaryKeyRow, InventoryError> {
    require_len(fields, 3, "primary key")?;

    Ok(PrimaryKeyRow {
        table_name: fields[0].clone(),
        column_name: fields[1].clone(),
        ordinal_position: parse_u32(&fields[2], "primary key ordinal")?,
    })
}

fn parse_view_row(fields: &[String]) -> Result<ViewRow, InventoryError> {
    require_len(fields, 2, "view")?;

    Ok(ViewRow {
        table_name: fields[0].clone(),
        view_definition: fields[1].clone(),
    })
}

fn parse_trigger_row(fields: &[String]) -> Result<TriggerRow, InventoryError> {
    require_len(fields, 5, "trigger")?;

    Ok(TriggerRow {
        trigger_name: fields[0].clone(),
        event_manipulation: fields[1].clone(),
        action_timing: fields[2].clone(),
        event_object_table: fields[3].clone(),
        action_statement: fields[4].clone(),
    })
}

fn parse_routine_row(fields: &[String]) -> Result<RoutineRow, InventoryError> {
    require_len(fields, 3, "routine")?;

    Ok(RoutineRow {
        routine_name: fields[0].clone(),
        routine_type: fields[1].clone(),
        routine_definition: optional_string(&fields[2]),
    })
}

fn parse_event_row(fields: &[String]) -> Result<EventRow, InventoryError> {
    require_len(fields, 3, "event")?;

    Ok(EventRow {
        event_name: fields[0].clone(),
        status: fields[1].clone(),
        event_definition: fields[2].clone(),
    })
}

fn require_len(fields: &[String], expected: usize, row_type: &str) -> Result<(), InventoryError> {
    if fields.len() == expected {
        Ok(())
    } else {
        Err(InventoryError::new(format!(
            "{row_type} row has {} fields, expected {expected}",
            fields.len()
        )))
    }
}

fn optional_string(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn parse_u32(value: &str, field_name: &str) -> Result<u32, InventoryError> {
    value
        .parse()
        .map_err(|_| InventoryError::new(format!("{field_name} is not numeric: {value}")))
}

fn tables_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME",
        quote_sql_string(schema)
    )
}

fn columns_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME, ORDINAL_POSITION",
        quote_sql_string(schema)
    )
}

fn primary_keys_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = {} AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY TABLE_NAME, ORDINAL_POSITION",
        quote_sql_string(schema)
    )
}

fn views_query(schema: &str) -> String {
    format!(
        "SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME",
        quote_sql_string(schema)
    )
}

fn triggers_query(schema: &str) -> String {
    format!(
        "SELECT TRIGGER_NAME, EVENT_MANIPULATION, ACTION_TIMING, EVENT_OBJECT_TABLE, ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = {} ORDER BY TRIGGER_NAME",
        quote_sql_string(schema)
    )
}

fn routines_query(schema: &str) -> String {
    format!(
        "SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = {} ORDER BY ROUTINE_NAME",
        quote_sql_string(schema)
    )
}

fn events_query(schema: &str) -> String {
    format!(
        "SELECT EVENT_NAME, STATUS, EVENT_DEFINITION FROM information_schema.EVENTS WHERE EVENT_SCHEMA = {} ORDER BY EVENT_NAME",
        quote_sql_string(schema)
    )
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn build_tables(
    table_rows: Vec<TableRow>,
    columns: BTreeMap<String, Vec<ColumnInventory>>,
    primary_keys: BTreeMap<String, Vec<String>>,
) -> Vec<TableInventory> {
    table_rows
        .into_iter()
        .filter(|row| row.table_type == BASE_TABLE_TYPE)
        .map(|row| {
            let table_columns = columns.get(&row.table_name).cloned().unwrap_or_default();
            let primary_key = primary_keys
                .get(&row.table_name)
                .cloned()
                .unwrap_or_default();

            TableInventory {
                name: row.table_name,
                table_type: row.table_type,
                engine: row.engine,
                collation: row.table_collation,
                primary_key,
                columns: table_columns,
            }
        })
        .collect()
}

fn group_columns(rows: Vec<ColumnRow>) -> BTreeMap<String, Vec<ColumnInventory>> {
    let mut columns_by_table: BTreeMap<String, Vec<ColumnInventory>> = BTreeMap::new();

    for row in rows {
        columns_by_table
            .entry(row.table_name.clone())
            .or_default()
            .push(build_column(row));
    }

    for columns in columns_by_table.values_mut() {
        columns.sort_by_key(|column| column.ordinal_position);
    }

    columns_by_table
}

fn build_column(row: ColumnRow) -> ColumnInventory {
    let generated = build_generated_column(&row);

    ColumnInventory {
        name: row.column_name,
        ordinal_position: row.ordinal_position,
        column_type: row.column_type,
        data_type: row.data_type,
        is_nullable: row.is_nullable,
        default_value: row.column_default,
        extra: row.extra.clone(),
        generated,
    }
}

fn build_generated_column(row: &ColumnRow) -> Option<GeneratedColumn> {
    let expression = row.generation_expression.as_ref()?;

    Some(GeneratedColumn {
        expression: expression.clone(),
        generation_kind: generation_kind(&row.extra).to_string(),
    })
}

fn generation_kind(extra: &str) -> &'static str {
    if extra.to_ascii_uppercase().contains("STORED") {
        "STORED"
    } else {
        "VIRTUAL"
    }
}

fn group_primary_keys(rows: Vec<PrimaryKeyRow>) -> BTreeMap<String, Vec<String>> {
    let mut rows_by_table: BTreeMap<String, Vec<PrimaryKeyRow>> = BTreeMap::new();

    for row in rows {
        rows_by_table
            .entry(row.table_name.clone())
            .or_default()
            .push(row);
    }

    rows_by_table
        .into_iter()
        .map(|(table, mut rows)| {
            rows.sort_by_key(|row| row.ordinal_position);
            let columns = rows.into_iter().map(|row| row.column_name).collect();
            (table, columns)
        })
        .collect()
}

fn build_views(rows: Vec<ViewRow>) -> Vec<ViewInventory> {
    rows.into_iter()
        .map(|row| ViewInventory {
            name: row.table_name,
            definition: row.view_definition,
        })
        .collect()
}

fn build_triggers(rows: Vec<TriggerRow>) -> Vec<TriggerInventory> {
    rows.into_iter()
        .map(|row| TriggerInventory {
            name: row.trigger_name,
            table: row.event_object_table,
            timing: row.action_timing,
            event: row.event_manipulation,
            statement: row.action_statement,
        })
        .collect()
}

fn build_routines(rows: Vec<RoutineRow>) -> Vec<RoutineInventory> {
    rows.into_iter()
        .map(|row| RoutineInventory {
            name: row.routine_name,
            routine_type: row.routine_type,
            definition: row.routine_definition,
        })
        .collect()
}

fn build_events(rows: Vec<EventRow>) -> Vec<EventInventory> {
    rows.into_iter()
        .map(|row| EventInventory {
            name: row.event_name,
            status: row.status,
            definition: row.event_definition,
        })
        .collect()
}

#[cfg(test)]
mod tests;
