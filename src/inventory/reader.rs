use super::model::{
    ColumnRow, EventRow, ForeignKeyRow, IndexRow, InventoryConfig, InventoryError, InventoryReader,
    PrimaryKeyRow, RoutineRow, SchemaDefaults, SourceBinlogSettings, SourceMasterCoordinate,
    TableRow, TableRuntimeMetadata, TriggerRow, ViewRow,
};
use super::parse::{
    parse_canonical_foreign_key_row, parse_column_row, parse_event_row, parse_foreign_key_row,
    parse_index_row, parse_primary_key_row, parse_routine_row, parse_schema_defaults,
    parse_source_master_coordinate, parse_table_row, parse_table_runtime_row, parse_trigger_row,
    parse_view_row,
};
use super::query::{
    canonical_foreign_keys_query, columns_query, events_query, foreign_keys_query, indexes_query,
    primary_keys_query, routines_query, schema_defaults_query, source_master_coordinate_query,
    table_runtime_query, tables_query, triggers_query, views_query,
};
use super::retry::{
    inventory_attempt_error, inventory_retry_error, is_retryable_inventory_error,
    log_inventory_connection_reset,
};
use super::values::row_to_inventory_fields;
use crate::conflict_repair::CanonicalForeignKeyRow;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Row};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

pub(crate) trait InventoryQueryConnection {
    fn query_rows(&mut self, query: &str) -> Result<Vec<Vec<String>>, mysql::Error>;
}

pub(crate) trait InventoryConnectionFactory {
    fn connect(
        &self,
        config: &InventoryConfig,
    ) -> Result<Box<dyn InventoryQueryConnection>, InventoryQueryFailure>;
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
    ) -> Result<Box<dyn InventoryQueryConnection>, InventoryQueryFailure> {
        let opts = inventory_opts(config).map_err(|error| InventoryQueryFailure {
            error,
            retryable: false,
            connection_age: None,
        })?;
        Conn::new(opts)
            .map(|conn| {
                Box::new(MySqlInventoryConnection(conn)) as Box<dyn InventoryQueryConnection>
            })
            .map_err(|error| InventoryQueryFailure {
                retryable: is_retryable_inventory_error(&error),
                error: format!(
                    "{} inventory connection failed: {error}",
                    config.endpoint_role.as_str()
                ),
                connection_age: None,
            })
    }
}

struct InventoryConnectionState {
    connection: Box<dyn InventoryQueryConnection>,
    connected_at: Instant,
}

pub(crate) struct InventoryQueryFailure {
    pub(crate) error: String,
    pub(crate) retryable: bool,
    pub(crate) connection_age: Option<Duration>,
}

#[derive(Clone, Copy)]
pub(crate) enum InventoryQueryStage {
    Tables,
    Columns,
    PrimaryKeys,
    Indexes,
    ForeignKeys,
    CanonicalForeignKeys,
    Views,
    Triggers,
    Routines,
    Events,
    BinlogSettings,
    TableRuntime,
    SchemaDefaults,
    MasterCoordinate,
}

impl InventoryQueryStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Tables => "tables",
            Self::Columns => "columns",
            Self::PrimaryKeys => "primary_keys",
            Self::Indexes => "indexes",
            Self::ForeignKeys => "foreign_keys",
            Self::CanonicalForeignKeys => "canonical_foreign_keys",
            Self::Views => "views",
            Self::Triggers => "triggers",
            Self::Routines => "routines",
            Self::Events => "events",
            Self::BinlogSettings => "binlog_settings",
            Self::TableRuntime => "table_runtime",
            Self::SchemaDefaults => "schema_defaults",
            Self::MasterCoordinate => "master_coordinate",
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

    pub(crate) fn with_factory(
        config: InventoryConfig,
        factory: Rc<dyn InventoryConnectionFactory>,
    ) -> Self {
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
            Err(first_failure) if first_failure.retryable => {
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
                retryable: is_retryable_inventory_error(&error),
                error: error.to_string(),
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

    pub fn read_schema_defaults(&self, schema: &str) -> Result<SchemaDefaults, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::SchemaDefaults,
            schema,
            &schema_defaults_query(schema),
        )?;
        let [row] = rows.as_slice() else {
            return Err(InventoryError::new(format!(
                "expected one schema defaults row for {schema}, found {}",
                rows.len()
            )));
        };
        parse_schema_defaults(row)
    }

    pub fn read_source_master_coordinate(&self) -> Result<SourceMasterCoordinate, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::MasterCoordinate,
            "global",
            source_master_coordinate_query(),
        )?;
        let [row] = rows.as_slice() else {
            return Err(InventoryError::new(format!(
                "expected one source master coordinate row, found {}",
                rows.len()
            )));
        };
        parse_source_master_coordinate(row)
    }

    pub fn read_table_runtime(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<TableRuntimeMetadata, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::TableRuntime,
            schema,
            &table_runtime_query(schema, table),
        )?;
        let [row] = rows.as_slice() else {
            return Err(InventoryError::new(format!(
                "expected one table runtime row for {schema}.{table}, found {}",
                rows.len()
            )));
        };
        parse_table_runtime_row(row)
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
        let connection = self.factory.connect(&self.config)?;
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

    fn read_indexes(&self, schema: &str) -> Result<Vec<IndexRow>, InventoryError> {
        let rows = self.query_rows(InventoryQueryStage::Indexes, schema, &indexes_query(schema))?;
        rows.iter().map(|row| parse_index_row(row)).collect()
    }

    fn read_foreign_keys(&self, schema: &str) -> Result<Vec<ForeignKeyRow>, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::ForeignKeys,
            schema,
            &foreign_keys_query(schema),
        )?;
        rows.iter().map(|row| parse_foreign_key_row(row)).collect()
    }

    fn read_canonical_foreign_keys(
        &self,
        schema: &str,
    ) -> Result<Vec<CanonicalForeignKeyRow>, InventoryError> {
        let rows = self.query_rows(
            InventoryQueryStage::CanonicalForeignKeys,
            schema,
            &canonical_foreign_keys_query(schema),
        )?;
        rows.iter()
            .map(|row| parse_canonical_foreign_key_row(row))
            .collect()
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

pub(crate) fn inventory_opts(config: &InventoryConfig) -> Result<Opts, String> {
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(Some(&config.host))
        .tcp_port(config.port)
        .user(Some(&config.user))
        .pass(Some(&config.password))
        .prefer_socket(false);
    if config.use_tls {
        let ca_file = config.tls_ca_file.as_deref().ok_or_else(|| {
            format!(
                "{} `{}`:{} TLS CA file is required",
                config.endpoint_role.as_str(),
                config.host,
                config.port
            )
        })?;
        let endpoint = format!(
            "{} `{}`:{}",
            config.endpoint_role.as_str(),
            config.host,
            config.port
        );
        builder = builder.ssl_opts(crate::mysql_support::ssl_opts_from_ca(
            &endpoint,
            &config.host,
            ca_file,
        )?);
    }
    Ok(Opts::from(
        crate::mysql_support::apply_mysql_connection_liveness(builder),
    ))
}
