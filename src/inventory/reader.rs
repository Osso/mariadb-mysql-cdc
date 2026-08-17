use super::model::{
    ColumnRow, EventRow, ForeignKeyRow, IndexRow, InventoryConfig, InventoryEndpointRole,
    InventoryError, InventoryReader, PrimaryKeyRow, RoutineRow, SchemaDefaults,
    SourceBinlogSettings, SourceMasterCoordinate, TableRow, TableRuntimeMetadata, TriggerRow,
    ViewRow,
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
use crate::mysql_client::PersistentMySqlSource;
use crate::repair_drift::CanonicalForeignKeyRow;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Row};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

pub(crate) trait InventoryQueryConnection {
    fn query_rows(&mut self, query: &str) -> Result<Vec<Vec<String>>, mysql::Error>;

    /// Runs several statements in one round-trip and returns one row set per statement.
    fn query_result_sets(&mut self, query: &str) -> Result<Vec<Vec<Vec<String>>>, mysql::Error>;
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

    fn query_result_sets(&mut self, query: &str) -> Result<Vec<Vec<Vec<String>>>, mysql::Error> {
        let mut result = self.0.query_iter(query)?;
        let mut sets = Vec::new();
        while let Some(set) = result.iter() {
            let mut rows = Vec::new();
            for row in set {
                rows.push(row_to_inventory_fields(row?));
            }
            sets.push(rows);
        }
        Ok(sets)
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

#[derive(Clone, Copy, Eq, PartialEq)]
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
    /// Restricts every table-scoped query to one table. `None` reads the whole schema.
    table: RefCell<Option<String>>,
    /// One table's row sets, fetched in a single round-trip. Verifying hundreds of tables one
    /// query at a time costs far more in round-trips than in the metadata itself.
    scoped_rows: RefCell<Option<ScopedRows>>,
}

/// Every row set a scoped read needs, in the order the batch requests them.
struct ScopedRows {
    table: String,
    sets: Vec<Vec<Vec<String>>>,
}

/// The stages a scoped batch covers, in request order. Views, triggers, routines, and events
/// are schema-wide and tiny, and they join the batch so a scoped read is one round-trip.
const SCOPED_STAGES: [InventoryQueryStage; 10] = [
    InventoryQueryStage::Tables,
    InventoryQueryStage::Columns,
    InventoryQueryStage::PrimaryKeys,
    InventoryQueryStage::Indexes,
    InventoryQueryStage::ForeignKeys,
    InventoryQueryStage::CanonicalForeignKeys,
    InventoryQueryStage::Views,
    InventoryQueryStage::Triggers,
    InventoryQueryStage::Routines,
    InventoryQueryStage::Events,
];

impl MariaDbInventoryReader {
    pub fn new(config: InventoryConfig) -> Self {
        Self::with_factory(config, Rc::new(MySqlInventoryConnectionFactory))
    }

    /// Restrict later reads to one table, so verifying a table does not read the metadata of
    /// every other table in the schema. Reusing one reader keeps one connection across tables.
    pub fn scope_to_table(&self, table: &str) {
        self.table.replace(Some(table.to_string()));
    }

    pub(crate) fn with_factory(
        config: InventoryConfig,
        factory: Rc<dyn InventoryConnectionFactory>,
    ) -> Self {
        Self {
            config,
            conn: RefCell::new(None),
            factory,
            table: RefCell::new(None),
            scoped_rows: RefCell::new(None),
        }
    }

    /// Rows for one stage of the current table scope, fetching the whole batch on first use.
    fn scoped_stage_rows(
        &self,
        stage: InventoryQueryStage,
        schema: &str,
    ) -> Result<Option<Vec<Vec<String>>>, InventoryError> {
        let Some(table) = self.table.borrow().clone() else {
            return Ok(None);
        };
        let position = SCOPED_STAGES
            .iter()
            .position(|candidate| *candidate == stage);
        let Some(position) = position else {
            return Ok(None);
        };
        let cached = self
            .scoped_rows
            .borrow()
            .as_ref()
            .is_some_and(|rows| rows.table == table);
        if !cached {
            let scope = Some(table.as_str());
            let batch = [
                tables_query(schema, scope),
                columns_query(schema, scope),
                primary_keys_query(schema, scope),
                indexes_query(schema, self.config.endpoint_role, scope),
                foreign_keys_query(schema, scope),
                canonical_foreign_keys_query(schema, scope),
                views_query(schema),
                triggers_query(schema),
                routines_query(schema),
                events_query(schema),
            ]
            .join(";");
            let sets = self.query_result_sets(InventoryQueryStage::Tables, schema, &batch)?;
            if sets.len() != SCOPED_STAGES.len() {
                return Err(InventoryError::new(format!(
                    "scoped inventory batch returned {} row sets, expected {}",
                    sets.len(),
                    SCOPED_STAGES.len()
                )));
            }
            self.scoped_rows.replace(Some(ScopedRows {
                table: table.clone(),
                sets,
            }));
        }
        Ok(self
            .scoped_rows
            .borrow()
            .as_ref()
            .map(|rows| rows.sets[position].clone()))
    }

    fn query_result_sets(
        &self,
        stage: InventoryQueryStage,
        schema: &str,
        query: &str,
    ) -> Result<Vec<Vec<Vec<String>>>, InventoryError> {
        match self.result_sets_once(query) {
            Ok(sets) => Ok(sets),
            Err(first_failure) if first_failure.retryable => {
                self.conn.replace(None);
                log_inventory_connection_reset(stage, schema, &self.config, &first_failure);
                self.result_sets_once(query).map_err(|retry_failure| {
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

    /// Rows for one stage, served from the current table scope's single-round-trip batch when
    /// there is a table scope.
    fn stage_rows(
        &self,
        stage: InventoryQueryStage,
        schema: &str,
        query: impl FnOnce() -> String,
    ) -> Result<Vec<Vec<String>>, InventoryError> {
        if let Some(rows) = self.scoped_stage_rows(stage, schema)? {
            return Ok(rows);
        }
        self.query_rows(stage, schema, &query())
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

    fn result_sets_once(
        &self,
        query: &str,
    ) -> Result<Vec<Vec<Vec<String>>>, InventoryQueryFailure> {
        self.expire_connection();
        self.ensure_connection()?;
        let mut connection = self.conn.borrow_mut();
        let state = connection
            .as_mut()
            .expect("inventory connection initialized");
        let connection_age = state.connected_at.elapsed();
        state
            .connection
            .query_result_sets(query)
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

pub(crate) trait SnapshotInventoryQuery {
    fn query_rows_as_strings(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, String>;
}

impl SnapshotInventoryQuery for PersistentMySqlSource {
    fn query_rows_as_strings(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, String> {
        PersistentMySqlSource::query_rows_as_strings(self, query).map_err(|error| error.to_string())
    }
}

pub(crate) struct SnapshotInventoryReader<'a> {
    source: &'a dyn SnapshotInventoryQuery,
    endpoint_role: InventoryEndpointRole,
}

impl<'a> SnapshotInventoryReader<'a> {
    pub(crate) fn new(
        source: &'a dyn SnapshotInventoryQuery,
        endpoint_role: InventoryEndpointRole,
    ) -> Self {
        Self {
            source,
            endpoint_role,
        }
    }

    fn query_rows(&self, query: String) -> Result<Vec<Vec<String>>, InventoryError> {
        self.source
            .query_rows_as_strings(&query)
            .map_err(|error| {
                InventoryError::new(format!("snapshot inventory query failed: {error}"))
            })
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| value.unwrap_or_default())
                            .collect()
                    })
                    .collect()
            })
    }
}

impl InventoryReader for SnapshotInventoryReader<'_> {
    fn read_tables(&self, schema: &str) -> Result<Vec<TableRow>, InventoryError> {
        self.query_rows(tables_query(schema, None))?
            .iter()
            .map(|row| parse_table_row(row))
            .collect()
    }

    fn read_columns(&self, schema: &str) -> Result<Vec<ColumnRow>, InventoryError> {
        self.query_rows(columns_query(schema, None))?
            .iter()
            .map(|row| parse_column_row(row))
            .collect()
    }

    fn read_primary_keys(&self, schema: &str) -> Result<Vec<PrimaryKeyRow>, InventoryError> {
        self.query_rows(primary_keys_query(schema, None))?
            .iter()
            .map(|row| parse_primary_key_row(row))
            .collect()
    }

    fn read_indexes(&self, schema: &str) -> Result<Vec<IndexRow>, InventoryError> {
        self.query_rows(indexes_query(schema, self.endpoint_role, None))?
            .iter()
            .map(|row| parse_index_row(row))
            .collect()
    }

    fn read_foreign_keys(&self, schema: &str) -> Result<Vec<ForeignKeyRow>, InventoryError> {
        self.query_rows(foreign_keys_query(schema, None))?
            .iter()
            .map(|row| parse_foreign_key_row(row))
            .collect()
    }

    fn read_canonical_foreign_keys(
        &self,
        schema: &str,
    ) -> Result<Vec<CanonicalForeignKeyRow>, InventoryError> {
        self.query_rows(canonical_foreign_keys_query(schema, None))?
            .iter()
            .map(|row| parse_canonical_foreign_key_row(row))
            .collect()
    }

    fn read_views(&self, schema: &str) -> Result<Vec<ViewRow>, InventoryError> {
        self.query_rows(views_query(schema))?
            .iter()
            .map(|row| parse_view_row(row))
            .collect()
    }

    fn read_triggers(&self, schema: &str) -> Result<Vec<TriggerRow>, InventoryError> {
        self.query_rows(triggers_query(schema))?
            .iter()
            .map(|row| parse_trigger_row(row))
            .collect()
    }

    fn read_routines(&self, schema: &str) -> Result<Vec<RoutineRow>, InventoryError> {
        self.query_rows(routines_query(schema))?
            .iter()
            .map(|row| parse_routine_row(row))
            .collect()
    }

    fn read_events(&self, schema: &str) -> Result<Vec<EventRow>, InventoryError> {
        self.query_rows(events_query(schema))?
            .iter()
            .map(|row| parse_event_row(row))
            .collect()
    }
}

impl InventoryReader for MariaDbInventoryReader {
    fn read_tables(&self, schema: &str) -> Result<Vec<TableRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::Tables, schema, || {
            tables_query(schema, self.table.borrow().as_deref())
        })?;
        rows.iter().map(|row| parse_table_row(row)).collect()
    }

    fn read_columns(&self, schema: &str) -> Result<Vec<ColumnRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::Columns, schema, || {
            columns_query(schema, self.table.borrow().as_deref())
        })?;
        rows.iter().map(|row| parse_column_row(row)).collect()
    }

    fn read_primary_keys(&self, schema: &str) -> Result<Vec<PrimaryKeyRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::PrimaryKeys, schema, || {
            primary_keys_query(schema, self.table.borrow().as_deref())
        })?;
        rows.iter().map(|row| parse_primary_key_row(row)).collect()
    }

    fn read_indexes(&self, schema: &str) -> Result<Vec<IndexRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::Indexes, schema, || {
            indexes_query(
                schema,
                self.config.endpoint_role,
                self.table.borrow().as_deref(),
            )
        })?;
        rows.iter().map(|row| parse_index_row(row)).collect()
    }

    fn read_foreign_keys(&self, schema: &str) -> Result<Vec<ForeignKeyRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::ForeignKeys, schema, || {
            foreign_keys_query(schema, self.table.borrow().as_deref())
        })?;
        rows.iter().map(|row| parse_foreign_key_row(row)).collect()
    }

    fn read_canonical_foreign_keys(
        &self,
        schema: &str,
    ) -> Result<Vec<CanonicalForeignKeyRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::CanonicalForeignKeys, schema, || {
            canonical_foreign_keys_query(schema, self.table.borrow().as_deref())
        })?;
        rows.iter()
            .map(|row| parse_canonical_foreign_key_row(row))
            .collect()
    }

    fn read_views(&self, schema: &str) -> Result<Vec<ViewRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::Views, schema, || views_query(schema))?;
        rows.iter().map(|row| parse_view_row(row)).collect()
    }

    fn read_triggers(&self, schema: &str) -> Result<Vec<TriggerRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::Triggers, schema, || {
            triggers_query(schema)
        })?;
        rows.iter().map(|row| parse_trigger_row(row)).collect()
    }

    fn read_routines(&self, schema: &str) -> Result<Vec<RoutineRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::Routines, schema, || {
            routines_query(schema)
        })?;
        rows.iter().map(|row| parse_routine_row(row)).collect()
    }

    fn read_events(&self, schema: &str) -> Result<Vec<EventRow>, InventoryError> {
        let rows = self.stage_rows(InventoryQueryStage::Events, schema, || events_query(schema))?;
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
