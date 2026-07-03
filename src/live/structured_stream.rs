use super::{
    ApplyBinlogConfig, ApplyBinlogError, QuarantineRecorder, RecordingQuarantine,
    SourceBinlogConfig,
};
use crate::inventory::{InventoryConfig, MariaDbInventoryReader, SchemaInventory, build_inventory};
use crate::probe::BinlogCoordinate;
use crate::row::{DeleteRowsEvent, RowApplier, RowImage, RowTableMap, RowUpdate, TableMapEvent};
use crate::statement::{StatementApplier, StatementEvent, StatementOutcome};
use crate::target::TargetExecutor;
use mysql::Value;
use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::errors::Error as MysqlCdcError;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::events::intvar_event::IntVarEvent;
use mysql_cdc::events::row_events::mysql_value::{Date, DateTime, MySqlValue, Time};
use mysql_cdc::events::row_events::row_data::RowData;
use mysql_cdc::events::table_map_event::TableMapEvent as MysqlCdcTableMapEvent;
use mysql_cdc::events::uservar_event::UserVarEvent;
use mysql_cdc::metadata::table_metadata::TableMetadata;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::thread;
use std::time::Duration;

use super::reconnect::{StreamCheckpointStore, run_stream_reconnect_loop};

const DEFAULT_REPLICA_SERVER_ID: u32 = 65_535;
const MYSQL_CDC_HEARTBEAT_SECONDS: u64 = 30;
const MYSQL_COLUMN_TYPE_ENUM: u8 = 247;
const MILLIS_PER_SECOND: u64 = 1_000;
const SECONDS_PER_DAY: i64 = 86_400;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StructuredEventOutcome {
    pub policy: EventPolicy,
    pub resume_coordinate: Option<BinlogCoordinate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EventPolicy {
    Ignore,
    IgnoreAnnotation,
    ApplyTableMap,
    ApplyRows,
    CommitTransaction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StructuredEventState {
    source_database: Option<String>,
    ignored_table_ids: BTreeSet<u64>,
    pending_intvars: Vec<PendingIntVar>,
    pending_uservars: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingIntVar {
    intvar_type: u8,
    value: u64,
}

impl StructuredEventState {
    fn new(source_database: Option<String>) -> Self {
        Self {
            source_database,
            ignored_table_ids: BTreeSet::new(),
            pending_intvars: Vec::new(),
            pending_uservars: Vec::new(),
        }
    }

    fn should_apply_schema(&self, schema: &str) -> bool {
        self.source_database
            .as_ref()
            .is_none_or(|source_database| source_database == schema)
    }

    fn ignore_table_id(&mut self, table_id: u64) {
        self.ignored_table_ids.insert(table_id);
    }

    fn apply_table_id(&mut self, table_id: u64) {
        self.ignored_table_ids.remove(&table_id);
    }

    fn is_ignored_table_id(&self, table_id: u64) -> bool {
        self.ignored_table_ids.contains(&table_id)
    }

    fn record_intvar(&mut self, event: &IntVarEvent) {
        self.pending_intvars.push(PendingIntVar {
            intvar_type: event.intvar_type,
            value: event.value,
        });
    }

    fn record_uservar(&mut self, event: &UserVarEvent) {
        self.pending_uservars.push(event.name.clone());
    }

    fn clear_query_context(&mut self) {
        self.pending_intvars.clear();
        self.pending_uservars.clear();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedTableSchema {
    columns: Vec<String>,
    primary_key: Vec<String>,
    generated_columns: Vec<String>,
    signed_columns: Vec<String>,
    enum_columns: BTreeMap<String, Vec<String>>,
}

trait TableSchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError>;
}

struct SourceInventorySchemaResolver {
    reader: MariaDbInventoryReader,
    inventories: RefCell<BTreeMap<String, SchemaInventory>>,
}

impl SourceInventorySchemaResolver {
    fn new(config: &ApplyBinlogConfig) -> Self {
        Self {
            reader: MariaDbInventoryReader::new(source_inventory_config(config)),
            inventories: RefCell::new(BTreeMap::new()),
        }
    }
}

impl TableSchemaResolver for SourceInventorySchemaResolver {
    fn resolve_table_schema(
        &self,
        schema: &str,
        table: &str,
        column_count: usize,
    ) -> Result<ResolvedTableSchema, ApplyBinlogError> {
        self.ensure_schema_inventory(schema)?;
        let inventories = self.inventories.borrow();
        let inventory = inventories.get(schema).ok_or_else(|| {
            mapping_error(format!("source schema {schema} inventory was not cached"))
        })?;
        let table = inventory
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| mapping_error(format!("source table {schema}.{table} was not found")))?;
        let columns = table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let generated_columns = table
            .columns
            .iter()
            .filter(|column| column.generated.is_some())
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let signed_columns = table
            .columns
            .iter()
            .filter(|column| is_signed_integer_column(&column.data_type, &column.column_type))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        validate_column_count(schema, &table.name, column_count, &columns)?;
        Ok(ResolvedTableSchema {
            columns,
            primary_key: table.primary_key.clone(),
            generated_columns,
            signed_columns,
            enum_columns: BTreeMap::new(),
        })
    }
}

impl SourceInventorySchemaResolver {
    fn ensure_schema_inventory(&self, schema: &str) -> Result<(), ApplyBinlogError> {
        if self.inventories.borrow().contains_key(schema) {
            return Ok(());
        }

        let inventory = build_inventory(schema, &self.reader).map_err(|error| {
            mapping_error(format!("failed to read source schema {schema}: {error}"))
        })?;
        self.inventories
            .borrow_mut()
            .insert(schema.to_string(), inventory);
        Ok(())
    }
}

pub(super) fn stream_remote_binlog(config: &ApplyBinlogConfig) -> Result<(), ApplyBinlogError> {
    match &config.checkpoint_file {
        Some(path) => {
            let checkpoint_store = crate::checkpoint::FileCheckpointStore::new(path);
            stream_with_checkpoint_store(config, Some(&checkpoint_store))
        }
        None => {
            let checkpoint_store = crate::stream_checkpoint::MySqlStreamCheckpointStore::new(
                config.mariadb.clone(),
                config.target.clone(),
                config.checkpoint_table.clone(),
            );
            stream_with_checkpoint_store(config, Some(&checkpoint_store))
        }
    }
}

fn stream_with_checkpoint_store<C>(
    config: &ApplyBinlogConfig,
    checkpoint_store: Option<&C>,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    run_stream_reconnect_loop(
        config,
        checkpoint_store,
        |attempt_config| stream_once(attempt_config, checkpoint_store),
        thread::sleep,
    )
}

fn stream_once(
    config: &ApplyBinlogConfig,
    checkpoint_store: Option<&impl StreamCheckpointStore>,
) -> Result<(), ApplyBinlogError> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new(&config.target)
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    let mut applier = RowApplier::new(executor);
    let schema_resolver = SourceInventorySchemaResolver::new(config);
    let mut client = BinlogClient::new(replica_options_from_source(&config.source)?);
    let mut events = client.replicate().map_err(source_error)?;
    let mut current_file = config.source.binlog_file.clone();
    let mut state = StructuredEventState::new(config.source.database.clone());

    for result in &mut events {
        let (header, event) = result.map_err(source_error)?;
        let outcome = handle_structured_event(
            &mut applier,
            &schema_resolver,
            &mut state,
            &current_file,
            &header,
            &event,
        )?;
        save_outcome_checkpoint(checkpoint_store, &mut current_file, &event, &outcome)?;
        client.commit(&header, &event);
    }

    Err(stream_ended_error())
}

fn stream_ended_error() -> ApplyBinlogError {
    ApplyBinlogError::SourceCommand("mysql_cdc binlog stream ended at EOF".to_string())
}

fn save_outcome_checkpoint<C>(
    checkpoint_store: Option<&C>,
    current_file: &mut String,
    event: &BinlogEvent,
    outcome: &StructuredEventOutcome,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
{
    let Some(coordinate) = &outcome.resume_coordinate else {
        return Ok(());
    };

    super::reconnect::save_coordinate_checkpoint(checkpoint_store, coordinate, event_name(event))?;
    *current_file = coordinate.file.clone();
    Ok(())
}

fn handle_structured_event<E, R>(
    applier: &mut RowApplier<E>,
    schema_resolver: &R,
    state: &mut StructuredEventState,
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Result<StructuredEventOutcome, ApplyBinlogError>
where
    E: TargetExecutor,
    R: TableSchemaResolver,
{
    let coordinate = event_coordinate(current_file, header, event);
    let policy = apply_structured_event(applier, schema_resolver, state, &coordinate, event)?;
    Ok(StructuredEventOutcome {
        policy,
        resume_coordinate: resume_coordinate(current_file, header, event),
    })
}

fn apply_structured_event<E, R>(
    applier: &mut RowApplier<E>,
    schema_resolver: &R,
    state: &mut StructuredEventState,
    coordinate: &BinlogCoordinate,
    event: &BinlogEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
    R: TableSchemaResolver,
{
    match event {
        BinlogEvent::TableMapEvent(table_map) => {
            apply_table_map_event(applier, schema_resolver, state, coordinate, table_map)
        }
        BinlogEvent::WriteRowsEvent(rows) => {
            apply_write_rows_event(applier, state, coordinate, rows)
        }
        BinlogEvent::UpdateRowsEvent(rows) => {
            apply_update_rows_event(applier, state, coordinate, rows)
        }
        BinlogEvent::DeleteRowsEvent(rows) => {
            apply_delete_rows_event(applier, state, coordinate, rows)
        }
        BinlogEvent::XidEvent(_) => Ok(EventPolicy::CommitTransaction),
        BinlogEvent::IntVarEvent(event) => {
            state.record_intvar(event);
            Ok(EventPolicy::Ignore)
        }
        BinlogEvent::UserVarEvent(event) => {
            state.record_uservar(event);
            Ok(EventPolicy::Ignore)
        }
        BinlogEvent::QueryEvent(query) => apply_query_event(applier, state, coordinate, query),
        BinlogEvent::RowsQueryEvent(_) => Ok(EventPolicy::IgnoreAnnotation),
        _ => Ok(EventPolicy::Ignore),
    }
}

fn apply_query_event<E>(
    applier: &RowApplier<E>,
    state: &mut StructuredEventState,
    coordinate: &BinlogCoordinate,
    query: &mysql_cdc::events::query_event::QueryEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    if !state.should_apply_schema(&query.database_name) {
        state.clear_query_context();
        return Ok(EventPolicy::Ignore);
    }

    reject_ambiguous_query_database(&query.sql_statement)?;
    apply_query_context(applier.executor(), state)?;

    let event = StatementEvent {
        coordinate: coordinate.clone(),
        resume_position: coordinate.position,
        default_database: Some(query.database_name.clone()),
        sql: query.sql_statement.clone(),
    };
    let statement_applier =
        StatementApplier::new(applier.executor(), RecordingQuarantine::default());

    let result = match statement_applier.apply(&event) {
        Ok(StatementOutcome::Replayed) => Ok(EventPolicy::ApplyRows),
        Ok(StatementOutcome::Quarantined(_)) => Err(ApplyBinlogError::Quarantined(
            statement_applier
                .quarantine_recorder()
                .recorded_statements(),
        )),
        Err(error) => Err(ApplyBinlogError::Statement(error.to_string())),
    };
    state.clear_query_context();
    result
}

fn apply_query_context<E>(
    executor: &E,
    state: &StructuredEventState,
) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
{
    if !state.pending_uservars.is_empty() {
        return Err(mapping_error(format!(
            "cannot replay QueryEvent with user variables: {}",
            state.pending_uservars.join(", ")
        )));
    }

    for intvar in &state.pending_intvars {
        apply_intvar(executor, intvar)?;
    }
    Ok(())
}

fn apply_intvar<E>(executor: &E, intvar: &PendingIntVar) -> Result<(), ApplyBinlogError>
where
    E: TargetExecutor,
{
    const INSERT_ID: u8 = 2;
    if intvar.intvar_type != INSERT_ID {
        return Err(mapping_error(format!(
            "cannot replay unsupported IntVarEvent type {}",
            intvar.intvar_type
        )));
    }

    executor
        .execute(&crate::target::SqlStatement {
            sql: "SET INSERT_ID = ?".to_string(),
            params: vec![Value::UInt(intvar.value)],
        })
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))
}

fn reject_ambiguous_query_database(sql: &str) -> Result<(), ApplyBinlogError> {
    if query_contains_qualified_identifier(sql) {
        return Err(mapping_error(format!(
            "cannot replay QueryEvent with qualified identifier: {}",
            sql.chars().take(120).collect::<String>()
        )));
    }
    Ok(())
}

fn query_contains_qualified_identifier(sql: &str) -> bool {
    sql.contains("`.`")
}

fn apply_table_map_event<E, R>(
    applier: &mut RowApplier<E>,
    schema_resolver: &R,
    state: &mut StructuredEventState,
    coordinate: &BinlogCoordinate,
    table_map: &MysqlCdcTableMapEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
    R: TableSchemaResolver,
{
    if !state.should_apply_schema(&table_map.database_name) {
        state.ignore_table_id(table_map.table_id);
        return Ok(EventPolicy::Ignore);
    }

    state.apply_table_id(table_map.table_id);
    let event = map_table_map_event(coordinate, table_map, schema_resolver)?;
    applier.apply_table_map(event);
    Ok(EventPolicy::ApplyTableMap)
}

fn apply_write_rows_event<E>(
    applier: &mut RowApplier<E>,
    state: &StructuredEventState,
    coordinate: &BinlogCoordinate,
    rows: &mysql_cdc::events::row_events::write_rows_event::WriteRowsEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    if state.is_ignored_table_id(rows.table_id) {
        return Ok(EventPolicy::Ignore);
    }
    require_full_row_image(&rows.columns_present, "write")?;
    let table = row_event_table_map(applier, rows.table_id, coordinate)?;
    let event = crate::row::WriteRowsEvent {
        coordinate: coordinate.clone(),
        table_id: rows.table_id,
        rows: map_row_data_list(&rows.rows, &table)?,
    };
    applier
        .apply_write_rows(&event)
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    Ok(EventPolicy::ApplyRows)
}

fn apply_update_rows_event<E>(
    applier: &mut RowApplier<E>,
    state: &StructuredEventState,
    coordinate: &BinlogCoordinate,
    rows: &mysql_cdc::events::row_events::update_rows_event::UpdateRowsEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    if state.is_ignored_table_id(rows.table_id) {
        return Ok(EventPolicy::Ignore);
    }
    require_full_row_image(&rows.columns_before_update, "update before")?;
    require_full_row_image(&rows.columns_after_update, "update after")?;
    let table = row_event_table_map(applier, rows.table_id, coordinate)?;
    let event = crate::row::UpdateRowsEvent {
        coordinate: coordinate.clone(),
        table_id: rows.table_id,
        rows: rows
            .rows
            .iter()
            .map(|row| map_update_row_data(row, &table))
            .collect::<Result<Vec<_>, _>>()?,
    };
    applier
        .apply_update_rows(&event)
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    Ok(EventPolicy::ApplyRows)
}

fn apply_delete_rows_event<E>(
    applier: &mut RowApplier<E>,
    state: &StructuredEventState,
    coordinate: &BinlogCoordinate,
    rows: &mysql_cdc::events::row_events::delete_rows_event::DeleteRowsEvent,
) -> Result<EventPolicy, ApplyBinlogError>
where
    E: TargetExecutor,
{
    if state.is_ignored_table_id(rows.table_id) {
        return Ok(EventPolicy::Ignore);
    }
    require_full_row_image(&rows.columns_present, "delete")?;
    let table = row_event_table_map(applier, rows.table_id, coordinate)?;
    let event = DeleteRowsEvent {
        coordinate: coordinate.clone(),
        table_id: rows.table_id,
        rows: map_row_data_list(&rows.rows, &table)?,
    };
    applier
        .apply_delete_rows(&event)
        .map_err(|error| ApplyBinlogError::Target(error.to_string()))?;
    Ok(EventPolicy::ApplyRows)
}

pub(crate) fn replica_options_from_source(
    source: &SourceBinlogConfig,
) -> Result<ReplicaOptions, ApplyBinlogError> {
    let server_id = source
        .stop_never_slave_server_id
        .unwrap_or(DEFAULT_REPLICA_SERVER_ID);

    Ok(ReplicaOptions {
        port: source.port,
        hostname: source.host.clone(),
        ssl_mode: SslMode::Disabled,
        username: source.user.clone(),
        password: source.password.clone(),
        database: source.database.clone(),
        server_id,
        blocking: true,
        heartbeat_interval: Duration::from_secs(MYSQL_CDC_HEARTBEAT_SECONDS),
        binlog: binlog_options_from_source_position(
            source.binlog_file.clone(),
            source.start_position,
        )?,
    })
}

pub(crate) fn binlog_options_from_source_position(
    filename: String,
    position: u64,
) -> Result<BinlogOptions, ApplyBinlogError> {
    let position = u32::try_from(position).map_err(|_| {
        ApplyBinlogError::Config(format!(
            "start position {position} exceeds mysql_cdc u32 position limit"
        ))
    })?;
    Ok(BinlogOptions::from_position(filename, position))
}

#[cfg(test)]
pub(crate) fn classify_event(
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
) -> StructuredEventOutcome {
    StructuredEventOutcome {
        policy: event_policy(event),
        resume_coordinate: resume_coordinate(current_file, header, event),
    }
}

#[cfg(test)]
fn event_policy(event: &BinlogEvent) -> EventPolicy {
    match event {
        BinlogEvent::QueryEvent(_) => EventPolicy::Ignore,
        BinlogEvent::RowsQueryEvent(_) => EventPolicy::IgnoreAnnotation,
        BinlogEvent::TableMapEvent(_) => EventPolicy::ApplyTableMap,
        BinlogEvent::WriteRowsEvent(_)
        | BinlogEvent::UpdateRowsEvent(_)
        | BinlogEvent::DeleteRowsEvent(_) => EventPolicy::ApplyRows,
        BinlogEvent::XidEvent(_) => EventPolicy::CommitTransaction,
        _ => EventPolicy::Ignore,
    }
}

fn map_table_map_event<R>(
    coordinate: &BinlogCoordinate,
    table_map: &MysqlCdcTableMapEvent,
    schema_resolver: &R,
) -> Result<TableMapEvent, ApplyBinlogError>
where
    R: TableSchemaResolver,
{
    let schema = resolve_table_schema(table_map, schema_resolver)?;
    Ok(TableMapEvent {
        coordinate: coordinate.clone(),
        table: RowTableMap {
            table_id: table_map.table_id,
            schema: table_map.database_name.clone(),
            table: table_map.table_name.clone(),
            columns: schema.columns,
            primary_key: schema.primary_key,
            generated_columns: schema.generated_columns,
            signed_columns: schema.signed_columns,
            enum_columns: schema.enum_columns,
        },
    })
}

fn resolve_table_schema<R>(
    table_map: &MysqlCdcTableMapEvent,
    schema_resolver: &R,
) -> Result<ResolvedTableSchema, ApplyBinlogError>
where
    R: TableSchemaResolver,
{
    let column_count = table_map.column_types.len();
    let fallback = || {
        schema_resolver.resolve_table_schema(
            &table_map.database_name,
            &table_map.table_name,
            column_count,
        )
    };
    let Some(metadata) = &table_map.table_metadata else {
        return fallback();
    };
    let Some(columns) = metadata.column_names.clone() else {
        return fallback();
    };
    validate_column_count(
        &table_map.database_name,
        &table_map.table_name,
        column_count,
        &columns,
    )?;

    let metadata_primary_key = primary_key_from_metadata(metadata, &columns)?;
    let fallback_schema = if metadata_primary_key.is_some() {
        fallback().ok()
    } else {
        Some(fallback()?)
    };
    let primary_key = match metadata_primary_key {
        Some(primary_key) => primary_key,
        None => fallback_schema
            .as_ref()
            .expect("fallback schema exists when metadata lacks primary key")
            .primary_key
            .clone(),
    };
    let enum_columns = enum_columns_from_metadata(table_map, metadata, &columns)?;
    let (generated_columns, signed_columns) = fallback_schema
        .map(|schema| (schema.generated_columns, schema.signed_columns))
        .unwrap_or_default();
    Ok(ResolvedTableSchema {
        columns,
        primary_key,
        generated_columns,
        signed_columns,
        enum_columns,
    })
}

fn enum_columns_from_metadata(
    table_map: &MysqlCdcTableMapEvent,
    metadata: &TableMetadata,
    columns: &[String],
) -> Result<BTreeMap<String, Vec<String>>, ApplyBinlogError> {
    let Some(enum_value_sets) = &metadata.enum_string_values else {
        return Ok(BTreeMap::new());
    };
    let enum_column_indexes = table_map
        .column_types
        .iter()
        .enumerate()
        .filter_map(|(index, column_type)| {
            (*column_type == MYSQL_COLUMN_TYPE_ENUM).then_some(index)
        })
        .collect::<Vec<_>>();
    if enum_column_indexes.len() != enum_value_sets.len() {
        return Err(mapping_error(format!(
            "table map enum metadata has {} enum columns but {} enum value sets",
            enum_column_indexes.len(),
            enum_value_sets.len()
        )));
    }

    enum_column_indexes
        .into_iter()
        .zip(enum_value_sets.iter())
        .map(|(column_index, values)| {
            let column = columns.get(column_index).cloned().ok_or_else(|| {
                mapping_error(format!("enum column index {column_index} is out of range"))
            })?;
            Ok((column, values.clone()))
        })
        .collect()
}

fn is_signed_integer_column(data_type: &str, column_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
    ) && !column_type.to_ascii_lowercase().contains("unsigned")
}

fn primary_key_from_metadata(
    metadata: &TableMetadata,
    columns: &[String],
) -> Result<Option<Vec<String>>, ApplyBinlogError> {
    let Some(primary_key_indexes) = &metadata.simple_primary_keys else {
        return Ok(None);
    };
    primary_key_indexes
        .iter()
        .map(|index| primary_key_column(*index, columns))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn primary_key_column(index: u32, columns: &[String]) -> Result<String, ApplyBinlogError> {
    columns
        .get(index as usize)
        .cloned()
        .ok_or_else(|| mapping_error(format!("primary key column index {index} is out of range")))
}

fn validate_column_count(
    schema: &str,
    table: &str,
    expected: usize,
    columns: &[String],
) -> Result<(), ApplyBinlogError> {
    if columns.len() == expected {
        return Ok(());
    }

    Err(mapping_error(format!(
        "schema for {schema}.{table} has {} columns but row event table map has {expected}",
        columns.len()
    )))
}

fn row_event_table_map<E>(
    applier: &RowApplier<E>,
    table_id: u64,
    coordinate: &BinlogCoordinate,
) -> Result<RowTableMap, ApplyBinlogError>
where
    E: TargetExecutor,
{
    applier.table_map(table_id).cloned().ok_or_else(|| {
        mapping_error(format!(
            "missing table map for table id {table_id} at {}:{}",
            coordinate.file, coordinate.position
        ))
    })
}

fn map_update_row_data(
    row: &mysql_cdc::events::row_events::row_data::UpdateRowData,
    table: &RowTableMap,
) -> Result<RowUpdate, ApplyBinlogError> {
    Ok(RowUpdate {
        before: map_row_data(&row.before_update, table)?,
        after: map_row_data(&row.after_update, table)?,
    })
}

fn map_row_data_list(
    rows: &[RowData],
    table: &RowTableMap,
) -> Result<Vec<RowImage>, ApplyBinlogError> {
    rows.iter().map(|row| map_row_data(row, table)).collect()
}

fn map_row_data(row: &RowData, table: &RowTableMap) -> Result<RowImage, ApplyBinlogError> {
    if row.cells.len() != table.columns.len() {
        return Err(mapping_error(format!(
            "row has {} cells but table map has {} columns",
            row.cells.len(),
            table.columns.len()
        )));
    }

    table
        .columns
        .iter()
        .zip(&row.cells)
        .map(|(column, value)| {
            let signed = table.signed_columns.contains(column);
            mysql_value_to_target_value(value, signed, table.enum_columns.get(column))
                .map(|target_value| (column.clone(), target_value))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()
}

fn mysql_value_to_target_value(
    value: &Option<MySqlValue>,
    signed: bool,
    enum_values: Option<&Vec<String>>,
) -> Result<Value, ApplyBinlogError> {
    let target_value = match value {
        None => Value::NULL,
        Some(MySqlValue::TinyInt(value)) if signed => Value::Int(i64::from(*value as i8)),
        Some(MySqlValue::TinyInt(value)) => Value::UInt(u64::from(*value)),
        Some(MySqlValue::SmallInt(value)) if signed => Value::Int(i64::from(*value as i16)),
        Some(MySqlValue::SmallInt(value)) => Value::UInt(u64::from(*value)),
        Some(MySqlValue::MediumInt(value)) if signed => Value::Int(sign_extend_u24(*value)),
        Some(MySqlValue::MediumInt(value)) => Value::UInt(u64::from(*value)),
        Some(MySqlValue::Int(value)) if signed => Value::Int(i64::from(*value as i32)),
        Some(MySqlValue::Int(value)) => Value::UInt(u64::from(*value)),
        Some(MySqlValue::BigInt(value)) if signed => Value::Int(*value as i64),
        Some(MySqlValue::BigInt(value)) => Value::UInt(*value),
        Some(MySqlValue::Float(value)) => Value::Float(*value),
        Some(MySqlValue::Double(value)) => Value::Double(*value),
        Some(MySqlValue::Decimal(value)) => bytes_value(value.as_str()),
        Some(MySqlValue::String(value)) => bytes_value(value.as_str()),
        Some(MySqlValue::Bit(value)) => Value::Bytes(pack_bit_value(value)),
        Some(MySqlValue::Enum(value)) => enum_value_to_target_value(*value, enum_values)?,
        Some(MySqlValue::Set(value)) => Value::UInt(*value),
        Some(MySqlValue::Blob(value)) => Value::Bytes(value.clone()),
        Some(MySqlValue::Year(value)) => Value::UInt(u64::from(*value)),
        Some(MySqlValue::Date(value)) => bytes_value(format_date(value)),
        Some(MySqlValue::Time(value)) => bytes_value(format_time(value)),
        Some(MySqlValue::DateTime(value)) => bytes_value(format_datetime(value)),
        Some(MySqlValue::Timestamp(value)) => bytes_value(format_timestamp(*value)),
    };
    Ok(target_value)
}

fn enum_value_to_target_value(
    ordinal: u32,
    enum_values: Option<&Vec<String>>,
) -> Result<Value, ApplyBinlogError> {
    let Some(enum_values) = enum_values else {
        return Ok(Value::UInt(u64::from(ordinal)));
    };
    let value_index = usize::try_from(ordinal)
        .map_err(|_| mapping_error(format!("enum ordinal {ordinal} cannot fit usize")))?
        .checked_sub(1)
        .ok_or_else(|| {
            mapping_error("enum ordinal 0 is not a valid MySQL enum value".to_string())
        })?;
    let value = enum_values.get(value_index).ok_or_else(|| {
        mapping_error(format!(
            "enum ordinal {ordinal} exceeds {} metadata values",
            enum_values.len()
        ))
    })?;
    Ok(bytes_value(value.as_str()))
}

fn bytes_value(value: impl Into<Vec<u8>>) -> Value {
    Value::Bytes(value.into())
}

fn sign_extend_u24(value: u32) -> i64 {
    if value & 0x80_0000 == 0 {
        i64::from(value)
    } else {
        i64::from(value) - 0x1_000000
    }
}

fn require_full_row_image(
    columns_present: &[bool],
    operation: &str,
) -> Result<(), ApplyBinlogError> {
    if columns_present.iter().all(|present| *present) {
        return Ok(());
    }

    Err(mapping_error(format!(
        "cannot apply {operation} row event without FULL binlog row image"
    )))
}

fn event_coordinate(
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
) -> BinlogCoordinate {
    resume_coordinate(current_file, header, event).unwrap_or_else(|| BinlogCoordinate {
        file: current_file.to_string(),
        position: u64::from(header.next_event_position),
    })
}

fn resume_coordinate(
    current_file: &str,
    header: &EventHeader,
    event: &BinlogEvent,
) -> Option<BinlogCoordinate> {
    match event {
        BinlogEvent::RotateEvent(rotate) => Some(BinlogCoordinate {
            file: rotate.binlog_filename.clone(),
            position: rotate.binlog_position,
        }),
        BinlogEvent::XidEvent(_) if header.next_event_position > 0 => Some(BinlogCoordinate {
            file: current_file.to_string(),
            position: u64::from(header.next_event_position),
        }),
        _ => None,
    }
}

fn source_inventory_config(config: &ApplyBinlogConfig) -> InventoryConfig {
    InventoryConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        mariadb: config.mariadb.clone(),
    }
}

fn event_name(event: &BinlogEvent) -> &'static str {
    match event {
        BinlogEvent::UnknownEvent => "UnknownEvent",
        BinlogEvent::DeleteRowsEvent(_) => "DeleteRowsEvent",
        BinlogEvent::UpdateRowsEvent(_) => "UpdateRowsEvent",
        BinlogEvent::WriteRowsEvent(_) => "WriteRowsEvent",
        BinlogEvent::XidEvent(_) => "XidEvent",
        BinlogEvent::IntVarEvent(_) => "IntVarEvent",
        BinlogEvent::UserVarEvent(_) => "UserVarEvent",
        BinlogEvent::QueryEvent(_) => "QueryEvent",
        BinlogEvent::TableMapEvent(_) => "TableMapEvent",
        BinlogEvent::RotateEvent(_) => "RotateEvent",
        BinlogEvent::RowsQueryEvent(_) => "RowsQueryEvent",
        BinlogEvent::HeartbeatEvent(_) => "HeartbeatEvent",
        BinlogEvent::FormatDescriptionEvent(_) => "FormatDescriptionEvent",
        BinlogEvent::MySqlGtidEvent(_) => "MySqlGtidEvent",
        BinlogEvent::MySqlPrevGtidsEvent(_) => "MySqlPrevGtidsEvent",
        BinlogEvent::MariaDbGtidEvent(_) => "MariaDbGtidEvent",
        BinlogEvent::MariaDbGtidListEvent(_) => "MariaDbGtidListEvent",
    }
}

fn source_error(error: MysqlCdcError) -> ApplyBinlogError {
    ApplyBinlogError::SourceCommand(format!("mysql_cdc error: {error:?}"))
}

fn mapping_error(message: String) -> ApplyBinlogError {
    ApplyBinlogError::Statement(message)
}

fn pack_bit_value(bits: &[bool]) -> Vec<u8> {
    let numeric_value = bits
        .iter()
        .fold(0_u64, |value, bit| (value << 1) | u64::from(*bit));
    let byte_count = bits.len().max(1).div_ceil(8);

    (0..byte_count)
        .map(|index| {
            let shift = (byte_count - index - 1) * 8;
            ((numeric_value >> shift) & 0xff) as u8
        })
        .collect()
}

fn format_date(value: &Date) -> String {
    format!("{:04}-{:02}-{:02}", value.year, value.month, value.day)
}

fn format_time(value: &Time) -> String {
    let base = format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second);
    append_millis(base, value.millis)
}

fn format_datetime(value: &DateTime) -> String {
    let base = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        value.year, value.month, value.day, value.hour, value.minute, value.second
    );
    append_millis(base, value.millis)
}

fn format_timestamp(millis: u64) -> String {
    let seconds = (millis / MILLIS_PER_SECOND) as i64;
    let (date, time) = split_unix_seconds(seconds);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        date.year, date.month, date.day, time.hour, time.minute, time.second
    )
}

fn append_millis(base: String, millis: u32) -> String {
    if millis == 0 {
        base
    } else {
        format!("{base}.{millis:03}")
    }
}

fn split_unix_seconds(seconds: i64) -> (DateParts, TimeParts) {
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let seconds_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    (civil_from_days(days), time_from_seconds(seconds_of_day))
}

fn civil_from_days(days_since_epoch: i64) -> DateParts {
    let days = days_since_epoch + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_phase = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_phase + 2) / 5 + 1;
    let month = month_phase + if month_phase < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };

    DateParts {
        year,
        month: month as u8,
        day: day as u8,
    }
}

fn time_from_seconds(seconds_of_day: i64) -> TimeParts {
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    TimeParts {
        hour: hour as u8,
        minute: minute as u8,
        second: second as u8,
    }
}

struct DateParts {
    year: i64,
    month: u8,
    day: u8,
}

struct TimeParts {
    hour: u8,
    minute: u8,
    second: u8,
}

#[cfg(test)]
mod tests;
