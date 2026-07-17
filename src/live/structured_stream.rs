use super::ddl_event::DdlEvent;
use super::ddl_replay_journal::{
    DdlFamily, DdlReplayAction, DdlReplayJournal, DdlReplayStatus, MySqlDdlReplayJournal,
    PreparedReconciliation, prepared_reconciliation_block_reason, reconcile_prepared,
    replay_action,
};
use super::ddl_semantics::{
    DdlSemanticEvidence, DdlSemanticInventory, LiveDdlSemanticInventory, parse_ddl_operation,
    supports_automatic_index_ddl, supports_automatic_semantic_recovery,
    supports_production_alter_table,
};
use super::{
    ApplyBinlogConfig, ApplyBinlogError, QuarantineRecorder, RecordingQuarantine,
    SourceBinlogConfig,
};
use crate::conflict_repair::MySqlConflictStore;
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, MariaDbInventoryReader, SchemaInventory,
    SourceBinlogSettings, build_inventory,
};
use crate::probe::BinlogCoordinate;
use crate::row::{
    DeleteRowsEvent, RowApplier, RowConflictContext, RowImage, RowTableMap, RowUpdate,
    TableMapEvent,
};
use crate::statement::{StatementApplier, StatementEvent, StatementOutcome};
use crate::target::{TargetExecutor, TransactionalTargetExecutor};
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
use std::time::{Duration, Instant};

use super::progress::{StreamProgress, format_stream_progress};
use super::reconnect::{StreamCheckpointStore, run_stream_reconnect_loop};

const DEFAULT_REPLICA_SERVER_ID: u32 = 65_535;
const MYSQL_CDC_HEARTBEAT_SECONDS: u64 = 30;
// Bounds read-ahead memory while keeping the source socket drained during slow
// applies, so the server's net_write_timeout does not kill the dump connection.
const READ_AHEAD_EVENT_BUFFER: usize = 1024;
const MYSQL_COLUMN_TYPE_ENUM: u8 = 247;
const MILLIS_PER_SECOND: u64 = 1_000;
const SECONDS_PER_DAY: i64 = 86_400;
mod ddl;
mod event;
mod init;
mod rows;
mod schema;
mod token;
mod transaction;
mod value;

use ddl::*;
use event::*;
pub(crate) use init::stream_remote_binlog;
use init::*;
use rows::*;
use schema::*;
use token::*;
use transaction::*;
use value::*;

#[cfg(feature = "integration-failpoints")]
fn trigger_integration_failpoint(expected: super::IntegrationFailpoint, boundary: &str) {
    if super::integration_failpoint_enabled(expected) {
        eprintln!("cdc_integration_failpoint boundary={boundary}");
        std::process::exit(70);
    }
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopPositionDecision {
    Dispatch,
    DispatchAndStop,
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

// Resolves row-event schemas from the TARGET database. The live source schema
// can be ahead of the stream position (later DDL), while the target schema stays
// position-consistent because operators apply and resolve each DDL boundary
// before the stream checkpoints past it.

// Reads binlog events on a dedicated thread so the source socket stays drained
// while the applier works; a stalled applier otherwise trips the server's
// net_write_timeout and resets the dump connection.

struct StreamEventContext<'a, R, C> {
    schema_resolver: &'a R,
    state: &'a mut StructuredEventState,
    target_transaction: &'a mut TargetTransaction,
    checkpoint_store: Option<&'a C>,
    transaction_checkpoint_table: Option<&'a str>,
    transaction_checkpoint_name: Option<&'a str>,
    current_file: &'a mut String,
    group_config: TargetTransactionGroupConfig,
}

struct AutomaticDdlDependencies<'a, J, S> {
    journal: &'a J,
    semantic_inventory: &'a S,
    source_identity: &'a str,
}

struct AutomaticDdlInput<'a, 'b, R, C> {
    context: &'a mut StreamEventContext<'b, R, C>,
    header: &'a EventHeader,
    event: &'a BinlogEvent,
}

fn stop_position_decision(
    stop_position: Option<u64>,
    header: &EventHeader,
    source_row_transaction_open: bool,
) -> Result<StopPositionDecision, ApplyBinlogError> {
    let Some(stop_position) = stop_position else {
        return Ok(StopPositionDecision::Dispatch);
    };
    let event_end = u64::from(header.next_event_position);
    if event_end == 0 || event_end < stop_position {
        return Ok(StopPositionDecision::Dispatch);
    }
    if event_end == stop_position {
        return Ok(StopPositionDecision::DispatchAndStop);
    }

    let event_start = event_end.saturating_sub(u64::from(header.event_length));
    if source_row_transaction_open {
        return Err(ApplyBinlogError::SourceCommand(format!(
            "stop position {stop_position} falls inside an open transaction before event ending at {event_end}"
        )));
    }
    if event_start < stop_position {
        return Err(ApplyBinlogError::SourceCommand(format!(
            "stop position {stop_position} falls inside event {event_start}..{event_end}"
        )));
    }
    Err(ApplyBinlogError::SourceCommand(format!(
        "stop position {stop_position} cannot be reached before event ending at {event_end}"
    )))
}

fn bounded_stop_completion_error(
    source_row_transaction_open: bool,
) -> Result<(), ApplyBinlogError> {
    if source_row_transaction_open {
        return Err(ApplyBinlogError::SourceCommand(
            "bounded stop position falls inside an open transaction; refusing partial transaction"
                .to_string(),
        ));
    }
    Ok(())
}

fn bounded_stop_not_reached_error(stop_position: u64) -> ApplyBinlogError {
    ApplyBinlogError::SourceCommand(format!(
        "bounded stream ended before reaching stop position {stop_position}"
    ))
}

fn update_source_row_transaction_state(open: &mut bool, event: &BinlogEvent) {
    match event {
        BinlogEvent::WriteRowsEvent(_)
        | BinlogEvent::UpdateRowsEvent(_)
        | BinlogEvent::DeleteRowsEvent(_) => *open = true,
        BinlogEvent::XidEvent(_) => *open = false,
        _ => {}
    }
}

fn stream_ended_error() -> ApplyBinlogError {
    ApplyBinlogError::SourceCommand("mysql_cdc binlog stream ended at EOF".to_string())
}

fn log_stream_progress(progress: &mut StreamProgress, outcome: &StructuredEventOutcome) {
    let Some(coordinate) = &outcome.resume_coordinate else {
        return;
    };
    if progress.record_applied(coordinate) {
        println!("{}", format_stream_progress(progress));
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
