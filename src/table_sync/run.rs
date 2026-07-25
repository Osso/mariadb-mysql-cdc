use super::mysql::{
    GUEST_COLUMNS, HOME_FEED_CARD_COLUMNS, MySqlSyncReader, RECOVERY_CREATE_TIME_EPOCH_ALIAS,
    RECOVERY_UTC_SESSION_SQL, guest_columns, home_feed_card_columns,
};
use super::range::sync_table_with_progress_range;
use super::recent::{RecentUpdateSyncContext, sync_recent_updates_with_progress};
use super::*;
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, MariaDbInventoryReader, SchemaInventory,
    build_inventory,
};
use std::time::Duration;

const SYNC_CONNECTION_ATTEMPTS: usize = 5;
const SYNC_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(1);

pub fn sync_table(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<SyncTableReport, TableSyncError> {
    let mut progress_store = NoopSyncProgressStore;
    sync_table_with_progress(
        table,
        chunk_size,
        mode,
        source,
        target,
        repair_target,
        &mut progress_store,
    )
}

pub fn sync_table_with_progress(
    table: &SyncTable,
    chunk_size: usize,
    mode: SyncMode,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
) -> Result<SyncTableReport, TableSyncError> {
    sync_table_with_progress_range(
        table,
        SyncRunOptions {
            run_id: "ephemeral".to_string(),
            run_scope: "ephemeral".to_string(),
            chunk_size,
            mode,
            start_after: None,
            end_at: None,
        },
        source,
        target,
        repair_target,
        progress_store,
    )
}

pub(crate) trait ExactParentReader {
    fn read_guest_identity_rows(
        &self,
        guest_id: &str,
        guest_hash: &str,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, TableSyncError>;

    fn read_home_feed_card_rows_by_id(
        &self,
        card_id: &str,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, TableSyncError>;

    fn read_home_feed_card_identity_rows(
        &self,
        card_id: &str,
        card_type_id: &str,
        source_id: Option<&str>,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, TableSyncError>;
}

impl ExactParentReader for MySqlSyncReader {
    fn read_guest_identity_rows(
        &self,
        guest_id: &str,
        guest_hash: &str,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, TableSyncError> {
        self.read_guest_identity_rows(guest_id, guest_hash)
    }

    fn read_home_feed_card_rows_by_id(
        &self,
        card_id: &str,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, TableSyncError> {
        self.read_home_feed_card_rows_by_id(card_id)
    }

    fn read_home_feed_card_identity_rows(
        &self,
        card_id: &str,
        card_type_id: &str,
        source_id: Option<&str>,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, TableSyncError> {
        self.read_home_feed_card_identity_rows(card_id, card_type_id, source_id)
    }
}

pub(crate) fn reconcile_exact_parent(
    request: &crate::live::ExactParentRecovery,
    source: &impl ExactParentReader,
    target: &impl ExactParentReader,
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    let (source_rows, target_rows) = match request {
        crate::live::ExactParentRecovery::SessionsGuest(request) => {
            validate_sessions_guest_request(request)?;
            let source_rows =
                source.read_guest_identity_rows(&request.guest_id, &request.guest_hash)?;
            let target_rows =
                target.read_guest_identity_rows(&request.guest_id, &request.guest_hash)?;
            (source_rows, target_rows)
        }
        crate::live::ExactParentRecovery::HomeFeedCard(request) => {
            validate_home_feed_card_request(request)?;
            let source_rows = source.read_home_feed_card_rows_by_id(&request.card_id)?;
            let source_row = require_exact_home_feed_card_row("source", &source_rows, request)?;
            let card_type_id =
                required_row_value(source_row, "card_type_id", "source home feed card")?;
            let source_id = source_row
                .values
                .get("source_id")
                .and_then(Option::as_deref);
            let target_rows = target.read_home_feed_card_identity_rows(
                &request.card_id,
                card_type_id,
                source_id,
            )?;
            (source_rows, target_rows)
        }
    };
    reconcile_loaded_exact_parent(request, &source_rows, &target_rows, repair_target)
}
pub(crate) fn reconcile_exact_parent_live(
    config: &crate::live::ApplyBinlogConfig,
    request: &crate::live::ExactParentRecovery,
) -> Result<(), TableSyncError> {
    let sync_config = match request {
        crate::live::ExactParentRecovery::SessionsGuest(request) => {
            exact_guest_sync_config(config, request)
        }
        crate::live::ExactParentRecovery::HomeFeedCard(request) => {
            exact_home_feed_card_sync_config(config, request)
        }
    };
    let (source, target) = build_sessions_guest_recovery_readers(config)?;
    let mut repair_target = connect_mysql_recovery_target(&sync_config)?;
    reconcile_exact_parent(request, &source, &target, &mut repair_target)
}
/// Reads the parent's columns and primary key from the source schema inventory, because a generic
/// parent table cannot have them enumerated in code.
pub(crate) fn read_parent_table_inventory(
    source: &crate::mysql_snapshot::MySqlConnectionConfig,
    schema: &str,
    table: &str,
) -> Result<crate::inventory::TableInventory, TableSyncError> {
    let reader = MariaDbInventoryReader::new(InventoryConfig {
        host: source.host.clone(),
        port: source.port,
        user: source.user.clone(),
        password: source.password.clone(),
        endpoint_role: InventoryEndpointRole::Source,
        use_tls: false,
        tls_ca_file: None,
        ..InventoryConfig::default()
    });
    let inventory = build_inventory(schema, &reader)
        .map_err(|error| TableSyncError::Read(error.to_string()))?;
    let parent = inventory
        .tables
        .into_iter()
        .find(|candidate| candidate.name == table)
        .ok_or_else(|| {
            TableSyncError::Repair(format!(
                "parent table `{schema}`.`{table}` is absent from the source inventory"
            ))
        })?;
    if parent.primary_key.is_empty() {
        return Err(TableSyncError::Repair(format!(
            "parent table `{schema}`.`{table}` has no primary key"
        )));
    }
    Ok(parent)
}

/// Reads the one source parent row owning a referenced foreign-key identity, for in-transaction
/// recovery of an absent parent.
///
/// The target side is not consulted: the caller has already proved under lock that the identity is
/// absent there. Everything else stays the planner's decision, so an ambiguous or incomplete source
/// parent still fails closed.
// Unwired with generic missing-parent deferral (see live::missing_parent); kept for re-enable.
#[allow(dead_code)]
pub(crate) fn read_exact_source_parent_row(
    source: &crate::mysql_snapshot::MySqlConnectionConfig,
    violation: &crate::live::ForeignKeyViolation,
    child_foreign_key_values: &[Option<String>],
) -> Result<
    (
        crate::inventory::TableInventory,
        crate::snapshot::SnapshotRow,
    ),
    TableSyncError,
> {
    let schema = violation
        .parent_schema
        .clone()
        .unwrap_or_else(|| violation.child_schema.clone());
    let parent = read_parent_table_inventory(source, &schema, &violation.parent_table)?;
    let reader = MySqlSyncReader::new(crate::mysql_snapshot::MySqlConnectionConfig {
        database: schema.clone(),
        ..source.clone()
    })
    .with_recovery_utc();
    let source_rows = reader.read_parent_identity_rows(
        &parent,
        &violation.parent_columns,
        child_foreign_key_values,
    )?;
    let plan = crate::live::plan_missing_parent_recovery(&crate::live::MissingParentInput {
        violation,
        child_foreign_key_values,
        source_parent_rows: &source_rows,
        target_parent_rows: &[],
    })
    .map_err(|rejection| {
        TableSyncError::Repair(format!(
            "missing parent recovery rejected for constraint {}: {rejection}",
            violation.constraint
        ))
    })?;
    match plan {
        crate::live::MissingParentPlan::InsertParent(row) => Ok((parent, row)),
        crate::live::MissingParentPlan::AlreadyReconciled => Err(TableSyncError::Repair(
            "locked parent was absent but the planner reported it reconciled".to_string(),
        )),
    }
}
pub(crate) fn reconcile_loaded_exact_parent(
    request: &crate::live::ExactParentRecovery,
    source_rows: &[crate::snapshot::SnapshotRow],
    target_rows: &[crate::snapshot::SnapshotRow],
    repair_target: &mut impl SyncRepairTarget,
) -> Result<(), TableSyncError> {
    let reconciliation = match request {
        crate::live::ExactParentRecovery::SessionsGuest(request) => {
            plan_loaded_sessions_guest(source_rows, target_rows, request)?
        }
        crate::live::ExactParentRecovery::HomeFeedCard(request) => {
            let source_row = require_exact_home_feed_card_row("source", source_rows, request)?;
            validate_parent_temporal_order(
                recovery_create_time_epoch(source_row, "home feed card")?,
                request.child_event_timestamp,
            )?;
            plan_loaded_home_feed_card(source_row, target_rows, request)?
        }
    };
    let GuestReconciliation::Insert(source_row) = reconciliation else {
        return Ok(());
    };
    repair_target.insert_row(&source_row)
}

fn build_sessions_guest_recovery_readers(
    config: &crate::live::ApplyBinlogConfig,
) -> Result<(MySqlSyncReader, MySqlSyncReader), TableSyncError> {
    let source_database = config.source.database.clone().ok_or_else(|| {
        TableSyncError::InvalidTable("sessions guest recovery requires source database".to_string())
    })?;
    let source_config = crate::mysql_snapshot::MySqlConnectionConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        database: source_database,
    };
    let source_tls_ca =
        (!config.source.tls_ca_file.is_empty()).then(|| config.source.tls_ca_file.clone());
    let source = MySqlSyncReader::new_with_tls_ca(source_config, source_tls_ca).with_recovery_utc();
    let target = MySqlSyncReader::new_with_target(
        target_connection_config_for_apply(config),
        &config.target,
    )
    .map_err(TableSyncError::Read)?
    .with_recovery_utc();
    Ok((source, target))
}

fn validate_sessions_guest_request(
    request: &crate::live::SessionsGuestRecovery,
) -> Result<(), TableSyncError> {
    if is_supported_recovery_scope(request) && has_complete_recovery_identity(request) {
        return Ok(());
    }
    Err(TableSyncError::InvalidTable(
        "unsupported sessions guest recovery request".to_string(),
    ))
}

fn is_supported_recovery_scope(request: &crate::live::SessionsGuestRecovery) -> bool {
    request.schema == crate::live::SESSIONS_GUEST_CHILD_SCHEMA
        && request.table == crate::live::SESSIONS_GUEST_CHILD_TABLE
        && request.constraint == crate::live::SESSIONS_GUEST_CONSTRAINT
}

fn has_complete_recovery_identity(request: &crate::live::SessionsGuestRecovery) -> bool {
    let has_session_id = !request.session_id.is_empty();
    let has_guest_id = !request.guest_id.is_empty();
    let has_guest_hash = !request.guest_hash.is_empty();

    has_session_id && has_guest_id && has_guest_hash
}

fn validate_home_feed_card_request(
    request: &crate::live::HomeFeedCardRecovery,
) -> Result<(), TableSyncError> {
    let has_exact_scope = request.schema == crate::live::HOME_FEED_SLIDE_CHILD_SCHEMA
        && request.table == crate::live::HOME_FEED_SLIDE_CHILD_TABLE
        && request.constraint == crate::live::HOME_FEED_SLIDE_CONSTRAINT;
    let has_valid_identity = parse_positive_integer(&request.slide_id).is_some()
        && parse_positive_integer(&request.card_id).is_some();
    if has_exact_scope && has_valid_identity {
        return Ok(());
    }
    Err(TableSyncError::InvalidTable(
        "unsupported home feed card recovery request".to_string(),
    ))
}

fn parse_positive_integer(value: &str) -> Option<u64> {
    value.parse::<u64>().ok().filter(|value| *value > 0)
}

enum GuestReconciliation {
    Insert(crate::snapshot::SnapshotRow),
    Existing,
}

fn plan_loaded_sessions_guest(
    source_rows: &[crate::snapshot::SnapshotRow],
    target_rows: &[crate::snapshot::SnapshotRow],
    request: &crate::live::SessionsGuestRecovery,
) -> Result<GuestReconciliation, TableSyncError> {
    let source_row = require_exact_guest_row("source", source_rows, request)?;
    validate_parent_temporal_order(
        recovery_create_time_epoch(source_row, "sessions guest")?,
        request.child_event_timestamp,
    )?;
    let source_row = canonical_guest_row(source_row);
    if target_rows.is_empty() {
        return Ok(GuestReconciliation::Insert(source_row));
    }
    let target_row = canonical_guest_row(require_exact_guest_row("target", target_rows, request)?);
    if target_row.values != source_row.values {
        return Err(TableSyncError::Repair(
            "target guests row diverges from exact source image".to_string(),
        ));
    }
    Ok(GuestReconciliation::Existing)
}
fn plan_loaded_home_feed_card(
    source_row: &crate::snapshot::SnapshotRow,
    target_rows: &[crate::snapshot::SnapshotRow],
    request: &crate::live::HomeFeedCardRecovery,
) -> Result<GuestReconciliation, TableSyncError> {
    let source_row = canonical_recovery_row(source_row);
    if target_rows.is_empty() {
        return Ok(GuestReconciliation::Insert(source_row));
    }
    let target_row = require_exact_home_feed_card_row("target", target_rows, request)?;
    if canonical_recovery_row(target_row).values != source_row.values {
        return Err(TableSyncError::Repair(
            "target home_feed_cards row diverges from exact source image".to_string(),
        ));
    }
    Ok(GuestReconciliation::Existing)
}

fn require_exact_home_feed_card_row<'a>(
    side: &str,
    rows: &'a [crate::snapshot::SnapshotRow],
    request: &crate::live::HomeFeedCardRecovery,
) -> Result<&'a crate::snapshot::SnapshotRow, TableSyncError> {
    let Some(row) = rows.first() else {
        return Err(home_feed_card_identity_error(side));
    };
    let has_exact_identity = rows.len() == 1 && row.primary_key == [request.card_id.clone()];
    let has_complete_image = row.values.len() == HOME_FEED_CARD_COLUMNS.len() + 1
        && HOME_FEED_CARD_COLUMNS
            .iter()
            .all(|column| row.values.contains_key(*column))
        && row.values.contains_key(RECOVERY_CREATE_TIME_EPOCH_ALIAS);
    if has_exact_identity && has_complete_image {
        return Ok(row);
    }
    Err(home_feed_card_identity_error(side))
}

fn required_row_value<'a>(
    row: &'a crate::snapshot::SnapshotRow,
    column: &str,
    row_name: &str,
) -> Result<&'a str, TableSyncError> {
    row.values
        .get(column)
        .and_then(Option::as_deref)
        .ok_or_else(|| TableSyncError::Repair(format!("{row_name} {column} is missing")))
}

fn home_feed_card_identity_error(side: &str) -> TableSyncError {
    TableSyncError::Repair(format!(
        "{side} home_feed_cards identity is absent, colliding, divergent, or incomplete"
    ))
}

fn require_exact_guest_row<'a>(
    side: &str,
    rows: &'a [crate::snapshot::SnapshotRow],
    request: &crate::live::SessionsGuestRecovery,
) -> Result<&'a crate::snapshot::SnapshotRow, TableSyncError> {
    let Some(row) = rows.first() else {
        return Err(guest_identity_error(side));
    };
    let has_exact_identity = rows.len() == 1
        && row.primary_key == [request.guest_id.clone()]
        && row.values.get("guest_hash").and_then(Option::as_deref)
            == Some(request.guest_hash.as_str());
    let has_complete_image = row.values.len() == GUEST_COLUMNS.len() + 1
        && GUEST_COLUMNS
            .iter()
            .all(|column| row.values.contains_key(*column))
        && row.values.contains_key(RECOVERY_CREATE_TIME_EPOCH_ALIAS);
    if has_exact_identity && has_complete_image {
        return Ok(row);
    }
    Err(guest_identity_error(side))
}

fn canonical_guest_row(row: &crate::snapshot::SnapshotRow) -> crate::snapshot::SnapshotRow {
    canonical_recovery_row(row)
}

fn canonical_recovery_row(row: &crate::snapshot::SnapshotRow) -> crate::snapshot::SnapshotRow {
    let mut row = row.clone();
    row.values.remove(RECOVERY_CREATE_TIME_EPOCH_ALIAS);
    row
}

fn recovery_create_time_epoch(
    row: &crate::snapshot::SnapshotRow,
    recovery_name: &str,
) -> Result<u64, TableSyncError> {
    row.values
        .get(RECOVERY_CREATE_TIME_EPOCH_ALIAS)
        .and_then(Option::as_deref)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            TableSyncError::Repair(format!(
                "{recovery_name} recovery parent create_time epoch is missing or invalid"
            ))
        })
}

fn guest_identity_error(side: &str) -> TableSyncError {
    TableSyncError::Repair(format!(
        "{side} guests identity is absent, colliding, divergent, or incomplete"
    ))
}

fn validate_parent_temporal_order(
    parent_create_time_epoch: u64,
    child_event_timestamp: u64,
) -> Result<(), TableSyncError> {
    if child_event_timestamp == 0 {
        return Err(TableSyncError::Repair(
            "sessions guest recovery child event timestamp is missing".to_string(),
        ));
    }
    if parent_create_time_epoch > child_event_timestamp {
        return Err(TableSyncError::Repair(
            "sessions guest recovery parent was created after child event".to_string(),
        ));
    }
    Ok(())
}

fn target_connection_config_for_apply(
    config: &crate::live::ApplyBinlogConfig,
) -> crate::mysql_snapshot::MySqlConnectionConfig {
    crate::mysql_snapshot::MySqlConnectionConfig {
        host: config.target.host.clone(),
        port: config.target.port,
        user: config.target.user.clone(),
        password: config.target.password.clone(),
        database: config.target.database.clone(),
    }
}

fn exact_home_feed_card_sync_config(
    config: &crate::live::ApplyBinlogConfig,
    request: &crate::live::HomeFeedCardRecovery,
) -> SyncTableConfig {
    SyncTableConfig {
        source: crate::mysql_snapshot::MySqlConnectionConfig {
            host: config.source.host.clone(),
            port: config.source.port,
            user: config.source.user.clone(),
            password: config.source.password.clone(),
            database: crate::live::HOME_FEED_SLIDE_CHILD_SCHEMA.to_string(),
        },
        target: config.target.clone(),
        table: SyncTable {
            name: crate::live::HOME_FEED_CARD_PARENT_TABLE.to_string(),
            primary_key: vec![crate::live::HOME_FEED_CARD_PARENT_PRIMARY_KEY.to_string()],
            columns: home_feed_card_columns(),
        },
        chunk_size: 1,
        mode: SyncMode::MissingPrimaryKeys,
        progress_table: "cdc.sync_table_progress".to_string(),
        run_id: format!("stream-home-feed-slide-{}", request.slide_id),
        start_after: None,
        end_at: Some(vec![request.card_id.clone()]),
        updated_since: None,
        plan_hash: None,
    }
}

fn exact_guest_sync_config(
    config: &crate::live::ApplyBinlogConfig,
    request: &crate::live::SessionsGuestRecovery,
) -> SyncTableConfig {
    SyncTableConfig {
        source: crate::mysql_snapshot::MySqlConnectionConfig {
            host: config.source.host.clone(),
            port: config.source.port,
            user: config.source.user.clone(),
            password: config.source.password.clone(),
            database: crate::live::SESSIONS_GUEST_CHILD_SCHEMA.to_string(),
        },
        target: config.target.clone(),
        table: SyncTable {
            name: crate::live::SESSIONS_GUEST_PARENT_TABLE.to_string(),
            primary_key: vec![crate::live::SESSIONS_GUEST_PARENT_PRIMARY_KEY.to_string()],
            columns: guest_columns(),
        },
        chunk_size: 1,
        mode: SyncMode::MissingPrimaryKeys,
        progress_table: "cdc.sync_table_progress".to_string(),
        run_id: format!("stream-sessions-{}", request.session_id),
        start_after: None,
        end_at: Some(vec![request.guest_id.clone()]),
        updated_since: None,
        plan_hash: None,
    }
}

pub fn run_sync_table(config: &SyncTableConfig) -> Result<SyncTableReport, TableSyncError> {
    progress::MySqlSyncRunProgressStore::new(config.target.clone(), config.progress_table.clone())
        .ensure()?;
    let _reservation = crate::table_catalog::reserve_sync_worker(
        &config.target,
        &config.progress_table,
        &config.table.name,
    )
    .map_err(TableSyncError::Progress)?
    .ok_or_else(|| {
        TableSyncError::Progress(format!(
            "table sync capacity or table reservation unavailable for `{}`",
            config.table.name
        ))
    })?;
    run_sync_table_reserved(config)
}

pub(crate) fn run_sync_table_reserved(
    config: &SyncTableConfig,
) -> Result<SyncTableReport, TableSyncError> {
    let result = retry_sync_table_operation(
        config.mode,
        SYNC_CONNECTION_ATTEMPTS,
        SYNC_CONNECTION_RETRY_DELAY,
        || run_sync_table_phase(config, SyncPhase::All),
    );
    record_terminal_sync_run_error(config, result)
}

fn record_terminal_sync_run_error(
    config: &SyncTableConfig,
    result: Result<SyncTableReport, TableSyncError>,
) -> Result<SyncTableReport, TableSyncError> {
    let Err(error) = result else {
        return result;
    };
    if !should_record_terminal_sync_run_error(&error) {
        return Err(error);
    }
    let mut progress_store = progress::MySqlSyncRunProgressStore::new(
        config.target.clone(),
        config.progress_table.clone(),
    );
    if let Err(save_error) = progress_store.save_error(&config.run_id, &error) {
        return Err(TableSyncError::Progress(format!(
            "{error}; also failed to persist run error: {save_error}"
        )));
    }
    Err(error)
}

pub(crate) fn retry_sync_table_operation<F>(
    mode: SyncMode,
    max_attempts: usize,
    retry_delay: Duration,
    mut operation: F,
) -> Result<SyncTableReport, TableSyncError>
where
    F: FnMut() -> Result<SyncTableReport, TableSyncError>,
{
    let attempts = if matches!(mode, SyncMode::Apply | SyncMode::MissingPrimaryKeys) {
        max_attempts.max(1)
    } else {
        1
    };
    for attempt in 1..=attempts {
        match operation() {
            Ok(report) => return Ok(report),
            Err(error) if attempt < attempts && is_retryable_sync_error(&error) => {
                if !retry_delay.is_zero() {
                    std::thread::sleep(retry_delay);
                }
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("sync retry loop has at least one attempt")
}

/// `Verification` is deliberately absent. The terminal parity pass is read-only: `repair_chunk`
/// returns after counting for `SyncPhase::Verify`. A retry resumes the chunk phase at the saved
/// tail primary key, so it cannot repair drift the pass found earlier in the table, then re-runs the
/// same read-only pass and reaches the same conclusion. Retrying only multiplies a full-table scan.
pub(crate) fn is_retryable_sync_error(error: &TableSyncError) -> bool {
    if matches!(
        error,
        TableSyncError::Read(_) | TableSyncError::Progress(_) | TableSyncError::Duplicate(_)
    ) {
        return true;
    }
    let TableSyncError::Repair(message) = error else {
        return false;
    };
    let message = message.to_ascii_lowercase();
    [
        "timed out",
        "timeout",
        "connection reset",
        "connection refused",
        "connection closed",
        "connection aborted",
        "broken pipe",
        "unexpected eof",
        "server has gone away",
        "lost connection",
        "network is unreachable",
        "could not connect",
        "not connected",
        "packet out of sync",
        "resource temporarily unavailable",
        "error 1205",
        "error 1213",
        "deadlock",
        "lock wait timeout",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

pub fn run_sync_table_phase(
    config: &SyncTableConfig,
    phase: SyncPhase,
) -> Result<SyncTableReport, TableSyncError> {
    run_sync_table_phase_with_run_spec(config, phase, None)
}

pub(crate) fn run_sync_table_phase_with_run_spec(
    config: &SyncTableConfig,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
) -> Result<SyncTableReport, TableSyncError> {
    validate_sync_table_config(config)?;
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new_with_target(target_connection_config(config), &config.target)
        .map_err(TableSyncError::Read)?;
    let mut progress_store = progress::MySqlSyncRunProgressStore::new(
        config.target.clone(),
        config.progress_table.clone(),
    );
    let mut repair_target = mysql_repair_target(config)?;
    run_sync_table_with_targets_phase(
        config,
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        phase,
        run_spec_json,
    )
}

pub(crate) fn should_record_sync_run_error(error: &TableSyncError) -> bool {
    matches!(error, TableSyncError::Read(_) | TableSyncError::Repair(_))
        && !is_retryable_sync_error(error)
}

/// Retries are exhausted by the time a run returns, so any surviving error ends the run. Recording
/// it keeps the durable row from staying `running` with no live worker, which otherwise reads as an
/// in-flight sync. Progress and table-validation errors are excluded because they must not replace
/// an already saved run status.
pub(crate) fn should_record_terminal_sync_run_error(error: &TableSyncError) -> bool {
    !matches!(
        error,
        TableSyncError::Progress(_) | TableSyncError::InvalidTable(_)
    )
}

fn connect_mysql_recovery_target(
    config: &SyncTableConfig,
) -> Result<MySqlSyncRepairTarget, TableSyncError> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new_for_sync(&config.target)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    executor
        .execute_raw_sql(RECOVERY_UTC_SESSION_SQL)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    Ok(build_mysql_repair_target(config, executor))
}

fn mysql_repair_target(config: &SyncTableConfig) -> Result<MySqlSyncRepairTarget, TableSyncError> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new_for_sync(&config.target)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    let source_inventory = read_source_inventory(config)?;
    let target_inventory = read_target_inventory(config)?;
    let writer = crate::target::TargetMySqlWriter::from_snapshot_table(
        &snapshot_table(&config.table),
        executor,
        sync_insert_mode(config),
    );
    let source = MySqlSyncReader::new(config.source.clone());
    let target = MySqlSyncReader::new_with_target(target_connection_config(config), &config.target)
        .map_err(TableSyncError::Read)?;
    Ok(MySqlSyncRepairTarget::new_with_fk_repair(
        writer,
        source,
        target,
        source_inventory,
        target_inventory,
    ))
}

fn read_source_inventory(config: &SyncTableConfig) -> Result<SchemaInventory, TableSyncError> {
    let reader = MariaDbInventoryReader::new(InventoryConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        endpoint_role: InventoryEndpointRole::Source,
        use_tls: false,
        tls_ca_file: None,
        ..InventoryConfig::default()
    });
    build_inventory(&config.source.database, &reader)
        .map_err(|error| TableSyncError::Read(error.to_string()))
}

fn read_target_inventory(config: &SyncTableConfig) -> Result<SchemaInventory, TableSyncError> {
    let reader = MariaDbInventoryReader::new(InventoryConfig {
        host: config.target.host.clone(),
        port: config.target.port,
        user: config.target.user.clone(),
        password: config.target.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(config.target.tls_ca_file.clone()),
        ..InventoryConfig::default()
    });
    build_inventory(&config.target.database, &reader)
        .map_err(|error| TableSyncError::Read(error.to_string()))
}

fn build_mysql_repair_target(
    config: &SyncTableConfig,
    executor: crate::mysql_client::PersistentTargetExecutor,
) -> MySqlSyncRepairTarget {
    MySqlSyncRepairTarget::new(crate::target::TargetMySqlWriter::from_snapshot_table(
        &snapshot_table(&config.table),
        executor,
        sync_insert_mode(config),
    ))
}

pub(crate) fn expected_sync_run_spec_json(
    config: &SyncTableConfig,
) -> Result<String, TableSyncError> {
    super::range::build_run_spec_json(
        &build_sync_run_scope(config)?,
        &config.table,
        config.chunk_size,
        config.mode,
        &config.start_after,
        &config.end_at,
    )
}

pub(crate) fn build_sync_run_scope(config: &SyncTableConfig) -> Result<String, TableSyncError> {
    let insert_conflict_policy = match config.target.insert_conflict_policy {
        crate::live::InsertConflictPolicy::Error => "error",
        crate::live::InsertConflictPolicy::IgnoreDuplicate => "ignore-duplicate",
        crate::live::InsertConflictPolicy::ReplaceDivergentPk => "replace-divergent-pk",
    };
    serde_json::to_string(&SyncRunScope {
        source_host: &config.source.host,
        source_port: config.source.port,
        source_database: &config.source.database,
        target_host: &config.target.host,
        target_port: config.target.port,
        target_database: &config.target.database,
        insert_conflict_policy,
        plan_hash: config.plan_hash.as_deref(),
    })
    .map_err(|error| TableSyncError::Progress(format!("serialize run scope: {error}")))
}

fn run_sync_table_with_targets_phase(
    config: &SyncTableConfig,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
) -> Result<SyncTableReport, TableSyncError> {
    if phase.is_verification() && config.updated_since.is_some() {
        return Err(TableSyncError::InvalidTable(
            "verify phase cannot use updated_since".to_string(),
        ));
    }
    match &config.updated_since {
        Some(updated_since) => run_recent_update_sync(
            config,
            source,
            repair_target,
            progress_store,
            updated_since.clone(),
        ),
        None => run_range_sync(
            config,
            source,
            target,
            repair_target,
            progress_store,
            phase,
            run_spec_json,
        ),
    }
}

fn run_recent_update_sync(
    config: &SyncTableConfig,
    source: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
    updated_since: UpdatedSince,
) -> Result<SyncTableReport, TableSyncError> {
    sync_recent_updates_with_progress(
        &config.run_id,
        &build_sync_run_scope(config)?,
        RecentUpdateSyncContext {
            table: &config.table,
            chunk_size: config.chunk_size,
            mode: config.mode,
            source,
            repair_target,
            progress_store,
            updated_since,
        },
    )
}

fn run_range_sync(
    config: &SyncTableConfig,
    source: &impl SyncTableReader,
    target: &impl SyncTableReader,
    repair_target: &mut impl SyncRepairTarget,
    progress_store: &mut impl SyncProgressStore,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
) -> Result<SyncTableReport, TableSyncError> {
    sync_table_with_progress_range_phase_with_run_spec(
        RangeSyncRequest {
            table: &config.table,
            options: SyncRunOptions {
                run_id: config.run_id.clone(),
                run_scope: build_sync_run_scope(config)?,
                chunk_size: config.chunk_size,
                mode: config.mode,
                start_after: config.start_after.clone(),
                end_at: config.end_at.clone(),
            },
            source,
            target,
            repair_target,
            progress_store,
            phase,
        },
        run_spec_json,
    )
}

pub(crate) fn find_compatible_failed_run(
    config: &SyncTableConfig,
    phase: SyncPhase,
    table: &str,
) -> Result<Option<SyncRunCandidate>, TableSyncError> {
    if config.mode != SyncMode::Apply || phase != SyncPhase::InsertMissing {
        return Ok(None);
    }
    let mut progress_store = progress::MySqlSyncRunProgressStore::new(
        config.target.clone(),
        config.progress_table.clone(),
    );
    progress_store.ensure()?;
    let mut resumed_config = config.clone();
    resumed_config.mode = SyncMode::MissingPrimaryKeys;
    resumed_config.plan_hash = None;
    let expected_run_spec_json = super::range::build_run_spec_json(
        &build_sync_run_scope(&resumed_config)?,
        &resumed_config.table,
        resumed_config.chunk_size,
        resumed_config.mode,
        &resumed_config.start_after,
        &resumed_config.end_at,
    )?;
    claim_compatible_failed_run(&mut progress_store, table, phase, &expected_run_spec_json)
}

#[cfg(test)]
mod sessions_guest_recovery_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn request() -> crate::live::SessionsGuestRecovery {
        crate::live::SessionsGuestRecovery {
            source_file: "mysqld-bin.002709".to_string(),
            source_start_position: 224_141_039,
            source_end_position: 224_142_261,
            child_event_timestamp: 1_784_246_400,
            schema: "globalcomix".to_string(),
            table: "sessions".to_string(),
            constraint: "fk_sessions_guest".to_string(),
            session_id: "109018328".to_string(),
            guest_id: "78011674".to_string(),
            guest_hash: "fb42c5a9-b717-4022-9f27-6b467e0ca28d515m".to_string(),
        }
    }

    fn guest_row(guest_id: &str, guest_hash: &str) -> crate::snapshot::SnapshotRow {
        let values = [
            ("guest_id", Some(guest_id)),
            ("guest_hash", Some(guest_hash)),
            ("country", Some("us")),
            ("original_ref", Some("https://globalcomix.com/")),
            ("original_uri", Some("/browse")),
            ("first_user_id", None),
            ("geo_region_id", Some("2")),
            ("ui_lang", Some("en")),
            ("device_type", Some("0")),
            ("et_id", None),
            ("utm_medium", Some("organic")),
            ("utm_source", None),
            ("utm_campaign", None),
            ("utm_term", None),
            ("utm_id", None),
            ("http_user_agent", Some("Mozilla/5.0")),
            ("create_time", Some("2026-06-26 00:00:00")),
            (RECOVERY_CREATE_TIME_EPOCH_ALIAS, Some("1782432000")),
            ("is_bot", Some("0")),
            ("params", Some("?reason=recovery")),
            ("application_user_access_token_id", None),
            ("application_id", Some("1")),
            ("supports_cookies", Some("1")),
            ("reason", None),
        ];
        crate::snapshot::SnapshotRow {
            primary_key: vec![guest_id.to_string()],
            values: values
                .into_iter()
                .map(|(column, value)| (column.to_string(), value.map(ToString::to_string)))
                .collect(),
        }
    }

    fn partial_guest_row(guest_id: &str, guest_hash: &str) -> crate::snapshot::SnapshotRow {
        crate::snapshot::SnapshotRow {
            primary_key: vec![guest_id.to_string()],
            values: BTreeMap::from([
                ("guest_id".to_string(), Some(guest_id.to_string())),
                ("guest_hash".to_string(), Some(guest_hash.to_string())),
                (
                    "create_time".to_string(),
                    Some("2026-06-26 00:00:00".to_string()),
                ),
            ]),
        }
    }

    #[test]
    fn rejects_unsupported_conflict_scope() {
        let mut unsupported = request();
        unsupported.constraint = "other_fk".to_string();

        assert!(validate_sessions_guest_request(&unsupported).is_err());
    }

    #[test]
    fn rejects_absent_or_nonmatching_source_parent() {
        let request = request();
        assert!(require_exact_guest_row("source", &[], &request).is_err());
        assert!(
            require_exact_guest_row(
                "source",
                &[guest_row(&request.guest_id, "different")],
                &request,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_target_id_or_hash_collision() {
        let request = request();
        let rows = [
            guest_row(&request.guest_id, "different"),
            guest_row("999", &request.guest_hash),
        ];

        assert!(require_exact_guest_row("target", &rows, &request).is_err());
    }

    #[test]
    fn recovery_order_uses_epoch_not_timezone_rendered_timestamp_text() {
        let request = request();
        let mut earlier_parent = guest_row(&request.guest_id, &request.guest_hash);
        earlier_parent.values.insert(
            "create_time".to_string(),
            Some("2099-12-31 23:59:59".to_string()),
        );
        assert!(plan_loaded_sessions_guest(&[earlier_parent], &[], &request).is_ok());

        let mut later_parent = guest_row(&request.guest_id, &request.guest_hash);
        later_parent.values.insert(
            "create_time".to_string(),
            Some("1970-01-01 00:00:00".to_string()),
        );
        later_parent.values.insert(
            RECOVERY_CREATE_TIME_EPOCH_ALIAS.to_string(),
            Some((request.child_event_timestamp + 1).to_string()),
        );
        assert!(plan_loaded_sessions_guest(&[later_parent], &[], &request).is_err());
    }

    #[test]
    fn rejects_partial_guest_source_image_and_requires_all_canonical_columns() {
        let request = request();
        let rows = [partial_guest_row(&request.guest_id, &request.guest_hash)];

        assert!(require_exact_guest_row("source", &rows, &request).is_err());
        assert_eq!(
            exact_guest_sync_config(&crate::live::ApplyBinlogConfig::default(), &request)
                .table
                .columns
                .len(),
            23
        );
    }

    #[test]
    fn inserts_complete_source_guest_image_with_required_and_nullable_fields() {
        let request = request();
        let source_row = guest_row(&request.guest_id, &request.guest_hash);
        let reconciliation =
            plan_loaded_sessions_guest(std::slice::from_ref(&source_row), &[], &request)
                .expect("plan exact guest insert");
        let GuestReconciliation::Insert(row_to_insert) = reconciliation else {
            panic!("missing target guest must require insert");
        };
        let mut target = crate::table_sync::tests_support::RecordingRepairTarget::default();
        target
            .insert_row(&row_to_insert)
            .expect("insert exact guest image");

        let inserted = target.inserts.borrow();
        assert_eq!(
            inserted.as_slice(),
            std::slice::from_ref(&canonical_guest_row(&source_row))
        );
        assert_eq!(inserted[0].values.len(), 23);
        assert_eq!(inserted[0].values["geo_region_id"].as_deref(), Some("2"));
        assert_eq!(inserted[0].values["first_user_id"], None);
        assert_eq!(
            inserted[0].values["params"].as_deref(),
            Some("?reason=recovery")
        );
    }

    #[test]
    fn accepts_only_complete_existing_parent_matching_source_image() {
        let request = request();
        let source_row = guest_row(&request.guest_id, &request.guest_hash);
        let mut target_row = source_row.clone();
        target_row
            .values
            .insert("country".to_string(), Some("ca".to_string()));

        assert!(
            plan_loaded_sessions_guest(
                std::slice::from_ref(&source_row),
                std::slice::from_ref(&target_row),
                &request,
            )
            .is_err()
        );
        assert!(matches!(
            plan_loaded_sessions_guest(
                std::slice::from_ref(&source_row),
                std::slice::from_ref(&source_row),
                &request,
            ),
            Ok(GuestReconciliation::Existing)
        ));
    }
}

#[cfg(test)]
mod home_feed_card_recovery_tests {
    use super::*;

    fn request() -> crate::live::HomeFeedCardRecovery {
        crate::live::HomeFeedCardRecovery {
            source_file: "mysqld-bin.002709".to_string(),
            source_start_position: 308_259_855,
            source_end_position: 308_261_441,
            child_event_timestamp: 1_784_588_463,
            schema: "globalcomix".to_string(),
            table: "home_feed_card_slides".to_string(),
            constraint: "fk_hfcs_card".to_string(),
            slide_id: "4508905".to_string(),
            card_id: "2492683".to_string(),
        }
    }

    fn card_row(card_id: &str) -> crate::snapshot::SnapshotRow {
        let values = [
            ("id", Some(card_id)),
            ("card_type_id", Some("1")),
            ("status", Some("active")),
            ("reading_direction", Some("l")),
            ("comic_id", Some("10175")),
            ("release_id", Some("50715")),
            ("caption", Some("exact source caption")),
            ("hook_image_url", Some("https://example.test/hook.jpg")),
            ("source_id", Some("50151")),
            ("filter_reason", None),
            ("retired_reason", None),
            ("first_published", None),
            ("last_active_time", Some("2026-07-20 22:01:03")),
            ("view_count", Some("0")),
            ("reaction_count", Some("0")),
            ("click_count", Some("0")),
            ("curator_user_id", None),
            ("curated_score", None),
            ("facets_json", None),
            ("create_time", Some("2026-06-23 05:01:16")),
            (RECOVERY_CREATE_TIME_EPOCH_ALIAS, Some("1782190876")),
        ];
        crate::snapshot::SnapshotRow {
            primary_key: vec![card_id.to_string()],
            values: values
                .into_iter()
                .map(|(column, value)| (column.to_string(), value.map(ToString::to_string)))
                .collect(),
        }
    }

    #[test]
    fn validates_only_exact_home_feed_card_recovery_scope_and_positive_ids() {
        let mut invalid = request();
        invalid.constraint = "other_fk".to_string();
        assert!(validate_home_feed_card_request(&invalid).is_err());

        let mut invalid = request();
        invalid.card_id = "0".to_string();
        assert!(validate_home_feed_card_request(&invalid).is_err());
        assert!(validate_home_feed_card_request(&request()).is_ok());
    }

    #[test]
    fn inserts_all_twenty_canonical_parent_columns() {
        let request = request();
        let source_row = card_row(&request.card_id);
        let source_row =
            require_exact_home_feed_card_row("source", std::slice::from_ref(&source_row), &request)
                .expect("complete exact source row");
        let GuestReconciliation::Insert(row) =
            plan_loaded_home_feed_card(source_row, &[], &request).expect("plan insert")
        else {
            panic!("missing target card must require insert");
        };

        assert_eq!(row.values.len(), HOME_FEED_CARD_COLUMNS.len());
        assert_eq!(row.values["source_id"].as_deref(), Some("50151"));
        assert_eq!(row.values["filter_reason"], None);
        assert_eq!(
            exact_home_feed_card_sync_config(&crate::live::ApplyBinlogConfig::default(), &request,)
                .table
                .columns,
            home_feed_card_columns()
        );
    }

    #[test]
    fn rejects_missing_partial_late_divergent_and_unique_collision_rows() {
        let request = request();
        assert!(require_exact_home_feed_card_row("source", &[], &request).is_err());

        let mut partial = card_row(&request.card_id);
        partial.values.remove("caption");
        assert!(require_exact_home_feed_card_row("source", &[partial], &request).is_err());

        let mut late = card_row(&request.card_id);
        late.values.insert(
            RECOVERY_CREATE_TIME_EPOCH_ALIAS.to_string(),
            Some((request.child_event_timestamp + 1).to_string()),
        );
        assert!(
            validate_parent_temporal_order(
                recovery_create_time_epoch(&late, "home feed card").unwrap(),
                request.child_event_timestamp,
            )
            .is_err()
        );

        let source = card_row(&request.card_id);
        let mut divergent = source.clone();
        divergent
            .values
            .insert("caption".to_string(), Some("different".to_string()));
        assert!(plan_loaded_home_feed_card(&source, &[divergent], &request).is_err());

        let collision = card_row("9999999");
        assert!(
            plan_loaded_home_feed_card(&source, &[source.clone(), collision], &request).is_err()
        );
    }
}
