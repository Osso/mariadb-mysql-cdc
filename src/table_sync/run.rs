use super::mysql::{GUEST_COLUMNS, MySqlSyncReader, guest_columns};
use super::range::sync_table_with_progress_range;
use super::recent::{RecentUpdateSyncContext, sync_recent_updates_with_progress};
use super::*;
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
            max_deletes: Some(0),
        },
        source,
        target,
        repair_target,
        progress_store,
    )
}

pub(crate) fn reconcile_exact_sessions_guest(
    config: &crate::live::ApplyBinlogConfig,
    request: &crate::live::SessionsGuestRecovery,
) -> Result<(), TableSyncError> {
    validate_sessions_guest_request(request)?;
    let source_config = crate::mysql_snapshot::MySqlConnectionConfig {
        host: config.source.host.clone(),
        port: config.source.port,
        user: config.source.user.clone(),
        password: config.source.password.clone(),
        database: config.source.database.clone().ok_or_else(|| {
            TableSyncError::InvalidTable(
                "sessions guest recovery requires source database".to_string(),
            )
        })?,
    };
    let source = MySqlSyncReader::new_with_tls_ca(
        source_config,
        (!config.source.tls_ca_file.is_empty()).then(|| config.source.tls_ca_file.clone()),
    );
    let target = MySqlSyncReader::new_with_target(
        target_connection_config_for_apply(config),
        &config.target,
    )
    .map_err(TableSyncError::Read)?;
    let source_rows = source.read_guest_identity_rows(&request.guest_id, &request.guest_hash)?;
    let target_rows = target.read_guest_identity_rows(&request.guest_id, &request.guest_hash)?;
    let reconciliation = plan_loaded_sessions_guest(&source_rows, &target_rows, request)?;
    let GuestReconciliation::Insert(source_row) = reconciliation else {
        return Ok(());
    };
    let sync_config = exact_guest_sync_config(config, request);
    mysql_repair_target(&sync_config)?.insert_row(source_row)
}

fn validate_sessions_guest_request(
    request: &crate::live::SessionsGuestRecovery,
) -> Result<(), TableSyncError> {
    if request.schema == "globalcomix"
        && request.table == "sessions"
        && request.constraint == "fk_sessions_guest"
        && !request.session_id.is_empty()
        && !request.guest_id.is_empty()
        && !request.guest_hash.is_empty()
    {
        return Ok(());
    }
    Err(TableSyncError::InvalidTable(
        "unsupported sessions guest recovery request".to_string(),
    ))
}

enum GuestReconciliation<'a> {
    Insert(&'a crate::snapshot::SnapshotRow),
    Existing,
}

fn plan_loaded_sessions_guest<'a>(
    source_rows: &'a [crate::snapshot::SnapshotRow],
    target_rows: &[crate::snapshot::SnapshotRow],
    request: &crate::live::SessionsGuestRecovery,
) -> Result<GuestReconciliation<'a>, TableSyncError> {
    let source_row = require_exact_guest_row("source", source_rows, request)?;
    validate_parent_temporal_order(
        source_row
            .values
            .get("create_time")
            .and_then(Option::as_deref),
        request.child_event_timestamp,
    )?;
    if target_rows.is_empty() {
        return Ok(GuestReconciliation::Insert(source_row));
    }
    let target_row = require_exact_guest_row("target", target_rows, request)?;
    if target_row.values != source_row.values {
        return Err(TableSyncError::Repair(
            "target guests row diverges from exact source image".to_string(),
        ));
    }
    Ok(GuestReconciliation::Existing)
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
    let has_complete_image = row.values.len() == GUEST_COLUMNS.len()
        && GUEST_COLUMNS
            .iter()
            .all(|column| row.values.contains_key(*column));
    if !has_exact_identity || !has_complete_image {
        return Err(guest_identity_error(side));
    }
    Ok(row)
}

fn guest_identity_error(side: &str) -> TableSyncError {
    TableSyncError::Repair(format!(
        "{side} guests identity is absent, colliding, divergent, or incomplete"
    ))
}

fn validate_parent_temporal_order(
    create_time: Option<&str>,
    child_event_timestamp: u64,
) -> Result<(), TableSyncError> {
    if child_event_timestamp == 0 {
        return Err(TableSyncError::Repair(
            "sessions guest recovery child event timestamp is missing".to_string(),
        ));
    }
    let create_time = create_time.ok_or_else(|| {
        TableSyncError::Repair("sessions guest recovery parent create_time is missing".to_string())
    })?;
    let parent_timestamp = parse_mysql_datetime(create_time).ok_or_else(|| {
        TableSyncError::Repair("sessions guest recovery parent create_time is invalid".to_string())
    })?;
    if parent_timestamp > child_event_timestamp {
        return Err(TableSyncError::Repair(
            "sessions guest recovery parent was created after child event".to_string(),
        ));
    }
    Ok(())
}

fn parse_mysql_datetime(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() != 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b' '
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = value[0..4].parse::<i64>().ok()?;
    let month = value[5..7].parse::<u32>().ok()?;
    let day = value[8..10].parse::<u32>().ok()?;
    let hour = value[11..13].parse::<u32>().ok()?;
    let minute = value[14..16].parse::<u32>().ok()?;
    let second = value[17..19].parse::<u32>().ok()?;
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let days = days_from_civil(year, month, day)?;
    u64::try_from(days)
        .ok()
        .map(|days| days * 86_400 + u64::from(hour * 3_600 + minute * 60 + second))
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year.rem_euclid(400) == 0 || year.rem_euclid(4) == 0 && year.rem_euclid(100) != 0 => {
            29
        }
        2 => 28,
        _ => 31,
    }
}

fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    (days >= 0).then_some(days)
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
            database: "globalcomix".to_string(),
        },
        target: config.target.clone(),
        table: SyncTable {
            name: "guests".to_string(),
            primary_key: vec!["guest_id".to_string()],
            columns: guest_columns(),
        },
        chunk_size: 1,
        mode: SyncMode::MissingPrimaryKeys,
        progress_table: "cdc.sync_table_progress".to_string(),
        run_id: format!("stream-sessions-{}", request.session_id),
        start_after: None,
        end_at: Some(vec![request.guest_id.clone()]),
        max_deletes: Some(0),
        updated_since: None,
        plan_hash: None,
    }
}

pub fn run_sync_table(config: &SyncTableConfig) -> Result<SyncTableReport, TableSyncError> {
    retry_sync_table_operation(
        config.mode,
        SYNC_CONNECTION_ATTEMPTS,
        SYNC_CONNECTION_RETRY_DELAY,
        || run_sync_table_phase(config, SyncPhase::All),
    )
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
    let attempts = if mode == SyncMode::MissingPrimaryKeys {
        max_attempts.max(1)
    } else {
        1
    };
    for attempt in 1..=attempts {
        match operation() {
            Ok(report) => return Ok(report),
            Err(error) if attempt < attempts && is_retryable_connection_error(&error) => {
                if !retry_delay.is_zero() {
                    std::thread::sleep(retry_delay);
                }
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("sync retry loop has at least one attempt")
}

fn is_retryable_connection_error(error: &TableSyncError) -> bool {
    if matches!(error, TableSyncError::Read(_) | TableSyncError::Progress(_)) {
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
}

fn mysql_repair_target(config: &SyncTableConfig) -> Result<MySqlSyncRepairTarget, TableSyncError> {
    let executor = crate::mysql_client::PersistentTargetExecutor::new(&config.target)
        .map_err(|error| TableSyncError::Repair(error.to_string()))?;
    Ok(MySqlSyncRepairTarget::new(
        crate::target::TargetMySqlWriter::from_snapshot_table(
            &snapshot_table(&config.table),
            executor,
            sync_insert_mode(config),
        ),
    ))
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
                max_deletes: config.max_deletes,
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
        resumed_config.max_deletes,
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
    fn rejects_parent_created_after_child_event() {
        assert!(
            validate_parent_temporal_order(Some("2026-07-18 00:00:00"), 1_784_246_400,).is_err()
        );
        assert!(validate_parent_temporal_order(None, 1_784_246_400).is_err());
        assert!(validate_parent_temporal_order(Some("not-a-timestamp"), 1_784_246_400).is_err());
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
            .insert_row(row_to_insert)
            .expect("insert exact guest image");

        let inserted = target.inserts.borrow();
        assert_eq!(inserted.as_slice(), std::slice::from_ref(&source_row));
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
