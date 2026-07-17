use super::config::{parse_repair_drift_config, validate_repair_drift_config};
use super::plan::{
    RepairTableInputs, build_runtime_repair_plan, collect_repair_table_inputs, drifted_table_names,
    ordered_candidate_tables,
};
use super::{RepairDriftConfig, RepairDriftError, RepairDriftReport, RepairDriftTableReport};
use crate::drift_check::{self, DriftCheckConfig};
use crate::inventory::{InventoryConfig, InventoryEndpointRole, SchemaInventory, build_inventory};
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::mysql_support::SOURCE_TLS_CA_FILE;
use crate::table_sync::{self, SyncMode, SyncPhase, SyncTable, SyncTableConfig};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static RUN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn run_repair_drift(
    config: &RepairDriftConfig,
) -> Result<RepairDriftReport, RepairDriftError> {
    validate_repair_drift_config(config).map_err(RepairDriftError::Config)?;
    let run_id = configured_run_id(config);
    let (source, target) = load_inventories(config)?;
    let plan = build_runtime_repair_plan(config, &run_id, &source, &target)?;
    let tables = ordered_candidate_tables(config, &source, &plan)?;
    execute_repair_drift(config, run_id, source, target, plan, tables)
}

fn configured_run_id(config: &RepairDriftConfig) -> String {
    config
        .run_id
        .clone()
        .unwrap_or_else(|| fresh_run_id(&config.run_id_prefix))
}

fn load_inventories(
    config: &RepairDriftConfig,
) -> Result<(SchemaInventory, SchemaInventory), RepairDriftError> {
    let source = build_endpoint_inventory(&config.source, InventoryEndpointRole::Source)
        .map_err(|error| RepairDriftError::Inventory(error.to_string()))?;
    let target = build_target_inventory(&config.target)
        .map_err(|error| RepairDriftError::Inventory(error.to_string()))?;
    Ok((source, target))
}

fn execute_repair_drift(
    config: &RepairDriftConfig,
    run_id: String,
    source: SchemaInventory,
    target: SchemaInventory,
    plan: crate::repair_drift::RepairPlan,
    tables: Vec<String>,
) -> Result<RepairDriftReport, RepairDriftError> {
    if tables.is_empty() {
        return Ok(empty_report(&run_id, &source, &target));
    }
    let drift = run_drift_check(config, tables.clone())?;
    let (repair_tables, skipped) =
        collect_repair_table_inputs(&tables, &drift.comparisons, &source, &target);
    let repaired = run_repair_phases(config, &run_id, &plan, &repair_tables)?;
    Ok(RepairDriftReport {
        run_id,
        source_tables: source.tables.len(),
        target_tables: target.tables.len(),
        compared_tables: drift.comparisons.len(),
        drifted_tables: drifted_table_names(&drift.comparisons).len(),
        repaired,
        skipped,
    })
}

fn empty_report(
    run_id: &str,
    source: &SchemaInventory,
    target: &SchemaInventory,
) -> RepairDriftReport {
    RepairDriftReport {
        run_id: run_id.to_string(),
        source_tables: source.tables.len(),
        target_tables: target.tables.len(),
        compared_tables: 0,
        drifted_tables: 0,
        repaired: Vec::new(),
        skipped: Vec::new(),
    }
}

fn run_drift_check(
    config: &RepairDriftConfig,
    tables: Vec<String>,
) -> Result<crate::drift_check::DriftCheckReport, RepairDriftError> {
    drift_check::run_drift_check(&build_drift_check_config(config, tables))
        .map_err(|error| RepairDriftError::DriftCheck(error.to_string()))
}

pub(crate) fn run_repair_drift_command(args: Vec<String>, usage: &str) {
    let config = match parse_repair_drift_config(args) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}\n\n{usage}");
            std::process::exit(2);
        }
    };
    print_repair_result(run_repair_drift(&config));
}

fn print_repair_result(result: Result<RepairDriftReport, RepairDriftError>) {
    match result {
        Ok(report) => println!("{}", format_repair_drift_report(&report)),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn format_repair_drift_report(report: &RepairDriftReport) -> String {
    let mut lines = vec![format_summary_line(report)];
    lines.extend(report.repaired.iter().map(format_repaired_line));
    lines.extend(report.skipped.iter().map(format_skipped_line));
    lines.join("\n")
}

fn format_summary_line(report: &RepairDriftReport) -> String {
    format!(
        "repair_drift run_id={} source_tables={} target_tables={} compared_tables={} drifted_tables={} repaired_tables={} skipped_tables={}",
        report.run_id,
        report.source_tables,
        report.target_tables,
        report.compared_tables,
        report.drifted_tables,
        report.repaired.len(),
        report.skipped.len()
    )
}

fn format_repaired_line(table: &RepairDriftTableReport) -> String {
    format!(
        "repair_drift_table table={} run_id={} source_count={} target_count={} inserts={} updates={} extra_target_rows={}",
        table.table,
        table.run_id,
        table.source_count,
        table.target_count,
        table.sync_report.inserts,
        table.sync_report.updates,
        table.sync_report.extra_target_rows
    )
}

fn format_skipped_line(table: &super::RepairDriftSkip) -> String {
    format!(
        "repair_drift_skipped table={} reason={}",
        table.table,
        serde_json::to_string(&table.reason).expect("serialize skip reason")
    )
}

pub(crate) fn build_drift_check_config(
    config: &RepairDriftConfig,
    tables: Vec<String>,
) -> DriftCheckConfig {
    DriftCheckConfig {
        source: config.source.clone(),
        target: config.target.clone(),
        tables,
        content_check: config.content_check,
        chunk_size: config.chunk_size,
    }
}

fn run_repair_phases(
    config: &RepairDriftConfig,
    run_id: &str,
    plan: &crate::repair_drift::RepairPlan,
    repair_tables: &RepairTableInputs,
) -> Result<Vec<RepairDriftTableReport>, RepairDriftError> {
    let mut conflict_store = initialize_conflict_store(config)?;
    let mut state = RepairState {
        deleted_rows: 0,
        repaired_by_table: BTreeMap::new(),
    };
    for (phase, order) in repair_phases(plan) {
        for table_name in order {
            run_repair_phase_for_table(RepairPhaseRequest {
                config,
                run_id,
                plan,
                repair_tables,
                phase,
                table_name,
                run: RepairPhaseRun {
                    conflict_store: &mut conflict_store,
                    state: &mut state,
                },
            })?;
        }
    }
    Ok(state.repaired_by_table.into_values().collect())
}

struct RepairState {
    deleted_rows: u64,
    repaired_by_table: BTreeMap<String, RepairDriftTableReport>,
}

fn repair_phases(plan: &crate::repair_drift::RepairPlan) -> [(SyncPhase, &[String]); 4] {
    [
        (SyncPhase::DeleteExtras, &plan.delete_order),
        (SyncPhase::InsertMissing, &plan.insert_order),
        (SyncPhase::UpdateDivergent, &plan.update_order),
        (SyncPhase::Verify, &plan.update_order),
    ]
}

fn initialize_conflict_store(
    config: &RepairDriftConfig,
) -> Result<Option<crate::conflict_repair::MySqlConflictStore>, RepairDriftError> {
    if config.mode != SyncMode::Apply {
        return Ok(None);
    }
    let store =
        crate::conflict_repair::MySqlConflictStore::new(&config.target, "cdc.row_conflicts")
            .map_err(RepairDriftError::Repair)?;
    store.ensure().map_err(RepairDriftError::Repair)?;
    Ok(Some(store))
}

struct RepairPhaseRun<'a> {
    conflict_store: &'a mut Option<crate::conflict_repair::MySqlConflictStore>,
    state: &'a mut RepairState,
}

struct RepairPhaseRequest<'a> {
    config: &'a RepairDriftConfig,
    run_id: &'a str,
    plan: &'a crate::repair_drift::RepairPlan,
    repair_tables: &'a RepairTableInputs,
    phase: SyncPhase,
    table_name: &'a str,
    run: RepairPhaseRun<'a>,
}

fn run_repair_phase_for_table(request: RepairPhaseRequest<'_>) -> Result<(), RepairDriftError> {
    let Some((source_count, target_count, table)) = request.repair_tables.get(request.table_name)
    else {
        return Ok(());
    };
    let phase_config = phase_config_for_request(&request, table);
    let phase_report = run_sync_phase(&phase_config, request.phase, request.table_name)?;
    complete_phase(request, source_count, target_count, phase_report)
}

fn phase_config_for_request(
    request: &RepairPhaseRequest<'_>,
    table: &SyncTable,
) -> SyncTableConfig {
    build_phase_config(
        request.config,
        request.plan,
        request.phase,
        table,
        request.run_id,
        request.table_name,
        request.run.state.deleted_rows,
    )
}

fn complete_phase(
    request: RepairPhaseRequest<'_>,
    source_count: &u64,
    target_count: &u64,
    phase_report: table_sync::SyncTableReport,
) -> Result<(), RepairDriftError> {
    record_phase(
        RecordPhaseContext {
            config: request.config,
            run_id: request.run_id,
            phase: request.phase,
            table_name: request.table_name,
            run: request.run,
        },
        RecordPhaseInput {
            source_count: *source_count,
            target_count: *target_count,
            phase_report,
        },
    )
}

fn build_phase_config(
    config: &RepairDriftConfig,
    plan: &crate::repair_drift::RepairPlan,
    phase: SyncPhase,
    table: &SyncTable,
    run_id: &str,
    table_name: &str,
    deleted_rows: u64,
) -> SyncTableConfig {
    let phase_run_id = child_run_id(&format!("{}-{}", run_id, phase_name(phase)), table_name);
    let mut phase_config = sync_config(config, table.clone(), phase_run_id, &plan.plan_hash);
    phase_config.max_deletes = phase_max_deletes(config, phase, deleted_rows);
    phase_config
}

struct RecordPhaseContext<'a> {
    config: &'a RepairDriftConfig,
    run_id: &'a str,
    phase: SyncPhase,
    table_name: &'a str,
    run: RepairPhaseRun<'a>,
}

struct RecordPhaseInput {
    source_count: u64,
    target_count: u64,
    phase_report: table_sync::SyncTableReport,
}

fn record_phase(
    context: RecordPhaseContext<'_>,
    input: RecordPhaseInput,
) -> Result<(), RepairDriftError> {
    context.run.state.deleted_rows += input.phase_report.extra_target_rows;
    resolve_verified_conflicts(
        context.config,
        context.run_id,
        context.phase,
        context.table_name,
        &input.phase_report,
        context.run.conflict_store.as_mut(),
    )?;
    merge_phase_report(
        &mut context.run.state.repaired_by_table,
        context.run_id,
        context.table_name,
        input.source_count,
        input.target_count,
        input.phase_report,
    );
    Ok(())
}

fn run_sync_phase(
    config: &SyncTableConfig,
    phase: SyncPhase,
    table_name: &str,
) -> Result<table_sync::SyncTableReport, RepairDriftError> {
    table_sync::run_sync_table_phase(config, phase)
        .map_err(|error| RepairDriftError::Repair(format!("{table_name} {phase:?}: {error}")))
}

fn sync_config(
    config: &RepairDriftConfig,
    table: SyncTable,
    run_id: String,
    plan_hash: &str,
) -> SyncTableConfig {
    SyncTableConfig {
        source: config.source.clone(),
        target: config.target.clone(),
        table,
        chunk_size: config.chunk_size,
        mode: config.mode,
        progress_table: config.progress_table.clone(),
        run_id,
        start_after: config.start_after.clone(),
        end_at: config.end_at.clone(),
        max_deletes: config.max_deletes,
        updated_since: None,
        plan_hash: Some(plan_hash.to_string()),
    }
}

fn phase_max_deletes(
    config: &RepairDriftConfig,
    phase: SyncPhase,
    deleted_rows: u64,
) -> Option<u64> {
    if phase == SyncPhase::DeleteExtras {
        return config
            .max_deletes
            .map(|limit| limit.saturating_sub(deleted_rows));
    }
    Some(0)
}

fn resolve_verified_conflicts(
    config: &RepairDriftConfig,
    run_id: &str,
    phase: SyncPhase,
    table_name: &str,
    report: &table_sync::SyncTableReport,
    conflict_store: Option<&mut crate::conflict_repair::MySqlConflictStore>,
) -> Result<(), RepairDriftError> {
    if !can_resolve_verified_conflicts_after_verify(config, phase, report) {
        return Ok(());
    }
    let Some(store) = conflict_store else {
        return Ok(());
    };
    store
        .resolve_verified_table(
            &config.source_identity,
            table_name,
            run_id,
            &verified_conflict_evidence(table_name),
        )
        .map_err(RepairDriftError::Repair)
}

pub(crate) fn can_resolve_verified_conflicts(config: &RepairDriftConfig) -> bool {
    config.start_after.is_none() && config.end_at.is_none()
}

pub(crate) fn can_resolve_verified_conflicts_after_verify(
    config: &RepairDriftConfig,
    phase: SyncPhase,
    report: &table_sync::SyncTableReport,
) -> bool {
    phase == SyncPhase::Verify
        && can_resolve_verified_conflicts(config)
        && report.inserts == 0
        && report.updates == 0
        && report.extra_target_rows == 0
}

pub(crate) fn verified_conflict_evidence(table_name: &str) -> String {
    format!("verified source/target equality for table `{table_name}` across full-table scope")
}

fn merge_phase_report(
    repaired_by_table: &mut BTreeMap<String, RepairDriftTableReport>,
    run_id: &str,
    table_name: &str,
    source_count: u64,
    target_count: u64,
    phase_report: table_sync::SyncTableReport,
) {
    let entry = repaired_by_table
        .entry(table_name.to_string())
        .or_insert_with(|| new_table_report(run_id, table_name, source_count, target_count));
    entry.sync_report.chunks += phase_report.chunks;
    entry.sync_report.rows_scanned += phase_report.rows_scanned;
    entry.sync_report.inserts += phase_report.inserts;
    entry.sync_report.updates += phase_report.updates;
    entry.sync_report.extra_target_rows += phase_report.extra_target_rows;
}

fn new_table_report(
    run_id: &str,
    table_name: &str,
    source_count: u64,
    target_count: u64,
) -> RepairDriftTableReport {
    RepairDriftTableReport {
        table: table_name.to_string(),
        run_id: run_id.to_string(),
        source_count,
        target_count,
        sync_report: table_sync::SyncTableReport {
            table: table_name.to_string(),
            ..Default::default()
        },
    }
}

fn build_endpoint_inventory(
    source: &MySqlConnectionConfig,
    endpoint_role: InventoryEndpointRole,
) -> Result<SchemaInventory, crate::inventory::InventoryError> {
    let reader = crate::inventory::MariaDbInventoryReader::new(InventoryConfig {
        host: source.host.clone(),
        port: source.port,
        user: source.user.clone(),
        password: source.password.clone(),
        endpoint_role,
        use_tls: true,
        tls_ca_file: Some(source_tls_ca_file(source)),
        ..InventoryConfig::default()
    });
    build_inventory(&source.database, &reader)
}

fn source_tls_ca_file(source: &MySqlConnectionConfig) -> String {
    source
        .tls_ca_file
        .clone()
        .unwrap_or_else(|| SOURCE_TLS_CA_FILE.to_string())
}

fn build_target_inventory(
    target: &crate::live::TargetMySqlConfig,
) -> Result<SchemaInventory, crate::inventory::InventoryError> {
    let reader = crate::inventory::MariaDbInventoryReader::new(InventoryConfig {
        host: target.host.clone(),
        port: target.port,
        user: target.user.clone(),
        password: target.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(target.tls_ca_file.clone()),
        ..InventoryConfig::default()
    });
    build_inventory(&target.database, &reader)
}

pub(crate) fn fresh_run_id(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let sequence = RUN_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{millis}-{}-{sequence}", std::process::id())
}

fn phase_name(phase: SyncPhase) -> &'static str {
    match phase {
        SyncPhase::All => "all",
        SyncPhase::DeleteExtras => "delete-extras",
        SyncPhase::InsertMissing => "insert-missing",
        SyncPhase::UpdateDivergent => "update-divergent",
        SyncPhase::Verify => "verify",
    }
}

fn child_run_id(run_id: &str, table: &str) -> String {
    let slug = table
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("{run_id}-{slug}")
}
