use super::config::{parse_repair_drift_config, validate_repair_drift_config};
use super::equivalent_conflicts::reconcile_exact_equivalent_conflicts;
use super::plan::{
    RepairTableInputs, build_runtime_recovery_repair_plan, build_runtime_repair_plan,
    collect_full_repair_table_inputs, collect_repair_table_inputs, drifted_table_names,
    ordered_candidate_tables,
};
use super::{
    EquivalentConflictReport, RepairDriftConfig, RepairDriftError, RepairDriftReport,
    RepairDriftTableReport,
};
use crate::drift_check::{self, DriftCheckConfig};
use crate::inventory::{InventoryConfig, InventoryEndpointRole, SchemaInventory, build_inventory};
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::table_sync::{self, SyncMode, SyncPhase, SyncTable, SyncTableConfig};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const PROGRESS_RUN_ID_MAX_BYTES: usize = 128;
const TRUNCATED_RUN_ID_HASH_CHARACTERS: usize = 32;
const RUN_ID_HASH_SEPARATOR: &str = "-";

static RUN_COUNTER: AtomicU64 = AtomicU64::new(0);
pub(crate) fn run_repair_drift(
    config: &RepairDriftConfig,
) -> Result<RepairDriftReport, RepairDriftError> {
    #[cfg(feature = "integration-failpoints")]
    crate::live::configure_integration_failpoint(config.integration_failpoint);
    validate_repair_drift_config(config).map_err(RepairDriftError::Config)?;
    let run_id = configured_run_id(config);
    let (source, target) = load_inventories(config)?;
    let plan = build_runtime_repair_plan(config, &run_id, &source, &target)?;
    let tables = ordered_candidate_tables(config, &source, &plan)?;
    execute_repair_drift(config, run_id, source, target, plan, tables)
}

pub(crate) fn run_consistent_snapshot_repair(
    config: &RepairDriftConfig,
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: SchemaInventory,
    target_inventory: SchemaInventory,
) -> Result<RepairDriftReport, RepairDriftError> {
    let prepared =
        prepare_consistent_snapshot_repair(config, &source_inventory, &target_inventory)?;
    let repaired = run_consistent_snapshot_repair_phases(
        config,
        &prepared.run_id,
        &prepared.plan,
        &prepared.repair_tables,
        shared_source,
        &source_inventory,
        &target_inventory,
    )?;
    Ok(consistent_snapshot_repair_report(
        prepared,
        repaired,
        &source_inventory,
        &target_inventory,
    ))
}

fn consistent_snapshot_repair_report(
    prepared: ConsistentSnapshotRepairPlan,
    repaired: Vec<RepairDriftTableReport>,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> RepairDriftReport {
    let compared_tables = prepared.repair_tables.len();
    RepairDriftReport {
        run_id: prepared.run_id,
        source_tables: source_inventory.tables.len(),
        target_tables: target_inventory.tables.len(),
        compared_tables,
        drifted_tables: compared_tables,
        equivalent_conflicts: EquivalentConflictReport::default(),
        repaired,
        skipped: Vec::new(),
    }
}

struct ConsistentSnapshotRepairPlan {
    run_id: String,
    plan: crate::repair_drift::RepairPlan,
    repair_tables: RepairTableInputs,
}

fn prepare_consistent_snapshot_repair(
    config: &RepairDriftConfig,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<ConsistentSnapshotRepairPlan, RepairDriftError> {
    validate_repair_drift_config(config).map_err(RepairDriftError::Config)?;
    validate_complete_snapshot_repair_config(config)?;
    let run_id = configured_run_id(config);
    let plan =
        build_runtime_recovery_repair_plan(config, &run_id, source_inventory, target_inventory)?;
    let tables = ordered_candidate_tables(config, source_inventory, &plan)?;
    let (repair_tables, skipped) =
        collect_full_repair_table_inputs(&tables, source_inventory, target_inventory);
    require_supported_snapshot_scope(&skipped)?;
    Ok(ConsistentSnapshotRepairPlan {
        run_id,
        plan,
        repair_tables,
    })
}

fn require_supported_snapshot_scope(
    skipped: &[crate::repair_drift::RepairDriftSkip],
) -> Result<(), RepairDriftError> {
    if skipped.is_empty() {
        return Ok(());
    }
    let reasons = skipped
        .iter()
        .map(|item| format!("{}: {}", item.table, item.reason))
        .collect::<Vec<_>>()
        .join("; ");
    Err(RepairDriftError::Repair(format!(
        "consistent snapshot scope is unsupported: {reasons}"
    )))
}

fn validate_complete_snapshot_repair_config(
    config: &RepairDriftConfig,
) -> Result<(), RepairDriftError> {
    if config.mode != SyncMode::Apply
        || !config.tables.is_empty()
        || config.start_after.is_some()
        || config.end_at.is_some()
    {
        return Err(RepairDriftError::Config(
            "lost-binlog recovery requires full-scope apply without table or range filters"
                .to_string(),
        ));
    }
    Ok(())
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
    let equivalent_conflicts =
        reconcile_exact_equivalent_conflicts(config, &run_id, &source, &tables)?;
    if config.conflict_reconcile_limit > 0 || tables.is_empty() {
        return Ok(empty_report(
            &run_id,
            &source,
            &target,
            equivalent_conflicts,
        ));
    }
    let drift = run_drift_check(config, tables.clone())?;
    let (repair_tables, skipped) =
        collect_repair_table_inputs(&plan.tables, &drift.comparisons, &source, &target);
    let repaired = run_repair_phases(config, &run_id, &plan, &repair_tables)?;
    Ok(RepairDriftReport {
        run_id,
        source_tables: source.tables.len(),
        target_tables: target.tables.len(),
        compared_tables: drift.comparisons.len(),
        drifted_tables: drifted_table_names(&drift.comparisons).len(),
        equivalent_conflicts,
        repaired,
        skipped,
    })
}

fn run_consistent_snapshot_repair_phases(
    config: &RepairDriftConfig,
    run_id: &str,
    plan: &crate::repair_drift::RepairPlan,
    repair_tables: &RepairTableInputs,
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<Vec<RepairDriftTableReport>, RepairDriftError> {
    let mut conflict_store = initialize_conflict_store(config)?;
    let mut state = RepairState::default();
    for (phase, order) in repair_phases(plan) {
        run_consistent_snapshot_phase_order(
            ConsistentSnapshotPhaseContext {
                config,
                run_id,
                plan,
                repair_tables,
                phase,
                conflict_store: &mut conflict_store,
                state: &mut state,
            },
            order,
            Rc::clone(&shared_source),
            source_inventory,
            target_inventory,
        )?;
    }
    Ok(state.repaired_by_table.into_values().collect())
}

struct ConsistentSnapshotPhaseContext<'a> {
    config: &'a RepairDriftConfig,
    run_id: &'a str,
    plan: &'a crate::repair_drift::RepairPlan,
    repair_tables: &'a RepairTableInputs,
    phase: SyncPhase,
    conflict_store: &'a mut Option<crate::conflict_repair::MySqlConflictStore>,
    state: &'a mut RepairState,
}

fn run_consistent_snapshot_phase_order(
    mut context: ConsistentSnapshotPhaseContext<'_>,
    order: &[String],
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<(), RepairDriftError> {
    let batches = phase_batches(
        context.phase,
        order,
        source_inventory,
        context.config.parallelism,
    );
    for batch in batches {
        log_phase_batch(context.phase, batch.len(), context.config.parallelism);
        run_consistent_snapshot_phase_batch(
            &mut context,
            &batch,
            Rc::clone(&shared_source),
            source_inventory,
            target_inventory,
        )?;
    }
    Ok(())
}

fn log_phase_batch(phase: SyncPhase, batch_size: usize, parallelism: usize) {
    println!(
        "repair_drift_phase phase={phase:?} batch_tables={batch_size} source_connections={}",
        batch_size.min(parallelism)
    );
}

fn run_consistent_snapshot_phase_batch(
    context: &mut ConsistentSnapshotPhaseContext<'_>,
    batch: &[String],
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<(), RepairDriftError> {
    if let [table_name] = batch {
        return run_sequential_consistent_snapshot_phase_table(
            context,
            table_name,
            shared_source,
            source_inventory,
            target_inventory,
        );
    }
    let results = run_parallel_consistent_snapshot_phase_batch(
        context,
        batch,
        source_inventory,
        target_inventory,
    )?;
    record_parallel_phase_results(context, results)
}

fn run_sequential_consistent_snapshot_phase_table(
    context: &mut ConsistentSnapshotPhaseContext<'_>,
    table_name: &str,
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<(), RepairDriftError> {
    run_repair_phase_for_table_with_consistent_source(
        RepairPhaseRequest {
            config: context.config,
            run_id: context.run_id,
            plan: context.plan,
            repair_tables: context.repair_tables,
            phase: context.phase,
            table_name,
            run: RepairPhaseRun {
                conflict_store: context.conflict_store,
                state: context.state,
            },
        },
        shared_source,
        source_inventory,
        target_inventory,
    )
}

fn phase_batches(
    phase: SyncPhase,
    order: &[String],
    source_inventory: &SchemaInventory,
    parallelism: usize,
) -> Vec<Vec<String>> {
    let direction = match phase {
        SyncPhase::DeleteExtras => DependencyDirection::ChildFirst,
        SyncPhase::InsertMissing | SyncPhase::UpdateDivergent => DependencyDirection::ParentFirst,
        SyncPhase::Verify | SyncPhase::VerifyNoTargetExtras | SyncPhase::All => {
            return parallel_phase_batches(
                order,
                &[],
                DependencyDirection::ParentFirst,
                parallelism,
            );
        }
    };
    parallel_phase_batches(
        order,
        &source_inventory.foreign_keys,
        direction,
        parallelism,
    )
}

fn run_parallel_consistent_snapshot_phase_batch(
    context: &ConsistentSnapshotPhaseContext<'_>,
    batch: &[String],
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<Vec<Option<ParallelPhaseResult>>, RepairDriftError> {
    let worker_context = ParallelPhaseTableContext {
        config: context.config,
        run_id: context.run_id,
        plan: context.plan,
        repair_tables: context.repair_tables,
        phase: context.phase,
        source_inventory,
        target_inventory,
    };
    spawn_parallel_phase_workers(&worker_context, batch)
}

fn spawn_parallel_phase_workers(
    context: &ParallelPhaseTableContext<'_>,
    batch: &[String],
) -> Result<Vec<Option<ParallelPhaseResult>>, RepairDriftError> {
    std::thread::scope(|scope| {
        let handles = batch
            .iter()
            .map(|table_name| scope.spawn(move || run_parallel_phase_worker(context, table_name)))
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle.join().map_err(|_| {
                    RepairDriftError::Repair("parallel table worker panicked".to_string())
                })?
            })
            .collect()
    })
}

fn run_parallel_phase_worker(
    context: &ParallelPhaseTableContext<'_>,
    table_name: &str,
) -> Result<Option<ParallelPhaseResult>, RepairDriftError> {
    let source = Rc::new(
        crate::mysql_client::PersistentMySqlSource::new_without_operation_timeout(
            &context.config.source,
        )
        .map_err(|error| {
            RepairDriftError::Repair(format!(
                "{table_name} {:?}: connect source: {error}",
                context.phase
            ))
        })?,
    );
    run_parallel_consistent_snapshot_phase_table(context, table_name, source)
}

fn record_parallel_phase_results(
    context: &mut ConsistentSnapshotPhaseContext<'_>,
    results: Vec<Option<ParallelPhaseResult>>,
) -> Result<(), RepairDriftError> {
    for result in results.into_iter().flatten() {
        complete_phase(
            RepairPhaseRequest {
                config: context.config,
                run_id: context.run_id,
                plan: context.plan,
                repair_tables: context.repair_tables,
                phase: context.phase,
                table_name: &result.table_name,
                run: RepairPhaseRun {
                    conflict_store: context.conflict_store,
                    state: context.state,
                },
            },
            &result.source_count,
            &result.target_count,
            result.phase_report,
        )?;
    }
    Ok(())
}

struct ParallelPhaseResult {
    table_name: String,
    source_count: u64,
    target_count: u64,
    phase_report: table_sync::SyncTableReport,
}

struct ParallelPhaseTableContext<'a> {
    config: &'a RepairDriftConfig,
    run_id: &'a str,
    plan: &'a crate::repair_drift::RepairPlan,
    repair_tables: &'a RepairTableInputs,
    phase: SyncPhase,
    source_inventory: &'a SchemaInventory,
    target_inventory: &'a SchemaInventory,
}

fn run_parallel_consistent_snapshot_phase_table(
    context: &ParallelPhaseTableContext<'_>,
    table_name: &str,
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
) -> Result<Option<ParallelPhaseResult>, RepairDriftError> {
    let Some((source_count, target_count, table)) = context.repair_tables.get(table_name) else {
        return Ok(None);
    };
    let (phase_config, run_spec_json) = resumable_phase_config_parts(
        context.config,
        context.plan,
        context.phase,
        table,
        context.run_id,
        table_name,
    )
    .map_err(RepairDriftError::Repair)?;
    let phase_report = table_sync::run_sync_table_phase_with_consistent_source(
        &phase_config,
        context.phase,
        run_spec_json.as_deref(),
        shared_source,
        context.source_inventory,
        context.target_inventory,
    )
    .map_err(|error| {
        RepairDriftError::Repair(format!("{table_name} {:?}: {error}", context.phase))
    })?;
    Ok(Some(ParallelPhaseResult {
        table_name: table_name.to_string(),
        source_count: *source_count,
        target_count: *target_count,
        phase_report,
    }))
}

fn empty_report(
    run_id: &str,
    source: &SchemaInventory,
    target: &SchemaInventory,
    equivalent_conflicts: EquivalentConflictReport,
) -> RepairDriftReport {
    RepairDriftReport {
        run_id: run_id.to_string(),
        source_tables: source.tables.len(),
        target_tables: target.tables.len(),
        compared_tables: 0,
        drifted_tables: 0,
        equivalent_conflicts,
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
        "repair_drift run_id={} source_tables={} target_tables={} compared_tables={} drifted_tables={} equivalent_conflicts_examined={} equivalent_conflicts_resolved={} equivalent_conflicts_deferred={} repaired_tables={} skipped_tables={}",
        report.run_id,
        report.source_tables,
        report.target_tables,
        report.compared_tables,
        report.drifted_tables,
        report.equivalent_conflicts.examined,
        report.equivalent_conflicts.resolved,
        report.equivalent_conflicts.deferred,
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
    let mut state = RepairState::default();
    for (phase, order) in repair_phases(plan) {
        run_repair_phase_order(
            RepairPhaseOrderContext {
                config,
                run_id,
                plan,
                repair_tables,
                conflict_store: &mut conflict_store,
                state: &mut state,
            },
            phase,
            order,
        )?;
    }
    Ok(state.repaired_by_table.into_values().collect())
}

struct RepairPhaseOrderContext<'a> {
    config: &'a RepairDriftConfig,
    run_id: &'a str,
    plan: &'a crate::repair_drift::RepairPlan,
    repair_tables: &'a RepairTableInputs,
    conflict_store: &'a mut Option<crate::conflict_repair::MySqlConflictStore>,
    state: &'a mut RepairState,
}

fn run_repair_phase_order(
    context: RepairPhaseOrderContext<'_>,
    phase: SyncPhase,
    order: &[String],
) -> Result<(), RepairDriftError> {
    if phase == SyncPhase::Verify {
        return run_verification_phases(
            context.config,
            context.run_id,
            context.plan,
            context.repair_tables,
            context.conflict_store,
            context.state,
        );
    }
    run_nonverification_phase_order(context, phase, order)
}

fn run_nonverification_phase_order(
    context: RepairPhaseOrderContext<'_>,
    phase: SyncPhase,
    order: &[String],
) -> Result<(), RepairDriftError> {
    for table_name in order {
        run_repair_phase_for_table(RepairPhaseRequest {
            config: context.config,
            run_id: context.run_id,
            plan: context.plan,
            repair_tables: context.repair_tables,
            phase,
            table_name,
            run: RepairPhaseRun {
                conflict_store: context.conflict_store,
                state: context.state,
            },
        })?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VerifyScope {
    FullEquality,
    NoTargetExtras,
}

#[derive(Default)]
struct RepairState {
    repaired_by_table: BTreeMap<String, RepairDriftTableReport>,
    observed_verify_scopes: BTreeMap<String, VerifyScope>,
}

fn repair_phases(plan: &crate::repair_drift::RepairPlan) -> [(SyncPhase, &[String]); 4] {
    [
        (SyncPhase::DeleteExtras, &plan.delete_order),
        (SyncPhase::InsertMissing, &plan.insert_order),
        (SyncPhase::UpdateDivergent, &plan.update_order),
        (SyncPhase::Verify, &plan.tables),
    ]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DependencyDirection {
    ParentFirst,
    ChildFirst,
}

fn parallel_phase_batches(
    order: &[String],
    foreign_keys: &[crate::inventory::ForeignKeyInventory],
    direction: DependencyDirection,
    parallelism: usize,
) -> Vec<Vec<String>> {
    let parallelism = parallelism.max(1);
    let levels = phase_dependency_levels(order, foreign_keys, direction);
    split_phase_levels(order, &levels, parallelism)
}

fn phase_dependency_levels(
    order: &[String],
    foreign_keys: &[crate::inventory::ForeignKeyInventory],
    direction: DependencyDirection,
) -> BTreeMap<String, usize> {
    let positions = order
        .iter()
        .enumerate()
        .map(|(index, table)| (table.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut levels = BTreeMap::new();

    for table in order {
        let level = foreign_keys
            .iter()
            .filter_map(|foreign_key| dependency_table(foreign_key, table, direction))
            .filter(|dependency| {
                positions
                    .get(dependency.as_str())
                    .is_some_and(|position| *position < positions[table.as_str()])
            })
            .filter_map(|dependency| levels.get(dependency.as_str()).copied())
            .map(|dependency_level| dependency_level + 1)
            .max()
            .unwrap_or(0);
        levels.insert(table.clone(), level);
    }
    levels
}

fn split_phase_levels(
    order: &[String],
    levels: &BTreeMap<String, usize>,
    parallelism: usize,
) -> Vec<Vec<String>> {
    let max_level = levels.values().copied().max().unwrap_or(0);
    (0..=max_level)
        .flat_map(|level| {
            let tables = order
                .iter()
                .filter(|table| levels.get(table.as_str()) == Some(&level))
                .cloned()
                .collect::<Vec<_>>();
            tables
                .chunks(parallelism)
                .map(|batch| batch.to_vec())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn dependency_table(
    foreign_key: &crate::inventory::ForeignKeyInventory,
    table: &str,
    direction: DependencyDirection,
) -> Option<String> {
    match direction {
        DependencyDirection::ParentFirst if foreign_key.table == table => {
            Some(foreign_key.referenced_table.clone())
        }
        DependencyDirection::ChildFirst if foreign_key.referenced_table == table => {
            Some(foreign_key.table.clone())
        }
        _ => None,
    }
}

fn run_verification_phases(
    config: &RepairDriftConfig,
    run_id: &str,
    plan: &crate::repair_drift::RepairPlan,
    repair_tables: &RepairTableInputs,
    conflict_store: &mut Option<crate::conflict_repair::MySqlConflictStore>,
    state: &mut RepairState,
) -> Result<(), RepairDriftError> {
    for table_name in &plan.tables {
        let Some(scope) = state.observed_verify_scopes.get(table_name).copied() else {
            continue;
        };
        let phase = match scope {
            VerifyScope::FullEquality => SyncPhase::Verify,
            VerifyScope::NoTargetExtras => SyncPhase::VerifyNoTargetExtras,
        };
        run_repair_phase_for_table(RepairPhaseRequest {
            config,
            run_id,
            plan,
            repair_tables,
            phase,
            table_name,
            run: RepairPhaseRun {
                conflict_store,
                state,
            },
        })?;
    }
    Ok(())
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
    let mut phase_config = phase_config_for_request(&request, table);
    let candidate =
        table_sync::find_compatible_failed_run(&phase_config, request.phase, request.table_name)
            .map_err(|error| {
                RepairDriftError::Repair(format!(
                    "{} {:?}: {error}",
                    request.table_name, request.phase
                ))
            })?;
    let run_spec_json = candidate
        .as_ref()
        .map(|candidate| candidate.run_spec_json.clone());
    if let Some(candidate) = candidate {
        phase_config.run_id = candidate.run_id;
        phase_config.mode = SyncMode::MissingPrimaryKeys;
    }
    let phase_report = run_sync_phase(
        &phase_config,
        request.phase,
        run_spec_json.as_deref(),
        request.table_name,
    )?;
    complete_phase(request, source_count, target_count, phase_report)
}

fn run_repair_phase_for_table_with_consistent_source(
    request: RepairPhaseRequest<'_>,
    shared_source: Rc<crate::mysql_client::PersistentMySqlSource>,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<(), RepairDriftError> {
    let Some((source_count, target_count, table)) = request.repair_tables.get(request.table_name)
    else {
        return Ok(());
    };
    let (phase_config, run_spec_json) = resumable_phase_config(&request, table)?;
    let phase_report = table_sync::run_sync_table_phase_with_consistent_source(
        &phase_config,
        request.phase,
        run_spec_json.as_deref(),
        shared_source,
        source_inventory,
        target_inventory,
    )
    .map_err(|error| repair_phase_error(&request, error))?;
    complete_phase(request, source_count, target_count, phase_report)
}

fn resumable_phase_config(
    request: &RepairPhaseRequest<'_>,
    table: &SyncTable,
) -> Result<(SyncTableConfig, Option<String>), RepairDriftError> {
    resumable_phase_config_parts(
        request.config,
        request.plan,
        request.phase,
        table,
        request.run_id,
        request.table_name,
    )
    .map_err(|error| repair_phase_error(request, error))
}

fn resumable_phase_config_parts(
    config: &RepairDriftConfig,
    plan: &crate::repair_drift::RepairPlan,
    phase: SyncPhase,
    table: &SyncTable,
    run_id: &str,
    table_name: &str,
) -> Result<(SyncTableConfig, Option<String>), String> {
    let mut config = build_phase_config(config, plan, phase, table, run_id, table_name);
    let candidate = table_sync::find_compatible_failed_run(&config, phase, table_name)
        .map_err(|error| error.to_string())?;
    let run_spec_json = candidate
        .as_ref()
        .map(|candidate| candidate.run_spec_json.clone());
    if let Some(candidate) = candidate {
        config.run_id = candidate.run_id;
        config.mode = SyncMode::MissingPrimaryKeys;
    }
    Ok((config, run_spec_json))
}

fn repair_phase_error(
    request: &RepairPhaseRequest<'_>,
    error: impl std::fmt::Display,
) -> RepairDriftError {
    RepairDriftError::Repair(format!(
        "{} {:?}: {error}",
        request.table_name, request.phase
    ))
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
) -> SyncTableConfig {
    let phase_run_id = child_run_id(&format!("{}-{}", run_id, phase_name(phase)), table_name);
    sync_config(config, table.clone(), phase_run_id, &plan.plan_hash)
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
    observe_verify_scope(
        &mut context.run.state.observed_verify_scopes,
        context.phase,
        context.table_name,
        &input.phase_report,
    );
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

fn observe_verify_scope(
    scopes: &mut BTreeMap<String, VerifyScope>,
    phase: SyncPhase,
    table_name: &str,
    _report: &table_sync::SyncTableReport,
) {
    match phase {
        SyncPhase::DeleteExtras => {
            scopes.insert(table_name.to_string(), VerifyScope::NoTargetExtras);
        }
        SyncPhase::InsertMissing | SyncPhase::UpdateDivergent => {
            scopes.insert(table_name.to_string(), VerifyScope::FullEquality);
        }
        _ => {}
    }
}

fn run_sync_phase(
    config: &SyncTableConfig,
    phase: SyncPhase,
    run_spec_json: Option<&str>,
    table_name: &str,
) -> Result<table_sync::SyncTableReport, RepairDriftError> {
    table_sync::run_sync_table_phase_with_run_spec(config, phase, run_spec_json)
        .map_err(|error| RepairDriftError::Repair(format!("{table_name} {phase:?}: {error}")))
}

pub(crate) fn sync_config(
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
        updated_since: None,
        plan_hash: Some(plan_hash.to_string()),
    }
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
        use_tls: false,
        tls_ca_file: None,
        ..InventoryConfig::default()
    });
    build_inventory(&source.database, &reader)
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
        SyncPhase::VerifyNoTargetExtras => "verify-no-target-extras",
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
    limit_progress_run_id(format!("{run_id}-{slug}"))
}

fn limit_progress_run_id(candidate: String) -> String {
    if candidate.len() <= PROGRESS_RUN_ID_MAX_BYTES {
        return candidate;
    }

    let hash = format!("{:x}", Sha256::digest(candidate.as_bytes()));
    let prefix_limit =
        PROGRESS_RUN_ID_MAX_BYTES - TRUNCATED_RUN_ID_HASH_CHARACTERS - RUN_ID_HASH_SEPARATOR.len();
    let prefix = utf8_prefix(&candidate, prefix_limit);
    format!(
        "{prefix}{RUN_ID_HASH_SEPARATOR}{}",
        &hash[..TRUNCATED_RUN_ID_HASH_CHARACTERS]
    )
}

fn utf8_prefix(value: &str, max_bytes: usize) -> &str {
    let end = (0..=max_bytes.min(value.len()))
        .rev()
        .find(|index| value.is_char_boundary(*index))
        .unwrap_or_default();
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_endpoint_inventory_uses_plaintext_without_ca() {
        let source = MySqlConnectionConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            user: "reader".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
        };

        let error = build_endpoint_inventory(&source, InventoryEndpointRole::Source)
            .expect_err("source inventory connection should fail");
        let message = error.to_string();

        assert!(!message.contains("TLS CA file"));
    }

    #[test]
    fn target_endpoint_inventory_keeps_configured_ca() {
        let target = crate::live::TargetMySqlConfig {
            host: "target-db".to_string(),
            tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
            ..crate::live::TargetMySqlConfig::default()
        };
        let inventory = InventoryConfig {
            host: target.host.clone(),
            port: target.port,
            user: target.user.clone(),
            password: target.password.clone(),
            endpoint_role: InventoryEndpointRole::Target,
            use_tls: true,
            tls_ca_file: Some(target.tls_ca_file.clone()),
            ..InventoryConfig::default()
        };

        let opts =
            crate::inventory::reader::inventory_opts(&inventory).expect("target inventory TLS");
        let ssl = opts.get_ssl_opts().expect("target TLS configured");

        assert_eq!(
            ssl.root_cert_path(),
            Some(std::path::Path::new(&target.tls_ca_file))
        );
        assert!(!ssl.skip_domain_validation());
        assert!(!ssl.accept_invalid_certs());
    }

    #[test]
    fn observed_phase_reports_select_verification_property() {
        let mut scopes = BTreeMap::new();
        observe_verify_scope(
            &mut scopes,
            SyncPhase::DeleteExtras,
            "orders",
            &table_sync::SyncTableReport::default(),
        );
        observe_verify_scope(
            &mut scopes,
            SyncPhase::InsertMissing,
            "customers",
            &table_sync::SyncTableReport::default(),
        );
        observe_verify_scope(
            &mut scopes,
            SyncPhase::UpdateDivergent,
            "invoices",
            &table_sync::SyncTableReport::default(),
        );

        assert_eq!(scopes["orders"], VerifyScope::NoTargetExtras);
        assert_eq!(scopes["customers"], VerifyScope::FullEquality);
        assert_eq!(scopes["invoices"], VerifyScope::FullEquality);
    }

    #[test]
    fn verify_phase_covers_union_of_directional_execution_scopes() {
        let plan = crate::conflict_repair::RepairPlan {
            run_id: "run".to_string(),
            source_identity: "source".to_string(),
            target_identity: "target".to_string(),
            inventory_hash: "inventory".to_string(),
            plan_hash: "plan".to_string(),
            tables: vec!["customers".to_string(), "orders".to_string()],
            delete_order: vec!["orders".to_string(), "customers".to_string()],
            insert_order: vec!["customers".to_string()],
            update_order: vec!["customers".to_string()],
        };

        let phases = repair_phases(&plan);

        assert_eq!(phases[3].0, SyncPhase::Verify);
        assert_eq!(phases[3].1, plan.tables.as_slice());
    }

    #[test]
    fn parallel_phase_batches_keep_foreign_key_dependencies_in_separate_batches() {
        let foreign_keys = vec![
            crate::inventory::ForeignKeyInventory {
                table: "children".to_string(),
                name: "children_parent_a".to_string(),
                columns: vec!["parent_a_id".to_string()],
                referenced_schema: "globalcomix".to_string(),
                referenced_table: "parent_a".to_string(),
                referenced_columns: vec!["id".to_string()],
            },
            crate::inventory::ForeignKeyInventory {
                table: "children".to_string(),
                name: "children_parent_b".to_string(),
                columns: vec!["parent_b_id".to_string()],
                referenced_schema: "globalcomix".to_string(),
                referenced_table: "parent_b".to_string(),
                referenced_columns: vec!["id".to_string()],
            },
        ];

        let insert_batches = parallel_phase_batches(
            &["parent_a", "parent_b", "children"]
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>(),
            &foreign_keys,
            DependencyDirection::ParentFirst,
            16,
        );
        let delete_batches = parallel_phase_batches(
            &["children", "parent_a", "parent_b"]
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>(),
            &foreign_keys,
            DependencyDirection::ChildFirst,
            16,
        );

        assert_eq!(
            insert_batches,
            vec![vec!["parent_a", "parent_b"], vec!["children"]]
        );
        assert_eq!(
            delete_batches,
            vec![vec!["children"], vec!["parent_a", "parent_b"]]
        );
    }

    #[test]
    fn parallel_phase_batches_cap_unrelated_tables_at_requested_parallelism() {
        let tables = (0..20).map(|index| format!("table_{index}"));
        let batches = parallel_phase_batches(
            &tables.clone().collect::<Vec<_>>(),
            &[],
            DependencyDirection::ParentFirst,
            16,
        );

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 16);
        assert_eq!(batches[1].len(), 4);
    }

    #[test]
    fn child_run_id_bounds_the_failed_resync_table_without_losing_determinism() {
        let phase_run_id = "resync-stream:globalcomix-prod-mariadb-resync-2026-08-13-delete-extras";
        let failed_table = "paid_subscriptions_users_pages_2026_03_07_invalidation_backup";

        let first = child_run_id(phase_run_id, failed_table);
        let repeated = child_run_id(phase_run_id, failed_table);
        let neighboring = child_run_id(
            phase_run_id,
            "paid_subscriptions_users_pages_2026_03_08_invalidation_backup",
        );

        assert!(
            first.len() <= 128,
            "run ID was {} bytes: {first}",
            first.len()
        );
        assert_eq!(first, repeated);
        assert_ne!(first, neighboring);
    }

    #[test]
    fn child_run_id_preserves_existing_ids_that_fit_the_progress_schema() {
        let phase_run_id = "x".repeat(125);
        let expected = format!("{phase_run_id}-ab");

        assert_eq!(expected.len(), 128);
        assert_eq!(child_run_id(&phase_run_id, "ab"), expected);
    }
}
