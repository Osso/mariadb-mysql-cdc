use super::model::*;
use super::store::{ConflictStore, RepairExecutor, RepairProgressStore};
use crate::snapshot::SnapshotRow;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct DirectionalRepairInventories<'a> {
    pub source_insert_update: &'a RepairInventory,
    pub target_insert_update: &'a RepairInventory,
    pub source_delete: &'a RepairInventory,
    pub target_delete: &'a RepairInventory,
}

pub fn build_repair_plan(
    run_id: &str,
    source_identity: &str,
    target_identity: &str,
    source: &RepairInventory,
    target: &RepairInventory,
) -> Result<RepairPlan, RepairPlanError> {
    build_repair_plan_with_directional_scopes(
        run_id,
        source_identity,
        target_identity,
        DirectionalRepairInventories {
            source_insert_update: source,
            target_insert_update: target,
            source_delete: source,
            target_delete: target,
        },
    )
}

pub(crate) fn build_repair_plan_with_directional_scopes(
    run_id: &str,
    source_identity: &str,
    target_identity: &str,
    inventories: DirectionalRepairInventories<'_>,
) -> Result<RepairPlan, RepairPlanError> {
    validate_inventory_match(
        inventories.source_insert_update,
        inventories.target_insert_update,
    )?;
    validate_inventory_match(inventories.source_delete, inventories.target_delete)?;
    validate_foreign_keys(inventories.source_insert_update)?;
    validate_foreign_keys(inventories.source_delete)?;
    let insert_order = topological_order(
        &inventories.source_insert_update.tables,
        &inventories.source_insert_update.foreign_keys,
    )?;
    let delete_order = reversed(&topological_order(
        &inventories.source_delete.tables,
        &inventories.source_delete.foreign_keys,
    )?);
    let tables = merged_tables(&insert_order, &delete_order);
    let (inventory_hash, plan_hash) =
        build_directional_plan_hashes(run_id, source_identity, target_identity, inventories);
    Ok(assemble_repair_plan(RepairPlanAssemblyInput {
        run_id: run_id.to_string(),
        source_identity: source_identity.to_string(),
        target_identity: target_identity.to_string(),
        inventory_hash,
        plan_hash,
        tables,
        insert_order: insert_order.clone(),
        delete_order,
    }))
}

fn build_directional_plan_hashes(
    run_id: &str,
    source_identity: &str,
    target_identity: &str,
    inventories: DirectionalRepairInventories<'_>,
) -> (String, String) {
    let inventory_hash = stable_hash(&(
        inventories.source_insert_update,
        inventories.target_insert_update,
        inventories.source_delete,
        inventories.target_delete,
    ));
    let plan_hash = stable_hash(&(run_id, source_identity, target_identity, &inventory_hash));
    (inventory_hash, plan_hash)
}

struct RepairPlanAssemblyInput {
    run_id: String,
    source_identity: String,
    target_identity: String,
    inventory_hash: String,
    plan_hash: String,
    tables: Vec<String>,
    insert_order: Vec<String>,
    delete_order: Vec<String>,
}

fn assemble_repair_plan(input: RepairPlanAssemblyInput) -> RepairPlan {
    RepairPlan {
        run_id: input.run_id,
        source_identity: input.source_identity,
        target_identity: input.target_identity,
        inventory_hash: input.inventory_hash,
        plan_hash: input.plan_hash,
        tables: input.tables,
        delete_order: input.delete_order,
        insert_order: input.insert_order.clone(),
        update_order: input.insert_order,
    }
}

fn merged_tables(insert_order: &[String], delete_order: &[String]) -> Vec<String> {
    insert_order
        .iter()
        .chain(delete_order)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn validate_inventory_match(
    source: &RepairInventory,
    target: &RepairInventory,
) -> Result<(), RepairPlanError> {
    if source.schema != target.schema || sorted(&source.tables) != sorted(&target.tables) {
        return Err(RepairPlanError::SchemaMismatch(
            "source and target table inventory differs".to_string(),
        ));
    }
    if sorted_fks(&source.foreign_keys) != sorted_fks(&target.foreign_keys) {
        return Err(RepairPlanError::SchemaMismatch(
            "source and target foreign-key inventory differs".to_string(),
        ));
    }
    Ok(())
}

fn validate_foreign_keys(source: &RepairInventory) -> Result<(), RepairPlanError> {
    source
        .foreign_keys
        .iter()
        .find_map(|fk| {
            (fk.child_schema != source.schema || fk.parent_schema != source.schema).then(|| {
                RepairPlanError::CrossSchema(format!(
                    "cross-schema foreign key {}.{} requires manual review",
                    fk.constraint_schema, fk.constraint_name
                ))
            })
        })
        .map_or(Ok(()), Err)
}

fn sorted_fks(foreign_keys: &[CanonicalForeignKey]) -> Vec<CanonicalForeignKey> {
    let mut sorted = foreign_keys.to_vec();
    sorted.sort();
    sorted
}

fn reversed(values: &[String]) -> Vec<String> {
    let mut reversed = values.to_vec();
    reversed.reverse();
    reversed
}

fn topological_order(
    tables: &[String],
    foreign_keys: &[CanonicalForeignKey],
) -> Result<Vec<String>, RepairPlanError> {
    let table_set = tables.iter().cloned().collect::<BTreeSet<_>>();
    let mut graph = build_dependency_graph(&table_set, foreign_keys)?;
    let result = consume_ready_tables(&mut graph.indegree, &graph.children);
    if result.len() == table_set.len() {
        Ok(result)
    } else {
        Err(RepairPlanError::Cycle(remaining_tables(graph.indegree)))
    }
}

struct DependencyGraph {
    indegree: BTreeMap<String, usize>,
    children: BTreeMap<String, BTreeSet<String>>,
}

fn build_dependency_graph(
    table_set: &BTreeSet<String>,
    foreign_keys: &[CanonicalForeignKey],
) -> Result<DependencyGraph, RepairPlanError> {
    let mut indegree: BTreeMap<String, usize> =
        table_set.iter().map(|table| (table.clone(), 0)).collect();
    let mut children = BTreeMap::new();
    for fk in foreign_keys
        .iter()
        .filter(|fk| fk.enforced && fk.child_table != fk.parent_table)
    {
        validate_dependency(table_set, fk)?;
        if children
            .entry(fk.parent_table.clone())
            .or_insert_with(BTreeSet::new)
            .insert(fk.child_table.clone())
        {
            *indegree.get_mut(&fk.child_table).expect("child table") += 1;
        }
    }
    Ok(DependencyGraph { indegree, children })
}

fn validate_dependency(
    table_set: &BTreeSet<String>,
    fk: &CanonicalForeignKey,
) -> Result<(), RepairPlanError> {
    if table_set.contains(&fk.child_table) && table_set.contains(&fk.parent_table) {
        Ok(())
    } else {
        Err(RepairPlanError::SchemaMismatch(format!(
            "foreign key {} references a table outside repair inventory",
            fk.constraint_name
        )))
    }
}

fn consume_ready_tables(
    indegree: &mut BTreeMap<String, usize>,
    children: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    let mut ready: BTreeSet<String> = indegree
        .iter()
        .filter_map(|(table, degree)| (*degree == 0).then_some(table.clone()))
        .collect();
    let mut result = Vec::with_capacity(indegree.len());
    while let Some(table) = ready.pop_first() {
        result.push(table.clone());
        release_children(&table, indegree, children, &mut ready);
    }
    result
}

fn release_children(
    table: &str,
    indegree: &mut BTreeMap<String, usize>,
    children: &BTreeMap<String, BTreeSet<String>>,
    ready: &mut BTreeSet<String>,
) {
    for child in children.get(table).into_iter().flatten() {
        let degree = indegree.get_mut(child).expect("child degree");
        *degree -= 1;
        if *degree == 0 {
            ready.insert(child.clone());
        }
    }
}

fn remaining_tables(indegree: BTreeMap<String, usize>) -> Vec<String> {
    indegree
        .into_iter()
        .filter_map(|(table, degree)| (degree > 0).then_some(table))
        .collect()
}

fn sorted(values: &[String]) -> Vec<String> {
    let mut values = values.to_vec();
    values.sort();
    values
}

const REPAIR_PLAN_HASH_FNV_OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
const REPAIR_PLAN_HASH_FNV_PRIME: u64 = 1_099_511_628_211;

fn stable_hash<T: Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).expect("repair state serializable");
    let mut hash = REPAIR_PLAN_HASH_FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(REPAIR_PLAN_HASH_FNV_PRIME);
    }
    format!("{hash:016x}")
}

struct ApplyPhaseContext<'a, S, E> {
    state: &'a mut RepairRunState,
    store: &'a mut S,
    executor: &'a mut E,
}

type RepairOperationBuilder = fn(&str, &[SnapshotRow], &[SnapshotRow]) -> Vec<RepairOperation>;

struct ApplyPhaseInput<'a> {
    phase: RepairPhase,
    order: &'a [String],
    input: &'a RepairInput,
    build_operations: RepairOperationBuilder,
}

pub fn run_phased_repair<S, E, C>(
    plan: &RepairPlan,
    input: &RepairInput,
    store: &mut S,
    executor: &mut E,
    conflicts: &mut C,
) -> Result<RepairReport, String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
    C: ConflictStore,
{
    conflicts.ensure()?;
    let mut state = load_repair_state(plan, store)?;
    validate_repair_state(plan, &state)?;
    let result = run_phased_repair_inner(plan, input, &mut state, store, executor, conflicts);
    if let Err(error) = &result {
        save_repair_error(&mut state, store, error);
    }
    result
}

fn load_repair_state<S: RepairProgressStore>(
    plan: &RepairPlan,
    store: &S,
) -> Result<RepairRunState, String> {
    Ok(store.load(&plan.run_id)?.unwrap_or_else(|| RepairRunState {
        run_id: plan.run_id.clone(),
        plan_hash: plan.plan_hash.clone(),
        phase: RepairPhase::Preflight,
        completed_operations: BTreeSet::new(),
        status: "running".to_string(),
        last_error: None,
    }))
}

fn validate_repair_state(plan: &RepairPlan, state: &RepairRunState) -> Result<(), String> {
    if state.plan_hash != plan.plan_hash {
        return Err("repair run immutable plan hash mismatch; start a fresh run".to_string());
    }
    if state.phase == RepairPhase::Complete {
        return Err("repair run is complete; start a fresh run".to_string());
    }
    Ok(())
}

fn save_repair_error<S: RepairProgressStore>(
    state: &mut RepairRunState,
    store: &mut S,
    error: &str,
) {
    state.status = "error".to_string();
    state.last_error = Some(error.to_string());
    let _ = store.save(state);
}

fn run_phased_repair_inner<S, E, C>(
    plan: &RepairPlan,
    input: &RepairInput,
    state: &mut RepairRunState,
    store: &mut S,
    executor: &mut E,
    conflicts: &mut C,
) -> Result<RepairReport, String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
    C: ConflictStore,
{
    run_repair_phases(plan, input, state, store, executor)?;
    verify_and_complete(plan, input, state, store, executor, conflicts)
}

fn run_repair_phases<S, E>(
    plan: &RepairPlan,
    input: &RepairInput,
    state: &mut RepairRunState,
    store: &mut S,
    executor: &mut E,
) -> Result<(), String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
{
    if state.phase == RepairPhase::Preflight {
        run_repair_preflight(plan, input, state, store, executor)?;
    }
    apply_delete_phase(plan, input, state, store, executor)?;
    apply_insert_phase(plan, input, state, store, executor)?;
    apply_update_phase(plan, input, state, store, executor)
}

fn apply_delete_phase<S, E>(
    plan: &RepairPlan,
    input: &RepairInput,
    state: &mut RepairRunState,
    store: &mut S,
    executor: &mut E,
) -> Result<(), String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
{
    apply_phase(
        ApplyPhaseContext {
            state,
            store,
            executor,
        },
        ApplyPhaseInput {
            phase: RepairPhase::DeleteExtras,
            order: &plan.delete_order,
            input,
            build_operations: build_delete_operations,
        },
    )
}

fn apply_insert_phase<S, E>(
    plan: &RepairPlan,
    input: &RepairInput,
    state: &mut RepairRunState,
    store: &mut S,
    executor: &mut E,
) -> Result<(), String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
{
    apply_phase(
        ApplyPhaseContext {
            state,
            store,
            executor,
        },
        ApplyPhaseInput {
            phase: RepairPhase::InsertMissing,
            order: &plan.insert_order,
            input,
            build_operations: build_insert_operations,
        },
    )
}

fn apply_update_phase<S, E>(
    plan: &RepairPlan,
    input: &RepairInput,
    state: &mut RepairRunState,
    store: &mut S,
    executor: &mut E,
) -> Result<(), String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
{
    apply_phase(
        ApplyPhaseContext {
            state,
            store,
            executor,
        },
        ApplyPhaseInput {
            phase: RepairPhase::UpdateDivergent,
            order: &plan.update_order,
            input,
            build_operations: build_update_operations,
        },
    )
}

fn verify_and_complete<S, E, C>(
    plan: &RepairPlan,
    input: &RepairInput,
    state: &mut RepairRunState,
    store: &mut S,
    executor: &mut E,
    conflicts: &mut C,
) -> Result<RepairReport, String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
    C: ConflictStore,
{
    state.phase = RepairPhase::Verify;
    store.save(state)?;
    let actionable_mismatches = verify_repair(plan, input, executor, conflicts)?;
    if actionable_mismatches != 0 {
        return Err(format!(
            "repair verification found {actionable_mismatches} actionable mismatches"
        ));
    }
    complete_repair(state, store)?;
    Ok(build_repair_report(state, actionable_mismatches))
}

fn run_repair_preflight<S, E>(
    _plan: &RepairPlan,
    _input: &RepairInput,
    state: &mut RepairRunState,
    store: &mut S,
    _executor: &mut E,
) -> Result<(), String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
{
    state.phase = RepairPhase::DeleteExtras;
    store.save(state)
}

fn build_delete_operations(
    table: &str,
    source: &[SnapshotRow],
    target: &[SnapshotRow],
) -> Vec<RepairOperation> {
    extra_rows(source, target)
        .into_iter()
        .map(|row| RepairOperation::Delete {
            table: table.to_string(),
            primary_key: row.primary_key.clone(),
        })
        .collect()
}

fn build_insert_operations(
    table: &str,
    source: &[SnapshotRow],
    target: &[SnapshotRow],
) -> Vec<RepairOperation> {
    let target_keys = target
        .iter()
        .map(|row| row.primary_key.clone())
        .collect::<BTreeSet<_>>();
    source
        .iter()
        .filter(|row| !target_keys.contains(&row.primary_key))
        .cloned()
        .map(|row| RepairOperation::Insert {
            table: table.to_string(),
            row,
        })
        .collect()
}

fn build_update_operations(
    table: &str,
    source: &[SnapshotRow],
    target: &[SnapshotRow],
) -> Vec<RepairOperation> {
    let target_by_key = target
        .iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect::<BTreeMap<_, _>>();
    source
        .iter()
        .filter(|row| {
            target_by_key
                .get(&row.primary_key)
                .is_some_and(|target| *target != *row)
        })
        .cloned()
        .map(|row| RepairOperation::Update {
            table: table.to_string(),
            row,
        })
        .collect()
}

fn verify_repair<E, C>(
    plan: &RepairPlan,
    input: &RepairInput,
    executor: &mut E,
    conflicts: &mut C,
) -> Result<usize, String>
where
    E: RepairExecutor,
    C: ConflictStore,
{
    plan.tables.iter().try_fold(0, |total, table| {
        let source = input.source_rows.get(table).cloned().unwrap_or_default();
        let target = executor.target_rows(table);
        Ok(total
            + verify_table(plan, table, &source, &target, conflicts)?
            + count_extra_keys(&source, &target))
    })
}

fn verify_table<C: ConflictStore>(
    plan: &RepairPlan,
    table: &str,
    source: &[SnapshotRow],
    target: &[SnapshotRow],
    conflicts: &mut C,
) -> Result<usize, String> {
    let target_by_key = target
        .iter()
        .map(|row| (row.primary_key.clone(), row))
        .collect::<BTreeMap<_, _>>();
    source.iter().try_fold(0, |mismatches, row| {
        let equal = target_by_key
            .get(&row.primary_key)
            .is_some_and(|target| *target == row);
        conflicts.resolve_if_equal(
            table,
            &row.primary_key,
            equal,
            &plan.run_id,
            "verified source/target row equality",
        )?;
        Ok(mismatches + usize::from(!equal))
    })
}

fn count_extra_keys(source: &[SnapshotRow], target: &[SnapshotRow]) -> usize {
    target
        .iter()
        .filter(|row| {
            !source
                .iter()
                .any(|source| source.primary_key == row.primary_key)
        })
        .count()
}

fn complete_repair<S>(state: &mut RepairRunState, store: &mut S) -> Result<(), String>
where
    S: RepairProgressStore,
{
    state.phase = RepairPhase::Complete;
    state.status = "complete".to_string();
    state.last_error = None;
    store.save(state)
}

fn build_repair_report(state: &RepairRunState, actionable_mismatches: usize) -> RepairReport {
    RepairReport {
        phase: RepairPhase::Complete,
        actionable_mismatches,
        deletes: count_completed_operations(state, RepairPhase::DeleteExtras),
        inserts: count_completed_operations(state, RepairPhase::InsertMissing),
        updates: count_completed_operations(state, RepairPhase::UpdateDivergent),
    }
}

fn count_completed_operations(state: &RepairRunState, phase: RepairPhase) -> usize {
    state
        .completed_operations
        .iter()
        .filter(|operation| operation.phase == phase)
        .count()
}

fn apply_phase<S: RepairProgressStore, E: RepairExecutor>(
    context: ApplyPhaseContext<'_, S, E>,
    input: ApplyPhaseInput<'_>,
) -> Result<(), String> {
    let ApplyPhaseContext {
        state,
        store,
        executor,
    } = context;
    let ApplyPhaseInput {
        phase,
        order,
        input,
        build_operations,
    } = input;
    state.phase = phase.clone();
    store.save(state)?;
    for table in order {
        apply_table_operations(
            state,
            store,
            executor,
            input,
            &phase,
            table,
            build_operations,
        )?;
    }
    Ok(())
}

fn apply_table_operations<S, E>(
    state: &mut RepairRunState,
    store: &mut S,
    executor: &mut E,
    input: &RepairInput,
    phase: &RepairPhase,
    table: &str,
    build_operations: RepairOperationBuilder,
) -> Result<(), String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
{
    let source = input
        .source_rows
        .get(table)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let target = executor.target_rows(table);
    for operation in build_operations(table, source, &target) {
        apply_operation(state, store, executor, phase, operation)?;
    }
    Ok(())
}

fn apply_operation<S, E>(
    state: &mut RepairRunState,
    store: &mut S,
    executor: &mut E,
    phase: &RepairPhase,
    operation: RepairOperation,
) -> Result<(), String>
where
    S: RepairProgressStore,
    E: RepairExecutor,
{
    let key = operation_key(phase, &operation);
    if state.completed_operations.contains(&key) {
        return Ok(());
    }
    executor.apply(&operation)?;
    state.completed_operations.insert(key);
    store.save(state)
}

fn operation_key(phase: &RepairPhase, operation: &RepairOperation) -> RepairOperationKey {
    let (table, primary_key) = match operation {
        RepairOperation::Delete { table, primary_key } => (table, primary_key.clone()),
        RepairOperation::Insert { table, row } | RepairOperation::Update { table, row } => {
            (table, row.primary_key.clone())
        }
    };
    RepairOperationKey {
        phase: phase.clone(),
        table: table.clone(),
        primary_key,
    }
}

fn extra_rows<'a>(source: &'a [SnapshotRow], target: &'a [SnapshotRow]) -> Vec<&'a SnapshotRow> {
    let source_keys = source
        .iter()
        .map(|row| row.primary_key.clone())
        .collect::<BTreeSet<_>>();
    target
        .iter()
        .filter(|row| !source_keys.contains(&row.primary_key))
        .collect()
}
