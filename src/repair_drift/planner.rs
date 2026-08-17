use super::model::*;
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
