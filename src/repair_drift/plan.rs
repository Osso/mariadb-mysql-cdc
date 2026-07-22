use super::{RepairDriftConfig, RepairDriftError, RepairDriftSkip};
use crate::conflict_repair::{
    CanonicalForeignKey, DirectionalRepairInventories, RepairInventory, RepairPlan,
    RepairPlanError, build_repair_plan, build_repair_plan_with_directional_scopes,
};
use crate::drift_check::DriftComparison;
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, SchemaInventory, TableInventory,
    build_canonical_foreign_key_inventory,
};
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::table_sync::SyncTable;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) type RepairTableInputs = BTreeMap<String, (u64, u64, SyncTable)>;

struct DependencyRepairScopes {
    insert_update: RepairInventory,
    delete: RepairInventory,
}

pub(crate) fn build_fk_aware_repair_plan(
    run_id: &str,
    source_identity: &str,
    target_identity: &str,
    source: &RepairInventory,
    target: &RepairInventory,
    max_deletes: u64,
) -> Result<RepairPlan, RepairPlanError> {
    build_repair_plan(
        run_id,
        source_identity,
        target_identity,
        source,
        target,
        max_deletes,
    )
}

pub(crate) fn order_table_names(
    all_tables: &[String],
    parent_first: &[String],
) -> Result<Vec<String>, String> {
    let available = all_tables.iter().collect::<BTreeSet<_>>();
    let mut ordered = explicit_parent_order(parent_first, &available)?;
    let seen = ordered.iter().collect::<BTreeSet<_>>();
    let mut remaining = all_tables
        .iter()
        .filter(|table| !seen.contains(table))
        .cloned()
        .collect::<Vec<_>>();
    remaining.sort();
    ordered.extend(remaining);
    Ok(ordered)
}

fn explicit_parent_order(
    parent_first: &[String],
    available: &BTreeSet<&String>,
) -> Result<Vec<String>, String> {
    let mut ordered = Vec::new();
    let mut seen = BTreeSet::new();
    for table in parent_first {
        if !available.contains(table) {
            return Err(format!(
                "parent-first table `{table}` is not in the repair inventory"
            ));
        }
        if seen.insert(table) {
            ordered.push(table.clone());
        }
    }
    Ok(ordered)
}

pub(crate) fn drifted_table_names(comparisons: &[DriftComparison]) -> Vec<String> {
    comparisons
        .iter()
        .filter(|comparison| !comparison.matches())
        .map(|comparison| comparison.table.clone())
        .collect()
}

pub(crate) fn candidate_table_names(
    config: &RepairDriftConfig,
    source: &SchemaInventory,
) -> Result<Vec<String>, String> {
    let source_names = source
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    if config.tables.is_empty() {
        return Ok(source_names.into_iter().map(str::to_string).collect());
    }
    selected_table_names(config, &source_names)
}

fn selected_table_names(
    config: &RepairDriftConfig,
    source_names: &BTreeSet<&str>,
) -> Result<Vec<String>, String> {
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    for table in &config.tables {
        if !source_names.contains(table.as_str()) {
            return Err(format!("table `{table}` is not in the source inventory"));
        }
        if seen.insert(table.as_str()) {
            selected.push(table.clone());
        }
    }
    Ok(selected)
}

pub(crate) fn compatible_sync_table(
    source: &TableInventory,
    target: &TableInventory,
    skipped: &mut Vec<RepairDriftSkip>,
) -> Option<SyncTable> {
    if let Some(reason) = primary_key_compatibility_error(source, target) {
        skipped.push(skip_table(source, reason));
        return None;
    }
    let columns = sync_columns(source);
    if let Some(reason) = missing_target_columns(&columns, target) {
        skipped.push(skip_table(source, reason));
        return None;
    }
    Some(SyncTable {
        name: source.name.clone(),
        primary_key: source.primary_key.clone(),
        columns,
    })
}

fn primary_key_compatibility_error(
    source: &TableInventory,
    target: &TableInventory,
) -> Option<String> {
    if source.primary_key.is_empty() {
        return Some("source table has no primary key".to_string());
    }
    if source.primary_key != target.primary_key {
        return Some("source and target primary keys differ".to_string());
    }
    None
}

fn sync_columns(source: &TableInventory) -> Vec<String> {
    source
        .columns
        .iter()
        .filter(|column| column.generated.is_none())
        .map(|column| column.name.clone())
        .collect()
}

fn missing_target_columns(columns: &[String], target: &TableInventory) -> Option<String> {
    let target_columns = target
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    let missing = columns
        .iter()
        .filter(|column| !target_columns.contains(column.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    (!missing.is_empty()).then(|| {
        format!(
            "target table is missing source columns: {}",
            missing.join(", ")
        )
    })
}

fn skip_table(source: &TableInventory, reason: String) -> RepairDriftSkip {
    RepairDriftSkip {
        table: source.name.clone(),
        reason,
    }
}

pub(crate) fn collect_repair_table_inputs(
    ordered_tables: &[String],
    comparisons: &[DriftComparison],
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> (RepairTableInputs, Vec<RepairDriftSkip>) {
    let source_by_name = index_inventory_tables(source_inventory);
    let target_by_name = index_inventory_tables(target_inventory);
    let mut inputs = BTreeMap::new();
    let mut skipped = Vec::new();
    for table_name in ordered_tables {
        collect_repair_table(
            table_name,
            comparisons,
            &source_by_name,
            &target_by_name,
            &mut inputs,
            &mut skipped,
        );
    }
    (inputs, skipped)
}

fn collect_repair_table(
    table_name: &str,
    comparisons: &[DriftComparison],
    source_by_name: &BTreeMap<&str, &TableInventory>,
    target_by_name: &BTreeMap<&str, &TableInventory>,
    inputs: &mut RepairTableInputs,
    skipped: &mut Vec<RepairDriftSkip>,
) {
    let Some(comparison) = comparisons.iter().find(|item| item.table == table_name) else {
        return;
    };
    match collect_repair_table_input(comparison, source_by_name, target_by_name) {
        Ok(Some(input)) => {
            inputs.insert(comparison.table.clone(), input);
        }
        Ok(None) => {}
        Err(skip) => skipped.push(skip),
    }
}

fn index_inventory_tables(inventory: &SchemaInventory) -> BTreeMap<&str, &TableInventory> {
    inventory
        .tables
        .iter()
        .map(|table| (table.name.as_str(), table))
        .collect()
}

fn collect_repair_table_input(
    comparison: &DriftComparison,
    source_by_name: &BTreeMap<&str, &TableInventory>,
    target_by_name: &BTreeMap<&str, &TableInventory>,
) -> Result<Option<(u64, u64, SyncTable)>, RepairDriftSkip> {
    let source_count = required_count(
        comparison.source_count,
        comparison,
        "source count unavailable",
    )?;
    let target_count = required_count(
        comparison.target_count,
        comparison,
        "target table is missing from inventory",
    )?;
    let source_table = required_table(
        source_by_name,
        comparison,
        "source table is missing from inventory",
    )?;
    let target_table = required_table(
        target_by_name,
        comparison,
        "target table is missing from inventory",
    )?;
    let table = compatible_table_or_skip(comparison, source_table, target_table)?;
    Ok(Some((source_count, target_count, table)))
}

fn compatible_table_or_skip(
    comparison: &DriftComparison,
    source: &TableInventory,
    target: &TableInventory,
) -> Result<SyncTable, RepairDriftSkip> {
    let mut skips = Vec::new();
    compatible_sync_table(source, target, &mut skips).ok_or_else(|| {
        skips
            .pop()
            .unwrap_or_else(|| repair_drift_skip(comparison, "table is not repairable"))
    })
}

fn required_count(
    count: Option<u64>,
    comparison: &DriftComparison,
    reason: &str,
) -> Result<u64, RepairDriftSkip> {
    count.ok_or_else(|| repair_drift_skip(comparison, reason))
}

fn required_table<'a>(
    tables: &'a BTreeMap<&str, &'a TableInventory>,
    comparison: &DriftComparison,
    reason: &str,
) -> Result<&'a TableInventory, RepairDriftSkip> {
    tables
        .get(comparison.table.as_str())
        .copied()
        .ok_or_else(|| repair_drift_skip(comparison, reason))
}

fn repair_drift_skip(comparison: &DriftComparison, reason: &str) -> RepairDriftSkip {
    RepairDriftSkip {
        table: comparison.table.clone(),
        reason: reason.to_string(),
    }
}

pub(crate) fn build_runtime_repair_plan(
    config: &RepairDriftConfig,
    run_id: &str,
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
) -> Result<RepairPlan, RepairDriftError> {
    let source = build_source_repair_inventory(config, source_inventory)?;
    let target = build_target_repair_inventory(config, target_inventory)?;
    build_plan(config, run_id, source, target)
}

fn build_source_repair_inventory(
    config: &RepairDriftConfig,
    inventory: &SchemaInventory,
) -> Result<DependencyRepairScopes, RepairDriftError> {
    let repair_inventory = build_repair_inventory(
        &config.source,
        InventoryEndpointRole::Source,
        false,
        None,
        inventory,
    )
    .map_err(|error| RepairDriftError::Inventory(error.to_string()))?;
    Ok(reduce_to_dependency_scopes(
        repair_inventory,
        selected_tables_for_closure(config, inventory),
    ))
}

fn build_target_repair_inventory(
    config: &RepairDriftConfig,
    inventory: &SchemaInventory,
) -> Result<DependencyRepairScopes, RepairDriftError> {
    let target_source = target_as_connection_config(config);
    let mut repair_inventory = build_repair_inventory(
        &target_source,
        InventoryEndpointRole::Target,
        true,
        Some(&config.target.tls_ca_file),
        inventory,
    )
    .map_err(|error| RepairDriftError::Inventory(error.to_string()))?;
    exclude_progress_table(&mut repair_inventory, &config.progress_table);
    Ok(reduce_to_dependency_scopes(
        repair_inventory,
        selected_tables_for_closure(config, inventory),
    ))
}

fn target_as_connection_config(config: &RepairDriftConfig) -> MySqlConnectionConfig {
    MySqlConnectionConfig {
        host: config.target.host.clone(),
        port: config.target.port,
        user: config.target.user.clone(),
        password: config.target.password.clone(),
        database: config.target.database.clone(),
    }
}

fn build_plan(
    config: &RepairDriftConfig,
    run_id: &str,
    source: DependencyRepairScopes,
    target: DependencyRepairScopes,
) -> Result<RepairPlan, RepairDriftError> {
    build_repair_plan_with_directional_scopes(
        run_id,
        &config.source_identity,
        &format!("{}:{}", config.target.host, config.target.database),
        DirectionalRepairInventories {
            source_insert_update: &source.insert_update,
            target_insert_update: &target.insert_update,
            source_delete: &source.delete,
            target_delete: &target.delete,
        },
        config.max_deletes.unwrap_or(0),
    )
    .map_err(|error| RepairDriftError::Inventory(error.to_string()))
}

pub(crate) fn exclude_progress_table(inventory: &mut RepairInventory, progress_table: &str) {
    let (schema, table) =
        crate::mysql_support::qualified_table_parts(&inventory.schema, progress_table);
    if schema == inventory.schema {
        inventory.tables.retain(|name| name != &table);
        inventory
            .foreign_keys
            .retain(|fk| fk.child_table != table && fk.parent_table != table);
    }
}

fn selected_tables_for_closure(
    config: &RepairDriftConfig,
    inventory: &SchemaInventory,
) -> Vec<String> {
    if config.tables.is_empty() {
        inventory
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect()
    } else {
        config.tables.clone()
    }
}

#[cfg(test)]
pub(crate) fn reduce_to_dependency_closure(
    inventory: RepairInventory,
    selected_tables: Vec<String>,
) -> RepairInventory {
    let scopes = reduce_to_dependency_scopes(inventory.clone(), selected_tables);
    merge_dependency_scopes(&inventory, &scopes)
}

fn reduce_to_dependency_scopes(
    inventory: RepairInventory,
    selected_tables: Vec<String>,
) -> DependencyRepairScopes {
    let selected = selected_tables.into_iter().collect::<BTreeSet<_>>();
    let insert_update_tables =
        directional_table_closure(&inventory.foreign_keys, &selected, |foreign_key| {
            (&foreign_key.child_table, &foreign_key.parent_table)
        });
    let delete_tables =
        directional_table_closure(&inventory.foreign_keys, &selected, |foreign_key| {
            (&foreign_key.parent_table, &foreign_key.child_table)
        });
    DependencyRepairScopes {
        insert_update: filter_repair_inventory(&inventory, &insert_update_tables),
        delete: filter_repair_inventory(&inventory, &delete_tables),
    }
}

fn directional_table_closure(
    foreign_keys: &[CanonicalForeignKey],
    selected: &BTreeSet<String>,
    edge: impl Fn(&CanonicalForeignKey) -> (&String, &String),
) -> BTreeSet<String> {
    let mut closure = selected.clone();
    loop {
        let previous_size = closure.len();
        for foreign_key in foreign_keys
            .iter()
            .filter(|foreign_key| foreign_key.enforced)
        {
            let (from, to) = edge(foreign_key);
            if closure.contains(from) {
                closure.insert(to.clone());
            }
        }
        if closure.len() == previous_size {
            return closure;
        }
    }
}

fn filter_repair_inventory(
    inventory: &RepairInventory,
    tables: &BTreeSet<String>,
) -> RepairInventory {
    RepairInventory {
        schema: inventory.schema.clone(),
        tables: inventory
            .tables
            .iter()
            .filter(|table| tables.contains(*table))
            .cloned()
            .collect(),
        foreign_keys: inventory
            .foreign_keys
            .iter()
            .filter(|foreign_key| {
                tables.contains(&foreign_key.child_table)
                    && tables.contains(&foreign_key.parent_table)
            })
            .cloned()
            .collect(),
    }
}

#[cfg(test)]
fn merge_dependency_scopes(
    inventory: &RepairInventory,
    scopes: &DependencyRepairScopes,
) -> RepairInventory {
    let tables = scopes
        .insert_update
        .tables
        .iter()
        .chain(&scopes.delete.tables)
        .cloned()
        .collect::<BTreeSet<_>>();
    let foreign_keys = scopes
        .insert_update
        .foreign_keys
        .iter()
        .chain(&scopes.delete.foreign_keys)
        .cloned()
        .collect::<BTreeSet<_>>();
    RepairInventory {
        schema: inventory.schema.clone(),
        tables: inventory
            .tables
            .iter()
            .filter(|table| tables.contains(*table))
            .cloned()
            .collect(),
        foreign_keys: inventory
            .foreign_keys
            .iter()
            .filter(|foreign_key| foreign_keys.contains(*foreign_key))
            .cloned()
            .collect(),
    }
}

pub(crate) fn ordered_candidate_tables(
    config: &RepairDriftConfig,
    source_inventory: &SchemaInventory,
    plan: &RepairPlan,
) -> Result<Vec<String>, RepairDriftError> {
    candidate_table_names(config, source_inventory).map_err(RepairDriftError::Config)?;
    Ok(plan.tables.clone())
}

fn build_repair_inventory(
    source: &MySqlConnectionConfig,
    endpoint_role: InventoryEndpointRole,
    use_tls: bool,
    tls_ca_file: Option<&str>,
    inventory: &SchemaInventory,
) -> Result<RepairInventory, crate::inventory::InventoryError> {
    let reader = crate::inventory::MariaDbInventoryReader::new(InventoryConfig {
        host: source.host.clone(),
        port: source.port,
        user: source.user.clone(),
        password: source.password.clone(),
        endpoint_role,
        use_tls,
        tls_ca_file: tls_ca_file.map(str::to_string),
        ..InventoryConfig::default()
    });
    Ok(RepairInventory {
        schema: inventory.schema.clone(),
        tables: inventory
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect(),
        foreign_keys: build_canonical_foreign_key_inventory(&inventory.schema, &reader)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{ColumnInventory, TableInventory};

    fn table(name: &str) -> TableInventory {
        TableInventory {
            name: name.to_string(),
            table_type: "BASE TABLE".to_string(),
            engine: Some("InnoDB".to_string()),
            collation: None,
            primary_key: vec!["id".to_string()],
            columns: vec![ColumnInventory {
                name: "id".to_string(),
                ordinal_position: 1,
                column_type: "bigint".to_string(),
                data_type: "bigint".to_string(),
                is_nullable: false,
                character_set: None,
                collation: None,
                default_value: None,
                extra: String::new(),
                comment: String::new(),
                generated: None,
            }],
        }
    }

    fn schema_inventory(names: &[&str]) -> SchemaInventory {
        SchemaInventory {
            schema: "app".to_string(),
            tables: names.iter().map(|name| table(name)).collect(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            routines: Vec::new(),
            events: Vec::new(),
        }
    }

    fn repair_inventory(
        tables: &[&str],
        foreign_keys: Vec<CanonicalForeignKey>,
    ) -> RepairInventory {
        RepairInventory {
            schema: "app".to_string(),
            tables: tables.iter().map(|table| (*table).to_string()).collect(),
            foreign_keys,
        }
    }

    fn fk(child_table: &str, parent_table: &str) -> CanonicalForeignKey {
        CanonicalForeignKey {
            constraint_schema: "app".to_string(),
            constraint_name: format!("{child_table}_{parent_table}_fk"),
            child_schema: "app".to_string(),
            child_table: child_table.to_string(),
            child_columns: vec!["parent_id".to_string()],
            parent_schema: "app".to_string(),
            parent_table: parent_table.to_string(),
            parent_columns: vec!["id".to_string()],
            update_rule: "RESTRICT".to_string(),
            delete_rule: "RESTRICT".to_string(),
            match_option: "NONE".to_string(),
            enforced: true,
        }
    }

    fn runtime_plan(config: &RepairDriftConfig, inventory: RepairInventory) -> RepairPlan {
        let source = reduce_to_dependency_scopes(inventory.clone(), config.tables.clone());
        let target = reduce_to_dependency_scopes(inventory, config.tables.clone());
        build_plan(config, "run", source, target).expect("directional repair plan")
    }

    #[test]
    fn matching_dependency_table_remains_available_for_verify_input() {
        let source = schema_inventory(&["customers", "orders"]);
        let target = schema_inventory(&["customers", "orders"]);
        let comparisons = vec![
            DriftComparison {
                table: "customers".to_string(),
                source_count: Some(2),
                target_count: Some(2),
                content: None,
            },
            DriftComparison {
                table: "orders".to_string(),
                source_count: Some(3),
                target_count: Some(2),
                content: None,
            },
        ];

        let (inputs, skipped) = collect_repair_table_inputs(
            &["customers".to_string(), "orders".to_string()],
            &comparisons,
            &source,
            &target,
        );

        assert!(skipped.is_empty());
        assert_eq!(
            inputs.keys().collect::<Vec<_>>(),
            vec!["customers", "orders"]
        );
        assert_eq!(inputs["customers"].0, 2);
        assert_eq!(inputs["customers"].1, 2);
    }

    #[test]
    fn selected_child_candidates_include_parentward_repairs_before_child() {
        let mut config = super::super::config::default_repair_drift_config();
        config.tables = vec!["orders".to_string()];
        let source = schema_inventory(&["customers", "orders", "invoices", "unrelated"]);
        let plan = runtime_plan(
            &config,
            repair_inventory(
                &["customers", "orders", "invoices", "unrelated"],
                vec![fk("orders", "customers"), fk("invoices", "customers")],
            ),
        );

        assert_eq!(plan.insert_order, vec!["customers", "orders"]);
        assert_eq!(plan.delete_order, vec!["orders"]);
        assert_eq!(
            ordered_candidate_tables(&config, &source, &plan).expect("candidate tables"),
            vec!["customers", "orders"]
        );
    }

    #[test]
    fn selected_parent_candidates_include_childward_delete_safety_scope() {
        let mut config = super::super::config::default_repair_drift_config();
        config.tables = vec!["customers".to_string()];
        let source = schema_inventory(&["customers", "orders", "invoices", "unrelated"]);
        let plan = runtime_plan(
            &config,
            repair_inventory(
                &["customers", "orders", "invoices", "unrelated"],
                vec![fk("orders", "customers"), fk("invoices", "customers")],
            ),
        );

        assert_eq!(plan.insert_order, vec!["customers"]);
        assert_eq!(plan.delete_order, vec!["orders", "invoices", "customers"]);
        assert_eq!(
            ordered_candidate_tables(&config, &source, &plan).expect("candidate tables"),
            plan.tables
        );
        assert!(!plan.tables.contains(&"unrelated".to_string()));
    }
}
