use super::{EquivalentConflictReport, RepairDriftConfig, RepairDriftError};
use crate::conflict_repair::{ConflictKey, ConflictResolution, ConflictStore, MySqlConflictStore};
use crate::inventory::{SchemaInventory, TableInventory};
use crate::table_sync::{MySqlSyncReader, SyncMode};
use std::collections::BTreeSet;

const RESOLUTION_EVIDENCE: &str =
    "verified complete source/target row equality in bounded conflict reconciliation";

pub(crate) fn reconcile_exact_equivalent_conflicts(
    config: &RepairDriftConfig,
    run_id: &str,
    source_inventory: &SchemaInventory,
    selected_tables: &[String],
) -> Result<EquivalentConflictReport, RepairDriftError> {
    if config.mode != SyncMode::Apply || config.conflict_reconcile_limit == 0 {
        return Ok(EquivalentConflictReport::default());
    }

    let mut context = ReconcileContext::connect(config, run_id, source_inventory)?;
    let candidates = context.read_candidates(selected_tables)?;
    let unique_candidates = unique_source_rows(candidates);
    let mut report = EquivalentConflictReport::default();
    for candidate in unique_candidates {
        report.examined += 1;
        match context.reconcile(candidate)? {
            ReconcileOutcome::Resolved => report.resolved += 1,
            ReconcileOutcome::Deferred => report.deferred += 1,
        }
    }
    Ok(report)
}

struct ReconcileContext<'a> {
    config: &'a RepairDriftConfig,
    run_id: &'a str,
    source_inventory: &'a SchemaInventory,
    store: MySqlConflictStore,
    source_reader: MySqlSyncReader,
    target_reader: MySqlSyncReader,
}

impl<'a> ReconcileContext<'a> {
    fn connect(
        config: &'a RepairDriftConfig,
        run_id: &'a str,
        source_inventory: &'a SchemaInventory,
    ) -> Result<Self, RepairDriftError> {
        let store = MySqlConflictStore::new(&config.target, "cdc.row_conflicts")
            .map_err(RepairDriftError::Repair)?;
        store.ensure().map_err(RepairDriftError::Repair)?;
        let source_reader = MySqlSyncReader::new(config.source.clone());
        let target_reader = MySqlSyncReader::new_with_target(config.source.clone(), &config.target)
            .map_err(RepairDriftError::Repair)?;
        Ok(Self {
            config,
            run_id,
            source_inventory,
            store,
            source_reader,
            target_reader,
        })
    }

    fn read_candidates(
        &self,
        selected_tables: &[String],
    ) -> Result<Vec<ConflictKey>, RepairDriftError> {
        self.store
            .unresolved_source_rows(
                &self.config.source_identity,
                &self.config.source.database,
                selected_tables,
                self.config.conflict_reconcile_limit,
            )
            .map_err(RepairDriftError::Repair)
    }

    fn reconcile(&mut self, candidate: ConflictKey) -> Result<ReconcileOutcome, RepairDriftError> {
        let Some(table) = conflict_table(self.source_inventory, &candidate.table) else {
            return Ok(ReconcileOutcome::Deferred);
        };
        let Some(identity) = primary_key_identity(table, &candidate.source_primary_key) else {
            return Ok(ReconcileOutcome::Deferred);
        };
        let source_rows = self
            .source_reader
            .read_exact_inventory_rows(table, &identity)
            .map_err(|error| RepairDriftError::Repair(error.to_string()))?;
        let target_rows = self
            .target_reader
            .read_exact_inventory_rows(table, &identity)
            .map_err(|error| RepairDriftError::Repair(error.to_string()))?;
        let ([source_row], [target_row]) = (source_rows.as_slice(), target_rows.as_slice()) else {
            return Ok(ReconcileOutcome::Deferred);
        };
        if source_row != target_row {
            return Ok(ReconcileOutcome::Deferred);
        }
        self.resolve(candidate)?;
        Ok(ReconcileOutcome::Resolved)
    }

    fn resolve(&mut self, candidate: ConflictKey) -> Result<(), RepairDriftError> {
        self.store
            .resolve_existing(ConflictResolution {
                source_identity: self.config.source_identity.clone(),
                schema: candidate.schema,
                table: candidate.table,
                source_primary_key: candidate.source_primary_key,
                repair_run_id: self.run_id.to_string(),
                evidence: RESOLUTION_EVIDENCE.to_string(),
            })
            .map_err(RepairDriftError::Repair)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconcileOutcome {
    Resolved,
    Deferred,
}

fn unique_source_rows(candidates: Vec<ConflictKey>) -> Vec<ConflictKey> {
    let mut seen = BTreeSet::new();
    candidates
        .into_iter()
        .filter(|candidate| {
            seen.insert((
                candidate.schema.clone(),
                candidate.table.clone(),
                candidate.source_primary_key.clone(),
            ))
        })
        .collect()
}

fn conflict_table<'a>(
    inventory: &'a SchemaInventory,
    table_name: &str,
) -> Option<&'a TableInventory> {
    inventory
        .tables
        .iter()
        .find(|table| table.name == table_name)
}

fn primary_key_identity(
    table: &TableInventory,
    primary_key: &[String],
) -> Option<Vec<(String, String)>> {
    if table.primary_key.len() != primary_key.len() || table.primary_key.is_empty() {
        return None;
    }
    Some(
        table
            .primary_key
            .iter()
            .cloned()
            .zip(primary_key.iter().cloned())
            .collect(),
    )
}
