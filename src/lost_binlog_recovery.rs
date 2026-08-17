use crate::checkpoint::Checkpoint;
use crate::inventory::{
    InventoryConfig, InventoryEndpointRole, MariaDbInventoryReader, SchemaInventory,
    SnapshotInventoryReader, build_canonical_foreign_key_inventory, build_inventory,
};
use crate::live::TargetMySqlConfig;
use crate::lost_binlog_recovery_store::MySqlLostBinlogRecoveryStore;
use crate::mysql_client::PersistentMySqlSource;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::repair_drift::{RepairDriftConfig, RepairDriftReport, run_consistent_snapshot_repair};
use crate::sync_schema::{
    SchemaSourceEvidence, read_snapshot_check_constraints,
    run_schema_convergence_from_source_evidence,
};
use crate::table_sync::SyncMode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct LostBinlogBarrier {
    pub source_identity: String,
    pub binlog_file: String,
    pub event_start_position: u64,
    pub event_end_position: u64,
    pub raw_sql: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct LostBinlogRecoveryRequest {
    pub recovery_id: String,
    pub checkpoint_name: String,
    pub expected_checkpoint: Checkpoint,
    pub expected_barrier: LostBinlogBarrier,
    #[serde(default)]
    pub scope_hash: String,
    pub operator_identity: String,
    pub reason: String,
    #[serde(default)]
    pub prepared_evidence_json: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LostBinlogRecoveryStatus {
    Prepared,
    Committed,
    Verified,
    Abandoned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LostBinlogRecoveryRecord {
    pub recovery_id: String,
    pub checkpoint_name: String,
    pub source_identity: String,
    pub scope_hash: String,
    pub operator_identity: String,
    pub reason: String,
    pub prepared_evidence_json: String,
    pub expected_checkpoint: Checkpoint,
    pub expected_barrier: LostBinlogBarrier,
    pub new_checkpoint: Checkpoint,
    pub status: LostBinlogRecoveryStatus,
    pub abandoned_evidence_json: Option<String>,
    pub abandoned_at: Option<String>,
}

impl LostBinlogRecoveryRecord {
    pub fn prepared(request: &LostBinlogRecoveryRequest, new_checkpoint: Checkpoint) -> Self {
        Self {
            recovery_id: request.recovery_id.clone(),
            checkpoint_name: request.checkpoint_name.clone(),
            source_identity: request.expected_barrier.source_identity.clone(),
            scope_hash: request.scope_hash.clone(),
            operator_identity: request.operator_identity.clone(),
            reason: request.reason.clone(),
            prepared_evidence_json: request.prepared_evidence_json.clone(),
            expected_checkpoint: request.expected_checkpoint.clone(),
            expected_barrier: request.expected_barrier.clone(),
            new_checkpoint,
            status: LostBinlogRecoveryStatus::Prepared,
            abandoned_evidence_json: None,
            abandoned_at: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LostBinlogReconciliationProof {
    pub recovery_id: String,
    pub source_identity: String,
    pub scope_hash: String,
    pub schema_converged: bool,
    pub data_converged: bool,
    pub unsupported_scope: Vec<String>,
    pub evidence_json: String,
}

#[cfg(test)]
pub trait LostBinlogBoundaryReader {
    fn read_binlog_coordinate(&self) -> Result<Checkpoint, String>;
}

struct CommittedSourceBoundaryReader<'a> {
    source: &'a PersistentMySqlSource,
    schema: &'a str,
}

impl CommittedSourceBoundaryReader<'_> {
    fn read_binlog_coordinate_and_source_evidence(
        &self,
    ) -> Result<(Checkpoint, SchemaSourceEvidence), String> {
        read_committed_source_boundary(
            || {
                self.source
                    .read_binlog_coordinate()
                    .map_err(|error| format!("read source binlog coordinate: {error}"))
            },
            |_coordinate| read_source_evidence(self.source, self.schema),
        )
    }
}

fn read_committed_source_boundary<Coordinate, Evidence>(
    read_coordinate: impl FnOnce() -> Result<Coordinate, String>,
    read_evidence: impl FnOnce(&Coordinate) -> Result<Evidence, String>,
) -> Result<(Coordinate, Evidence), String> {
    let coordinate = read_coordinate()?;
    let evidence = read_evidence(&coordinate)?;
    Ok((coordinate, evidence))
}

#[cfg(test)]
impl LostBinlogBoundaryReader for CommittedSourceBoundaryReader<'_> {
    fn read_binlog_coordinate(&self) -> Result<Checkpoint, String> {
        self.read_binlog_coordinate_and_source_evidence()
            .map(|(coordinate, _evidence)| coordinate)
    }
}

pub trait LostBinlogRecoveryStore {
    fn acquire_stream_lease(&self, lease_name: &str) -> Result<(), String>;
    fn begin_transaction(&self) -> Result<(), String>;
    fn load_checkpoint_for_update(
        &self,
        checkpoint_name: &str,
    ) -> Result<Option<Checkpoint>, String>;
    fn load_barrier_for_update(
        &self,
        barrier: &LostBinlogBarrier,
    ) -> Result<Option<LostBinlogBarrier>, String>;
    fn load_recovery_for_update(
        &self,
        recovery_id: &str,
    ) -> Result<Option<LostBinlogRecoveryRecord>, String>;
    fn load_barrier_recovery_owner_for_update(
        &self,
        barrier: &LostBinlogBarrier,
    ) -> Result<Option<LostBinlogRecoveryRecord>, String>;
    fn mark_recovery_abandoned(
        &self,
        recovery: &LostBinlogRecoveryRecord,
        replacement_recovery_id: &str,
        evidence_json: &str,
    ) -> Result<(), String>;
    fn insert_prepared_recovery(&self, recovery: &LostBinlogRecoveryRecord) -> Result<(), String>;
    fn save_checkpoint(&self, checkpoint_name: &str, checkpoint: &Checkpoint)
    -> Result<(), String>;
    fn mark_recovery_committed(
        &self,
        recovery_id: &str,
        proof: &LostBinlogReconciliationProof,
    ) -> Result<(), String>;
    fn commit_transaction(&self) -> Result<(), String>;
    fn rollback_transaction(&self) -> Result<(), String>;
}

#[cfg(test)]
pub fn prepare_lost_binlog_recovery<B, S>(
    boundary_reader: &B,
    store: &S,
    request: &LostBinlogRecoveryRequest,
) -> Result<LostBinlogRecoveryRecord, String>
where
    B: LostBinlogBoundaryReader,
    S: LostBinlogRecoveryStore,
{
    validate_recovery_request(request)?;
    store.acquire_stream_lease(&request.checkpoint_name)?;
    let new_checkpoint = boundary_reader.read_binlog_coordinate()?;
    prepare_recovery_at_checkpoint(store, request, new_checkpoint)
}

fn insert_prepared_recovery<S>(
    store: &S,
    request: &LostBinlogRecoveryRequest,
    prepared: &LostBinlogRecoveryRecord,
) -> Result<(), String>
where
    S: LostBinlogRecoveryStore,
{
    require_expected_recovery_state(store, request)?;
    if store
        .load_recovery_for_update(&request.recovery_id)?
        .is_some()
    {
        return Err(format!(
            "lost-binlog recovery already exists: {}",
            request.recovery_id
        ));
    }

    let owner = store.load_barrier_recovery_owner_for_update(&request.expected_barrier)?;
    let Some(owner) = owner else {
        return store.insert_prepared_recovery(prepared);
    };

    validate_replacement_owner(request, &owner)?;
    let evidence_json = abandoned_evidence_json(&owner, request)?;
    store.mark_recovery_abandoned(&owner, &request.recovery_id, &evidence_json)?;
    store.insert_prepared_recovery(prepared)
}

fn validate_replacement_owner(
    request: &LostBinlogRecoveryRequest,
    owner: &LostBinlogRecoveryRecord,
) -> Result<(), String> {
    if owner.status != LostBinlogRecoveryStatus::Prepared {
        return Err(format!(
            "lost-binlog barrier recovery owner is not replaceable: {}",
            owner.recovery_id
        ));
    }
    if owner.checkpoint_name != request.checkpoint_name
        || owner.expected_checkpoint != request.expected_checkpoint
        || owner.expected_barrier != request.expected_barrier
        || owner.source_identity != request.expected_barrier.source_identity
    {
        return Err(format!(
            "lost-binlog barrier recovery owner does not match replacement authorization: {}",
            owner.recovery_id
        ));
    }
    Ok(())
}

fn abandoned_evidence_json(
    owner: &LostBinlogRecoveryRecord,
    replacement: &LostBinlogRecoveryRequest,
) -> Result<String, String> {
    serde_json::to_string(&serde_json::json!({
        "old_recovery_id": owner.recovery_id,
        "replacement_recovery_id": replacement.recovery_id,
        "operator_identity": replacement.operator_identity,
        "reason": replacement.reason,
        "checkpoint_name": replacement.checkpoint_name,
        "expected_checkpoint": replacement.expected_checkpoint,
        "expected_barrier": {
            "source_identity": replacement.expected_barrier.source_identity,
            "binlog_file": replacement.expected_barrier.binlog_file,
            "event_start_position": replacement.expected_barrier.event_start_position,
            "event_end_position": replacement.expected_barrier.event_end_position,
            "raw_sql": replacement.expected_barrier.raw_sql,
        },
        "scope_hash": replacement.scope_hash,
    }))
    .map_err(|error| format!("encode abandoned recovery evidence: {error}"))
}

pub fn commit_lost_binlog_recovery<S>(
    store: &S,
    request: &LostBinlogRecoveryRequest,
    proof: &LostBinlogReconciliationProof,
) -> Result<(), String>
where
    S: LostBinlogRecoveryStore,
{
    validate_reconciliation_proof(request, proof)?;
    store.acquire_stream_lease(&request.checkpoint_name)?;

    run_recovery_transaction(store, || {
        require_expected_recovery_state(store, request)?;
        let prepared = store
            .load_recovery_for_update(&request.recovery_id)?
            .ok_or_else(|| format!("prepared recovery is missing: {}", request.recovery_id))?;
        validate_prepared_recovery(request, &prepared)?;
        store.save_checkpoint(&request.checkpoint_name, &prepared.new_checkpoint)?;
        store.mark_recovery_committed(&request.recovery_id, proof)
    })
}

fn validate_recovery_request(request: &LostBinlogRecoveryRequest) -> Result<(), String> {
    validate_static_recovery_request(request)?;
    for (field, value) in [
        ("scope_hash", request.scope_hash.as_str()),
        (
            "prepared_evidence_json",
            request.prepared_evidence_json.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(format!("lost-binlog recovery {field} is empty"));
        }
    }
    Ok(())
}

fn validate_static_recovery_request(request: &LostBinlogRecoveryRequest) -> Result<(), String> {
    for (field, value) in [
        ("recovery_id", request.recovery_id.as_str()),
        ("checkpoint_name", request.checkpoint_name.as_str()),
        (
            "source_identity",
            request.expected_barrier.source_identity.as_str(),
        ),
        ("operator_identity", request.operator_identity.as_str()),
        ("reason", request.reason.as_str()),
        ("raw_sql", request.expected_barrier.raw_sql.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("lost-binlog recovery {field} is empty"));
        }
    }
    if request.expected_barrier.event_end_position <= request.expected_barrier.event_start_position
    {
        return Err("lost-binlog barrier coordinates are invalid".to_string());
    }
    Ok(())
}

fn validate_new_checkpoint(old: &Checkpoint, new: &Checkpoint) -> Result<(), String> {
    if source_coordinate_advances(old, new) {
        return Ok(());
    }
    Err(format!(
        "lost-binlog recovery boundary {}:{} does not advance checkpoint {}:{}",
        new.source_file, new.source_position, old.source_file, old.source_position
    ))
}

fn source_coordinate_advances(old: &Checkpoint, new: &Checkpoint) -> bool {
    if new.source_file == old.source_file {
        return new.source_position > old.source_position;
    }
    binlog_sequence(&new.source_file)
        .zip(binlog_sequence(&old.source_file))
        .is_some_and(|(new_sequence, old_sequence)| new_sequence > old_sequence)
}

fn binlog_sequence(file: &str) -> Option<u64> {
    file.rsplit_once('.')?.1.parse().ok()
}

fn require_expected_recovery_state<S>(
    store: &S,
    request: &LostBinlogRecoveryRequest,
) -> Result<(), String>
where
    S: LostBinlogRecoveryStore,
{
    let checkpoint = store
        .load_checkpoint_for_update(&request.checkpoint_name)?
        .ok_or_else(|| "lost-binlog checkpoint is missing".to_string())?;
    if checkpoint != request.expected_checkpoint {
        return Err("lost-binlog checkpoint mismatch".to_string());
    }
    let barrier = store
        .load_barrier_for_update(&request.expected_barrier)?
        .ok_or_else(|| "lost-binlog barrier is missing".to_string())?;
    if barrier != request.expected_barrier {
        return Err("lost-binlog barrier mismatch".to_string());
    }
    Ok(())
}

fn validate_reconciliation_proof(
    request: &LostBinlogRecoveryRequest,
    proof: &LostBinlogReconciliationProof,
) -> Result<(), String> {
    let identity_matches = proof.recovery_id == request.recovery_id;
    let source_matches = proof.source_identity == request.expected_barrier.source_identity;
    let scope_matches = proof.scope_hash == request.scope_hash;
    let scope_is_complete = proof.unsupported_scope.is_empty();
    let evidence_exists = !proof.evidence_json.trim().is_empty();
    if identity_matches
        && source_matches
        && scope_matches
        && proof.schema_converged
        && proof.data_converged
        && scope_is_complete
        && evidence_exists
    {
        return Ok(());
    }
    Err("lost-binlog reconciliation proof is incomplete".to_string())
}

fn validate_prepared_recovery(
    request: &LostBinlogRecoveryRequest,
    prepared: &LostBinlogRecoveryRecord,
) -> Result<(), String> {
    let expected = LostBinlogRecoveryRecord::prepared(request, prepared.new_checkpoint.clone());
    if prepared == &expected {
        return Ok(());
    }
    Err("prepared lost-binlog recovery does not match the authorized request".to_string())
}

fn run_recovery_transaction<S, T>(
    store: &S,
    operation: impl FnOnce() -> Result<T, String>,
) -> Result<T, String>
where
    S: LostBinlogRecoveryStore,
{
    store.begin_transaction()?;
    match operation() {
        Ok(value) => match store.commit_transaction() {
            Ok(()) => Ok(value),
            Err(error) => {
                let _ = store.rollback_transaction();
                Err(error)
            }
        },
        Err(error) => {
            let _ = store.rollback_transaction();
            Err(error)
        }
    }
}

pub struct RecoverLostBinlogConfig {
    pub source: MySqlConnectionConfig,
    pub source_identity: String,
    pub target: TargetMySqlConfig,
    pub authorization_file: PathBuf,
    pub checkpoint_table: String,
    pub journal_table: String,
    pub recovery_table: String,
    pub progress_table: String,
    pub chunk_size: usize,
}

#[derive(Debug, Serialize)]
pub struct RecoverLostBinlogReport {
    pub recovery_id: String,
    pub new_checkpoint: Checkpoint,
    pub scope_hash: String,
    pub repaired_tables: usize,
    pub compared_tables: usize,
}

pub struct ResyncStreamConfig {
    pub source: MySqlConnectionConfig,
    pub source_identity: String,
    pub target: TargetMySqlConfig,
    pub checkpoint_table: String,
    pub progress_table: String,
    pub chunk_size: usize,
    pub parallelism: usize,
}

#[derive(Debug, Serialize)]
pub struct ResyncStreamReport {
    pub source_identity: String,
    pub start_checkpoint: Checkpoint,
    pub repaired_tables: usize,
    pub compared_tables: usize,
}

pub fn run_resync_stream(config: &ResyncStreamConfig) -> Result<ResyncStreamReport, String> {
    println!("resync_stream_parallelism={}", config.parallelism);
    let source = PersistentMySqlSource::new_without_operation_timeout(&config.source)
        .map_err(|error| format!("connect resync source: {error}"))?;
    let start_checkpoint = source
        .read_binlog_coordinate()
        .map_err(|error| format!("read resync source coordinate: {error}"))?;
    let checkpoint_store = crate::stream_checkpoint::MySqlStreamCheckpointStore::new(
        config.target.clone(),
        config.checkpoint_table.clone(),
        &config.source_identity,
    );
    checkpoint_store.bootstrap(&start_checkpoint)?;
    let source_evidence = read_source_evidence(&source, &config.source.database)?;
    validate_transactional_scope(&source_evidence.inventory)?;
    let sync_config = resync_sync_config(config, &source_evidence.inventory);
    let rows = crate::sync::run_mysql_sync_with_evidence(sync_config, source_evidence)?;
    let (repaired_tables, compared_tables) = resync_table_counts(&rows);
    Ok(ResyncStreamReport {
        source_identity: config.source_identity.clone(),
        start_checkpoint,
        repaired_tables,
        compared_tables,
    })
}

pub(crate) fn resync_sync_config(
    config: &ResyncStreamConfig,
    source_inventory: &SchemaInventory,
) -> crate::sync::SyncConfig {
    crate::sync::SyncConfig {
        source: config.source.clone(),
        target: config.target.clone(),
        tables: source_inventory
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect(),
        chunk_size: config.chunk_size,
        parallelism: config.parallelism,
        progress_table: config.progress_table.clone(),
        run_id: Some(format!("resync-stream:{}", config.source_identity)),
        run_id_prefix: None,
    }
}

pub(crate) fn resync_table_counts(rows: &[crate::sync::SyncChunkProgress]) -> (usize, usize) {
    let repaired = rows
        .iter()
        .filter(|row| row.inserts > 0 || row.updates > 0 || row.deletes > 0)
        .count();
    (repaired, rows.len())
}

pub fn run_recover_lost_binlog(
    config: &RecoverLostBinlogConfig,
) -> Result<RecoverLostBinlogReport, String> {
    let preparation = prepare_recovery_context(config)?;
    run_anchored_recovery(config, preparation)
}

struct RecoveryPreparation {
    request: LostBinlogRecoveryRequest,
    source: Rc<PersistentMySqlSource>,
    store: MySqlLostBinlogRecoveryStore,
}

fn prepare_recovery_context(
    config: &RecoverLostBinlogConfig,
) -> Result<RecoveryPreparation, String> {
    let request = read_recovery_authorization(&config.authorization_file)?;
    validate_authorized_source(config, &request)?;
    validate_static_recovery_request(&request)?;
    let source = Rc::new(
        PersistentMySqlSource::new(&config.source)
            .map_err(|error| format!("connect recovery source: {error}"))?,
    );
    let store = MySqlLostBinlogRecoveryStore::new(
        &config.target,
        config.checkpoint_table.clone(),
        config.journal_table.clone(),
        config.recovery_table.clone(),
    )?;
    store.ensure()?;
    Ok(RecoveryPreparation {
        request,
        source,
        store,
    })
}

fn require_converged_schema(
    report: &crate::sync_schema::SchemaConvergenceReport,
) -> Result<(), String> {
    if report.overall_status == crate::sync_schema::OverallSchemaStatus::Converged {
        return Ok(());
    }
    Err("full schema convergence did not complete".to_string())
}

fn authorize_current_scope(
    request: &mut LostBinlogRecoveryRequest,
    source_inventory: &SchemaInventory,
) -> Result<String, String> {
    let scope_hash = inventory_scope_hash(source_inventory)?;
    if request.scope_hash.is_empty() {
        request.scope_hash = scope_hash.clone();
        return Ok(scope_hash);
    }
    if request.scope_hash == scope_hash {
        return Ok(scope_hash);
    }
    Err(format!(
        "authorized scope hash {} does not match current source scope {scope_hash}",
        request.scope_hash
    ))
}

fn prepared_evidence_json(
    source_inventory: &SchemaInventory,
    target_inventory: &SchemaInventory,
    scope_hash: &str,
) -> Result<String, String> {
    let source_schema_fingerprint = inventory_scope_hash(source_inventory)?;
    let target_schema_fingerprint = inventory_scope_hash(target_inventory)?;
    Ok(serde_json::json!({
        "scope_hash": scope_hash,
        "source_schema_fingerprint": source_schema_fingerprint,
        "target_schema_fingerprint": target_schema_fingerprint,
        "source_tables": source_inventory.tables.len(),
    })
    .to_string())
}

struct PreparedRecoverySnapshot {
    request: LostBinlogRecoveryRequest,
    prepared: LostBinlogRecoveryRecord,
    source_evidence: SchemaSourceEvidence,
    target_inventory: SchemaInventory,
    scope_hash: String,
}

fn run_anchored_recovery(
    config: &RecoverLostBinlogConfig,
    preparation: RecoveryPreparation,
) -> Result<RecoverLostBinlogReport, String> {
    let RecoveryPreparation {
        mut request,
        source,
        store,
    } = preparation;
    store.acquire_stream_lease(&request.checkpoint_name)?;
    let (new_checkpoint, source_evidence) =
        begin_committed_source_boundary(config, source.as_ref())?;
    let prepared_snapshot = capture_prepared_recovery_snapshot(
        config,
        &store,
        &mut request,
        new_checkpoint,
        source_evidence,
    )?;
    let repair = repair_consistent_snapshot(
        config,
        &prepared_snapshot.request,
        Rc::clone(&source),
        prepared_snapshot.source_evidence.inventory.clone(),
        prepared_snapshot.target_inventory.clone(),
    )?;
    let schema_report = run_schema_convergence_from_source_evidence(
        prepared_snapshot.source_evidence.clone(),
        config.target.clone(),
    )?;
    require_converged_schema(&schema_report)?;
    commit_anchored_recovery(
        config,
        source.as_ref(),
        &store,
        prepared_snapshot,
        repair,
        schema_report,
    )
}

fn capture_prepared_recovery_snapshot(
    config: &RecoverLostBinlogConfig,
    store: &MySqlLostBinlogRecoveryStore,
    request: &mut LostBinlogRecoveryRequest,
    new_checkpoint: Checkpoint,
    source_evidence: SchemaSourceEvidence,
) -> Result<PreparedRecoverySnapshot, String> {
    let target_inventory = read_target_inventory(&config.target)?;
    validate_transactional_scope(&source_evidence.inventory)?;
    let scope_hash = authorize_current_scope(request, &source_evidence.inventory)?;
    if request.prepared_evidence_json.trim().is_empty() {
        request.prepared_evidence_json =
            prepared_evidence_json(&source_evidence.inventory, &target_inventory, &scope_hash)?;
    }
    let prepared = prepare_recovery_at_checkpoint(store, request, new_checkpoint)?;
    Ok(PreparedRecoverySnapshot {
        request: request.clone(),
        prepared,
        source_evidence,
        target_inventory,
        scope_hash,
    })
}

fn commit_anchored_recovery(
    config: &RecoverLostBinlogConfig,
    source: &PersistentMySqlSource,
    store: &MySqlLostBinlogRecoveryStore,
    prepared: PreparedRecoverySnapshot,
    repair: RepairDriftReport,
    schema_report: crate::sync_schema::SchemaConvergenceReport,
) -> Result<RecoverLostBinlogReport, String> {
    require_unchanged_source_scope(source, &config.source.database, &prepared.scope_hash)?;
    let target_inventory = read_target_inventory(&config.target)?;
    require_exact_table_inventory(&prepared.source_evidence.inventory, &target_inventory)?;
    let proof = reconciliation_proof(
        &prepared.request,
        &repair,
        &prepared.scope_hash,
        &schema_report,
    );
    commit_lost_binlog_recovery(store, &prepared.request, &proof)?;
    Ok(recovery_report(
        &prepared.request,
        prepared.prepared,
        repair,
        prepared.scope_hash,
    ))
}

#[cfg(test)]
fn run_recovery_phases<Snapshot, Repair, Schema, Output>(
    begin_snapshot: impl FnOnce() -> Result<Snapshot, String>,
    reconcile_data: impl FnOnce(&Snapshot) -> Result<Repair, String>,
    converge_schema: impl FnOnce(&Snapshot, &Repair) -> Result<Schema, String>,
    commit: impl FnOnce(Snapshot, Repair, Schema) -> Result<Output, String>,
) -> Result<Output, String> {
    let snapshot = begin_snapshot()?;
    let repair = reconcile_data(&snapshot)?;
    let schema = converge_schema(&snapshot, &repair)?;
    commit(snapshot, repair, schema)
}

#[cfg(test)]
fn run_recovery_phases_with_source_evidence<Snapshot, Evidence, Repair, Schema, Output>(
    begin_snapshot: impl FnOnce() -> Result<Snapshot, String>,
    capture_source_evidence: impl FnOnce(&Snapshot) -> Result<Evidence, String>,
    reconcile_data: impl FnOnce(&Snapshot, &Evidence) -> Result<Repair, String>,
    converge_schema: impl FnOnce(&Snapshot, &Evidence, &Repair) -> Result<Schema, String>,
    commit: impl FnOnce(Snapshot, Evidence, Repair, Schema) -> Result<Output, String>,
) -> Result<Output, String> {
    let snapshot = begin_snapshot()?;
    let evidence = capture_source_evidence(&snapshot)?;
    let repair = reconcile_data(&snapshot, &evidence)?;
    let schema = converge_schema(&snapshot, &evidence, &repair)?;
    commit(snapshot, evidence, repair, schema)
}

fn begin_committed_source_boundary(
    config: &RecoverLostBinlogConfig,
    source: &PersistentMySqlSource,
) -> Result<(Checkpoint, SchemaSourceEvidence), String> {
    let boundary_reader = CommittedSourceBoundaryReader {
        source,
        schema: &config.source.database,
    };
    boundary_reader.read_binlog_coordinate_and_source_evidence()
}

fn prepare_recovery_at_checkpoint<S>(
    store: &S,
    request: &LostBinlogRecoveryRequest,
    new_checkpoint: Checkpoint,
) -> Result<LostBinlogRecoveryRecord, String>
where
    S: LostBinlogRecoveryStore,
{
    validate_recovery_request(request)?;
    validate_new_checkpoint(&request.expected_checkpoint, &new_checkpoint)?;
    let prepared = LostBinlogRecoveryRecord::prepared(request, new_checkpoint);
    run_recovery_transaction(store, || {
        insert_prepared_recovery(store, request, &prepared)
    })?;
    Ok(prepared)
}

fn repair_consistent_snapshot(
    config: &RecoverLostBinlogConfig,
    request: &LostBinlogRecoveryRequest,
    source: Rc<PersistentMySqlSource>,
    source_inventory: SchemaInventory,
    target_inventory: SchemaInventory,
) -> Result<RepairDriftReport, String> {
    run_consistent_snapshot_repair(
        &full_repair_config(config, request),
        source,
        source_inventory,
        target_inventory,
    )
    .map_err(|error| error.to_string())
}

fn require_unchanged_source_scope(
    source: &PersistentMySqlSource,
    schema: &str,
    expected_scope_hash: &str,
) -> Result<(), String> {
    let current_inventory = read_source_inventory(source, schema)?;
    let current_scope_hash = inventory_scope_hash(&current_inventory)?;
    if current_scope_hash == expected_scope_hash {
        return Ok(());
    }
    Err("source schema changed during the consistent snapshot repair".to_string())
}

fn require_exact_table_inventory(
    source: &SchemaInventory,
    target: &SchemaInventory,
) -> Result<(), String> {
    let (missing, extras) = table_inventory_difference(source, target);
    if missing.is_empty() && extras.is_empty() {
        return Ok(());
    }
    Err(format!(
        "final target table inventory differs: missing={} target_only={}",
        missing.join(","),
        extras.join(",")
    ))
}

fn table_inventory_difference(
    source: &SchemaInventory,
    target: &SchemaInventory,
) -> (Vec<String>, Vec<String>) {
    let source_tables = source
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    let target_tables = target
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    let missing = source_tables.difference(&target_tables).cloned().collect();
    let extras = target_tables.difference(&source_tables).cloned().collect();
    (missing, extras)
}

fn reconciliation_proof(
    request: &LostBinlogRecoveryRequest,
    repair: &RepairDriftReport,
    scope_hash: &str,
    schema_report: &crate::sync_schema::SchemaConvergenceReport,
) -> LostBinlogReconciliationProof {
    let unsupported_scope = repair
        .skipped
        .iter()
        .map(|item| item.table.clone())
        .collect::<Vec<_>>();
    LostBinlogReconciliationProof {
        recovery_id: request.recovery_id.clone(),
        source_identity: request.expected_barrier.source_identity.clone(),
        scope_hash: request.scope_hash.clone(),
        schema_converged: schema_report.overall_status
            == crate::sync_schema::OverallSchemaStatus::Converged,
        data_converged: unsupported_scope.is_empty()
            && repair.compared_tables == repair.source_tables,
        evidence_json: reconciliation_evidence_json(repair, scope_hash),
        unsupported_scope,
    }
}

fn reconciliation_evidence_json(repair: &RepairDriftReport, scope_hash: &str) -> String {
    let skipped_tables = repair
        .skipped
        .iter()
        .map(|item| serde_json::json!({"table": item.table, "reason": item.reason}))
        .collect::<Vec<_>>();
    serde_json::json!({
        "scope_hash": scope_hash,
        "compared_tables": repair.compared_tables,
        "repaired_tables": repair.repaired.len(),
        "skipped_tables": skipped_tables,
    })
    .to_string()
}

fn recovery_report(
    request: &LostBinlogRecoveryRequest,
    prepared: LostBinlogRecoveryRecord,
    repair: RepairDriftReport,
    scope_hash: String,
) -> RecoverLostBinlogReport {
    RecoverLostBinlogReport {
        recovery_id: request.recovery_id.clone(),
        new_checkpoint: prepared.new_checkpoint,
        scope_hash,
        repaired_tables: repair.repaired.len(),
        compared_tables: repair.compared_tables,
    }
}

fn read_recovery_authorization(path: &PathBuf) -> Result<LostBinlogRecoveryRequest, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("read recovery authorization {}: {error}", path.display()))?;
    serde_json::from_str(&contents)
        .map_err(|error| format!("decode recovery authorization {}: {error}", path.display()))
}

fn validate_authorized_source(
    config: &RecoverLostBinlogConfig,
    request: &LostBinlogRecoveryRequest,
) -> Result<(), String> {
    let expected_prefix = format!("{}#server-id=", config.source_identity);
    if !request
        .expected_barrier
        .source_identity
        .starts_with(&expected_prefix)
    {
        return Err(
            "recovery authorization source identity does not match configured source".to_string(),
        );
    }
    let expected_checkpoint_name =
        crate::stream_checkpoint::stream_checkpoint_name(&config.source_identity);
    if request.checkpoint_name != expected_checkpoint_name {
        return Err(format!(
            "recovery authorization checkpoint name must be {expected_checkpoint_name}"
        ));
    }
    Ok(())
}

fn validate_transactional_scope(inventory: &SchemaInventory) -> Result<(), String> {
    let unsupported = inventory
        .tables
        .iter()
        .filter(|table| table.engine.as_deref() != Some("InnoDB"))
        .map(|table| format!("{}:{:?}", table.name, table.engine))
        .collect::<Vec<_>>();
    if unsupported.is_empty() {
        return Ok(());
    }
    Err(format!(
        "consistent snapshot recovery does not support non-InnoDB tables: {}",
        unsupported.join(", ")
    ))
}

fn inventory_scope_hash(inventory: &SchemaInventory) -> Result<String, String> {
    let encoded = serde_json::to_vec(inventory)
        .map_err(|error| format!("encode recovery scope inventory: {error}"))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn read_source_inventory(
    source: &PersistentMySqlSource,
    schema: &str,
) -> Result<SchemaInventory, String> {
    let reader = SnapshotInventoryReader::new(source, InventoryEndpointRole::Source);
    build_inventory(schema, &reader)
        .map_err(|error| format!("snapshot source recovery inventory failed: {error}"))
}

fn read_source_evidence(
    source: &PersistentMySqlSource,
    schema: &str,
) -> Result<SchemaSourceEvidence, String> {
    let inventory = read_source_inventory(source, schema)?;
    let checks = read_snapshot_check_constraints(source, schema)?;
    let reader = SnapshotInventoryReader::new(source, InventoryEndpointRole::Source);
    let canonical_foreign_keys =
        build_canonical_foreign_key_inventory(schema, &reader).map_err(|error| {
            format!("snapshot source canonical foreign key inventory failed: {error}")
        })?;
    Ok(SchemaSourceEvidence {
        inventory,
        checks,
        canonical_foreign_keys,
    })
}

fn read_target_inventory(config: &TargetMySqlConfig) -> Result<SchemaInventory, String> {
    let reader = MariaDbInventoryReader::new(InventoryConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.user.clone(),
        password: config.password.clone(),
        endpoint_role: InventoryEndpointRole::Target,
        use_tls: true,
        tls_ca_file: Some(config.tls_ca_file.clone()),
        ..InventoryConfig::default()
    });
    build_inventory(&config.database, &reader)
        .map_err(|error| format!("target recovery inventory failed: {error}"))
}

fn full_repair_config(
    config: &RecoverLostBinlogConfig,
    request: &LostBinlogRecoveryRequest,
) -> RepairDriftConfig {
    RepairDriftConfig {
        source: config.source.clone(),
        source_identity: config.source_identity.clone(),
        target: config.target.clone(),
        tables: Vec::new(),
        parent_first: Vec::new(),
        start_after: None,
        end_at: None,
        content_check: true,
        mode: SyncMode::Apply,
        chunk_size: config.chunk_size,
        parallelism: 1,
        conflict_reconcile_limit: 0,
        progress_table: config.progress_table.clone(),
        run_id: Some(request.recovery_id.clone()),
        run_id_prefix: request.recovery_id.clone(),
        #[cfg(feature = "integration-failpoints")]
        integration_failpoint: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{Checkpoint, LastEvent};
    use std::cell::RefCell;

    #[derive(Default)]
    struct RecordingRecoveryStore {
        checkpoint: RefCell<Option<Checkpoint>>,
        barrier: RefCell<Option<LostBinlogBarrier>>,
        recovery: RefCell<Option<LostBinlogRecoveryRecord>>,
        committed_checkpoint: RefCell<Option<Checkpoint>>,
        operations: RefCell<Vec<&'static str>>,
        fail_mark_committed: bool,
    }

    impl LostBinlogRecoveryStore for RecordingRecoveryStore {
        fn acquire_stream_lease(&self, _lease_name: &str) -> Result<(), String> {
            self.operations.borrow_mut().push("LEASE");
            Ok(())
        }

        fn begin_transaction(&self) -> Result<(), String> {
            self.operations.borrow_mut().push("BEGIN");
            Ok(())
        }

        fn load_checkpoint_for_update(
            &self,
            _checkpoint_name: &str,
        ) -> Result<Option<Checkpoint>, String> {
            self.operations.borrow_mut().push("LOCK_CHECKPOINT");
            Ok(self.checkpoint.borrow().clone())
        }

        fn load_barrier_for_update(
            &self,
            _barrier: &LostBinlogBarrier,
        ) -> Result<Option<LostBinlogBarrier>, String> {
            self.operations.borrow_mut().push("LOCK_BARRIER");
            Ok(self.barrier.borrow().clone())
        }

        fn load_recovery_for_update(
            &self,
            _recovery_id: &str,
        ) -> Result<Option<LostBinlogRecoveryRecord>, String> {
            self.operations.borrow_mut().push("LOCK_RECOVERY");
            Ok(self.recovery.borrow().clone())
        }

        fn load_barrier_recovery_owner_for_update(
            &self,
            _barrier: &LostBinlogBarrier,
        ) -> Result<Option<LostBinlogRecoveryRecord>, String> {
            self.operations.borrow_mut().push("LOCK_ACTIVE_OWNER");
            Ok(None)
        }

        fn mark_recovery_abandoned(
            &self,
            _recovery: &LostBinlogRecoveryRecord,
            _replacement_recovery_id: &str,
            _evidence_json: &str,
        ) -> Result<(), String> {
            self.operations.borrow_mut().push("ABANDON_RECOVERY");
            Ok(())
        }

        fn insert_prepared_recovery(
            &self,
            recovery: &LostBinlogRecoveryRecord,
        ) -> Result<(), String> {
            self.operations.borrow_mut().push("INSERT_RECOVERY");
            self.recovery.replace(Some(recovery.clone()));
            Ok(())
        }

        fn save_checkpoint(
            &self,
            _checkpoint_name: &str,
            checkpoint: &Checkpoint,
        ) -> Result<(), String> {
            self.operations.borrow_mut().push("SAVE_CHECKPOINT");
            self.committed_checkpoint.replace(Some(checkpoint.clone()));
            Ok(())
        }

        fn mark_recovery_committed(
            &self,
            _recovery_id: &str,
            _proof: &LostBinlogReconciliationProof,
        ) -> Result<(), String> {
            self.operations.borrow_mut().push("COMMIT_RECOVERY");
            if self.fail_mark_committed {
                return Err("injected recovery commit failure".to_string());
            }
            Ok(())
        }

        fn commit_transaction(&self) -> Result<(), String> {
            self.operations.borrow_mut().push("COMMIT");
            Ok(())
        }

        fn rollback_transaction(&self) -> Result<(), String> {
            self.operations.borrow_mut().push("ROLLBACK");
            self.committed_checkpoint.replace(None);
            Ok(())
        }
    }

    struct FixedBoundaryReader(Checkpoint);

    impl LostBinlogBoundaryReader for FixedBoundaryReader {
        fn read_binlog_coordinate(&self) -> Result<Checkpoint, String> {
            Ok(self.0.clone())
        }
    }

    type RecoveryTransactionSnapshot = (
        Option<Checkpoint>,
        std::collections::BTreeMap<String, LostBinlogRecoveryRecord>,
    );

    struct ReplacementRecoveryStore {
        checkpoint: RefCell<Option<Checkpoint>>,
        barrier: RefCell<Option<LostBinlogBarrier>>,
        recoveries: RefCell<std::collections::BTreeMap<String, LostBinlogRecoveryRecord>>,
        operations: RefCell<Vec<&'static str>>,
        fail_abandon: bool,
        fail_insert: bool,
        fail_commit: bool,
        transaction_snapshot: RefCell<Option<RecoveryTransactionSnapshot>>,
    }

    impl ReplacementRecoveryStore {
        fn new(
            checkpoint: Checkpoint,
            barrier: LostBinlogBarrier,
            recoveries: Vec<LostBinlogRecoveryRecord>,
        ) -> Self {
            Self {
                checkpoint: RefCell::new(Some(checkpoint)),
                barrier: RefCell::new(Some(barrier)),
                recoveries: RefCell::new(
                    recoveries
                        .into_iter()
                        .map(|recovery| (recovery.recovery_id.clone(), recovery))
                        .collect(),
                ),
                operations: RefCell::new(Vec::new()),
                fail_abandon: false,
                fail_insert: false,
                fail_commit: false,
                transaction_snapshot: RefCell::new(None),
            }
        }

        fn recovery(&self, recovery_id: &str) -> Option<LostBinlogRecoveryRecord> {
            self.recoveries.borrow().get(recovery_id).cloned()
        }
    }

    impl LostBinlogRecoveryStore for ReplacementRecoveryStore {
        fn acquire_stream_lease(&self, _lease_name: &str) -> Result<(), String> {
            self.operations.borrow_mut().push("LEASE");
            Ok(())
        }

        fn begin_transaction(&self) -> Result<(), String> {
            self.operations.borrow_mut().push("BEGIN");
            *self.transaction_snapshot.borrow_mut() = Some((
                self.checkpoint.borrow().clone(),
                self.recoveries.borrow().clone(),
            ));
            Ok(())
        }

        fn load_checkpoint_for_update(
            &self,
            _checkpoint_name: &str,
        ) -> Result<Option<Checkpoint>, String> {
            self.operations.borrow_mut().push("LOCK_CHECKPOINT");
            Ok(self.checkpoint.borrow().clone())
        }

        fn load_barrier_for_update(
            &self,
            _barrier: &LostBinlogBarrier,
        ) -> Result<Option<LostBinlogBarrier>, String> {
            self.operations.borrow_mut().push("LOCK_BARRIER");
            Ok(self.barrier.borrow().clone())
        }

        fn load_recovery_for_update(
            &self,
            recovery_id: &str,
        ) -> Result<Option<LostBinlogRecoveryRecord>, String> {
            self.operations.borrow_mut().push("LOCK_RECOVERY");
            Ok(self.recovery(recovery_id))
        }

        fn load_barrier_recovery_owner_for_update(
            &self,
            _barrier: &LostBinlogBarrier,
        ) -> Result<Option<LostBinlogRecoveryRecord>, String> {
            self.operations.borrow_mut().push("LOCK_ACTIVE_OWNER");
            Ok(self.recoveries.borrow().values().next().cloned())
        }

        fn mark_recovery_abandoned(
            &self,
            recovery: &LostBinlogRecoveryRecord,
            _replacement_recovery_id: &str,
            evidence_json: &str,
        ) -> Result<(), String> {
            self.operations.borrow_mut().push("ABANDON_RECOVERY");
            if self.fail_abandon {
                return Err("injected abandonment update failure".to_string());
            }
            let mut abandoned = recovery.clone();
            abandoned.status = LostBinlogRecoveryStatus::Abandoned;
            abandoned.abandoned_evidence_json = Some(evidence_json.to_string());
            abandoned.abandoned_at = Some("server-generated".to_string());
            self.recoveries
                .borrow_mut()
                .insert(abandoned.recovery_id.clone(), abandoned);
            Ok(())
        }

        fn insert_prepared_recovery(
            &self,
            recovery: &LostBinlogRecoveryRecord,
        ) -> Result<(), String> {
            self.operations.borrow_mut().push("INSERT_RECOVERY");
            if self.fail_insert {
                return Err("injected replacement insert failure".to_string());
            }
            self.recoveries
                .borrow_mut()
                .insert(recovery.recovery_id.clone(), recovery.clone());
            Ok(())
        }

        fn save_checkpoint(
            &self,
            _checkpoint_name: &str,
            checkpoint: &Checkpoint,
        ) -> Result<(), String> {
            self.operations.borrow_mut().push("SAVE_CHECKPOINT");
            self.checkpoint.replace(Some(checkpoint.clone()));
            Ok(())
        }

        fn mark_recovery_committed(
            &self,
            _recovery_id: &str,
            _proof: &LostBinlogReconciliationProof,
        ) -> Result<(), String> {
            self.operations.borrow_mut().push("COMMIT_RECOVERY");
            Ok(())
        }

        fn commit_transaction(&self) -> Result<(), String> {
            self.operations.borrow_mut().push("COMMIT");
            if self.fail_commit {
                return Err("injected preparation commit failure".to_string());
            }
            self.transaction_snapshot.borrow_mut().take();
            Ok(())
        }

        fn rollback_transaction(&self) -> Result<(), String> {
            self.operations.borrow_mut().push("ROLLBACK");
            if let Some((checkpoint, recoveries)) = self.transaction_snapshot.borrow_mut().take() {
                self.checkpoint.replace(checkpoint);
                self.recoveries.replace(recoveries);
            }
            Ok(())
        }
    }

    #[test]
    fn committed_source_boundary_reads_coordinate_then_committed_evidence_without_locking() {
        let steps = Rc::new(RefCell::new(Vec::new()));
        let expected_checkpoint = checkpoint("mysqld-bin.000010", 500);

        let result = read_committed_source_boundary(
            {
                let steps = Rc::clone(&steps);
                let expected_checkpoint = expected_checkpoint.clone();
                move || {
                    steps.borrow_mut().push("coordinate");
                    Ok(expected_checkpoint)
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |captured_checkpoint| {
                    steps.borrow_mut().push("source_evidence");
                    assert_eq!(captured_checkpoint.source_position, 500);
                    Ok("evidence")
                }
            },
        );

        assert_eq!(result, Ok((expected_checkpoint, "evidence")));
        assert_eq!(steps.borrow().as_slice(), ["coordinate", "source_evidence"]);
    }

    #[test]
    fn committed_source_boundary_coordinate_error_stops_before_source_evidence() {
        let steps = Rc::new(RefCell::new(Vec::new()));
        let result = read_committed_source_boundary(
            {
                let steps = Rc::clone(&steps);
                move || {
                    steps.borrow_mut().push("coordinate");
                    Err::<Checkpoint, String>("coordinate failed".to_string())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_captured_checkpoint| {
                    steps.borrow_mut().push("source_evidence");
                    Ok::<&str, String>("evidence")
                }
            },
        );

        assert_eq!(result, Err("coordinate failed".to_string()));
        assert_eq!(steps.borrow().as_slice(), ["coordinate"]);
    }

    #[test]
    fn source_evidence_error_stops_repair_schema_and_commit_phases() {
        let steps = Rc::new(RefCell::new(Vec::new()));
        let result = run_recovery_phases_with_source_evidence(
            {
                let steps = Rc::clone(&steps);
                move || {
                    steps.borrow_mut().push("coordinate");
                    Ok("coordinate-1".to_string())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_coordinate| {
                    steps.borrow_mut().push("source_evidence");
                    Err::<&str, String>("source evidence failed".to_string())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_coordinate, _evidence| {
                    steps.borrow_mut().push("reconcile_data");
                    Ok("repaired")
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_coordinate, _evidence, _repair| {
                    steps.borrow_mut().push("converge_schema");
                    Ok("converged")
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_coordinate, _evidence, _repair, _schema| {
                    steps.borrow_mut().push("commit");
                    Ok(())
                }
            },
        );

        assert_eq!(result, Err("source evidence failed".to_string()));
        assert_eq!(steps.borrow().as_slice(), ["coordinate", "source_evidence"]);
    }

    #[test]
    fn recovery_reuses_committed_source_evidence_for_schema() {
        let steps = Rc::new(RefCell::new(Vec::new()));
        let source_evidence = Rc::new(RefCell::new(None::<String>));

        let result = run_recovery_phases_with_source_evidence(
            {
                let steps = Rc::clone(&steps);
                move || {
                    steps.borrow_mut().push("coordinate");
                    Ok("coordinate-1".to_string())
                }
            },
            {
                let steps = Rc::clone(&steps);
                let source_evidence = Rc::clone(&source_evidence);
                move |coordinate| {
                    steps.borrow_mut().push("source_evidence");
                    assert_eq!(coordinate.as_str(), "coordinate-1");
                    source_evidence.replace(Some("schema-1".to_string()));
                    Ok("schema-1".to_string())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |coordinate, evidence| {
                    steps.borrow_mut().push("reconcile_data");
                    assert_eq!(coordinate.as_str(), "coordinate-1");
                    assert_eq!(evidence.as_str(), "schema-1");
                    Ok("repaired".to_string())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_coordinate, evidence, _repair| {
                    steps.borrow_mut().push("converge_schema");
                    assert_eq!(evidence.as_str(), "schema-1");
                    Ok("converged".to_string())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_coordinate, evidence, _repair, _schema| {
                    steps.borrow_mut().push("commit");
                    assert_eq!(evidence.as_str(), "schema-1");
                    Ok(())
                }
            },
        );

        assert_eq!(result, Ok(()));
        assert_eq!(source_evidence.borrow().as_deref(), Some("schema-1"));
        assert_eq!(
            steps.borrow().as_slice(),
            [
                "coordinate",
                "source_evidence",
                "reconcile_data",
                "converge_schema",
                "commit"
            ]
        );
    }

    #[test]
    fn recovery_repairs_target_orphans_before_schema_fk_convergence() {
        let steps = Rc::new(RefCell::new(Vec::new()));
        let target_has_orphans = Rc::new(RefCell::new(true));

        let result = run_recovery_phases(
            {
                let steps = Rc::clone(&steps);
                move || {
                    steps.borrow_mut().push("snapshot");
                    Ok(())
                }
            },
            {
                let steps = Rc::clone(&steps);
                let target_has_orphans = Rc::clone(&target_has_orphans);
                move |_snapshot| {
                    steps.borrow_mut().push("reconcile_data");
                    target_has_orphans.replace(false);
                    Ok(())
                }
            },
            {
                let steps = Rc::clone(&steps);
                let target_has_orphans = Rc::clone(&target_has_orphans);
                move |_snapshot, _repair| {
                    steps.borrow_mut().push("converge_schema");
                    if *target_has_orphans.borrow() {
                        return Err("MySQL 1452: target FK has orphan rows".to_string());
                    }
                    Ok(())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_snapshot, _repair, _schema| {
                    steps.borrow_mut().push("commit");
                    Ok(())
                }
            },
        );

        assert_eq!(result, Ok(()));
        assert_eq!(
            steps.borrow().as_slice(),
            ["snapshot", "reconcile_data", "converge_schema", "commit"]
        );
    }

    #[test]
    fn recovery_schema_failure_stops_before_commit() {
        let steps = Rc::new(RefCell::new(Vec::new()));
        let result = run_recovery_phases(
            {
                let steps = Rc::clone(&steps);
                move || {
                    steps.borrow_mut().push("snapshot");
                    Ok(())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_snapshot| {
                    steps.borrow_mut().push("reconcile_data");
                    Ok(())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_snapshot, _repair| {
                    steps.borrow_mut().push("converge_schema");
                    Err::<(), String>("legacy table drop failed".to_string())
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_snapshot, _repair, _schema| {
                    steps.borrow_mut().push("commit");
                    Ok(())
                }
            },
        );

        assert_eq!(result, Err("legacy table drop failed".to_string()));
        assert_eq!(
            steps.borrow().as_slice(),
            ["snapshot", "reconcile_data", "converge_schema"]
        );
    }

    #[test]
    fn final_target_inventory_extras_block_recovery_proof() {
        let source = inventory_with_table_names(&["llm_conversations", "llm_messages"]);
        let target = inventory_with_table_names(&[
            "llm_conversations",
            "llm_messages",
            "capy_conversations",
        ]);

        let error = require_exact_table_inventory(&source, &target)
            .expect_err("target-only table must block recovery proof");

        assert!(error.contains("capy_conversations"));
    }

    #[test]
    fn replacement_abandons_old_prepared_owner_before_inserting_new_record() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut old_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        old_request.recovery_id = "recovery-old-owner".to_string();
        let old_prepared =
            LostBinlogRecoveryRecord::prepared(&old_request, latest_checkpoint.clone());
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        let store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![old_prepared]);

        let prepared = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect("replacement preparation should succeed");

        assert_eq!(prepared.recovery_id, "recovery-replacement");
        assert_eq!(
            format!("{:?}", store.recovery("recovery-old-owner").unwrap().status),
            "Abandoned"
        );
        assert_eq!(
            format!(
                "{:?}",
                store.recovery("recovery-replacement").unwrap().status
            ),
            "Prepared"
        );
        let operations = store.operations.borrow();
        let abandon = operations
            .iter()
            .position(|operation| *operation == "ABANDON_RECOVERY")
            .expect("old owner must be abandoned");
        let insert = operations
            .iter()
            .position(|operation| *operation == "INSERT_RECOVERY")
            .expect("replacement must be inserted");
        assert!(abandon < insert);
        assert_eq!(operations.first(), Some(&"LEASE"));
        assert_eq!(operations.get(1), Some(&"BEGIN"));
        assert_eq!(operations.get(2), Some(&"LOCK_CHECKPOINT"));
        assert_eq!(operations.get(3), Some(&"LOCK_BARRIER"));
        assert_eq!(operations.last(), Some(&"COMMIT"));
    }

    #[test]
    fn replacement_insert_failure_rolls_back_abandonment() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut old_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        old_request.recovery_id = "recovery-old-owner".to_string();
        let old_prepared =
            LostBinlogRecoveryRecord::prepared(&old_request, latest_checkpoint.clone());
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        let mut store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![old_prepared]);
        store.fail_insert = true;

        let error = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect_err("replacement insert failure must fail closed");

        assert!(error.contains("replacement insert failure"));
        assert_eq!(
            format!("{:?}", store.recovery("recovery-old-owner").unwrap().status),
            "Prepared"
        );
        assert!(store.recovery("recovery-replacement").is_none());
        assert_eq!(store.operations.borrow().last(), Some(&"ROLLBACK"));
    }

    #[test]
    fn abandonment_update_failure_rolls_back_without_inserting_replacement() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut old_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        old_request.recovery_id = "recovery-old-owner".to_string();
        let old_prepared =
            LostBinlogRecoveryRecord::prepared(&old_request, latest_checkpoint.clone());
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        let mut store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![old_prepared]);
        store.fail_abandon = true;

        let error = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect_err("abandonment failure must fail closed");

        assert!(error.contains("abandon"));
        assert_eq!(
            format!("{:?}", store.recovery("recovery-old-owner").unwrap().status),
            "Prepared"
        );
        assert!(store.recovery("recovery-replacement").is_none());
        assert_eq!(store.operations.borrow().last(), Some(&"ROLLBACK"));
    }

    #[test]
    fn preparation_commit_failure_rolls_back_old_and_new_recovery_rows() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut old_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        old_request.recovery_id = "recovery-old-owner".to_string();
        let old_prepared =
            LostBinlogRecoveryRecord::prepared(&old_request, latest_checkpoint.clone());
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        let mut store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![old_prepared]);
        store.fail_commit = true;

        let error = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect_err("transaction commit failure must fail closed");

        assert!(error.contains("preparation commit failure"));
        assert_eq!(
            format!("{:?}", store.recovery("recovery-old-owner").unwrap().status),
            "Prepared"
        );
        assert!(store.recovery("recovery-replacement").is_none());
        assert_eq!(store.operations.borrow().last(), Some(&"ROLLBACK"));
    }

    #[test]
    fn prepared_owner_with_committed_status_refuses_replacement() {
        assert_replacement_refuses_owner_status(LostBinlogRecoveryStatus::Committed);
    }

    #[test]
    fn prepared_owner_with_verified_status_refuses_replacement() {
        assert_replacement_refuses_owner_status(LostBinlogRecoveryStatus::Verified);
    }

    fn assert_replacement_refuses_owner_status(status: LostBinlogRecoveryStatus) {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut old_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        old_request.recovery_id = "recovery-old-owner".to_string();
        let mut old_owner =
            LostBinlogRecoveryRecord::prepared(&old_request, latest_checkpoint.clone());
        old_owner.status = status;
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        let store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![old_owner]);

        let error = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect_err("terminal owner status must refuse replacement");

        assert!(error.contains("owner"));
        assert!(store.recovery("recovery-replacement").is_none());
    }

    #[test]
    fn prepared_owner_with_abandoned_status_refuses_replacement() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut old_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        old_request.recovery_id = "recovery-old-owner".to_string();
        let mut abandoned_owner =
            LostBinlogRecoveryRecord::prepared(&old_request, latest_checkpoint.clone());
        abandoned_owner.status = LostBinlogRecoveryStatus::Abandoned;
        abandoned_owner.abandoned_evidence_json =
            Some("{\"old_recovery_id\":\"recovery-old-owner\"}".to_string());
        abandoned_owner.abandoned_at = Some("server-generated".to_string());
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        let store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![abandoned_owner]);

        let error = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect_err("abandoned owner must refuse replacement");

        assert!(error.contains("owner"));
        assert!(store.recovery("recovery-replacement").is_none());
    }

    #[test]
    fn replacement_abandonment_evidence_binds_exact_authorization_without_client_timestamp() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut old_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        old_request.recovery_id = "recovery-old-owner".to_string();
        let old_prepared =
            LostBinlogRecoveryRecord::prepared(&old_request, latest_checkpoint.clone());
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        replacement_request.operator_identity = "retry-operator@example.com".to_string();
        replacement_request.reason = "retry after stale prepared owner".to_string();
        let store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![old_prepared]);

        prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect("replacement preparation should succeed");

        let owner = store.recovery("recovery-old-owner").unwrap();
        let evidence = owner.abandoned_evidence_json.unwrap();
        for expected in [
            "old_recovery_id",
            "replacement_recovery_id",
            "retry-operator@example.com",
            "retry after stale prepared owner",
            "stream-binlog:source-1",
            "full-replicated-scope-sha256",
            "source-1#server-id=3",
            "mysqld-bin.000001",
            "DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives",
        ] {
            assert!(
                evidence.contains(expected),
                "missing evidence value: {expected}"
            );
        }
        assert!(!evidence.contains("abandoned_at"));
    }

    #[test]
    fn duplicate_new_recovery_id_refuses_replacement() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut request = recovery_request(old_checkpoint.clone(), barrier.clone());
        request.recovery_id = "recovery-duplicate".to_string();
        let existing = LostBinlogRecoveryRecord::prepared(&request, latest_checkpoint.clone());
        let store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![existing]);

        let error =
            prepare_lost_binlog_recovery(&FixedBoundaryReader(latest_checkpoint), &store, &request)
                .expect_err("duplicate recovery ID must refuse replacement");

        assert!(error.contains("already exists"));
        assert!(store.recovery("recovery-duplicate").is_some());
    }

    #[test]
    fn owner_checkpoint_mismatch_refuses_replacement() {
        let request_checkpoint = checkpoint("mysqld-bin.000002", 200);
        let owner_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000003", 300);
        let barrier = production_barrier();
        let mut owner_request = recovery_request(owner_checkpoint, barrier.clone());
        owner_request.recovery_id = "recovery-old-owner".to_string();
        let owner = LostBinlogRecoveryRecord::prepared(&owner_request, latest_checkpoint.clone());
        let mut replacement_request = recovery_request(request_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        let store = ReplacementRecoveryStore::new(request_checkpoint, barrier, vec![owner]);

        let error = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect_err("owner checkpoint mismatch must refuse replacement");

        assert!(error.contains("owner"));
        assert!(store.recovery("recovery-replacement").is_none());
    }

    #[test]
    fn owner_source_identity_mismatch_refuses_replacement() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut owner_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        owner_request.recovery_id = "recovery-old-owner".to_string();
        let mut owner =
            LostBinlogRecoveryRecord::prepared(&owner_request, latest_checkpoint.clone());
        owner.source_identity = "different-source#server-id=9".to_string();
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        let store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![owner]);

        let error = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect_err("owner source identity mismatch must refuse replacement");

        assert!(error.contains("owner"));
        assert!(store.recovery("recovery-replacement").is_none());
    }

    #[test]
    fn replacement_uses_current_scope_without_mutating_old_scope_evidence() {
        let old_checkpoint = checkpoint("mysqld-bin.000001", 100);
        let latest_checkpoint = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let mut owner_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        owner_request.recovery_id = "recovery-old-owner".to_string();
        owner_request.scope_hash = "old-scope-hash".to_string();
        let owner = LostBinlogRecoveryRecord::prepared(&owner_request, latest_checkpoint.clone());
        let mut replacement_request = recovery_request(old_checkpoint.clone(), barrier.clone());
        replacement_request.recovery_id = "recovery-replacement".to_string();
        replacement_request.scope_hash = "new-scope-hash".to_string();
        let store = ReplacementRecoveryStore::new(old_checkpoint, barrier, vec![owner]);

        let prepared = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(latest_checkpoint),
            &store,
            &replacement_request,
        )
        .expect("replacement may use the current scope hash");

        assert_eq!(prepared.scope_hash, "new-scope-hash");
        let abandoned = store.recovery("recovery-old-owner").unwrap();
        assert_eq!(abandoned.status, LostBinlogRecoveryStatus::Abandoned);
        assert_eq!(abandoned.scope_hash, "old-scope-hash");
        assert_eq!(abandoned.prepared_evidence_json, "{\"scope\":\"complete\"}");
        assert!(
            abandoned
                .abandoned_evidence_json
                .as_deref()
                .is_some_and(|evidence| evidence.contains("new-scope-hash"))
        );
        assert_eq!(
            store.recovery("recovery-replacement").unwrap().scope_hash,
            "new-scope-hash"
        );
    }

    #[test]
    fn recovery_schema_has_abandoned_history_and_one_active_barrier_owner() {
        let bootstrap = include_str!("../docs/stream-recovery-records-bootstrap.sql");
        assert!(bootstrap.contains("abandoned_evidence_json"));
        assert!(bootstrap.contains("abandoned_at"));
        assert!(bootstrap.contains("active_barrier_identity"));
        assert!(bootstrap.contains("stream_recovery_active_barrier"));
        assert!(bootstrap.contains("'abandoned'"));

        let migration_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/docs/stream-recovery-records-abandoned-replacement-migration.sql"
        );
        let migration = std::fs::read_to_string(migration_path)
            .expect("abandoned replacement migration must be tracked");
        assert!(migration.contains("stream_recovery_records_chk_6"));
        assert!(migration.contains("stream_recovery_exact_barrier"));
        assert!(migration.contains("stream_recovery_active_barrier"));
        assert!(migration.contains("prepared -> abandoned"));
    }

    #[test]
    fn prepares_recovery_with_latest_source_boundary_and_exact_old_state() {
        let old = checkpoint("mysqld-bin.000001", 100);
        let latest = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let store = RecordingRecoveryStore {
            checkpoint: RefCell::new(Some(old.clone())),
            barrier: RefCell::new(Some(barrier.clone())),
            ..Default::default()
        };
        let request = recovery_request(old, barrier);

        let prepared =
            prepare_lost_binlog_recovery(&FixedBoundaryReader(latest.clone()), &store, &request)
                .expect("prepare exact authorized recovery");

        assert_eq!(prepared.new_checkpoint, latest);
        assert_eq!(prepared.status, LostBinlogRecoveryStatus::Prepared);
        assert_eq!(
            store.operations.borrow().as_slice(),
            [
                "LEASE",
                "BEGIN",
                "LOCK_CHECKPOINT",
                "LOCK_BARRIER",
                "LOCK_RECOVERY",
                "LOCK_ACTIVE_OWNER",
                "INSERT_RECOVERY",
                "COMMIT"
            ]
        );
    }

    #[test]
    fn prepare_refuses_checkpoint_mismatch_without_recording_recovery() {
        let expected = checkpoint("mysqld-bin.000001", 100);
        let actual = checkpoint("mysqld-bin.000003", 4);
        let barrier = production_barrier();
        let store = RecordingRecoveryStore {
            checkpoint: RefCell::new(Some(actual)),
            barrier: RefCell::new(Some(barrier.clone())),
            ..Default::default()
        };
        let request = recovery_request(expected, barrier);

        let error = prepare_lost_binlog_recovery(
            &FixedBoundaryReader(checkpoint("mysqld-bin.000002", 300)),
            &store,
            &request,
        )
        .expect_err("mismatched checkpoint must fail closed");

        assert!(error.contains("checkpoint mismatch"));
        assert!(store.recovery.borrow().is_none());
        assert_eq!(store.operations.borrow().last(), Some(&"ROLLBACK"));
    }

    #[test]
    fn prepare_refuses_duplicate_recovery_id_without_mutating_state() {
        let old = checkpoint("mysqld-bin.000001", 100);
        let latest = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let request = recovery_request(old.clone(), barrier.clone());
        let existing = LostBinlogRecoveryRecord::prepared(&request, latest.clone());
        let store = RecordingRecoveryStore {
            checkpoint: RefCell::new(Some(old)),
            barrier: RefCell::new(Some(barrier)),
            recovery: RefCell::new(Some(existing)),
            ..Default::default()
        };

        let error = prepare_lost_binlog_recovery(&FixedBoundaryReader(latest), &store, &request)
            .expect_err("duplicate recovery IDs must fail closed");

        assert!(error.contains("recovery already exists"));
        assert!(store.committed_checkpoint.borrow().is_none());
        assert_eq!(store.operations.borrow().last(), Some(&"ROLLBACK"));
    }

    #[test]
    fn prepare_refuses_a_boundary_that_does_not_advance_the_checkpoint() {
        let old = checkpoint("mysqld-bin.000001", 100);
        let barrier = production_barrier();
        let store = RecordingRecoveryStore {
            checkpoint: RefCell::new(Some(old.clone())),
            barrier: RefCell::new(Some(barrier.clone())),
            ..Default::default()
        };
        let request = recovery_request(old.clone(), barrier);

        let error = prepare_lost_binlog_recovery(&FixedBoundaryReader(old), &store, &request)
            .expect_err("recovery boundary must advance beyond the obsolete checkpoint");

        assert!(error.contains("does not advance"));
        assert!(store.recovery.borrow().is_none());
    }

    #[test]
    fn commit_requires_complete_full_scope_reconciliation_proof() {
        let old = checkpoint("mysqld-bin.000001", 100);
        let latest = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let request = recovery_request(old.clone(), barrier.clone());
        let prepared = LostBinlogRecoveryRecord::prepared(&request, latest);
        let store = RecordingRecoveryStore {
            checkpoint: RefCell::new(Some(old)),
            barrier: RefCell::new(Some(barrier)),
            recovery: RefCell::new(Some(prepared)),
            ..Default::default()
        };
        let proof = LostBinlogReconciliationProof {
            recovery_id: request.recovery_id.clone(),
            source_identity: request.expected_barrier.source_identity.clone(),
            scope_hash: request.scope_hash.clone(),
            schema_converged: true,
            data_converged: false,
            unsupported_scope: vec!["audit_logs".to_string()],
            evidence_json: "{}".to_string(),
        };

        let error = commit_lost_binlog_recovery(&store, &request, &proof)
            .expect_err("incomplete proof must block checkpoint transition");

        assert!(error.contains("reconciliation proof is incomplete"));
        assert!(store.committed_checkpoint.borrow().is_none());
    }

    #[test]
    fn commit_rolls_back_checkpoint_when_recovery_record_cannot_commit() {
        let old = checkpoint("mysqld-bin.000001", 100);
        let latest = checkpoint("mysqld-bin.000002", 300);
        let barrier = production_barrier();
        let request = recovery_request(old.clone(), barrier.clone());
        let prepared = LostBinlogRecoveryRecord::prepared(&request, latest);
        let store = RecordingRecoveryStore {
            checkpoint: RefCell::new(Some(old)),
            barrier: RefCell::new(Some(barrier)),
            recovery: RefCell::new(Some(prepared)),
            fail_mark_committed: true,
            ..Default::default()
        };

        let error = commit_lost_binlog_recovery(&store, &request, &complete_proof(&request))
            .expect_err("recovery and checkpoint must commit atomically");

        assert!(error.contains("injected recovery commit failure"));
        assert!(store.committed_checkpoint.borrow().is_none());
        assert_eq!(store.operations.borrow().last(), Some(&"ROLLBACK"));
    }

    #[test]
    fn verified_recovery_exempts_only_its_exact_historical_barrier() {
        let barrier = production_barrier();
        let sql = crate::lost_binlog_recovery_store::build_active_barrier_select_sql(
            "cdc.ddl_replay_journal",
            "cdc.stream_recovery_records",
            &barrier.source_identity,
        );

        assert!(sql.contains("recovery.status IN ('committed','verified')"));
        assert!(sql.contains("recovery.old_barrier_source_identity = journal.source_identity"));
        assert!(sql.contains("recovery.old_barrier_file = journal.binlog_file"));
        assert!(sql.contains("recovery.old_barrier_start_position = journal.event_start_position"));
        assert!(sql.contains("recovery.old_barrier_end_position = journal.event_end_position"));
        assert!(sql.contains("recovery.old_barrier_raw_sql_sha256 = SHA2(journal.raw_sql, 256)"));
    }

    fn inventory_with_table_names(names: &[&str]) -> SchemaInventory {
        SchemaInventory {
            schema: "globalcomix".to_string(),
            tables: names
                .iter()
                .map(|name| crate::inventory::TableInventory {
                    name: (*name).to_string(),
                    table_type: "BASE TABLE".to_string(),
                    engine: Some("InnoDB".to_string()),
                    collation: None,
                    primary_key: vec!["id".to_string()],
                    columns: Vec::new(),
                })
                .collect(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            routines: Vec::new(),
            events: Vec::new(),
        }
    }

    fn checkpoint(file: &str, position: u64) -> Checkpoint {
        Checkpoint {
            source_file: file.to_string(),
            source_position: position,
            gtid: None,
            event_timestamp: 0,
            last_event: LastEvent {
                event_type: "LostBinlogRecovery".to_string(),
                description: "authorized lost-binlog recovery boundary".to_string(),
            },
        }
    }

    fn production_barrier() -> LostBinlogBarrier {
        LostBinlogBarrier {
            source_identity: "source-1#server-id=3".to_string(),
            binlog_file: "mysqld-bin.000001".to_string(),
            event_start_position: 120,
            event_end_position: 180,
            raw_sql: "DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives".to_string(),
        }
    }

    fn recovery_request(
        expected_checkpoint: Checkpoint,
        expected_barrier: LostBinlogBarrier,
    ) -> LostBinlogRecoveryRequest {
        LostBinlogRecoveryRequest {
            recovery_id: "fixture-lost-binlog-recovery".to_string(),
            checkpoint_name: "stream-binlog:source-1".to_string(),
            expected_checkpoint,
            expected_barrier,
            scope_hash: "full-replicated-scope-sha256".to_string(),
            operator_identity: "operator@example.com".to_string(),
            reason: "authorized recovery after source binlog purge".to_string(),
            prepared_evidence_json: "{\"scope\":\"complete\"}".to_string(),
        }
    }

    fn complete_proof(request: &LostBinlogRecoveryRequest) -> LostBinlogReconciliationProof {
        LostBinlogReconciliationProof {
            recovery_id: request.recovery_id.clone(),
            source_identity: request.expected_barrier.source_identity.clone(),
            scope_hash: request.scope_hash.clone(),
            schema_converged: true,
            data_converged: true,
            unsupported_scope: Vec::new(),
            evidence_json: "{\"schema\":\"converged\",\"data\":\"converged\"}".to_string(),
        }
    }
}
