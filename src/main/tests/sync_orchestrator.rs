use crate::inventory::{ColumnInventory, ForeignKeyInventory, SchemaInventory, TableInventory};
use crate::live::TargetMySqlConfig;
use crate::mysql_snapshot::MySqlConnectionConfig;
use crate::sync::{
    SyncChunkProgress, SyncConfig, SyncProgressRow, SyncProgressStatus, SyncRunExecutor,
    SyncRunIdentity, SyncRunProgressStore, SyncStage, SyncTable, build_sync_run_identity,
    run_sync_orchestration, sync_tables_from_source_inventory,
};
use crate::sync_schema::SchemaSourceEvidence;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn sync_orchestrator_runs_schema_rows_constraints_with_per_table_progress() {
    let (config, evidence, tables, identity) = fixture();
    let mut executor = RecordingSyncExecutor::default();
    let mut progress = MemoryRunProgress::default();

    let rows = run_sync_orchestration(
        &config,
        &identity,
        &evidence,
        tables,
        &mut executor,
        &mut progress,
    )
    .expect("unified sync orchestration");

    assert_eq!(
        executor.events,
        ["schema:prerequisite_schema", "rows", "schema:final_constraints"]
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(
        saved_stage_statuses(&progress),
        [
            ("prerequisite_schema", "alpha", "running"),
            ("prerequisite_schema", "beta", "running"),
            ("prerequisite_schema", "alpha", "complete"),
            ("prerequisite_schema", "beta", "complete"),
            ("final_constraints", "alpha", "running"),
            ("final_constraints", "beta", "running"),
            ("final_constraints", "alpha", "complete"),
            ("final_constraints", "beta", "complete"),
        ]
    );
}

#[test]
fn sync_orchestrator_skips_only_when_every_table_stage_is_complete() {
    let (config, evidence, tables, identity) = fixture();
    let mut progress = MemoryRunProgress::with_rows([
        stage_row(
            &identity,
            SyncStage::PrerequisiteSchema,
            "alpha",
            SyncProgressStatus::Complete,
            None,
        ),
        stage_row(
            &identity,
            SyncStage::PrerequisiteSchema,
            "beta",
            SyncProgressStatus::Complete,
            None,
        ),
        stage_row(
            &identity,
            SyncStage::FinalConstraints,
            "alpha",
            SyncProgressStatus::Complete,
            None,
        ),
        stage_row(
            &identity,
            SyncStage::FinalConstraints,
            "beta",
            SyncProgressStatus::Complete,
            None,
        ),
    ]);
    let mut executor = RecordingSyncExecutor::default();

    run_sync_orchestration(
        &config,
        &identity,
        &evidence,
        tables,
        &mut executor,
        &mut progress,
    )
    .expect("resume completed schema stages");

    assert_eq!(executor.events, ["rows"]);
    assert!(progress.saves.is_empty());
}

#[test]
fn sync_orchestrator_replays_running_error_and_partially_complete_stage() {
    let (config, evidence, tables, identity) = fixture();
    let mut progress = MemoryRunProgress::with_rows([
        stage_row(
            &identity,
            SyncStage::PrerequisiteSchema,
            "alpha",
            SyncProgressStatus::Complete,
            None,
        ),
        stage_row(
            &identity,
            SyncStage::PrerequisiteSchema,
            "beta",
            SyncProgressStatus::Error,
            Some("interrupted"),
        ),
        stage_row(
            &identity,
            SyncStage::FinalConstraints,
            "alpha",
            SyncProgressStatus::Running,
            None,
        ),
    ]);
    let mut executor = RecordingSyncExecutor::default();

    run_sync_orchestration(
        &config,
        &identity,
        &evidence,
        tables,
        &mut executor,
        &mut progress,
    )
    .expect("replay incomplete schema stages");

    assert_eq!(
        executor.events,
        ["schema:prerequisite_schema", "rows", "schema:final_constraints"]
    );
    assert_eq!(
        saved_stage_statuses(&progress),
        [
            ("prerequisite_schema", "beta", "running"),
            ("prerequisite_schema", "alpha", "complete"),
            ("prerequisite_schema", "beta", "complete"),
            ("final_constraints", "alpha", "running"),
            ("final_constraints", "beta", "running"),
            ("final_constraints", "alpha", "complete"),
            ("final_constraints", "beta", "complete"),
        ]
    );
}

#[test]
fn sync_orchestrator_replays_after_complete_progress_save_failure() {
    let (config, evidence, tables, identity) = fixture();
    let failure = SaveFailure::once(
        SyncStage::PrerequisiteSchema,
        "beta",
        SyncProgressStatus::Complete,
    );
    let mut progress = MemoryRunProgress::failing(failure);
    let mut executor = RecordingSyncExecutor::default();

    let error = run_sync_orchestration(
        &config,
        &identity,
        &evidence,
        tables.clone(),
        &mut executor,
        &mut progress,
    )
    .expect_err("complete progress failure");
    assert_eq!(
        error,
        "save sync prerequisite_schema progress for table `beta`: forced progress save failure"
    );
    assert_eq!(executor.events, ["schema:prerequisite_schema"]);

    executor.events.clear();
    run_sync_orchestration(
        &config,
        &identity,
        &evidence,
        tables,
        &mut executor,
        &mut progress,
    )
    .expect("replay after complete progress failure");
    assert_eq!(
        executor.events,
        ["schema:prerequisite_schema", "rows", "schema:final_constraints"]
    );
}

#[test]
fn sync_orchestrator_preserves_schema_error_and_appends_error_save_failure() {
    let (config, evidence, tables, identity) = fixture();
    let failure = SaveFailure::once(
        SyncStage::PrerequisiteSchema,
        "beta",
        SyncProgressStatus::Error,
    );
    let mut progress = MemoryRunProgress::failing(failure);
    let mut executor = RecordingSyncExecutor {
        schema_error: Some((
            SyncStage::PrerequisiteSchema,
            "forced prerequisite failure".to_string(),
        )),
        ..RecordingSyncExecutor::default()
    };

    let error = run_sync_orchestration(
        &config,
        &identity,
        &evidence,
        tables,
        &mut executor,
        &mut progress,
    )
    .expect_err("schema stage failure");

    assert_eq!(
        error,
        "forced prerequisite failure; additionally save sync prerequisite_schema error progress for table `beta`: forced progress save failure"
    );
    assert_eq!(executor.events, ["schema:prerequisite_schema"]);
    assert_eq!(
        progress
            .saves
            .iter()
            .filter(|row| row.status == SyncProgressStatus::Error)
            .map(|row| row.table_name.as_str())
            .collect::<Vec<_>>(),
        ["alpha"]
    );
}

#[test]
fn sync_orchestrator_rejects_progress_identity_before_any_action() {
    let (config, evidence, tables, identity) = fixture();
    let mut mismatched = stage_row(
        &identity,
        SyncStage::PrerequisiteSchema,
        "alpha",
        SyncProgressStatus::Complete,
        None,
    );
    mismatched.run_spec_json = "{}".to_string();
    let mut progress = MemoryRunProgress::with_rows([mismatched]);
    let mut executor = RecordingSyncExecutor::default();

    let error = run_sync_orchestration(
        &config,
        &identity,
        &evidence,
        tables,
        &mut executor,
        &mut progress,
    )
    .expect_err("mismatched stage progress");

    assert_eq!(
        error,
        "sync prerequisite_schema progress run specification mismatch for table `alpha`"
    );
    assert!(executor.events.is_empty());
    assert!(progress.saves.is_empty());
}

#[test]
fn sync_orchestrator_stops_before_final_constraints_after_row_failure() {
    let (config, evidence, tables, identity) = fixture();
    let mut executor = RecordingSyncExecutor {
        row_error: Some("forced row failure".to_string()),
        ..RecordingSyncExecutor::default()
    };
    let mut progress = MemoryRunProgress::default();

    let error = run_sync_orchestration(
        &config,
        &identity,
        &evidence,
        tables,
        &mut executor,
        &mut progress,
    )
    .expect_err("row failure");

    assert_eq!(error, "forced row failure");
    assert_eq!(executor.events, ["schema:prerequisite_schema", "rows"]);
    assert!(
        progress
            .saves
            .iter()
            .all(|row| row.stage != SyncStage::FinalConstraints)
    );
}

#[test]
fn sync_table_selection_requires_same_schema_parents() {
    let evidence = dependent_source_evidence();

    let error = sync_tables_from_source_inventory(
        &evidence.inventory,
        &["children".to_string()],
    )
    .expect_err("unselected parent");
    assert_eq!(
        error,
        "selected source table `children` depends on unselected source table `parents`"
    );

    let selected = sync_tables_from_source_inventory(
        &evidence.inventory,
        &["parents".to_string(), "children".to_string()],
    )
    .expect("closed dependency scope");
    assert_eq!(
        selected
            .iter()
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>(),
        ["parents", "children"]
    );
}

#[derive(Default)]
struct RecordingSyncExecutor {
    events: Vec<String>,
    schema_error: Option<(SyncStage, String)>,
    row_error: Option<String>,
}

impl SyncRunExecutor for RecordingSyncExecutor {
    fn run_schema_stage(
        &mut self,
        _config: &SyncConfig,
        _evidence: &SchemaSourceEvidence,
        stage: SyncStage,
    ) -> Result<(), String> {
        self.events.push(format!("schema:{}", stage.as_str()));
        if let Some((failed_stage, error)) = &self.schema_error {
            if *failed_stage == stage {
                return Err(error.clone());
            }
        }
        Ok(())
    }

    fn run_rows(
        &mut self,
        _config: &SyncConfig,
        identity: &SyncRunIdentity,
        tables: Vec<SyncTable>,
    ) -> Result<Vec<SyncChunkProgress>, String> {
        self.events.push("rows".to_string());
        if let Some(error) = &self.row_error {
            return Err(error.clone());
        }
        Ok(tables
            .into_iter()
            .map(|table| completed_progress(identity, &table.name))
            .collect())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct SaveFailure {
    stage: String,
    table: String,
    status: String,
}

impl SaveFailure {
    fn once(stage: SyncStage, table: &str, status: SyncProgressStatus) -> Self {
        Self {
            stage: stage.as_str().to_string(),
            table: table.to_string(),
            status: status.as_str().to_string(),
        }
    }
}

#[derive(Default)]
struct MemoryRunProgress {
    rows: BTreeMap<(String, String), SyncProgressRow>,
    saves: Vec<SyncProgressRow>,
    failures: BTreeSet<SaveFailure>,
}

impl MemoryRunProgress {
    fn with_rows<const N: usize>(rows: [SyncProgressRow; N]) -> Self {
        let rows = rows
            .into_iter()
            .map(|row| ((row.stage.as_str().to_string(), row.table_name.clone()), row))
            .collect();
        Self {
            rows,
            saves: Vec::new(),
            failures: BTreeSet::new(),
        }
    }

    fn failing(failure: SaveFailure) -> Self {
        Self {
            failures: BTreeSet::from([failure]),
            ..Self::default()
        }
    }
}

impl SyncRunProgressStore for MemoryRunProgress {
    fn load_stage(
        &mut self,
        _run_id: &str,
        stage: SyncStage,
        table_name: &str,
    ) -> Result<Option<SyncProgressRow>, String> {
        Ok(self
            .rows
            .get(&(stage.as_str().to_string(), table_name.to_string()))
            .cloned())
    }

    fn save_stage(&mut self, row: &SyncProgressRow) -> Result<(), String> {
        let failure = SaveFailure::once(row.stage, &row.table_name, row.status);
        if self.failures.remove(&failure) {
            return Err("forced progress save failure".to_string());
        }
        let key = (row.stage.as_str().to_string(), row.table_name.clone());
        self.rows.insert(key, row.clone());
        self.saves.push(row.clone());
        Ok(())
    }
}

fn saved_stage_statuses(progress: &MemoryRunProgress) -> Vec<(&str, &str, &str)> {
    progress
        .saves
        .iter()
        .map(|row| {
            (
                row.stage.as_str(),
                row.table_name.as_str(),
                row.status.as_str(),
            )
        })
        .collect()
}

fn fixture() -> (
    SyncConfig,
    SchemaSourceEvidence,
    Vec<SyncTable>,
    SyncRunIdentity,
) {
    let evidence = fixture_source_evidence();
    let config = fixture_config();
    let tables = sync_tables_from_source_inventory(&evidence.inventory, &config.tables)
        .expect("fixture sync tables");
    let identity = build_sync_run_identity(&config, tables.clone()).expect("run identity");
    (config, evidence, tables, identity)
}

fn fixture_source_evidence() -> SchemaSourceEvidence {
    SchemaSourceEvidence {
        inventory: SchemaInventory {
            schema: "source-db".to_string(),
            tables: vec![table_inventory("alpha"), table_inventory("beta")],
            indexes: vec![],
            foreign_keys: vec![],
            views: vec![],
            triggers: vec![],
            routines: vec![],
            events: vec![],
        },
        checks: vec![],
        canonical_foreign_keys: vec![],
    }
}

fn dependent_source_evidence() -> SchemaSourceEvidence {
    let mut evidence = fixture_source_evidence();
    evidence.inventory.tables = vec![table_inventory("parents"), child_table_inventory()];
    evidence.inventory.foreign_keys = vec![ForeignKeyInventory {
        table: "children".to_string(),
        name: "fk_children_parents".to_string(),
        columns: vec!["parent_id".to_string()],
        referenced_schema: "source-db".to_string(),
        referenced_table: "parents".to_string(),
        referenced_columns: vec!["id".to_string()],
    }];
    evidence
}

fn fixture_config() -> SyncConfig {
    let mut source = MySqlConnectionConfig::default();
    source.host = "source".to_string();
    source.user = "source-user".to_string();
    source.password = "source-password".to_string();
    source.database = "source-db".to_string();
    let target = TargetMySqlConfig {
        host: "target".to_string(),
        user: "target-user".to_string(),
        password: "target-password".to_string(),
        database: "target-db".to_string(),
        tls_ca_file: "/tmp/test-ca.pem".to_string(),
        ..TargetMySqlConfig::default()
    };
    SyncConfig {
        source,
        target,
        tables: vec!["alpha".to_string(), "beta".to_string()],
        chunk_size: 100,
        parallelism: 2,
        progress_table: "cdc.sync_runs".to_string(),
        run_id: Some("sync-run-42".to_string()),
        run_id_prefix: None,
    }
}

fn table_inventory(name: &str) -> TableInventory {
    TableInventory {
        name: name.to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        collation: Some("utf8mb4_0900_ai_ci".to_string()),
        primary_key: vec!["id".to_string()],
        columns: vec![column("id", 1)],
    }
}

fn child_table_inventory() -> TableInventory {
    let mut table = table_inventory("children");
    table.columns.push(column("parent_id", 2));
    table
}

fn column(name: &str, ordinal_position: u32) -> ColumnInventory {
    ColumnInventory {
        name: name.to_string(),
        ordinal_position,
        column_type: "bigint".to_string(),
        data_type: "bigint".to_string(),
        is_nullable: false,
        character_set: None,
        collation: None,
        default_value: None,
        extra: String::new(),
        comment: String::new(),
        generated: None,
    }
}

fn stage_row(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    table: &str,
    status: SyncProgressStatus,
    last_error: Option<&str>,
) -> SyncProgressRow {
    SyncProgressRow {
        run_id: identity.run_id.clone(),
        stage,
        table_name: table.to_string(),
        run_spec_json: identity.run_spec_json.clone(),
        last_primary_key: None,
        chunks: 0,
        rows_scanned: 0,
        inserts: 0,
        updates: 0,
        deletes: 0,
        status,
        last_error: last_error.map(str::to_string),
        created_at: String::new(),
        updated_at: String::new(),
        completed_at: None,
    }
}

fn completed_progress(identity: &SyncRunIdentity, table: &str) -> SyncChunkProgress {
    SyncChunkProgress {
        run_id: identity.run_id.clone(),
        table: table.to_string(),
        run_spec_json: identity.run_spec_json.clone(),
        last_primary_key: Some(vec!["done".to_string()]),
        complete: true,
        chunks: 1,
        rows_scanned: 1,
        inserts: 0,
        updates: 0,
        deletes: 0,
    }
}
