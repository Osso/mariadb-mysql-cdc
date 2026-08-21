use super::config::{
    SyncConfig, SyncRunIdentity, build_sync_run_identity, sync_table_from_inventory,
    validate_sync_config,
};
use super::model::{
    SyncChunkProgress, SyncProgressRow, SyncProgressStatus, SyncRunProgressStore, SyncStage,
    SyncTable,
};
use super::mysql::MySqlSyncProgressStore;
use super::run::run_mysql_sync_tables;
use super::run_spec_migration::{
    SyncRunSpecMigrationExecutor, SyncRunSpecMigrationOutcome, SyncRunSpecMigrationRequest,
    run_locked_sync_run_spec_migration,
};
use crate::inventory::SchemaInventory;
use crate::sync_schema::{
    SchemaSourceEvidence, SyncSchemaStageKind, read_sync_source_evidence,
    read_sync_target_inventory, run_sync_schema_stage,
};
use std::collections::BTreeSet;

pub(crate) trait SyncRunExecutor {
    fn run_schema_stage(
        &mut self,
        config: &SyncConfig,
        evidence: &SchemaSourceEvidence,
        stage: SyncStage,
    ) -> Result<(), String>;

    fn run_rows(
        &mut self,
        config: &SyncConfig,
        identity: &SyncRunIdentity,
        tables: Vec<SyncTable>,
    ) -> Result<Vec<SyncChunkProgress>, String>;
}

pub(crate) fn sync_tables_from_source_inventory(
    inventory: &SchemaInventory,
    selected: &[String],
) -> Result<Vec<SyncTable>, String> {
    validate_selected_tables(inventory, selected)?;
    selected
        .iter()
        .map(|name| {
            let table = inventory
                .tables
                .iter()
                .find(|table| table.name == *name)
                .expect("validated selected source table");
            sync_table_from_inventory(table)
        })
        .collect()
}

pub(crate) fn run_sync_orchestration(
    config: &SyncConfig,
    identity: &SyncRunIdentity,
    evidence: &SchemaSourceEvidence,
    tables: Vec<SyncTable>,
    executor: &mut impl SyncRunExecutor,
    progress: &mut impl SyncRunProgressStore,
) -> Result<Vec<SyncChunkProgress>, String> {
    validate_orchestration_identity(config, identity, &tables)?;
    let table_names = tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();

    run_durable_schema_stage(
        identity,
        SyncStage::PrerequisiteSchema,
        &table_names,
        progress,
        || executor.run_schema_stage(config, evidence, SyncStage::PrerequisiteSchema),
    )?;
    let rows = executor.run_rows(config, identity, tables)?;
    run_durable_schema_stage(
        identity,
        SyncStage::FinalConstraints,
        &table_names,
        progress,
        || executor.run_schema_stage(config, evidence, SyncStage::FinalConstraints),
    )?;
    Ok(rows)
}

pub(crate) fn read_sync_run_spec_migration_target_inventory(
    config: &SyncConfig,
    read_target: impl FnOnce(&crate::live::TargetMySqlConfig) -> Result<SchemaInventory, String>,
) -> Result<Option<SchemaInventory>, String> {
    if config.authorized_old_run_spec_sha256.is_none() {
        return Ok(None);
    }
    read_target(&config.target).map(Some)
}

pub(crate) fn run_optional_sync_run_spec_migration(
    config: &SyncConfig,
    current: &SyncRunIdentity,
    source: &SchemaInventory,
    target: Option<&SchemaInventory>,
    executor: &mut impl SyncRunSpecMigrationExecutor,
) -> Result<Option<SyncRunSpecMigrationOutcome>, String> {
    let Some(authorized_old_sha256) = config.authorized_old_run_spec_sha256.as_deref() else {
        return Ok(None);
    };
    let target = target.ok_or_else(|| {
        "authorized sync run-spec migration requires current target inventory".to_string()
    })?;
    let request = SyncRunSpecMigrationRequest {
        run_id: &current.run_id,
        authorized_old_sha256,
        current,
        source,
        target,
    };
    run_locked_sync_run_spec_migration(executor, &request).map(Some)
}

pub(crate) fn continue_after_sync_run_spec_migration<T>(
    migration: Result<Option<SyncRunSpecMigrationOutcome>, String>,
    emit_audit: impl FnOnce(&SyncRunSpecMigrationOutcome),
    action: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    let migration = migration?;
    if let Some(outcome) = &migration {
        emit_audit(outcome);
    }
    action()
}

struct SyncRunSpecMigrationAuditFields<'a> {
    status: &'static str,
    authorized_old_sha256: &'a str,
    old_sha256: &'a str,
    new_sha256: &'a str,
    locked_row_count: usize,
    affected_row_count: u64,
    changed_tables: &'a [super::config::AdditiveRunSpecTableChange],
}

pub(crate) fn format_sync_run_spec_migration_audit(
    run_id: &str,
    outcome: &SyncRunSpecMigrationOutcome,
) -> String {
    let fields = sync_run_spec_migration_audit_fields(outcome);
    serde_json::json!({
        "event": "sync_run_spec_migration",
        "run_id": run_id,
        "status": fields.status,
        "authorized_old_sha256": fields.authorized_old_sha256,
        "old_sha256": fields.old_sha256,
        "new_sha256": fields.new_sha256,
        "locked_row_count": fields.locked_row_count,
        "affected_row_count": fields.affected_row_count,
        "delta": sync_run_spec_migration_audit_delta(fields.changed_tables),
    })
    .to_string()
}

fn sync_run_spec_migration_audit_fields(
    outcome: &SyncRunSpecMigrationOutcome,
) -> SyncRunSpecMigrationAuditFields<'_> {
    match outcome {
        SyncRunSpecMigrationOutcome::AlreadyCurrent {
            locked_row_count,
            affected_row_count,
            authorized_old_sha256,
            current_sha256,
        } => already_current_migration_audit_fields(
            *locked_row_count,
            *affected_row_count,
            authorized_old_sha256,
            current_sha256,
        ),
        SyncRunSpecMigrationOutcome::Migrated {
            locked_row_count,
            affected_row_count,
            authorized_old_sha256,
            old_sha256,
            new_sha256,
            changed_tables,
        } => migrated_run_spec_audit_fields(
            *locked_row_count,
            *affected_row_count,
            authorized_old_sha256,
            old_sha256,
            new_sha256,
            changed_tables,
        ),
    }
}

fn already_current_migration_audit_fields<'a>(
    locked_row_count: usize,
    affected_row_count: u64,
    authorized_old_sha256: &'a str,
    current_sha256: &'a str,
) -> SyncRunSpecMigrationAuditFields<'a> {
    SyncRunSpecMigrationAuditFields {
        status: "already_current",
        authorized_old_sha256,
        old_sha256: authorized_old_sha256,
        new_sha256: current_sha256,
        locked_row_count,
        affected_row_count,
        changed_tables: &[],
    }
}

fn migrated_run_spec_audit_fields<'a>(
    locked_row_count: usize,
    affected_row_count: u64,
    authorized_old_sha256: &'a str,
    old_sha256: &'a str,
    new_sha256: &'a str,
    changed_tables: &'a [super::config::AdditiveRunSpecTableChange],
) -> SyncRunSpecMigrationAuditFields<'a> {
    SyncRunSpecMigrationAuditFields {
        status: "migrated",
        authorized_old_sha256,
        old_sha256,
        new_sha256,
        locked_row_count,
        affected_row_count,
        changed_tables,
    }
}

fn sync_run_spec_migration_audit_delta(
    changed_tables: &[super::config::AdditiveRunSpecTableChange],
) -> Vec<serde_json::Value> {
    changed_tables
        .iter()
        .map(|change| {
            serde_json::json!({
                "table": change.table,
                "added_columns": change.added_columns,
            })
        })
        .collect()
}

fn emit_sync_run_spec_migration_audit(run_id: &str, outcome: &SyncRunSpecMigrationOutcome) {
    eprintln!("{}", format_sync_run_spec_migration_audit(run_id, outcome));
}

pub(crate) fn run_mysql_sync(config: SyncConfig) -> Result<Vec<SyncChunkProgress>, String> {
    validate_sync_config(&config)?;
    let evidence = read_sync_source_evidence(&config.source)?;
    run_mysql_sync_with_evidence(config, evidence)
}

pub(crate) fn run_mysql_sync_with_evidence(
    config: SyncConfig,
    evidence: SchemaSourceEvidence,
) -> Result<Vec<SyncChunkProgress>, String> {
    validate_sync_config(&config)?;
    let tables = sync_tables_from_source_inventory(&evidence.inventory, &config.tables)?;
    let identity = build_sync_run_identity(&config, tables.clone())?;
    let target_inventory =
        read_sync_run_spec_migration_target_inventory(&config, read_sync_target_inventory)?;
    let mut progress = MySqlSyncProgressStore::new(&config.target, config.progress_table.clone())?;
    let migration = run_optional_sync_run_spec_migration(
        &config,
        &identity,
        &evidence.inventory,
        target_inventory.as_ref(),
        &mut progress,
    );
    let mut executor = MySqlSyncRunExecutor;
    continue_after_sync_run_spec_migration(
        migration,
        |outcome| emit_sync_run_spec_migration_audit(&identity.run_id, outcome),
        || {
            run_sync_orchestration(
                &config,
                &identity,
                &evidence,
                tables,
                &mut executor,
                &mut progress,
            )
        },
    )
}

fn validate_selected_tables(
    inventory: &SchemaInventory,
    selected: &[String],
) -> Result<(), String> {
    if selected.is_empty() {
        return Err("unified sync requires at least one selected source table".to_string());
    }
    let mut selected_names = BTreeSet::new();
    for name in selected {
        if !selected_names.insert(name.as_str()) {
            return Err(format!("selected source table `{name}` is duplicated"));
        }
        if !inventory.tables.iter().any(|table| table.name == *name) {
            return Err(format!("selected source table `{name}` is missing"));
        }
    }
    require_selected_same_schema_parents(inventory, &selected_names)
}

fn require_selected_same_schema_parents(
    inventory: &SchemaInventory,
    selected: &BTreeSet<&str>,
) -> Result<(), String> {
    for foreign_key in &inventory.foreign_keys {
        let child_selected = selected.contains(foreign_key.table.as_str());
        let same_schema = foreign_key.referenced_schema == inventory.schema;
        let parent_selected = selected.contains(foreign_key.referenced_table.as_str());
        if child_selected && same_schema && !parent_selected {
            return Err(format!(
                "selected source table `{}` depends on unselected source table `{}`",
                foreign_key.table, foreign_key.referenced_table
            ));
        }
    }
    Ok(())
}

fn validate_orchestration_identity(
    config: &SyncConfig,
    identity: &SyncRunIdentity,
    tables: &[SyncTable],
) -> Result<(), String> {
    let expected = build_sync_run_identity(config, tables.to_vec())?;
    if expected == *identity {
        return Ok(());
    }
    Err("sync orchestration identity does not match configuration and source inventory".to_string())
}

fn run_durable_schema_stage<F>(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    tables: &[String],
    progress: &mut impl SyncRunProgressStore,
    action: F,
) -> Result<(), String>
where
    F: FnOnce() -> Result<(), String>,
{
    let existing = load_stage_progress(identity, stage, tables, progress)?;
    let incomplete = incomplete_stage_tables(tables, &existing);
    if incomplete.is_empty() {
        return Ok(());
    }

    save_stage_statuses(
        identity,
        stage,
        &incomplete,
        SyncProgressStatus::Running,
        progress,
    )?;
    persist_schema_stage_result(identity, stage, tables, &incomplete, action(), progress)
}

fn incomplete_stage_tables(tables: &[String], existing: &[Option<SyncProgressRow>]) -> Vec<String> {
    tables
        .iter()
        .zip(existing)
        .filter(|(_, row)| {
            !row.as_ref()
                .is_some_and(|row| row.status == SyncProgressStatus::Complete)
        })
        .map(|(table, _)| table.clone())
        .collect()
}

fn persist_schema_stage_result(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    tables: &[String],
    incomplete: &[String],
    result: Result<(), String>,
    progress: &mut impl SyncRunProgressStore,
) -> Result<(), String> {
    match result {
        Ok(()) => save_stage_statuses(
            identity,
            stage,
            tables,
            SyncProgressStatus::Complete,
            progress,
        ),
        Err(primary_error) => {
            persist_schema_stage_error(identity, stage, incomplete, primary_error, progress)
        }
    }
}

fn persist_schema_stage_error(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    tables: &[String],
    primary_error: String,
    progress: &mut impl SyncRunProgressStore,
) -> Result<(), String> {
    let save_errors = save_stage_errors(identity, stage, tables, &primary_error, progress);
    if save_errors.is_empty() {
        Err(primary_error)
    } else {
        Err(format!(
            "{primary_error}; additionally {}",
            save_errors.join("; additionally ")
        ))
    }
}

fn save_stage_statuses(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    tables: &[String],
    status: SyncProgressStatus,
    progress: &mut impl SyncRunProgressStore,
) -> Result<(), String> {
    for table in tables {
        save_stage_progress(identity, stage, table, status, None, progress)?;
    }
    Ok(())
}

fn load_stage_progress(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    tables: &[String],
    progress: &mut impl SyncRunProgressStore,
) -> Result<Vec<Option<SyncProgressRow>>, String> {
    tables
        .iter()
        .map(|table| {
            let row = progress
                .load_stage(&identity.run_id, stage, table)
                .map_err(|error| {
                    format!(
                        "load sync {} progress for table `{table}`: {error}",
                        stage.as_str()
                    )
                })?;
            if let Some(row) = &row {
                validate_stage_progress(identity, stage, table, row)?;
            }
            Ok(row)
        })
        .collect()
}

fn validate_stage_progress(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    table: &str,
    progress: &SyncProgressRow,
) -> Result<(), String> {
    if progress.run_id != identity.run_id {
        return Err(format!(
            "sync {} progress run id mismatch for table `{table}`: expected `{}`, found `{}`",
            stage.as_str(),
            identity.run_id,
            progress.run_id
        ));
    }
    if progress.stage != stage {
        return Err(format!(
            "sync {} progress stage mismatch for table `{table}`: found `{}`",
            stage.as_str(),
            progress.stage.as_str()
        ));
    }
    if progress.table_name != table {
        return Err(format!(
            "sync {} progress table mismatch: expected `{table}`, found `{}`",
            stage.as_str(),
            progress.table_name
        ));
    }
    if progress.run_spec_json != identity.run_spec_json {
        return Err(format!(
            "sync {} progress run specification mismatch for table `{table}`",
            stage.as_str()
        ));
    }
    Ok(())
}

fn save_stage_progress(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    table: &str,
    status: SyncProgressStatus,
    last_error: Option<&str>,
    progress: &mut impl SyncRunProgressStore,
) -> Result<(), String> {
    let row = stage_progress_row(identity, stage, table, status, last_error);
    progress.save_stage(&row).map_err(|error| {
        format!(
            "save sync {} progress for table `{table}`: {error}",
            stage.as_str()
        )
    })
}

fn save_stage_errors(
    identity: &SyncRunIdentity,
    stage: SyncStage,
    tables: &[String],
    primary_error: &str,
    progress: &mut impl SyncRunProgressStore,
) -> Vec<String> {
    tables
        .iter()
        .filter_map(|table| {
            let row = stage_progress_row(
                identity,
                stage,
                table,
                SyncProgressStatus::Error,
                Some(primary_error),
            );
            progress.save_stage(&row).err().map(|error| {
                format!(
                    "save sync {} error progress for table `{table}`: {error}",
                    stage.as_str()
                )
            })
        })
        .collect()
}

fn stage_progress_row(
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

struct MySqlSyncRunExecutor;

impl SyncRunExecutor for MySqlSyncRunExecutor {
    fn run_schema_stage(
        &mut self,
        config: &SyncConfig,
        evidence: &SchemaSourceEvidence,
        stage: SyncStage,
    ) -> Result<(), String> {
        let schema_stage = match stage {
            SyncStage::PrerequisiteSchema => SyncSchemaStageKind::Prerequisite,
            SyncStage::FinalConstraints => SyncSchemaStageKind::FinalConstraints,
            SyncStage::Rows => return Err("rows are not a schema stage".to_string()),
        };
        run_sync_schema_stage(evidence, &config.target, &config.tables, schema_stage).map(|_| ())
    }

    fn run_rows(
        &mut self,
        config: &SyncConfig,
        identity: &SyncRunIdentity,
        tables: Vec<SyncTable>,
    ) -> Result<Vec<SyncChunkProgress>, String> {
        run_mysql_sync_tables(config, identity, tables)
    }
}
