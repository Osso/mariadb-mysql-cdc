use super::approved_group_wire::{ApprovedWireBounds, validate_source_only_group};
use super::position_repair::{position_approval, read_adjacent_ddl};
use super::*;
use crate::mysql_client::PersistentTargetExecutor;
use crate::mysql_support::quote_sql_literal;
use crate::stream_checkpoint::stream_checkpoint_name;
use serde::Deserialize;

pub(super) const SOURCE_ONLY_CREATE_APPROVAL_ENV: &str = "CDC_APPROVED_SOURCE_ONLY_CREATE_GROUP";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Approval {
    position: String,
    table: String,
    write_events: u16,
    annotation_bytes: u32,
    xid_end: u32,
}

pub(super) fn finish_approved_source_only_group(
    executor: &PersistentTargetExecutor,
    journal: &MySqlDdlReplayJournal,
    config: &ApplyBinlogConfig,
    approval: &str,
) -> Result<u64, ApplyBinlogError> {
    let approval: Approval =
        serde_json::from_str(approval).map_err(|_| rejected("invalid approval shape"))?;
    if approval.table.is_empty() || config.source.stop_position.is_some() {
        return Err(rejected(
            "source-only repair requires an exact table and unbounded stream mode",
        ));
    }
    let event = read_approved_group(config, &approval)?;
    let legacy = DdlEvent {
        event_start_position: 0,
        event_end_position: 0,
        ..event.clone()
    };
    verify_pending_rows(journal, [&legacy, &event])?;
    commit_source_only_group(executor, journal, config, &approval, [&legacy, &event])?;
    println!(
        "cdc_source_only_create_group_checkpointed file={} ddl_start={} xid_end={}",
        event.binlog_file, event.event_start_position, approval.xid_end
    );
    Ok(u64::from(approval.xid_end))
}

fn read_approved_group(
    config: &ApplyBinlogConfig,
    approval: &Approval,
) -> Result<DdlEvent, ApplyBinlogError> {
    let mut client = BinlogClient::new(replica_options_from_source(&config.source)?);
    let mut events = client
        .replicate()
        .map_err(|_| rejected("could not open the source stream"))?;
    let ddl = read_adjacent_ddl(config, &mut events)?;
    if approval.position != position_approval(config.source.start_position, &ddl) {
        return Err(rejected("source DDL does not match the exact approval"));
    }
    let bounds = ApprovedWireBounds {
        table: &approval.table,
        write_events: approval.write_events,
        annotation_bytes: approval.annotation_bytes,
        xid_end: approval.xid_end,
    };
    validate_source_only_group(&mut events, &ddl, bounds)?;
    Ok(ddl)
}

fn verify_pending_rows(
    journal: &MySqlDdlReplayJournal,
    events: [&DdlEvent; 2],
) -> Result<(), ApplyBinlogError> {
    for event in events {
        let status = journal
            .read_status(event)
            .map_err(|_| rejected("pending journal identity mismatch"))?;
        if status != Some(DdlReplayStatus::TranslationPending) {
            return Err(rejected(
                "both journal identities must still be translation pending",
            ));
        }
    }
    Ok(())
}

fn commit_source_only_group(
    executor: &PersistentTargetExecutor,
    journal: &MySqlDdlReplayJournal,
    config: &ApplyBinlogConfig,
    approval: &Approval,
    events: [&DdlEvent; 2],
) -> Result<(), ApplyBinlogError> {
    executor
        .begin_transaction()
        .map_err(|_| rejected("could not begin target transaction"))?;
    if let Err(error) = write_source_only_resolution(executor, journal, config, approval, events) {
        executor
            .rollback_transaction()
            .map_err(|_| rejected("target rollback not acknowledged"))?;
        return Err(error);
    }
    executor.commit_transaction().map_err(|_| {
        ApplyBinlogError::DdlBlocked(
            "source-only group commit not acknowledged; reloading durable state".to_string(),
        )
    })
}

fn write_source_only_resolution(
    executor: &PersistentTargetExecutor,
    journal: &MySqlDdlReplayJournal,
    config: &ApplyBinlogConfig,
    approval: &Approval,
    events: [&DdlEvent; 2],
) -> Result<(), ApplyBinlogError> {
    let checkpoint_name = stream_checkpoint_name(&config.source_identity);
    verify_target_preconditions(
        executor,
        journal,
        config,
        approval,
        &checkpoint_name,
        events,
    )?;
    let evidence = source_only_evidence(approval, events[1]);
    for event in events {
        for sql in journal.source_only_pending_transitions(event, &evidence) {
            executor
                .update_exactly_one_row(&sql)
                .map_err(|_| rejected("exact journal transition failed"))?;
        }
    }
    let coordinate = BinlogCoordinate {
        file: events[1].binlog_file.clone(),
        position: u64::from(approval.xid_end),
    };
    let checkpoint = super::super::reconnect::coordinate_checkpoint(&coordinate, "XidEvent");
    executor
        .save_transaction_checkpoint(&config.checkpoint_table, &checkpoint_name, &checkpoint)
        .map_err(|_| rejected("could not save the approved XID checkpoint"))?;
    Ok(())
}

fn verify_target_preconditions(
    executor: &PersistentTargetExecutor,
    journal: &MySqlDdlReplayJournal,
    config: &ApplyBinlogConfig,
    approval: &Approval,
    checkpoint_name: &str,
    events: [&DdlEvent; 2],
) -> Result<(), ApplyBinlogError> {
    let locked = executor
        .load_transaction_checkpoint_for_update(&config.checkpoint_table, checkpoint_name)
        .map_err(|_| rejected("could not lock the durable checkpoint"))?;
    if !locked.is_some_and(|saved| {
        saved.source_file == config.source.binlog_file
            && saved.source_position == config.source.start_position
    }) {
        return Err(rejected(
            "locked checkpoint does not match the approved predecessor",
        ));
    }
    let sql = journal.source_only_barriers_for_update(
        &config.source_identity,
        &config.source.binlog_file,
        u64::from(approval.xid_end),
    );
    let rows = executor
        .query_rows_as_strings(&sql)
        .map_err(|_| rejected("could not lock the journal barriers"))?;
    if rows != events.map(pending_barrier_fields) {
        return Err(rejected(
            "unexpected unresolved journal barrier before the approved XID",
        ));
    }
    ensure_target_backup_is_absent(executor, &config.target.database, &approval.table)
}

fn pending_barrier_fields(event: &DdlEvent) -> Vec<Option<String>> {
    vec![
        Some(event.source_identity.clone()),
        Some(event.binlog_file.clone()),
        Some(event.event_start_position.to_string()),
        Some(event.event_end_position.to_string()),
        Some("translation_pending".to_string()),
    ]
}

fn ensure_target_backup_is_absent(
    executor: &PersistentTargetExecutor,
    schema: &str,
    table: &str,
) -> Result<(), ApplyBinlogError> {
    let sql = format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA={} AND TABLE_NAME={} LIMIT 1",
        quote_sql_literal(schema),
        quote_sql_literal(table)
    );
    let tables = executor
        .query_rows_as_strings(&sql)
        .map_err(|_| rejected("could not verify the target backup is absent"))?;
    if !tables.is_empty() {
        return Err(rejected(
            "approved source-only backup already exists on target",
        ));
    }
    Ok(())
}

fn source_only_evidence(approval: &Approval, event: &DdlEvent) -> DdlSemanticEvidence {
    DdlSemanticEvidence {
        transformation_version: "approved-source-only-create-group-v1".to_string(),
        generated_sql: None,
        canonical_ast: serde_json::json!({
            "kind": "source_only_create_group", "schema": event.schema_name, "table": approval.table,
            "ddl_start": event.event_start_position, "ddl_end": event.event_end_position,
            "write_events": approval.write_events, "annotation_bytes": approval.annotation_bytes, "xid_end": approval.xid_end,
        }).to_string(),
        pre_state: "target backup absent".to_string(),
        expected_post_state: "target backup remains absent".to_string(),
    }
}

fn rejected(reason: &str) -> ApplyBinlogError {
    ApplyBinlogError::Statement(format!(
        "approved source-only CREATE group rejected: {reason}"
    ))
}
