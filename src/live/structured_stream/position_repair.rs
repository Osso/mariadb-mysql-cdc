use super::zero_query_position::ZeroQueryPosition;
use super::*;
use crate::stream_checkpoint::MySqlStreamCheckpointStore;
use sha2::{Digest, Sha256};

pub(super) const APPROVAL_ENV: &str = "CDC_APPROVED_DDL_POSITION_REPAIR";

pub(super) fn reanchor_pending_position(
    config: &ApplyBinlogConfig,
    journal: &impl DdlReplayJournal,
    approval: &str,
) -> Result<(), ApplyBinlogError> {
    let options = replica_options_from_source(&config.source)?;
    let mut client = BinlogClient::new(options);
    let mut events = client
        .replicate()
        .map_err(|_| repair_rejected("could not open the source stream"))?;
    let corrected = read_adjacent_ddl(config, &mut events)?;
    verify_durable_checkpoint(config)?;
    record_corrected_position(config, journal, approval, &corrected)?;
    Err(ApplyBinlogError::Statement(format!(
        "automatic DDL position verified at {}:{}-{}; legacy row preserved, DDL and checkpoint remain blocked",
        corrected.binlog_file, corrected.event_start_position, corrected.event_end_position
    )))
}

fn verify_durable_checkpoint(config: &ApplyBinlogConfig) -> Result<(), ApplyBinlogError> {
    let store = MySqlStreamCheckpointStore::new(
        config.target.clone(),
        config.checkpoint_table.clone(),
        &config.source_identity,
    );
    let checkpoint = store
        .load_checkpoint()
        .map_err(|_| repair_rejected("could not re-read the durable checkpoint"))?;
    let matches = checkpoint.is_some_and(|checkpoint| {
        checkpoint.source_file == config.source.binlog_file
            && checkpoint.source_position == config.source.start_position
    });
    if !matches {
        return Err(repair_rejected(
            "durable checkpoint changed before position repair",
        ));
    }
    Ok(())
}

fn read_adjacent_ddl(
    config: &ApplyBinlogConfig,
    events: &mut impl Iterator<Item = Result<(EventHeader, BinlogEvent), MysqlCdcError>>,
) -> Result<DdlEvent, ApplyBinlogError> {
    let (gtid_header, gtid) = read_checkpoint_event(events, &config.source)?;
    let gtid_start = gtid_header
        .next_event_position
        .checked_sub(gtid_header.event_length)
        .map(u64::from);
    if !matches!(gtid, BinlogEvent::MariaDbGtidEvent(_))
        || gtid_start != Some(config.source.start_position)
    {
        return Err(repair_rejected(
            "checkpoint does not begin the expected MariaDB GTID",
        ));
    }
    let (header, event) = read_wire_event(events)?;
    let mut position = ZeroQueryPosition::default();
    position.observe(&gtid_header, &gtid);
    let end = position
        .observe(&header, &event)
        .ok_or_else(|| repair_rejected("next event has no proven zero-ended Query range"))?;
    let BinlogEvent::QueryEvent(query) = event else {
        return Err(repair_rejected("next event is not a Query"));
    };
    if config.source.database.as_deref() != Some(query.database_name.as_str())
        || !crate::statement::is_create_table_statement(&query.sql_statement)
    {
        return Err(repair_rejected(
            "next Query is not CREATE TABLE in the configured source schema",
        ));
    }
    Ok(ddl_event_with_end(
        &config.source_identity,
        &config.source.binlog_file,
        &header,
        &query,
        end,
    ))
}

fn read_checkpoint_event(
    events: &mut impl Iterator<Item = Result<(EventHeader, BinlogEvent), MysqlCdcError>>,
    source: &SourceBinlogConfig,
) -> Result<(EventHeader, BinlogEvent), ApplyBinlogError> {
    let (header, event) = read_wire_event(events)?;
    let BinlogEvent::RotateEvent(rotate) = &event else {
        return Err(repair_rejected("expected a synthetic startup rotation"));
    };
    if header.timestamp != 0 || rotate.binlog_filename != source.binlog_file {
        return Err(repair_rejected(
            "startup rotation does not identify the checkpoint file",
        ));
    }
    let (header, event) = read_wire_event(events)?;
    if !matches!(event, BinlogEvent::FormatDescriptionEvent(_)) || header.next_event_position != 0 {
        return Err(repair_rejected("expected a resent format description"));
    }
    read_wire_event(events)
}

fn read_wire_event(
    events: &mut impl Iterator<Item = Result<(EventHeader, BinlogEvent), MysqlCdcError>>,
) -> Result<(EventHeader, BinlogEvent), ApplyBinlogError> {
    events
        .next()
        .ok_or_else(|| repair_rejected("source stream ended before the expected DDL"))?
        .map_err(|_| repair_rejected("could not read a source event"))
}

fn record_corrected_position(
    config: &ApplyBinlogConfig,
    journal: &impl DdlReplayJournal,
    approval: &str,
    corrected: &DdlEvent,
) -> Result<(), ApplyBinlogError> {
    if position_approval(config.source.start_position, corrected) != approval {
        return Err(repair_rejected(
            "wire position or statement does not match the approval",
        ));
    }
    let legacy = DdlEvent {
        event_start_position: 0,
        event_end_position: 0,
        ..corrected.clone()
    };
    let recorded = journal
        .read_status(&legacy)
        .map_err(|_| repair_rejected("immutable journal identity did not match"))?;
    if recorded != Some(DdlReplayStatus::TranslationPending) {
        return Err(repair_rejected("legacy row is not translation pending"));
    }
    ensure_translation_pending(journal, corrected)
        .map_err(|_| repair_rejected("could not confirm the corrected pending record"))
}

fn position_approval(checkpoint: u64, event: &DdlEvent) -> String {
    let source = Sha256::digest(&event.source_identity);
    let statement = Sha256::digest(&event.raw_sql);
    format!(
        "{source:x}:{}:{checkpoint}:{}:{}:{statement:x}",
        event.binlog_file, event.event_start_position, event.event_end_position
    )
}

fn repair_rejected(reason: &str) -> ApplyBinlogError {
    ApplyBinlogError::Statement(format!("automatic DDL position repair rejected: {reason}"))
}
