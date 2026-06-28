use super::{ApplyBinlogConfig, ApplyBinlogError};
use crate::checkpoint::{Checkpoint, CheckpointError, FileCheckpointStore, LastEvent};
use crate::statement::StatementEvent;
use crate::stream_checkpoint::MySqlStreamCheckpointStore;
use std::time::Duration;

pub(super) trait StreamCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<Checkpoint>, ApplyBinlogError>;
    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), ApplyBinlogError>;
}

impl StreamCheckpointStore for FileCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<Checkpoint>, ApplyBinlogError> {
        self.load().map_err(checkpoint_error)
    }

    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), ApplyBinlogError> {
        self.save(checkpoint).map_err(checkpoint_error)
    }
}

impl StreamCheckpointStore for MySqlStreamCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<Checkpoint>, ApplyBinlogError> {
        self.load().map_err(ApplyBinlogError::Checkpoint)
    }

    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), ApplyBinlogError> {
        self.save(checkpoint).map_err(ApplyBinlogError::Checkpoint)
    }
}

pub(super) fn save_stream_checkpoint(
    checkpoint_store: Option<&impl StreamCheckpointStore>,
    event: &StatementEvent,
) -> Result<(), ApplyBinlogError> {
    let Some(store) = checkpoint_store else {
        return Ok(());
    };

    let checkpoint = statement_checkpoint(event);
    if should_skip_checkpoint(store, &checkpoint)? {
        println!("{}", format_checkpoint_skip(&checkpoint));
        return Ok(());
    }
    store.save_checkpoint(&checkpoint)?;
    println!("{}", format_checkpoint_write(&checkpoint));
    Ok(())
}

fn should_skip_checkpoint(
    store: &impl StreamCheckpointStore,
    checkpoint: &Checkpoint,
) -> Result<bool, ApplyBinlogError> {
    if checkpoint.source_position == 0 {
        return Ok(true);
    }
    let Some(current) = store.load_checkpoint()? else {
        return Ok(false);
    };
    Ok(current.source_file == checkpoint.source_file
        && checkpoint.source_position <= current.source_position)
}

pub(super) fn statement_checkpoint(event: &StatementEvent) -> Checkpoint {
    Checkpoint {
        source_file: event.coordinate.file.clone(),
        source_position: event.resume_position,
        gtid: None,
        event_timestamp: 0,
        last_event: LastEvent {
            event_type: "StatementEvent".to_string(),
            description: event.sql.chars().take(120).collect(),
        },
    }
}

pub(super) fn should_reconnect(
    error: &ApplyBinlogError,
    attempt: u32,
    max_reconnects: u32,
    reconnect_forever: bool,
) -> bool {
    (reconnect_forever || attempt < max_reconnects) && is_transient_source_error(error)
}

pub(super) fn format_reconnect_start(
    config: &ApplyBinlogConfig,
    attempt: u32,
    error: &ApplyBinlogError,
) -> String {
    format!(
        "cdc_stream_reconnect_start attempt={} delay_seconds={} resume_file={} resume_position={} error={}",
        attempt,
        reconnect_delay(attempt).as_secs(),
        config.source.binlog_file,
        config.source.start_position,
        shell_word(&error.to_string())
    )
}

pub(super) fn reconnect_delay(attempt: u32) -> Duration {
    let seconds = 2_u64.saturating_pow(attempt.saturating_sub(1)).min(30);
    Duration::from_secs(seconds)
}

fn format_checkpoint_write(checkpoint: &Checkpoint) -> String {
    format!(
        "cdc_stream_checkpoint file={} position={} event_type={}",
        checkpoint.source_file, checkpoint.source_position, checkpoint.last_event.event_type
    )
}

fn format_checkpoint_skip(checkpoint: &Checkpoint) -> String {
    format!(
        "cdc_stream_checkpoint_skip file={} position={} event_type={}",
        checkpoint.source_file, checkpoint.source_position, checkpoint.last_event.event_type
    )
}

fn is_transient_source_error(error: &ApplyBinlogError) -> bool {
    let ApplyBinlogError::SourceCommand(message) = error else {
        return false;
    };
    let lower = message.to_ascii_lowercase();
    lower.contains("connection reset")
        || lower.contains("tls/ssl")
        || lower.contains("reading packet")
        || lower.contains("eof")
        || lower.contains("broken pipe")
        || lower.contains("timed out")
}

fn checkpoint_error(error: CheckpointError) -> ApplyBinlogError {
    ApplyBinlogError::Checkpoint(error.to_string())
}

fn shell_word(value: &str) -> String {
    value.replace(char::is_whitespace, "_")
}
