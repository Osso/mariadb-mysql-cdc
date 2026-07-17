use super::{ApplyBinlogConfig, ApplyBinlogError};
use crate::checkpoint::{Checkpoint, CheckpointError, FileCheckpointStore, LastEvent};
use crate::probe::BinlogCoordinate;
#[cfg(test)]
use crate::statement::StatementEvent;
use crate::stream_checkpoint::MySqlStreamCheckpointStore;
use std::time::Duration;

const MAX_RECONNECT_DELAY_SECONDS: u64 = 5;

pub(super) trait StreamCheckpointStore {
    fn load_checkpoint(&self) -> Result<Option<Checkpoint>, ApplyBinlogError>;
    fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), ApplyBinlogError>;

    fn checkpoint_for_skip(&self) -> Result<Option<Checkpoint>, ApplyBinlogError> {
        self.load_checkpoint()
    }
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

    fn checkpoint_for_skip(&self) -> Result<Option<Checkpoint>, ApplyBinlogError> {
        self.checkpoint_for_skip()
            .map_err(ApplyBinlogError::Checkpoint)
    }
}

#[cfg(test)]
pub(super) fn save_stream_checkpoint(
    checkpoint_store: Option<&impl StreamCheckpointStore>,
    event: &StatementEvent,
) -> Result<(), ApplyBinlogError> {
    let checkpoint = statement_checkpoint(event);
    save_checkpoint_if_advanced(checkpoint_store, &checkpoint)
}

pub(super) fn save_coordinate_checkpoint(
    checkpoint_store: Option<&impl StreamCheckpointStore>,
    coordinate: &BinlogCoordinate,
    event_type: &str,
) -> Result<(), ApplyBinlogError> {
    let checkpoint = coordinate_checkpoint(coordinate, event_type);
    save_checkpoint_if_advanced(checkpoint_store, &checkpoint)
}

fn save_checkpoint_if_advanced(
    checkpoint_store: Option<&impl StreamCheckpointStore>,
    checkpoint: &Checkpoint,
) -> Result<(), ApplyBinlogError> {
    let Some(store) = checkpoint_store else {
        return Ok(());
    };

    if should_skip_checkpoint(store, checkpoint)? {
        return Ok(());
    }
    store.save_checkpoint(checkpoint)?;
    Ok(())
}

fn should_skip_checkpoint(
    store: &impl StreamCheckpointStore,
    checkpoint: &Checkpoint,
) -> Result<bool, ApplyBinlogError> {
    if checkpoint.source_position == 0 {
        return Ok(true);
    }
    let Some(current) = store.checkpoint_for_skip()? else {
        return Ok(false);
    };
    Ok(current.source_file == checkpoint.source_file
        && checkpoint.source_position <= current.source_position)
}

#[cfg(test)]
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

pub(super) fn coordinate_checkpoint(coordinate: &BinlogCoordinate, event_type: &str) -> Checkpoint {
    Checkpoint {
        source_file: coordinate.file.clone(),
        source_position: coordinate.position,
        gtid: None,
        event_timestamp: 0,
        last_event: LastEvent {
            event_type: event_type.to_string(),
            description: format!(
                "structured binlog event at {}:{}",
                coordinate.file, coordinate.position
            ),
        },
    }
}

pub(super) fn run_stream_reconnect_loop<C, F, S>(
    config: &ApplyBinlogConfig,
    checkpoint_store: Option<&C>,
    mut run_attempt: F,
    sleep: S,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
    F: FnMut(&ApplyBinlogConfig) -> Result<(), ApplyBinlogError>,
    S: Fn(std::time::Duration),
{
    let mut attempt_config = config.clone();
    resume_from_checkpoint(&mut attempt_config, checkpoint_store)?;
    attempt_config.source.validate_start_coordinate()?;
    let mut attempt = 0;

    loop {
        match run_attempt(&attempt_config) {
            Ok(()) => return Ok(()),
            Err(error) if is_stale_or_missing_binlog_error(&error) => {
                return Err(ApplyBinlogError::SourceCommand(format!(
                    "stale or purged source binlog requires operator repair; checkpoint was not changed; error={error}"
                )));
            }
            Err(error)
                if checkpoint_store.is_some()
                    && should_reconnect(
                        &error,
                        attempt,
                        config.max_reconnects,
                        config.reconnect_forever,
                    ) =>
            {
                attempt += 1;
                resume_from_checkpoint(&mut attempt_config, checkpoint_store)?;
                attempt_config.source.validate_start_coordinate()?;
                println!(
                    "{}",
                    format_reconnect_start(&attempt_config, attempt, &error)
                );
                sleep(reconnect_delay(attempt));
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn resume_from_checkpoint(
    config: &mut ApplyBinlogConfig,
    checkpoint_store: Option<&impl StreamCheckpointStore>,
) -> Result<(), ApplyBinlogError> {
    let Some(store) = checkpoint_store else {
        return Ok(());
    };
    let Some(checkpoint) = store.load_checkpoint()? else {
        return Err(ApplyBinlogError::Checkpoint(
            "required source-scoped stream checkpoint is missing".to_string(),
        ));
    };

    config.source.binlog_file = checkpoint.source_file;
    config.source.start_position = checkpoint.source_position;
    Ok(())
}

pub(super) fn should_reconnect(
    error: &ApplyBinlogError,
    attempt: u32,
    max_reconnects: u32,
    reconnect_forever: bool,
) -> bool {
    (reconnect_forever || attempt < max_reconnects) && is_transient_source_error(error)
}

pub(super) fn is_stale_or_missing_binlog_error(error: &ApplyBinlogError) -> bool {
    let ApplyBinlogError::SourceCommand(message) = error else {
        return false;
    };
    let lower = message.to_ascii_lowercase();
    lower.contains("could not find first log file name in binary log index file")
        || lower.contains("could not find log file") && lower.contains("in binary log index file")
        || lower.contains("not found in binlog index")
        || lower.contains("not found in binary log index")
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
    let seconds = 2_u64
        .saturating_pow(attempt.saturating_sub(1))
        .min(MAX_RECONNECT_DELAY_SECONDS);
    Duration::from_secs(seconds)
}

fn is_transient_source_error(error: &ApplyBinlogError) -> bool {
    let ApplyBinlogError::SourceCommand(message) = error else {
        return false;
    };
    let lower = message.to_ascii_lowercase();
    lower.contains("connection reset")
        || lower.contains("connection refused")
        || lower.contains("tls/ssl")
        || lower.contains("reading packet")
        || lower.contains("eof")
        || lower.contains("broken pipe")
        || lower.contains("timed out")
        // Rollout race: the replaced pod still holds the dump connection when
        // the new pod registers with the same server_id (error 4052).
        || lower.contains("same server_id is already connected")
}

fn checkpoint_error(error: CheckpointError) -> ApplyBinlogError {
    ApplyBinlogError::Checkpoint(error.to_string())
}

fn shell_word(value: &str) -> String {
    value.replace(char::is_whitespace, "_")
}
