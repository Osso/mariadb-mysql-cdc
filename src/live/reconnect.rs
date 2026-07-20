use super::{ApplyBinlogConfig, ApplyBinlogError, RecoveryAttemptError};
use crate::checkpoint::{Checkpoint, CheckpointError, FileCheckpointStore, LastEvent};
use crate::probe::BinlogCoordinate;
#[cfg(test)]
use crate::statement::StatementEvent;
use crate::stream_checkpoint::MySqlStreamCheckpointStore;
use std::collections::BTreeSet;
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

#[cfg(test)]
pub(super) fn run_stream_reconnect_loop<C, F, S>(
    config: &ApplyBinlogConfig,
    checkpoint_store: Option<&C>,
    run_attempt: F,
    sleep: S,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
    F: FnMut(&ApplyBinlogConfig) -> Result<(), ApplyBinlogError>,
    S: Fn(std::time::Duration),
{
    run_stream_reconnect_loop_with_recovery(
        config,
        checkpoint_store,
        run_attempt,
        |_request| {
            Err(RecoveryAttemptError::ReconciliationFailed(
                "sessions guest recovery service unavailable".to_string(),
            ))
        },
        sleep,
    )
}

pub(super) fn run_stream_reconnect_loop_with_recovery<C, F, R, S>(
    config: &ApplyBinlogConfig,
    checkpoint_store: Option<&C>,
    mut run_attempt: F,
    mut recover: R,
    sleep: S,
) -> Result<(), ApplyBinlogError>
where
    C: StreamCheckpointStore,
    F: FnMut(&ApplyBinlogConfig) -> Result<(), ApplyBinlogError>,
    R: FnMut(&crate::live::SessionsGuestRecovery) -> Result<(), RecoveryAttemptError>,
    S: Fn(std::time::Duration),
{
    let mut attempt_config = config.clone();
    resume_from_checkpoint(&mut attempt_config, checkpoint_store)?;
    attempt_config.source.validate_start_coordinate()?;
    let mut attempt = 0;
    let mut attempted_recoveries = BTreeSet::new();

    loop {
        let error = match run_attempt(&attempt_config) {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        if is_stale_or_missing_binlog_error(&error) {
            return Err(ApplyBinlogError::SourceCommand(format!(
                "stale or purged source binlog requires operator repair; checkpoint was not changed; error={error}"
            )));
        }
        match failed_attempt_action(
            &error,
            checkpoint_store.is_some(),
            attempt,
            config.max_reconnects,
            config.reconnect_forever,
        ) {
            FailedAttemptAction::Return => {
                log_skipped_recovery(&error);
                return Err(error);
            }
            FailedAttemptAction::Reconnect => {}
        }
        if let Err(source) =
            attempt_exact_parent_recovery(&error, &mut attempted_recoveries, &mut recover)
        {
            let conflict = error
                .sessions_guest_recovery()
                .expect("recovery error requires conflict identity")
                .clone();
            return Err(ApplyBinlogError::SessionsGuestRecoveryFailed {
                conflict: Box::new(conflict),
                source,
            });
        }
        prepare_next_attempt(&mut attempt_config, checkpoint_store, &mut attempt, &error)?;
        sleep(reconnect_delay(attempt));
    }
}

enum FailedAttemptAction {
    Return,
    Reconnect,
}

fn failed_attempt_action(
    error: &ApplyBinlogError,
    has_checkpoint_store: bool,
    attempt: u32,
    max_reconnects: u32,
    reconnect_forever: bool,
) -> FailedAttemptAction {
    if has_checkpoint_store && should_reconnect(error, attempt, max_reconnects, reconnect_forever) {
        FailedAttemptAction::Reconnect
    } else {
        FailedAttemptAction::Return
    }
}

fn log_skipped_recovery(error: &ApplyBinlogError) {
    if let Some(request) = error.sessions_guest_recovery() {
        println!(
            "{}",
            format_recovery_log("skipped", request, "retry_ineligible")
        );
    }
}

fn attempt_exact_parent_recovery<R>(
    error: &ApplyBinlogError,
    attempted_recoveries: &mut BTreeSet<crate::live::SessionsGuestRecovery>,
    recover: &mut R,
) -> Result<(), RecoveryAttemptError>
where
    R: FnMut(&crate::live::SessionsGuestRecovery) -> Result<(), RecoveryAttemptError>,
{
    let Some(request) = error.sessions_guest_recovery() else {
        return Ok(());
    };
    if !attempted_recoveries.insert(request.clone()) {
        println!(
            "{}",
            format_recovery_log("skipped", request, "already_attempted")
        );
        return Ok(());
    }
    println!("{}", format_recovery_log("attempted", request, "started"));
    if let Err(error) = recover(request) {
        eprintln!(
            "{} error={}",
            format_recovery_log("failed", request, "error"),
            shell_word(&error.to_string())
        );
        return Err(error);
    }
    println!(
        "{}",
        format_recovery_log("succeeded", request, "parent_reconciled")
    );
    Ok(())
}

fn prepare_next_attempt<C: StreamCheckpointStore>(
    attempt_config: &mut ApplyBinlogConfig,
    checkpoint_store: Option<&C>,
    attempt: &mut u32,
    error: &ApplyBinlogError,
) -> Result<(), ApplyBinlogError> {
    *attempt += 1;
    resume_from_checkpoint(attempt_config, checkpoint_store)?;
    attempt_config.source.validate_start_coordinate()?;
    println!(
        "{}",
        format_reconnect_start(attempt_config, *attempt, error)
    );
    Ok(())
}

fn format_recovery_log(
    action: &str,
    request: &crate::live::SessionsGuestRecovery,
    outcome: &str,
) -> String {
    format!(
        "cdc_sessions_guest_recovery action={} source_file={} source_start_position={} source_end_position={} child_pk={} guest_id={} guest_hash={} outcome={}",
        action,
        shell_word(&request.source_file),
        request.source_start_position,
        request.source_end_position,
        shell_word(&request.session_id),
        shell_word(&request.guest_id),
        shell_word(&request.guest_hash),
        outcome,
    )
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
    (reconnect_forever || attempt < max_reconnects) && is_retryable_stream_error(error)
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

fn is_retryable_stream_error(error: &ApplyBinlogError) -> bool {
    if matches!(error, ApplyBinlogError::RowConflictPersisted { .. }) {
        return true;
    }
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
