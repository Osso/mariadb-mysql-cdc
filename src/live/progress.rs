#[cfg(test)]
use crate::live::ApplyBinlogConfig;
use crate::probe::BinlogCoordinate;
#[cfg(test)]
use crate::statement::QuarantineReason;
use std::time::{Duration, Instant};

const PROGRESS_STATEMENT_INTERVAL: usize = 10_000;
const PROGRESS_TIME_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub(super) struct StreamProgress {
    pub(super) applied_statements: usize,
    pub(super) quarantined_statements: usize,
    pub(super) last_coordinate: BinlogCoordinate,
    last_progress_log_at: Option<Instant>,
    last_progress_log_count: usize,
}

impl StreamProgress {
    pub(super) fn new(start: BinlogCoordinate) -> Self {
        Self {
            applied_statements: 0,
            quarantined_statements: 0,
            last_coordinate: start,
            last_progress_log_at: None,
            last_progress_log_count: 0,
        }
    }

    pub(super) fn record_applied(&mut self, coordinate: &BinlogCoordinate) -> bool {
        self.record_applied_at(coordinate, Instant::now())
    }

    fn record_applied_at(&mut self, coordinate: &BinlogCoordinate, now: Instant) -> bool {
        self.applied_statements += 1;
        self.last_coordinate = coordinate.clone();
        self.should_log_progress(now)
    }

    #[cfg(test)]
    pub(super) fn record_quarantined(&mut self, coordinate: &BinlogCoordinate) {
        self.quarantined_statements += 1;
        self.last_coordinate = coordinate.clone();
    }

    fn should_log_progress(&mut self, now: Instant) -> bool {
        if self.is_first_applied_statement()
            || self.reached_statement_interval()
            || self.reached_time_interval(now)
        {
            self.last_progress_log_at = Some(now);
            self.last_progress_log_count = self.applied_statements;
            return true;
        }

        false
    }

    fn is_first_applied_statement(&self) -> bool {
        self.applied_statements == 1
    }

    fn reached_statement_interval(&self) -> bool {
        self.applied_statements - self.last_progress_log_count >= PROGRESS_STATEMENT_INTERVAL
    }

    fn reached_time_interval(&self, now: Instant) -> bool {
        self.last_progress_log_at
            .and_then(|last_log_at| now.checked_duration_since(last_log_at))
            .is_some_and(|elapsed| elapsed >= PROGRESS_TIME_INTERVAL)
    }
}

#[cfg(test)]
pub(super) fn format_stream_start(config: &ApplyBinlogConfig) -> String {
    format!(
        "cdc_stream_start source_host={} source_database={} start_file={} start_position={} target_host={} target_database={}",
        config.source.host,
        optional_database_name(&config.source.database),
        config.source.binlog_file,
        config.source.start_position,
        config.target.host,
        config.target.database,
    )
}

pub(super) fn format_stream_progress(progress: &StreamProgress) -> String {
    format_stream_totals("cdc_stream_progress", progress)
}

#[cfg(test)]
pub(super) fn format_stream_quarantine(
    progress: &StreamProgress,
    reason: &QuarantineReason,
) -> String {
    format!(
        "{} reason={:?}",
        format_stream_totals("cdc_stream_quarantine", progress),
        reason,
    )
}

fn format_stream_totals(event_name: &str, progress: &StreamProgress) -> String {
    format!(
        "{} applied_statements={} quarantined_statements={} last_file={} last_position={}",
        event_name,
        progress.applied_statements,
        progress.quarantined_statements,
        progress.last_coordinate.file,
        progress.last_coordinate.position,
    )
}

#[cfg(test)]
fn optional_database_name(database: &Option<String>) -> &str {
    database.as_deref().unwrap_or("*")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{SourceBinlogConfig, TargetMySqlConfig};

    #[test]
    fn formats_stream_progress_as_parseable_key_values() {
        let progress = StreamProgress {
            applied_statements: 7,
            quarantined_statements: 1,
            last_coordinate: BinlogCoordinate {
                file: "mysqld-bin.000123".to_string(),
                position: 456,
            },
            last_progress_log_at: None,
            last_progress_log_count: 0,
        };

        assert_eq!(
            format_stream_progress(&progress),
            "cdc_stream_progress applied_statements=7 quarantined_statements=1 last_file=mysqld-bin.000123 last_position=456"
        );
    }

    #[test]
    fn throttles_stream_progress_by_first_event_count_and_time() {
        let start = BinlogCoordinate {
            file: "mysqld-bin.000123".to_string(),
            position: 4,
        };
        let event_coordinate = BinlogCoordinate {
            file: "mysqld-bin.000123".to_string(),
            position: 456,
        };
        let mut progress = StreamProgress::new(start);
        let first_log_at = Instant::now();

        assert!(progress.record_applied_at(&event_coordinate, first_log_at));
        assert!(
            !progress.record_applied_at(&event_coordinate, first_log_at + Duration::from_secs(29))
        );

        progress.applied_statements = 10_000;
        assert!(
            progress.record_applied_at(&event_coordinate, first_log_at + Duration::from_secs(29))
        );
        assert!(
            !progress.record_applied_at(&event_coordinate, first_log_at + Duration::from_secs(29))
        );
        assert!(
            progress.record_applied_at(&event_coordinate, first_log_at + Duration::from_secs(60))
        );
    }

    #[test]
    fn formats_stream_start_without_credentials() {
        let config = ApplyBinlogConfig {
            source: SourceBinlogConfig {
                host: "10.0.0.2".to_string(),
                user: "cdc".to_string(),
                password: "source-secret".to_string(),
                database: Some("globalcomix".to_string()),
                binlog_file: "mysqld-bin.000123".to_string(),
                start_position: 456,
                ..SourceBinlogConfig::default()
            },
            target: TargetMySqlConfig {
                host: "target.db".to_string(),
                user: "target_user".to_string(),
                password: "target-secret".to_string(),
                database: "globalcomix".to_string(),
                ..TargetMySqlConfig::default()
            },
            ..ApplyBinlogConfig::default()
        };
        let line = format_stream_start(&config);

        assert!(line.contains("cdc_stream_start"));
        assert!(line.contains("source_database=globalcomix"));
        assert!(line.contains("target_database=globalcomix"));
        assert!(line.contains("start_file=mysqld-bin.000123"));
        assert!(!line.contains("source-secret"));
        assert!(!line.contains("target-secret"));
    }
}
