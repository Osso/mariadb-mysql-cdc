use super::ddl_event::DdlEvent;
use super::ddl_semantics::DdlSemanticEvidence;
use super::{TargetMySqlConfig, target_session_init_command};
use crate::mysql_support::target_mysql_opts;
use crate::target::SqlStatement;

#[cfg(test)]
mod grants;
mod schema;
mod store;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) use grants::validate_runtime_grants;
#[cfg(test)]
pub(super) use schema::validate_inventory_routine_definition;
#[cfg(test)]
pub(super) use schema::{
    JOURNAL_MONOTONIC_UPDATE_TRIGGER_BODY, JOURNAL_PENDING_INSERT_TRIGGER_BODY, JournalColumn,
    JournalConstraint, JournalKey, JournalRuntimeContract, JournalTriggerMetadata,
    expected_ddl_replay_journal_columns, expected_ddl_replay_journal_constraints,
    expected_ddl_replay_journal_keys, validate_ddl_replay_journal_columns,
    validate_ddl_replay_journal_constraints, validate_ddl_replay_journal_keys,
    validate_ddl_replay_journal_status_checks, validate_journal_runtime_contract,
    validate_journal_trigger_inventory,
};
pub use store::{DdlReplayJournal, MySqlDdlReplayJournal, replay_action};
#[cfg(test)]
pub(super) use store::{
    build_barrier_select_sql, build_prepare_sql, build_status_select_sql, build_transition_sql,
};

fn target_opts(target: &TargetMySqlConfig) -> Result<mysql::Opts, String> {
    target_mysql_opts(target)
}

fn mysql_error(error: mysql::Error) -> String {
    format!("automatic DDL journal mysql error: {error}")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlReplayStatus {
    TranslationPending,
    Prepared,
    Applied,
    Checkpointed,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlReplayAction {
    PrepareAndExecute,
    CheckpointOnly,
    AlreadyCheckpointed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlFamily {
    Table,
    Index,
    View,
    Procedure,
    Function,
    Event,
    Trigger,
    Rename,
    Truncate,
    Drop,
}

impl DdlFamily {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Index => "index",
            Self::View => "view",
            Self::Procedure => "procedure",
            Self::Function => "function",
            Self::Event => "event",
            Self::Trigger => "trigger",
            Self::Rename => "rename",
            Self::Truncate => "truncate",
            Self::Drop => "drop",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedReconciliation {
    ProvenApplied,
    Blocked,
}

pub fn reconcile_prepared(
    evidence: &DdlSemanticEvidence,
    observed_state: &str,
) -> PreparedReconciliation {
    let post_state_is_unique = evidence.pre_state != evidence.expected_post_state;
    if post_state_is_unique && observed_state == evidence.expected_post_state {
        PreparedReconciliation::ProvenApplied
    } else {
        PreparedReconciliation::Blocked
    }
}

pub fn prepared_reconciliation_block_reason(
    evidence: &DdlSemanticEvidence,
    observed_state: &str,
) -> &'static str {
    if evidence.pre_state == evidence.expected_post_state {
        "immutable pre-state and expected post-state are identical"
    } else if observed_state == evidence.pre_state {
        "observed target still matches immutable pre-state"
    } else if observed_state == evidence.expected_post_state {
        "observed target matches expected post-state"
    } else {
        "pre-state mismatch: observed target matches neither immutable pre-state nor expected post-state"
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalBarrier {
    pub binlog_file: String,
    pub event_start_position: u64,
    pub status: DdlReplayStatus,
}

pub fn enforce_no_overtake(
    unresolved: Option<&JournalBarrier>,
    next_file: &str,
    next_position: u64,
) -> Result<(), String> {
    let Some(unresolved) = unresolved else {
        return Ok(());
    };
    let unresolved_coordinate = (
        unresolved.binlog_file.as_str(),
        unresolved.event_start_position,
    );
    let next_coordinate = (next_file, next_position);
    if next_coordinate > unresolved_coordinate {
        return Err(format!(
            "automatic DDL barrier at {}:{} ({}) blocks later event {}:{}",
            unresolved.binlog_file,
            unresolved.event_start_position,
            unresolved.status.as_str(),
            next_file,
            next_position
        ));
    }
    Ok(())
}

impl DdlReplayStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TranslationPending => "translation_pending",
            Self::Prepared => "prepared",
            Self::Applied => "applied",
            Self::Checkpointed => "checkpointed",
            Self::Blocked => "blocked",
        }
    }
}
