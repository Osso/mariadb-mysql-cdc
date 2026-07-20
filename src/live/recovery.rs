use std::fmt;

pub(crate) const SESSIONS_GUEST_CHILD_SCHEMA: &str = "globalcomix";
pub(crate) const SESSIONS_GUEST_CHILD_TABLE: &str = "sessions";
pub(crate) const SESSIONS_GUEST_CONSTRAINT: &str = "fk_sessions_guest";
pub(crate) const SESSIONS_GUEST_FK_ERROR_CODE: u16 = 1452;
pub(crate) const SESSIONS_GUEST_FK_SIGNATURE: &str = "`globalcomix`.`sessions`, CONSTRAINT `fk_sessions_guest` FOREIGN KEY (`guest_id`, `guest_hash`)";
pub(crate) const SESSIONS_GUEST_PARENT_REFERENCE: &str =
    "REFERENCES `guests` (`guest_id`, `guest_hash`)";
pub(crate) const SESSIONS_GUEST_PARENT_TABLE: &str = "guests";
pub(crate) const SESSIONS_GUEST_PARENT_PRIMARY_KEY: &str = "guest_id";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionsGuestRecovery {
    pub source_file: String,
    pub source_start_position: u64,
    pub source_end_position: u64,
    pub child_event_timestamp: u64,
    pub schema: String,
    pub table: String,
    pub constraint: String,
    pub session_id: String,
    pub guest_id: String,
    pub guest_hash: String,
}

#[derive(Debug)]
pub enum RecoveryAttemptError {
    ReconciliationFailed(String),
}

impl fmt::Display for RecoveryAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReconciliationFailed(message) => {
                write!(
                    formatter,
                    "sessions guest parent reconciliation failed: {message}"
                )
            }
        }
    }
}

impl From<crate::table_sync::TableSyncError> for RecoveryAttemptError {
    fn from(error: crate::table_sync::TableSyncError) -> Self {
        Self::ReconciliationFailed(error.to_string())
    }
}
