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

pub(crate) const HOME_FEED_SLIDE_CHILD_SCHEMA: &str = "globalcomix";
pub(crate) const HOME_FEED_SLIDE_CHILD_TABLE: &str = "home_feed_card_slides";
pub(crate) const HOME_FEED_SLIDE_CONSTRAINT: &str = "fk_hfcs_card";
pub(crate) const HOME_FEED_SLIDE_FK_ERROR_CODE: u16 = 1452;
pub(crate) const HOME_FEED_SLIDE_FK_SIGNATURE: &str =
    "`globalcomix`.`home_feed_card_slides`, CONSTRAINT `fk_hfcs_card` FOREIGN KEY (`card_id`)";
pub(crate) const HOME_FEED_SLIDE_PARENT_REFERENCE: &str = "REFERENCES `home_feed_cards` (`id`)";
pub(crate) const HOME_FEED_CARD_PARENT_TABLE: &str = "home_feed_cards";
pub(crate) const HOME_FEED_CARD_PARENT_PRIMARY_KEY: &str = "id";

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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct HomeFeedCardRecovery {
    pub source_file: String,
    pub source_start_position: u64,
    pub source_end_position: u64,
    pub child_event_timestamp: u64,
    pub schema: String,
    pub table: String,
    pub constraint: String,
    pub slide_id: String,
    pub card_id: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExactParentRecovery {
    SessionsGuest(SessionsGuestRecovery),
    HomeFeedCard(HomeFeedCardRecovery),
}

impl ExactParentRecovery {
    pub(crate) fn source_file(&self) -> &str {
        match self {
            Self::SessionsGuest(request) => &request.source_file,
            Self::HomeFeedCard(request) => &request.source_file,
        }
    }

    pub(crate) fn source_start_position(&self) -> u64 {
        match self {
            Self::SessionsGuest(request) => request.source_start_position,
            Self::HomeFeedCard(request) => request.source_start_position,
        }
    }

    pub(crate) fn source_end_position(&self) -> u64 {
        match self {
            Self::SessionsGuest(request) => request.source_end_position,
            Self::HomeFeedCard(request) => request.source_end_position,
        }
    }

    pub(crate) fn set_source_end_position(&mut self, source_end_position: u64) {
        match self {
            Self::SessionsGuest(request) => request.source_end_position = source_end_position,
            Self::HomeFeedCard(request) => request.source_end_position = source_end_position,
        }
    }

    pub(crate) fn child_primary_key(&self) -> &str {
        match self {
            Self::SessionsGuest(request) => &request.session_id,
            Self::HomeFeedCard(request) => &request.slide_id,
        }
    }

    pub(crate) fn parent_identity(&self) -> String {
        match self {
            Self::SessionsGuest(request) => {
                format!(
                    "guest_id={} guest_hash={}",
                    request.guest_id, request.guest_hash
                )
            }
            Self::HomeFeedCard(request) => format!("card_id={}", request.card_id),
        }
    }

    pub(crate) fn recovery_kind(&self) -> &'static str {
        match self {
            Self::SessionsGuest(_) => "sessions_guest",
            Self::HomeFeedCard(_) => "home_feed_card",
        }
    }
}

#[derive(Debug)]
pub enum RecoveryAttemptError {
    ReconciliationFailed(String),
}

impl fmt::Display for RecoveryAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReconciliationFailed(message) => {
                write!(formatter, "exact parent reconciliation failed: {message}")
            }
        }
    }
}

impl From<crate::table_sync::TableSyncError> for RecoveryAttemptError {
    fn from(error: crate::table_sync::TableSyncError) -> Self {
        Self::ReconciliationFailed(error.to_string())
    }
}
