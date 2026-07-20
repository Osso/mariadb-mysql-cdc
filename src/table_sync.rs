mod model;
mod mysql;
pub(crate) mod progress;
mod range;
mod recent;
mod repair;
mod run;
mod target;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_progress;
#[cfg(test)]
mod tests_support;

pub use model::*;
pub use progress::{
    MySqlSyncProgressStore, MySqlSyncRunProgressStore, NoopSyncProgressStore, SyncProgressStatus,
    SyncProgressStore, SyncTableProgress,
};
pub use range::{sync_table_with_progress_range, sync_table_with_progress_range_phase};
pub use recent::sync_recent_updates;
pub use run::{run_sync_table, run_sync_table_phase, sync_table, sync_table_with_progress};
pub(crate) use target::MySqlSyncRepairTarget;
pub use target::SyncRepairTarget;

pub(crate) use model::{
    SyncRunScope, SyncRunSpec, last_primary_key, snapshot_table, sync_chunk_request,
    sync_chunk_request_with_updated_since, sync_insert_mode, target_connection_config,
    validate_sync_range, validate_sync_table, validate_sync_table_config,
};
#[cfg(test)]
pub(crate) use mysql::build_sync_select_sql;
pub(crate) use progress::{SyncRunCandidate, select_compatible_failed_run};
#[cfg(test)]
pub(crate) use range::build_run_spec_json;
pub(crate) use range::{
    RangeSyncRequest, complete_sync_progress, finish_sync_run, persist_sync_run_error,
    release_on_load_error, sync_table_with_progress_range_phase_with_run_spec,
    validate_resumable_progress,
};
#[cfg(test)]
pub(crate) use recent::{RecentUpdateSyncContext, sync_recent_updates_with_progress};
pub(crate) use repair::{
    apply_recent_update_chunk, count_extra_target_rows, ensure_delete_allowed, repair_chunk,
};
#[cfg(test)]
pub(crate) use run::{build_sync_run_scope, retry_sync_table_operation};
pub(crate) use run::{
    find_compatible_failed_run, run_sync_table_phase_with_run_spec, should_record_sync_run_error,
};
