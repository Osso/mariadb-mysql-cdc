mod chunk;
mod completed_rows;
mod component_locks;
mod config;
mod dependency_order;
mod fk_orphan_repair;
mod fk_transition;
pub(crate) mod guest_range_repair;
mod model;
mod mysql;
mod orchestrate;
mod phase_progress;
mod phased_run;
mod progress;
mod run;
mod sql;

#[cfg(test)]
pub(crate) use chunk::sync_next_chunk;
pub(crate) use completed_rows::load_completed_recovery_rows;
pub(crate) use config::{DEFAULT_SYNC_PROGRESS_TABLE, SyncConfig, validate_sync_config};
#[cfg(test)]
pub(crate) use config::{SyncRunIdentity, build_sync_run_identity, sync_table_from_inventory};
pub(crate) use fk_orphan_repair::run_fk_orphan_repair_command;
pub(crate) use model::SyncChunkProgress;
#[cfg(test)]
pub(crate) use model::{
    SyncChunkConfig, SyncChunkPage, SyncChunkProgressStore, SyncChunkReadRequest, SyncChunkSource,
    SyncChunkTargetSession, SyncMutationFailure, SyncPrimaryKeyOrdering, SyncProgressRow,
    SyncProgressStatus, SyncRunProgressStore, SyncStage, SyncTable, SyncUniqueIndex,
    SyncUniqueOwnerAction, SyncUniqueOwnerConflict,
};
#[cfg(test)]
pub(crate) use mysql::{
    SyncUniqueIndexColumn, build_strict_delete_batches, build_strict_update_batches,
    build_sync_mutation_failure, decode_sync_rows, format_unique_owner_reconciliation_event,
    resolve_sync_unique_index, retry_sync_connection_construction, strict_delete_batch_capacity,
    strict_insert_batch_capacity, strict_update_batch_capacity, sync_chunk_progress_from_row,
    sync_progress_row_from_chunk, validate_sync_target_lock_identity,
};
#[cfg(test)]
pub(crate) use orchestrate::{
    SyncRunExecutor, run_sync_orchestration, sync_tables_from_source_inventory,
};
pub(crate) use orchestrate::{run_mysql_sync, run_mysql_sync_with_evidence};
#[cfg(test)]
pub(crate) use progress::{
    build_create_sync_progress_schema_sql, build_create_sync_progress_table_sql,
    build_sync_progress_select_sql, build_sync_progress_upsert_sql, parse_sync_progress_row,
};
#[cfg(test)]
pub(crate) use run::{run_sync_tables_bounded, sync_table_to_completion};
#[cfg(test)]
pub(crate) use sql::{
    build_exact_primary_key_select_statement, build_lock_table_write_sql,
    build_strict_delete_rows_statement, build_strict_insert_statement,
    build_strict_update_rows_statement, build_sync_select_sql,
    build_unique_index_columns_statement, build_unique_owner_select_statement,
};
