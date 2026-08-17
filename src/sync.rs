mod chunk;
mod config;
mod model;
mod mysql;
mod orchestrate;
mod progress;
mod run;
mod sql;

pub(crate) use chunk::sync_next_chunk;
pub(crate) use config::{
    DEFAULT_SYNC_PROGRESS_TABLE, SyncConfig, SyncEndpointSpec, SyncRunIdentity, SyncRunSpec,
    build_sync_run_identity, sync_table_from_inventory, validate_sync_config,
};
pub(crate) use model::{
    SyncChunkConfig, SyncChunkProgress, SyncChunkProgressStore, SyncChunkReadRequest,
    SyncChunkSource, SyncChunkTargetSession, SyncPrimaryKeyOrdering, SyncProgressRow,
    SyncProgressStatus, SyncRunProgressStore, SyncStage, SyncTable,
};
pub(crate) use mysql::{
    MySqlSyncProgressStore, MySqlSyncSource, MySqlSyncTargetSession, build_strict_delete_batches,
    build_strict_insert_batches, build_strict_update_batches, decode_sync_rows,
    strict_delete_batch_capacity, strict_insert_batch_capacity, strict_update_batch_capacity,
    sync_chunk_progress_from_row, sync_progress_row_from_chunk, validate_sync_target_lock_identity,
};
pub(crate) use orchestrate::{
    SyncRunExecutor, run_mysql_sync, run_mysql_sync_with_evidence, run_sync_orchestration,
    sync_tables_from_source_inventory,
};
pub(crate) use progress::{
    build_create_sync_progress_schema_sql, build_create_sync_progress_table_sql,
    build_sync_progress_select_sql, build_sync_progress_upsert_sql, parse_sync_progress_row,
};
pub(crate) use run::{
    run_mysql_sync_table, run_mysql_sync_tables, run_sync_tables_bounded, sync_table_to_completion,
};
pub(crate) use sql::{
    build_lock_table_write_sql, build_strict_delete_rows_statement, build_strict_delete_statement,
    build_strict_insert_statement, build_strict_update_rows_statement,
    build_strict_update_statement, build_sync_select_sql,
};
