use crate::database_row::DatabaseRow;
use crate::sync::{
    SyncChunkProgress, SyncPrimaryKeyOrdering, SyncProgressStatus, SyncStage, SyncTable,
    SyncUniqueIndex, SyncUniqueIndexColumn, SyncUniqueOwnerAction, SyncUniqueOwnerConflict,
    build_strict_delete_batches, build_strict_update_batches, build_sync_insert_failure,
    decode_sync_rows, format_unique_owner_reconciliation_event,
    resolve_sync_unique_index, retry_sync_connection_construction, strict_delete_batch_capacity,
    strict_insert_batch_capacity, strict_update_batch_capacity, sync_chunk_progress_from_row,
    sync_progress_row_from_chunk, validate_sync_target_lock_identity,
};
use mysql::MySqlError;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::time::Duration;

#[test]
fn sync_connection_construction_retries_connectivity_errors_with_backoff_and_jitter() {
    let attempts = Cell::new(0);
    let mut delays = Vec::new();

    let result = retry_sync_connection_construction(
        || {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            if attempt < 3 {
                Err(mysql::Error::IoError(io::Error::from(
                    io::ErrorKind::WouldBlock,
                )))
            } else {
                Ok("connected")
            }
        },
        |delay| delays.push(delay),
        |base_delay| base_delay / 4,
    )
    .expect("transient connection succeeds");

    assert_eq!(result, "connected");
    assert_eq!(attempts.get(), 3);
    assert_eq!(
        delays,
        [Duration::from_millis(125), Duration::from_millis(250)]
    );
}

#[test]
fn sync_connection_construction_fails_fast_for_permanent_mysql_errors() {
    let attempts = Cell::new(0);
    let mut delays = Vec::new();

    let error = retry_sync_connection_construction(
        || {
            attempts.set(attempts.get() + 1);
            Err::<(), _>(mysql::Error::MySqlError(MySqlError {
                state: "28000".to_string(),
                message: "access denied".to_string(),
                code: 1045,
            }))
        },
        |delay| delays.push(delay),
        |_| Duration::from_millis(25),
    )
    .expect_err("permanent connection error");

    assert!(matches!(error, mysql::Error::MySqlError(_)));
    assert_eq!(attempts.get(), 1);
    assert!(delays.is_empty());
}

#[test]
fn sync_connection_construction_returns_last_error_after_bounded_attempts() {
    let attempts = Cell::new(0);
    let mut delays = Vec::new();

    let error = retry_sync_connection_construction(
        || {
            attempts.set(attempts.get() + 1);
            Err::<(), _>(mysql::Error::IoError(io::Error::from(
                io::ErrorKind::WouldBlock,
            )))
        },
        |delay| delays.push(delay),
        |_| Duration::ZERO,
    )
    .expect_err("exhausted connectivity retries");

    assert!(error.is_connectivity_error());
    assert_eq!(attempts.get(), 5);
    assert_eq!(
        delays,
        [
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(800),
        ]
    );
}

#[test]
fn sync_mysql_adapter_decodes_exact_selected_columns_and_rejects_invalid_rows() {
    let table = mutation_table();
    let decoded = decode_sync_rows(
        &table,
        vec![vec![
            Some("7".to_string()),
            Some("live".to_string()),
            None,
        ]],
    )
    .expect("valid sync row");

    assert_eq!(
        decoded,
        vec![DatabaseRow {
            primary_key: vec!["7".to_string()],
            values: BTreeMap::from([
                ("id".to_string(), Some("7".to_string())),
                ("status".to_string(), Some("live".to_string())),
                ("title".to_string(), None),
            ]),
        }]
    );

    let field_count_error = decode_sync_rows(
        &table,
        vec![vec![Some("7".to_string()), Some("live".to_string())]],
    )
    .expect_err("short sync row");
    assert_eq!(
        field_count_error,
        "sync row has 2 fields for 3 selected columns"
    );

    let null_primary_key_error = decode_sync_rows(
        &table,
        vec![vec![None, Some("live".to_string()), Some("Now".to_string())]],
    )
    .expect_err("NULL primary key");
    assert_eq!(
        null_primary_key_error,
        "primary-key column `id` was NULL"
    );

    let mut missing_primary_key_table = table;
    missing_primary_key_table.columns = strings(["status", "title"]);
    let missing_primary_key_error = decode_sync_rows(
        &missing_primary_key_table,
        vec![vec![Some("live".to_string()), Some("Now".to_string())]],
    )
    .expect_err("missing primary key");
    assert_eq!(
        missing_primary_key_error,
        "primary-key column `id` was not selected"
    );
}

#[test]
fn sync_mysql_adapter_batches_strict_mutations_within_placeholder_limits() {
    let table = mutation_table();
    let rows = (0..129)
        .map(|index| row(&index.to_string(), "live", "Now"))
        .collect::<Vec<_>>();
    let primary_keys = rows
        .iter()
        .map(|row| row.primary_key.clone())
        .collect::<Vec<_>>();

    assert_eq!(strict_insert_batch_capacity(&table), 128);
    assert_eq!(strict_update_batch_capacity(&table), 128);
    assert_eq!(strict_delete_batch_capacity(&table), 128);

    let updates = build_strict_update_batches(&table, &rows);
    let deletes = build_strict_delete_batches(&table, &primary_keys);

    assert_eq!(updates.len(), 2);
    assert_eq!(deletes.len(), 2);
    assert_eq!(updates[0].params.len(), 128 * 5);
    assert_eq!(deletes[0].params.len(), 128);

    let mutation_sql = updates
        .iter()
        .chain(&deletes)
        .map(|statement| statement.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n")
        .to_ascii_uppercase();
    for forbidden in ["INSERT IGNORE", "ON DUPLICATE KEY UPDATE", "REPLACE"] {
        assert!(
            !mutation_sql.contains(forbidden),
            "strict adapter mutation SQL contained `{forbidden}`:\n{mutation_sql}"
        );
    }

    assert!(build_strict_update_batches(&table, &[]).is_empty());
    assert!(build_strict_delete_batches(&table, &[]).is_empty());

    let wide_table = wide_mutation_table(1_000);
    assert_eq!(strict_insert_batch_capacity(&wide_table), 65);
    assert_eq!(strict_update_batch_capacity(&wide_table), 32);

    let wide_primary_key_table = wide_primary_key_table(1_000);
    assert_eq!(strict_delete_batch_capacity(&wide_primary_key_table), 65);
}

#[test]
fn sync_mysql_adapter_retains_unique_owner_failed_batch_and_remaining_insert_rows() {
    let rows = (0..260)
        .map(|index| row(&index.to_string(), "live", "Now"))
        .collect::<Vec<_>>();

    let failure = build_sync_insert_failure(
        &rows,
        128,
        128,
        Some(1062),
        "duplicate".to_string(),
    );

    assert_eq!(failure.failed_batch, rows[128..256]);
    assert_eq!(failure.remaining_rows, rows[256..]);
    assert_eq!(failure.retry_rows(), rows[128..]);
}

#[test]
fn sync_mysql_adapter_resolves_only_named_full_secondary_unique_owner_indexes() {
    let rows = vec![
        unique_index_column("PRIMARY", "id", 1, None),
        unique_index_column("uidx_token_page", "token", 1, None),
        unique_index_column("uidx_token_page", "page", 2, None),
    ];
    assert_eq!(
        resolve_sync_unique_index(
            "widgets",
            "Duplicate entry 'token-a-page-a' for key 'widgets.uidx_token_page'",
            rows.clone(),
        )
        .expect("qualified secondary unique index"),
        SyncUniqueIndex {
            name: "uidx_token_page".to_string(),
            columns: strings(["token", "page"]),
        }
    );
    assert!(
        resolve_sync_unique_index(
            "widgets",
            "Duplicate entry '10' for key 'PRIMARY'",
            rows.clone(),
        )
        .expect_err("PRIMARY must fail closed")
        .contains("PRIMARY")
    );

    let prefixed = vec![unique_index_column(
        "uidx_token_page",
        "token",
        1,
        Some(8),
    )];
    assert!(
        resolve_sync_unique_index(
            "widgets",
            "Duplicate entry 'token-a' for key 'uidx_token_page'",
            prefixed,
        )
        .expect_err("prefixed unique index must fail closed")
        .contains("prefixed")
    );

    let expression = vec![SyncUniqueIndexColumn {
        index: "uidx_expression".to_string(),
        column: None,
        sequence: 1,
        prefix_length: None,
    }];
    assert!(
        resolve_sync_unique_index(
            "widgets",
            "Duplicate entry 'x' for key 'uidx_expression'",
            expression,
        )
        .expect_err("expression unique index must fail closed")
        .contains("expression")
    );
}

#[test]
fn sync_mysql_adapter_formats_secret_free_reconciliation_event() {
    let conflict = SyncUniqueOwnerConflict {
        index: SyncUniqueIndex {
            name: "uidx_token_page".to_string(),
            columns: strings(["token", "page"]),
        },
        intended: unique_row("10", "token-a", "page-a", "intended-secret"),
        owner: unique_row("20", "token-a", "page-a", "owner-secret"),
    };
    let action = SyncUniqueOwnerAction::Update(unique_row(
        "20",
        "token-b",
        "page-b",
        "source-secret",
    ));

    let event = format_unique_owner_reconciliation_event("widgets", &conflict, &action);

    assert_eq!(
        event,
        r#"{"event":"sync_unique_owner_reconciliation","table":"widgets","index":"uidx_token_page","action":"update","intended_primary_key":["10"],"owner_primary_key":["20"]}"#
    );
    for secret in ["token-a", "page-a", "intended-secret", "owner-secret", "source-secret"] {
        assert!(!event.contains(secret), "event leaked `{secret}`: {event}");
    }
}

#[test]
fn sync_mysql_adapter_locks_only_its_bound_target_table() {
    assert_eq!(
        validate_sync_target_lock_identity(
            "target_database",
            "episodes",
            "target_database",
            "episodes",
        ),
        Ok(())
    );
    assert_eq!(
        validate_sync_target_lock_identity(
            "target_database",
            "episodes",
            "other_database",
            "episodes",
        ),
        Err(
            "sync target lock identity mismatch: expected `target_database`.`episodes`, found `other_database`.`episodes`"
                .to_string()
        )
    );
    assert_eq!(
        validate_sync_target_lock_identity(
            "target_database",
            "episodes",
            "target_database",
            "other_table",
        ),
        Err(
            "sync target lock identity mismatch: expected `target_database`.`episodes`, found `target_database`.`other_table`"
                .to_string()
        )
    );
}

#[test]
fn sync_mysql_adapter_maps_rows_progress_without_changing_identity() {
    let running = chunk_progress(false);
    let running_row = sync_progress_row_from_chunk(&running);

    assert_eq!(running_row.run_id, running.run_id);
    assert_eq!(running_row.stage, SyncStage::Rows);
    assert_eq!(running_row.table_name, running.table);
    assert_eq!(running_row.run_spec_json, running.run_spec_json);
    assert_eq!(running_row.last_primary_key, running.last_primary_key);
    assert_eq!(running_row.status, SyncProgressStatus::Running);
    assert_eq!(running_row.last_error, None);
    assert_eq!(
        sync_chunk_progress_from_row(running_row).expect("running rows progress"),
        running
    );

    let complete = chunk_progress(true);
    let complete_row = sync_progress_row_from_chunk(&complete);
    assert_eq!(complete_row.status, SyncProgressStatus::Complete);
    assert_eq!(
        sync_chunk_progress_from_row(complete_row).expect("complete rows progress"),
        complete
    );
}

#[test]
fn sync_mysql_adapter_rejects_non_row_and_error_progress() {
    let mut non_row = sync_progress_row_from_chunk(&chunk_progress(false));
    non_row.stage = SyncStage::PrerequisiteSchema;
    assert_eq!(
        sync_chunk_progress_from_row(non_row).expect_err("non-row progress"),
        "sync chunk progress requires `rows` stage, found `prerequisite_schema`"
    );

    let mut failed = sync_progress_row_from_chunk(&chunk_progress(false));
    failed.status = SyncProgressStatus::Error;
    failed.last_error = Some("write failed".to_string());
    assert_eq!(
        sync_chunk_progress_from_row(failed).expect_err("failed progress"),
        "sync progress for run `sync-run-42` table `episodes` is in error: write failed"
    );
}

fn mutation_table() -> SyncTable {
    SyncTable {
        name: "episodes".to_string(),
        primary_key: strings(["id"]),
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns: strings(["id", "status", "title"]),
    }
}

fn unique_index_column(
    index: &str,
    column: &str,
    sequence: u64,
    prefix_length: Option<u64>,
) -> SyncUniqueIndexColumn {
    SyncUniqueIndexColumn {
        index: index.to_string(),
        column: Some(column.to_string()),
        sequence,
        prefix_length,
    }
}

fn unique_row(id: &str, token: &str, page: &str, payload: &str) -> DatabaseRow {
    DatabaseRow {
        primary_key: strings([id]),
        values: BTreeMap::from([
            ("id".to_string(), Some(id.to_string())),
            ("token".to_string(), Some(token.to_string())),
            ("page".to_string(), Some(page.to_string())),
            ("payload".to_string(), Some(payload.to_string())),
        ]),
    }
}

fn wide_mutation_table(column_count: usize) -> SyncTable {
    let mut columns = vec!["id".to_string()];
    columns.extend((1..column_count).map(|index| format!("column_{index}")));
    SyncTable {
        name: "wide_rows".to_string(),
        primary_key: strings(["id"]),
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns,
    }
}

fn wide_primary_key_table(column_count: usize) -> SyncTable {
    let primary_key = (0..column_count)
        .map(|index| format!("key_{index}"))
        .collect::<Vec<_>>();
    SyncTable {
        name: "wide_keys".to_string(),
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native; column_count],
        columns: primary_key.clone(),
        primary_key,
    }
}

fn row(id: &str, status: &str, title: &str) -> DatabaseRow {
    DatabaseRow {
        primary_key: vec![id.to_string()],
        values: BTreeMap::from([
            ("id".to_string(), Some(id.to_string())),
            ("status".to_string(), Some(status.to_string())),
            ("title".to_string(), Some(title.to_string())),
        ]),
    }
}

fn chunk_progress(complete: bool) -> SyncChunkProgress {
    SyncChunkProgress {
        run_id: "sync-run-42".to_string(),
        table: "episodes".to_string(),
        run_spec_json: r#"{"chunk_size":250,"tables":["episodes"]}"#.to_string(),
        last_primary_key: Some(strings(["7"])),
        complete,
        chunks: 3,
        rows_scanned: 750,
        inserts: 4,
        updates: 5,
        deletes: 6,
    }
}

fn strings<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}
