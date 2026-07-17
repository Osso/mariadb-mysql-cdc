use super::tests_support::*;
use super::*;

#[test]
fn resumes_from_saved_table_progress_and_saves_each_chunk() {
    let source = FakeReader::new(vec![row("1", "old"), row("2", "bravo"), row("3", "coda")]);
    let target = FakeReader::new(vec![row("2", "bravo"), row("3", "coda")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::with_progress(SyncTableProgress {
        run_id: Some("ephemeral".to_string()),
        run_spec_json: Some(
            build_run_spec_json(
                "ephemeral",
                &account_table(),
                1,
                SyncMode::Apply,
                &None,
                &None,
                Some(0),
            )
            .expect("run spec"),
        ),
        table: "accounts".to_string(),
        last_primary_key: Some(vec!["1".to_string()]),
        chunks: 1,
        rows_scanned: 1,
        total_rows: None,
        inserts: 0,
        updates: 0,
        extra_target_rows: 0,
        mode: SyncMode::Apply,
        status: progress::SyncProgressStatus::Running,
        last_error: None,
    });

    let report = sync_table_with_progress(
        &account_table(),
        1,
        SyncMode::Apply,
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("sync report");

    assert_eq!(
        source.requests.borrow()[0].start_after,
        Some(vec!["1".to_string()])
    );
    assert_eq!(report.rows_scanned, 3);
    let saved = progress_store.saved.borrow();
    assert_eq!(
        saved.last().expect("saved progress").last_primary_key,
        Some(vec!["3".to_string()])
    );
    assert_eq!(
        saved.last().expect("saved progress").status,
        progress::SyncProgressStatus::Complete
    );
}

#[test]
fn run_scope_changes_with_endpoints_and_write_policy() {
    let mut first = SyncTableConfig {
        source: crate::mysql_snapshot::MySqlConnectionConfig {
            host: "source-a".to_string(),
            port: 3306,
            user: "reader".to_string(),
            password: "secret".to_string(),
            database: "app".to_string(),
        },
        target: crate::live::TargetMySqlConfig {
            host: "target-a".to_string(),
            port: 25060,
            user: "writer".to_string(),
            password: "secret".to_string(),
            database: "app".to_string(),
            tls_ca_file: "/tmp/custom-target-ca.pem".to_string(),
            insert_conflict_policy: crate::live::InsertConflictPolicy::Error,
        },
        table: account_table(),
        chunk_size: 10,
        mode: SyncMode::Apply,
        progress_table: "cdc.table_sync_runs".to_string(),
        run_id: "repair-01".to_string(),
        start_after: None,
        end_at: None,
        max_deletes: Some(0),
        updated_since: None,
        plan_hash: None,
    };
    let first_scope = build_sync_run_scope(&first).expect("first scope");
    first.target.host = "target-b".to_string();
    first.target.insert_conflict_policy = crate::live::InsertConflictPolicy::IgnoreDuplicate;

    let changed_scope = build_sync_run_scope(&first).expect("changed scope");

    assert_ne!(first_scope, changed_scope);
    assert!(first_scope.contains("source-a"));
    assert!(changed_scope.contains("target-b"));
    assert!(changed_scope.contains("ignore-duplicate"));
}

#[test]
fn progress_validation_errors_do_not_replace_saved_run_status() {
    assert!(!should_record_sync_run_error(&TableSyncError::Progress(
        "run id is terminal".to_string()
    )));
    assert!(!should_record_sync_run_error(
        &TableSyncError::InvalidTable("invalid bounds".to_string())
    ));
    assert!(should_record_sync_run_error(&TableSyncError::Repair(
        "target write failed".to_string()
    )));
}

#[test]
fn run_id_rejects_changed_immutable_specification() {
    let source = FakeReader::new(vec![]);
    let target = FakeReader::new(vec![]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::with_progress(SyncTableProgress {
        run_id: Some("repair-01".to_string()),
        run_spec_json: Some(
            build_run_spec_json(
                "test-scope",
                &account_table(),
                10,
                SyncMode::Apply,
                &Some(vec!["10".to_string()]),
                &Some(vec!["20".to_string()]),
                Some(1),
            )
            .expect("saved run spec"),
        ),
        table: "accounts".to_string(),
        last_primary_key: Some(vec!["15".to_string()]),
        chunks: 1,
        rows_scanned: 5,
        total_rows: None,
        inserts: 0,
        updates: 0,
        extra_target_rows: 0,
        mode: SyncMode::Apply,
        status: progress::SyncProgressStatus::Running,
        last_error: None,
    });

    let error = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "repair-01".to_string(),
            run_scope: "different-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: Some(vec!["100".to_string()]),
            end_at: Some(vec!["200".to_string()]),
            max_deletes: Some(1),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("changed run specification");

    assert_eq!(
        error.to_string(),
        "sync progress failed: run id `repair-01` already exists with a different immutable specification"
    );
    assert!(source.requests.borrow().is_empty());
}

#[test]
fn completed_run_id_is_terminal() {
    let source = FakeReader::new(vec![row("1", "alpha")]);
    let target = FakeReader::new(vec![]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::with_progress(SyncTableProgress {
        run_id: Some("ephemeral".to_string()),
        run_spec_json: Some(
            build_run_spec_json(
                "ephemeral",
                &account_table(),
                10,
                SyncMode::Apply,
                &None,
                &None,
                Some(0),
            )
            .expect("run spec"),
        ),
        table: "accounts".to_string(),
        last_primary_key: Some(vec!["99".to_string()]),
        chunks: 4,
        rows_scanned: 99,
        total_rows: Some(99),
        inserts: 10,
        updates: 20,
        extra_target_rows: 3,
        mode: SyncMode::Apply,
        status: progress::SyncProgressStatus::Complete,
        last_error: None,
    });

    let error = sync_table_with_progress(
        &account_table(),
        10,
        SyncMode::Apply,
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("completed run id must be terminal");

    assert_eq!(
        error.to_string(),
        "sync progress failed: run id `ephemeral` is already complete; use a new run id"
    );
    assert!(source.requests.borrow().is_empty());
    assert_eq!(
        progress_store.acquired_run_ids.borrow().as_slice(),
        &["ephemeral".to_string()]
    );
    assert_eq!(
        progress_store.released_run_ids.borrow().as_slice(),
        &["ephemeral".to_string()]
    );
}

#[test]
fn builds_sync_select_with_start_and_end_bounds() {
    let sql = build_sync_select_sql(&SyncChunkRequest {
        table: "accounts".to_string(),
        primary_key: vec!["id".to_string()],
        columns: vec!["id".to_string(), "name".to_string()],
        start_after: Some(vec!["10".to_string()]),
        end_at: Some(vec!["20".to_string()]),
        updated_since: None,
        limit: 100,
    });

    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `accounts` WHERE (`id` > '10') AND NOT ((`id` > '20')) ORDER BY `id` LIMIT 100"
    );
}

#[test]
fn builds_sync_select_with_updated_since_filter() {
    let sql = build_sync_select_sql(&SyncChunkRequest {
        table: "accounts".to_string(),
        primary_key: vec!["id".to_string()],
        columns: vec!["id".to_string(), "updated_at".to_string()],
        start_after: Some(vec!["10".to_string()]),
        end_at: None,
        updated_since: Some(UpdatedSince {
            column: "updated_at".to_string(),
            value: "2026-06-01 00:00:00".to_string(),
        }),
        limit: 100,
    });

    assert_eq!(
        sql,
        "SELECT `id`, `updated_at` FROM `accounts` WHERE (`id` > '10') AND `updated_at` >= '2026-06-01 00:00:00' ORDER BY `id` LIMIT 100"
    );
}
