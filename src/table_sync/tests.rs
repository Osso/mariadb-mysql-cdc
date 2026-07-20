use super::tests_support::*;
use super::*;
use std::cell::Cell;
use std::time::Duration;

#[test]
fn missing_primary_key_sync_retries_transient_connection_loss_with_a_bound() {
    let attempts = Cell::new(0);
    let report =
        retry_sync_table_operation(SyncMode::MissingPrimaryKeys, 3, Duration::ZERO, || {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                Err(TableSyncError::Read("connection timed out".to_string()))
            } else {
                Ok(SyncTableReport::default())
            }
        })
        .expect("retry succeeds");

    assert_eq!(report, SyncTableReport::default());
    assert_eq!(attempts.get(), 2);

    let failures = Cell::new(0);
    let error = retry_sync_table_operation(SyncMode::MissingPrimaryKeys, 3, Duration::ZERO, || {
        failures.set(failures.get() + 1);
        Err(TableSyncError::Read("connection reset".to_string()))
    })
    .expect_err("retry bound");

    assert_eq!(failures.get(), 3);
    assert_eq!(error.to_string(), "sync read failed: connection reset");

    let packet_attempts = Cell::new(0);
    retry_sync_table_operation(SyncMode::MissingPrimaryKeys, 3, Duration::ZERO, || {
        packet_attempts.set(packet_attempts.get() + 1);
        if packet_attempts.get() == 1 {
            Err(TableSyncError::Read("Packet out of sync".to_string()))
        } else {
            Ok(SyncTableReport::default())
        }
    })
    .expect("packet error retries");
    assert_eq!(packet_attempts.get(), 2);
}

#[test]
fn successful_sync_is_not_failed_by_lock_release_connection_loss() {
    let progress_store = RecordingProgressStore {
        release_error: Some("connection reset while releasing lock".to_string()),
        ..RecordingProgressStore::default()
    };

    let result = finish_sync_run("completed-run", Ok(42_u64), &progress_store);

    assert_eq!(result, Ok(42));
}

#[test]
fn target_connection_config_preserves_target_endpoint() {
    let config = SyncTableConfig {
        source: crate::mysql_snapshot::MySqlConnectionConfig::default(),
        target: crate::live::TargetMySqlConfig {
            host: "target".to_string(),
            port: 25060,
            user: "target_user".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            tls_ca_file: "/tmp/custom-target-ca.pem".to_string(),
            insert_conflict_policy: crate::live::InsertConflictPolicy::IgnoreDuplicate,
        },
        table: account_table(),
        chunk_size: 10,
        mode: SyncMode::DryRun,
        progress_table: "cdc.table_sync_runs".to_string(),
        run_id: "test-run".to_string(),
        start_after: None,
        end_at: None,
        max_deletes: Some(0),
        updated_since: None,
        plan_hash: None,
    };

    let target = target_connection_config(&config);

    assert_eq!(target.host, "target");
    assert_eq!(target.port, 25060);
    assert_eq!(target.database, "globalcomix");
}

#[test]
fn dry_run_reports_repairs_without_applying_them() {
    let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
    let target = FakeReader::new(vec![row("0", "extra"), row("1", "old")]);
    let mut repair_target = RecordingRepairTarget::default();

    let report = sync_table(
        &account_table(),
        10,
        SyncMode::DryRun,
        &source,
        &target,
        &mut repair_target,
    )
    .expect("sync report");

    assert_eq!(report.inserts, 1);
    assert_eq!(report.updates, 1);
    assert_eq!(report.extra_target_rows, 1);
    assert!(repair_target.inserts.borrow().is_empty());
    assert!(repair_target.updates.borrow().is_empty());
}

#[test]
fn apply_repairs_missing_different_and_extra_target_rows() {
    let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
    let target = FakeReader::new(vec![row("0", "extra"), row("1", "old")]);
    let mut repair_target = RecordingRepairTarget::default();

    let mut progress_store = RecordingProgressStore::default();
    let report = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "test-run".to_string(),
            run_scope: "test-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(1),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("sync report");

    assert_eq!(report.inserts, 1);
    assert_eq!(report.updates, 1);
    assert_eq!(report.extra_target_rows, 1);
    assert_eq!(
        repair_target.inserts.borrow().as_slice(),
        &[row("2", "bravo")]
    );
    assert_eq!(
        repair_target.updates.borrow().as_slice(),
        &[row("1", "alpha")]
    );
    assert_eq!(
        repair_target.deletes.borrow().as_slice(),
        &[vec!["0".to_string()]]
    );
}

#[test]
fn apply_stops_before_deleting_above_safety_threshold() {
    let source = FakeReader::new(vec![row("1", "alpha")]);
    let target = FakeReader::new(vec![row("0", "extra"), row("1", "alpha")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "test-run".to_string(),
            run_scope: "test-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("delete threshold");

    assert_eq!(
        error.to_string(),
        "sync repair failed: delete safety threshold exceeded: max_deletes=0"
    );
    assert!(repair_target.deletes.borrow().is_empty());
}

#[test]
fn recent_update_sync_upserts_filtered_source_rows_without_deletes() {
    let source = FakeReader::new(vec![
        row_with_updated_at("1", "alpha", "2026-05-01 00:00:00"),
        row_with_updated_at("2", "bravo", "2026-06-02 00:00:00"),
    ]);
    let mut repair_target = RecordingRepairTarget::default();

    let report = sync_recent_updates(
        &account_table_with_updated_at(),
        10,
        SyncMode::Apply,
        &source,
        &mut repair_target,
        UpdatedSince {
            column: "updated_at".to_string(),
            value: "2026-06-01 00:00:00".to_string(),
        },
    )
    .expect("recent sync");

    assert_eq!(report.rows_scanned, 1);
    assert_eq!(report.updates, 1);
    assert_eq!(
        repair_target.inserts.borrow().as_slice(),
        &[row_with_updated_at("2", "bravo", "2026-06-02 00:00:00")]
    );
    assert!(repair_target.deletes.borrow().is_empty());
}

#[test]
fn recent_update_retry_restarts_from_beginning_to_catch_newly_eligible_rows() {
    let table = account_table_with_updated_at();
    let updated_since = UpdatedSince {
        column: "updated_at".to_string(),
        value: "2026-06-01 00:00:00".to_string(),
    };
    let run_spec_json = serde_json::to_string(&SyncRunSpec {
        scope: "test-scope",
        table: &table,
        chunk_size: 10,
        mode: SyncMode::Apply,
        start_after: &None,
        end_at: &None,
        max_deletes: None,
        updated_since: Some(&updated_since),
    })
    .expect("run spec");
    let source = FakeReader::new(vec![
        row_with_updated_at("1", "already-applied", "2026-06-02 00:00:00"),
        row_with_updated_at("2", "resume-here", "2026-06-03 00:00:00"),
    ]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::with_progress(SyncTableProgress {
        run_id: Some("recent-01".to_string()),
        run_spec_json: Some(run_spec_json),
        table: "accounts".to_string(),
        last_primary_key: Some(vec!["1".to_string()]),
        chunks: 1,
        rows_scanned: 1,
        total_rows: None,
        inserts: 0,
        updates: 1,
        extra_target_rows: 0,
        mode: SyncMode::Apply,
        status: progress::SyncProgressStatus::Running,
        last_error: None,
    });

    let report = sync_recent_updates_with_progress(
        "recent-01",
        "test-scope",
        RecentUpdateSyncContext {
            table: &table,
            chunk_size: 10,
            mode: SyncMode::Apply,
            source: &source,
            repair_target: &mut repair_target,
            progress_store: &mut progress_store,
            updated_since,
        },
    )
    .expect("resumed recent update run");

    assert_eq!(source.requests.borrow()[0].start_after, None);
    assert_eq!(report.rows_scanned, 2);
    assert_eq!(report.updates, 2);
    assert_eq!(
        repair_target.inserts.borrow().as_slice(),
        &[
            row_with_updated_at("1", "already-applied", "2026-06-02 00:00:00"),
            row_with_updated_at("2", "resume-here", "2026-06-03 00:00:00"),
        ]
    );
}

#[test]
fn core_config_accepts_plaintext_source_without_tls_ca() {
    let config = SyncTableConfig {
        source: crate::mysql_snapshot::MySqlConnectionConfig::default(),
        target: crate::live::TargetMySqlConfig::default(),
        table: account_table_with_updated_at(),
        chunk_size: 10,
        mode: SyncMode::DryRun,
        progress_table: "cdc.table_sync_runs".to_string(),
        run_id: "test-run".to_string(),
        start_after: None,
        end_at: None,
        max_deletes: Some(0),
        updated_since: None,
        plan_hash: None,
    };

    validate_sync_table_config(&config).expect("plaintext source without CA should be accepted");
}

#[test]
fn core_config_rejects_updated_since_with_primary_key_bounds() {
    let source = crate::mysql_snapshot::MySqlConnectionConfig::default();
    let config = SyncTableConfig {
        source,
        target: crate::live::TargetMySqlConfig::default(),
        table: account_table_with_updated_at(),
        chunk_size: 10,
        mode: SyncMode::DryRun,
        progress_table: "cdc.table_sync_runs".to_string(),
        run_id: "test-run".to_string(),
        start_after: Some(vec!["10".to_string()]),
        end_at: None,
        max_deletes: Some(0),
        updated_since: Some(UpdatedSince {
            column: "updated_at".to_string(),
            value: "2026-06-01 00:00:00".to_string(),
        }),
        plan_hash: None,
    };

    let error = validate_sync_table_config(&config).expect_err("conflicting config");

    assert_eq!(
        error.to_string(),
        "invalid sync table: updated_since cannot be combined with start_after or end_at"
    );
}

#[test]
fn rejects_range_bounds_with_wrong_composite_primary_key_arity() {
    let source = FakeReader::new(vec![]);
    let target = FakeReader::new(vec![]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();
    let table = SyncTable {
        name: "accounts".to_string(),
        primary_key: vec!["tenant_id".to_string(), "id".to_string()],
        columns: vec!["tenant_id".to_string(), "id".to_string()],
    };

    let error = sync_table_with_progress_range(
        &table,
        SyncRunOptions {
            run_id: "test-run".to_string(),
            run_scope: "test-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::DryRun,
            start_after: Some(vec!["1".to_string()]),
            end_at: None,
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("bad arity");

    assert_eq!(
        error.to_string(),
        "invalid sync table: start_after has 1 values for 2 primary-key columns"
    );
}

#[test]
fn apply_repairs_target_tail_after_last_source_row() {
    let source = FakeReader::new(vec![row("1", "alpha")]);
    let target = FakeReader::new(vec![row("1", "alpha"), row("2", "extra")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let report = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "test-run".to_string(),
            run_scope: "test-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(1),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("sync report");

    assert_eq!(report.extra_target_rows, 1);
    assert_eq!(
        repair_target.deletes.borrow().as_slice(),
        &[vec!["2".to_string()]]
    );
}

#[test]
fn apply_repairs_source_empty_target_range() {
    let source = FakeReader::new(vec![]);
    let target = FakeReader::new(vec![row("2", "extra")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let report = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "test-run".to_string(),
            run_scope: "test-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: Some(vec!["1".to_string()]),
            end_at: Some(vec!["3".to_string()]),
            max_deletes: Some(1),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("sync report");

    assert_eq!(report.extra_target_rows, 1);
    assert_eq!(
        repair_target.deletes.borrow().as_slice(),
        &[vec!["2".to_string()]]
    );
}

#[test]
fn apply_rejects_total_extra_rows_before_any_mutation() {
    let source = FakeReader::new(vec![
        row("1", "new"),
        row("2", "missing"),
        row("3", "missing"),
        row("6", "missing"),
    ]);
    let target = FakeReader::new(vec![row("1", "old"), row("4", "extra"), row("5", "extra")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "test-run".to_string(),
            run_scope: "test-scope".to_string(),
            chunk_size: 2,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(1),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("delete ceiling");

    assert_eq!(
        error.to_string(),
        "sync repair failed: delete safety threshold exceeded: max_deletes=1"
    );
    assert!(repair_target.inserts.borrow().is_empty());
    assert!(repair_target.updates.borrow().is_empty());
    assert!(repair_target.deletes.borrow().is_empty());
    assert!(repair_target.operations.borrow().is_empty());
}

#[test]
fn apply_accepts_exact_total_extra_row_ceiling() {
    let source = FakeReader::new(vec![
        row("1", "new"),
        row("2", "missing"),
        row("3", "missing"),
        row("6", "missing"),
    ]);
    let target = FakeReader::new(vec![row("1", "old"), row("4", "extra"), row("5", "extra")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let report = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "test-run".to_string(),
            run_scope: "test-scope".to_string(),
            chunk_size: 2,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(2),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("sync report");

    assert_eq!(report.extra_target_rows, 2);
    assert_eq!(
        repair_target.operations.borrow().as_slice(),
        &[
            "update:1", "insert:2", "delete:4", "delete:5", "insert:3", "insert:6",
        ]
    );
}

#[test]
fn apply_releases_unique_conflicts_before_inserting_missing_rows() {
    let source = FakeReader::new(vec![row("10", "shared"), row("20", "correct")]);
    let target = FakeReader::new(vec![row("20", "shared")]);
    let mut repair_target = RecordingRepairTarget::default();

    sync_table(
        &account_table(),
        10,
        SyncMode::Apply,
        &source,
        &target,
        &mut repair_target,
    )
    .expect("sync report");

    assert_eq!(
        repair_target.operations.borrow().as_slice(),
        &["update:20".to_string(), "insert:10".to_string()]
    );
}

#[test]
fn target_read_is_bounded_by_source_chunk_end() {
    let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo"), row("3", "coda")]);
    let target = FakeReader::new(vec![]);
    let mut repair_target = RecordingRepairTarget::default();

    sync_table(
        &account_table(),
        2,
        SyncMode::DryRun,
        &source,
        &target,
        &mut repair_target,
    )
    .expect("sync report");

    let target_requests = target.requests.borrow();
    assert_eq!(target_requests[0].end_at, Some(vec!["2".to_string()]));
    assert_eq!(target_requests[1].start_after, Some(vec!["2".to_string()]));
    assert_eq!(target_requests[1].end_at, Some(vec!["3".to_string()]));
}

#[test]
fn target_read_allows_extra_rows_inside_source_window() {
    let source = FakeReader::new(vec![row("4", "delta")]);
    let target = FakeReader::new(vec![
        row("1", "extra"),
        row("2", "extra"),
        row("3", "extra"),
        row("4", "delta"),
    ]);
    let mut repair_target = RecordingRepairTarget::default();

    let report = sync_table(
        &account_table(),
        1,
        SyncMode::DryRun,
        &source,
        &target,
        &mut repair_target,
    )
    .expect("sync report");

    assert_eq!(report.extra_target_rows, 3);
    assert!(target.requests.borrow().len() > 1);
}

#[test]
fn verify_fails_for_missing_rows_without_mutation() {
    let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
    let target = FakeReader::new(vec![row("1", "alpha")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range_phase(
        &account_table(),
        SyncRunOptions {
            run_id: "verify-missing".to_string(),
            run_scope: "verify-missing-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        SyncPhase::Verify,
    )
    .expect_err("missing row must fail verification");

    assert!(error.to_string().contains("missing_rows=1"));
    assert!(repair_target.operations.borrow().is_empty());
}

#[test]
fn verify_no_target_extras_allows_source_missing_rows_without_mutation() {
    let source = FakeReader::new(vec![row("1", "alpha"), row("2", "source-only")]);
    let target = FakeReader::new(vec![row("1", "alpha")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let report = sync_table_with_progress_range_phase(
        &account_table(),
        SyncRunOptions {
            run_id: "verify-no-target-extras".to_string(),
            run_scope: "verify-no-target-extras-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        SyncPhase::VerifyNoTargetExtras,
    )
    .expect("source-only rows are outside delete-only verification scope");

    assert_eq!(report.inserts, 0);
    assert_eq!(report.updates, 0);
    assert_eq!(report.extra_target_rows, 0);
    assert!(repair_target.operations.borrow().is_empty());
}

#[test]
fn verify_fails_for_extra_rows_without_mutation() {
    let source = FakeReader::new(vec![row("1", "alpha")]);
    let target = FakeReader::new(vec![row("1", "alpha"), row("2", "extra")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range_phase(
        &account_table(),
        SyncRunOptions {
            run_id: "verify-extra".to_string(),
            run_scope: "verify-extra-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        SyncPhase::Verify,
    )
    .expect_err("extra row must fail verification");

    assert!(error.to_string().contains("extra_rows=1"));
    assert!(repair_target.operations.borrow().is_empty());
}

#[test]
fn verify_fails_for_divergent_rows_without_mutation() {
    let source = FakeReader::new(vec![row("1", "alpha")]);
    let target = FakeReader::new(vec![row("1", "old")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range_phase(
        &account_table(),
        SyncRunOptions {
            run_id: "verify-divergent".to_string(),
            run_scope: "verify-divergent-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        SyncPhase::Verify,
    )
    .expect_err("divergent row must fail verification");

    assert!(error.to_string().contains("divergent_rows=1"));
    assert!(repair_target.operations.borrow().is_empty());
}

#[test]
fn ignored_insert_remains_missing_and_fails_follow_up_verification() {
    let source = FakeReader::new(vec![row("1", "alpha")]);
    let target = FakeReader::new(vec![]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut insert_progress = RecordingProgressStore::default();

    sync_table_with_progress_range_phase(
        &account_table(),
        SyncRunOptions {
            run_id: "insert-ignore".to_string(),
            run_scope: "insert-ignore-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut insert_progress,
        SyncPhase::InsertMissing,
    )
    .expect("insert phase");

    let mut verify_progress = RecordingProgressStore::default();
    let error = sync_table_with_progress_range_phase(
        &account_table(),
        SyncRunOptions {
            run_id: "verify-after-insert-ignore".to_string(),
            run_scope: "verify-after-insert-ignore-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut verify_progress,
        SyncPhase::Verify,
    )
    .expect_err("ignored insert must remain unresolved");

    assert!(error.to_string().contains("missing_rows=1"));
    assert_eq!(
        repair_target.operations.borrow().as_slice(),
        &["insert:1".to_string()]
    );
}

#[test]
fn verify_only_reports_differences_inside_bounded_primary_key_window() {
    let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
    let target = FakeReader::new(vec![
        row("1", "old"),
        row("2", "bravo"),
        row("3", "outside-window"),
    ]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range_phase(
        &account_table(),
        SyncRunOptions {
            run_id: "verify-window".to_string(),
            run_scope: "verify-window-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: Some(vec!["2".to_string()]),
            max_deletes: Some(0),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        SyncPhase::Verify,
    )
    .expect_err("divergence inside selected window must fail verification");

    let message = error.to_string();
    assert!(message.contains("divergent_rows=1"));
    assert!(message.contains("extra_rows=0"));
    assert!(message.contains("end_at=[\"2\"]"));
    assert!(repair_target.operations.borrow().is_empty());
}

#[test]
fn missing_primary_keys_mode_inserts_only_absent_primary_keys() {
    let source = FakeReader::new(vec![row("1", "new"), row("3", "missing")]);
    let target = FakeReader::new(vec![row("1", "old"), row("2", "extra")]);
    let mut repair_target = RecordingRepairTarget::default();

    let report = sync_table(
        &account_table(),
        10,
        SyncMode::MissingPrimaryKeys,
        &source,
        &target,
        &mut repair_target,
    )
    .expect("missing primary-key sync");

    assert_eq!(report.inserts, 1);
    assert_eq!(report.updates, 0);
    assert_eq!(report.extra_target_rows, 0);
    assert_eq!(
        repair_target.inserts.borrow().as_slice(),
        &[row("3", "missing")]
    );
    assert!(repair_target.updates.borrow().is_empty());
    assert!(repair_target.deletes.borrow().is_empty());
    assert!(
        target
            .requests
            .borrow()
            .iter()
            .all(|request| request.columns == vec!["id".to_string()])
    );
}

#[test]
fn phase_sync_applies_only_requested_mutation_kind() {
    let source = FakeReader::new(vec![row("1", "new")]);
    let target = FakeReader::new(vec![row("1", "old"), row("2", "extra")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();
    let report = sync_table_with_progress_range_phase(
        &account_table(),
        SyncRunOptions {
            run_id: "phase-delete".to_string(),
            run_scope: "phase-delete-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
            max_deletes: Some(1),
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
        SyncPhase::DeleteExtras,
    )
    .expect("delete phase");

    assert_eq!(report.extra_target_rows, 1);
    assert!(repair_target.inserts.borrow().is_empty());
    assert!(repair_target.updates.borrow().is_empty());
    assert_eq!(
        repair_target.deletes.borrow().as_slice(),
        &[vec!["2".to_string()]]
    );
}
