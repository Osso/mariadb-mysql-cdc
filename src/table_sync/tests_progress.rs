use super::tests_support::*;
use super::*;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;

struct SharedProgressStore {
    progress: Rc<RefCell<Option<SyncTableProgress>>>,
}

impl SharedProgressStore {
    fn new(progress: Rc<RefCell<Option<SyncTableProgress>>>) -> Self {
        Self { progress }
    }
}

impl SyncProgressStore for SharedProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn load(&self, _run_id: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        Ok(self.progress.borrow().clone())
    }

    fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.progress.replace(Some(progress.clone()));
        Ok(())
    }

    fn save_error(&mut self, _run_id: &str, error: &TableSyncError) -> Result<(), TableSyncError> {
        let mut progress = self.progress.borrow().clone().expect("progress row");
        progress.status = progress::SyncProgressStatus::Error;
        progress.last_error = Some(error.to_string());
        self.progress.replace(Some(progress));
        Ok(())
    }
}

struct FailOnReadReader {
    reader: FakeReader,
    fail_on_read: usize,
}

impl FailOnReadReader {
    fn new(rows: Vec<crate::snapshot::SnapshotRow>, fail_on_read: usize) -> Self {
        Self {
            reader: FakeReader::new(rows),
            fail_on_read,
        }
    }
}

impl SyncTableReader for FailOnReadReader {
    fn read_rows(
        &self,
        request: &SyncChunkRequest,
    ) -> Result<Vec<crate::snapshot::SnapshotRow>, TableSyncError> {
        if self.reader.requests.borrow().len() + 1 == self.fail_on_read {
            return Err(TableSyncError::Read("repair connection lost".to_string()));
        }
        self.reader.read_rows(request)
    }
}

struct ReclamationProgressStore {
    progress: RefCell<BTreeMap<String, SyncTableProgress>>,
    saved: RefCell<Vec<SyncTableProgress>>,
    transition_after_first_enumeration: RefCell<Option<String>>,
    enumeration_count: Cell<usize>,
}

impl ReclamationProgressStore {
    fn new(progress: Vec<SyncTableProgress>, transition_after_first_enumeration: &str) -> Self {
        Self {
            progress: RefCell::new(
                progress
                    .into_iter()
                    .map(|progress| {
                        (
                            progress.run_id.clone().expect("run-scoped progress"),
                            progress,
                        )
                    })
                    .collect(),
            ),
            saved: RefCell::new(Vec::new()),
            transition_after_first_enumeration: RefCell::new(Some(
                transition_after_first_enumeration.to_string(),
            )),
            enumeration_count: Cell::new(0),
        }
    }

    fn get(&self, run_id: &str) -> SyncTableProgress {
        self.progress
            .borrow()
            .get(run_id)
            .cloned()
            .expect("progress row")
    }
}

impl SyncProgressStore for ReclamationProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn load(&self, run_id: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        Ok(self.progress.borrow().get(run_id).cloned())
    }

    fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        let run_id = progress.run_id.clone().expect("run-scoped progress");
        self.progress.borrow_mut().insert(run_id, progress.clone());
        self.saved.borrow_mut().push(progress.clone());
        Ok(())
    }

    fn save_error(&mut self, run_id: &str, error: &TableSyncError) -> Result<(), TableSyncError> {
        let mut progress = self.get(run_id);
        progress.status = progress::SyncProgressStatus::Error;
        progress.last_error = Some(error.to_string());
        self.progress
            .borrow_mut()
            .insert(run_id.to_string(), progress);
        Ok(())
    }
}

impl SyncRunSelectionStore for ReclamationProgressStore {
    fn find_failed_run_candidates(
        &self,
        table: &str,
    ) -> Result<Vec<SyncRunCandidate>, TableSyncError> {
        let enumeration = self.enumeration_count.get() + 1;
        self.enumeration_count.set(enumeration);
        let candidates = self
            .progress
            .borrow()
            .values()
            .filter(|progress| {
                progress.table == table && progress.status == progress::SyncProgressStatus::Error
            })
            .map(|progress| SyncRunCandidate {
                run_id: progress.run_id.clone().expect("run-scoped progress"),
                table: progress.table.clone(),
                run_spec_json: progress.run_spec_json.clone().expect("run specification"),
                mode: progress.mode,
                status: progress.status,
            })
            .collect();
        if enumeration == 1
            && let Some(run_id) = self.transition_after_first_enumeration.borrow_mut().take()
        {
            let mut progress = self.get(&run_id);
            progress.status = progress::SyncProgressStatus::Error;
            progress.last_error = Some("second run failed".to_string());
            self.progress.borrow_mut().insert(run_id, progress);
        }
        Ok(candidates)
    }
}

struct CleanupProgressStore {
    events: RefCell<Vec<&'static str>>,
    fail_acquire: bool,
    fail_begin: bool,
}

impl CleanupProgressStore {
    fn new(fail_acquire: bool, fail_begin: bool) -> Self {
        Self {
            events: RefCell::new(Vec::new()),
            fail_acquire,
            fail_begin,
        }
    }
}

impl SyncProgressStore for CleanupProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn load(&self, _run_id: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        Ok(None)
    }

    fn save(&mut self, _progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn save_error(&mut self, _run_id: &str, _error: &TableSyncError) -> Result<(), TableSyncError> {
        Ok(())
    }
}

impl SyncRunSelectionStore for CleanupProgressStore {
    fn find_failed_run_candidates(
        &self,
        _table: &str,
    ) -> Result<Vec<SyncRunCandidate>, TableSyncError> {
        Ok(Vec::new())
    }

    fn acquire_selection_lock(
        &self,
        _table: &str,
        _run_spec_json: &str,
    ) -> Result<(), TableSyncError> {
        self.events.borrow_mut().push("acquire");
        if self.fail_acquire {
            return Err(TableSyncError::Progress(
                "GET_LOCK response lost after acquisition".to_string(),
            ));
        }
        Ok(())
    }

    fn release_selection_lock(
        &self,
        _table: &str,
        _run_spec_json: &str,
    ) -> Result<(), TableSyncError> {
        self.events.borrow_mut().push("release");
        Ok(())
    }

    fn begin_selection_transaction(&self) -> Result<(), TableSyncError> {
        self.events.borrow_mut().push("begin");
        if self.fail_begin {
            return Err(TableSyncError::Progress(
                "START TRANSACTION response lost after start".to_string(),
            ));
        }
        Ok(())
    }

    fn commit_selection_transaction(&self) -> Result<(), TableSyncError> {
        self.events.borrow_mut().push("commit");
        Ok(())
    }

    fn rollback_selection_transaction(&self) -> Result<(), TableSyncError> {
        self.events.borrow_mut().push("rollback");
        Ok(())
    }
}

#[test]
fn selection_lock_error_still_attempts_advisory_lock_release() {
    let mut store = CleanupProgressStore::new(true, false);

    let error = claim_compatible_failed_run(
        &mut store,
        "guests",
        SyncPhase::InsertMissing,
        "expected-spec",
    )
    .expect_err("ambiguous GET_LOCK outcome");

    assert!(error.to_string().contains("GET_LOCK response lost"));
    assert_eq!(store.events.borrow().as_slice(), &["acquire", "release"]);
}

#[test]
fn transaction_start_error_still_rolls_back_and_releases_advisory_lock() {
    let mut store = CleanupProgressStore::new(false, true);

    let error = claim_compatible_failed_run(
        &mut store,
        "guests",
        SyncPhase::InsertMissing,
        "expected-spec",
    )
    .expect_err("ambiguous START TRANSACTION outcome");

    assert!(
        error
            .to_string()
            .contains("START TRANSACTION response lost")
    );
    assert_eq!(
        store.events.borrow().as_slice(),
        &["acquire", "begin", "rollback", "release"]
    );
}

fn failed_run_progress(
    run_id: &str,
    spec: &str,
    status: progress::SyncProgressStatus,
) -> SyncTableProgress {
    SyncTableProgress {
        run_id: Some(run_id.to_string()),
        run_spec_json: Some(spec.to_string()),
        table: "guests".to_string(),
        last_primary_key: Some(vec!["10".to_string()]),
        chunks: 2,
        rows_scanned: 20,
        total_rows: Some(100),
        inserts: 3,
        updates: 4,
        extra_target_rows: 5,
        delete_preflight_complete: false,
        mode: SyncMode::MissingPrimaryKeys,
        status,
        last_error: Some("original failure".to_string()),
    }
}

#[test]
fn concurrent_exact_failed_run_appearing_after_enumeration_fails_without_claim_mutation() {
    let expected_spec =
        r#"{"scope":"current","table":"guests","chunk_size":1,"mode":"missing_primary_keys"}"#;
    let selected_before = failed_run_progress(
        "selected",
        expected_spec,
        progress::SyncProgressStatus::Error,
    );
    let mut store = ReclamationProgressStore::new(
        vec![
            selected_before.clone(),
            failed_run_progress(
                "appears-after-enumeration",
                expected_spec,
                progress::SyncProgressStatus::Running,
            ),
        ],
        "appears-after-enumeration",
    );

    let error = claim_compatible_failed_run(
        &mut store,
        "guests",
        SyncPhase::InsertMissing,
        expected_spec,
    )
    .expect_err("new exact-compatible failure must make selection ambiguous");

    assert_eq!(
        error.to_string(),
        "sync progress failed: multiple compatible failed missing-primary-keys runs exist for table `guests`"
    );
    assert_eq!(store.get("selected"), selected_before);
    assert!(store.saved.borrow().is_empty());
}

#[test]
fn differing_immutable_spec_does_not_make_exact_candidate_ambiguous() {
    let expected_spec =
        r#"{"scope":"current","table":"guests","chunk_size":1,"mode":"missing_primary_keys"}"#;
    let candidates = vec![
        SyncRunCandidate::new(
            "different-bounds",
            "guests",
            r#"{"scope":"current","table":"guests","chunk_size":1,"mode":"missing_primary_keys","start_after":["10"]}"#,
            SyncMode::MissingPrimaryKeys,
            progress::SyncProgressStatus::Error,
        ),
        SyncRunCandidate::new(
            "exact",
            "guests",
            expected_spec,
            SyncMode::MissingPrimaryKeys,
            progress::SyncProgressStatus::Error,
        ),
    ];

    let selected = select_compatible_failed_run(
        &candidates,
        "guests",
        SyncPhase::InsertMissing,
        expected_spec,
    )
    .expect("candidate selection")
    .expect("exact candidate");

    assert_eq!(selected.run_id, "exact");
}

#[test]
fn selects_only_one_compatible_failed_missing_primary_keys_run() {
    let candidates = vec![
        SyncRunCandidate::new(
            "complete",
            "guests",
            r#"{"table":"guests","mode":"missing_primary_keys"}"#,
            SyncMode::MissingPrimaryKeys,
            progress::SyncProgressStatus::Complete,
        ),
        SyncRunCandidate::new(
            "wrong-table",
            "sessions",
            r#"{"table":"sessions","mode":"missing_primary_keys"}"#,
            SyncMode::MissingPrimaryKeys,
            progress::SyncProgressStatus::Error,
        ),
        SyncRunCandidate::new(
            "wrong-spec",
            "guests",
            r#"{"table":"sessions","mode":"missing_primary_keys"}"#,
            SyncMode::MissingPrimaryKeys,
            progress::SyncProgressStatus::Error,
        ),
        SyncRunCandidate::new(
            "compatible",
            "guests",
            r#"{"scope":"durable-fixture","table":"guests","mode":"missing_primary_keys"}"#,
            SyncMode::MissingPrimaryKeys,
            progress::SyncProgressStatus::Error,
        ),
    ];

    let selected = select_compatible_failed_run(
        &candidates,
        "guests",
        SyncPhase::InsertMissing,
        r#"{"scope":"durable-fixture","table":"guests","mode":"missing_primary_keys"}"#,
    )
    .expect("candidate selection")
    .expect("compatible failed run");

    assert_eq!(selected.run_id, "compatible");
    assert_eq!(
        selected.run_spec_json,
        r#"{"scope":"durable-fixture","table":"guests","mode":"missing_primary_keys"}"#
    );
}

#[test]
fn multiple_compatible_failed_runs_fail_closed() {
    let candidates = vec![
        SyncRunCandidate::new(
            "run-a",
            "guests",
            r#"{"table":"guests","mode":"missing_primary_keys"}"#,
            SyncMode::MissingPrimaryKeys,
            progress::SyncProgressStatus::Error,
        ),
        SyncRunCandidate::new(
            "run-b",
            "guests",
            r#"{"table":"guests","mode":"missing_primary_keys"}"#,
            SyncMode::MissingPrimaryKeys,
            progress::SyncProgressStatus::Error,
        ),
    ];

    let error = select_compatible_failed_run(
        &candidates,
        "guests",
        SyncPhase::InsertMissing,
        r#"{"table":"guests","mode":"missing_primary_keys"}"#,
    )
    .expect_err("ambiguous candidates");

    assert_eq!(
        error.to_string(),
        "sync progress failed: multiple compatible failed missing-primary-keys runs exist for table `guests`"
    );
}

#[test]
fn candidate_selection_is_disabled_outside_insert_missing_phase() {
    let candidates = vec![SyncRunCandidate::new(
        "compatible",
        "guests",
        r#"{"table":"guests","mode":"missing_primary_keys"}"#,
        SyncMode::MissingPrimaryKeys,
        progress::SyncProgressStatus::Error,
    )];

    assert_eq!(
        select_compatible_failed_run(
            &candidates,
            "guests",
            SyncPhase::Verify,
            r#"{"table":"guests","mode":"missing_primary_keys"}"#,
        )
        .expect("phase selection"),
        None
    );
}

#[test]
fn malformed_run_spec_is_incompatible() {
    let candidates = vec![SyncRunCandidate::new(
        "malformed",
        "guests",
        "not-json",
        SyncMode::MissingPrimaryKeys,
        progress::SyncProgressStatus::Error,
    )];

    assert_eq!(
        select_compatible_failed_run(
            &candidates,
            "guests",
            SyncPhase::InsertMissing,
            r#"{"table":"guests","mode":"missing_primary_keys"}"#,
        )
        .expect("candidate selection"),
        None
    );
}

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
        delete_preflight_complete: false,
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
fn first_chunk_mutation_immediately_persists_progress() {
    let source = FailOnReadReader::new(vec![row("1", "alpha"), row("2", "beta")], 2);
    let target = FakeReader::new(Vec::new());
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "immediate-first-chunk".to_string(),
            run_scope: "immediate-first-chunk-scope".to_string(),
            chunk_size: 1,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("second source read fails after first chunk commits");

    assert_eq!(
        repair_target
            .inserts
            .borrow()
            .iter()
            .map(|row| row.primary_key.clone())
            .collect::<Vec<_>>(),
        vec![vec!["1".to_string()]]
    );
    let saved = progress_store.saved.borrow();
    let first_chunk = saved
        .iter()
        .find(|progress| progress.last_primary_key == Some(vec!["1".to_string()]))
        .expect("first chunk progress persisted");
    assert_eq!(first_chunk.chunks, 1);
    assert_eq!(first_chunk.rows_scanned, 1);
    assert_eq!(first_chunk.inserts, 1);
}

#[test]
fn interruption_resumes_after_persisted_chunk_without_replay() {
    let durable_progress = Rc::new(RefCell::new(None));
    let first_source = FailOnReadReader::new(vec![row("1", "alpha"), row("2", "beta")], 2);
    let first_target = FakeReader::new(Vec::new());
    let mut first_repair_target = RecordingRepairTarget::default();
    let mut first_store = SharedProgressStore::new(Rc::clone(&durable_progress));

    sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "chunk-resume".to_string(),
            run_scope: "chunk-resume-scope".to_string(),
            chunk_size: 1,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &first_source,
        &first_target,
        &mut first_repair_target,
        &mut first_store,
    )
    .expect_err("interruption after first committed chunk");

    let resumed_source = FakeReader::new(vec![row("1", "alpha"), row("2", "beta")]);
    let resumed_target = FakeReader::new(vec![row("1", "alpha")]);
    let mut resumed_repair_target = RecordingRepairTarget::default();
    let mut resumed_store = SharedProgressStore::new(durable_progress);
    sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "chunk-resume".to_string(),
            run_scope: "chunk-resume-scope".to_string(),
            chunk_size: 1,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &resumed_source,
        &resumed_target,
        &mut resumed_repair_target,
        &mut resumed_store,
    )
    .expect("resume completes from next chunk");

    assert_eq!(
        resumed_source.requests.borrow()[0].start_after,
        Some(vec!["1".to_string()])
    );
    assert_eq!(
        resumed_repair_target
            .inserts
            .borrow()
            .iter()
            .map(|row| row.primary_key.clone())
            .collect::<Vec<_>>(),
        vec![vec!["2".to_string()]]
    );
}

#[test]
fn target_extras_delete_chunk_by_chunk_and_persist_progress() {
    let source = FakeReader::new(vec![row("10", "ten"), row("20", "twenty")]);
    let target = FakeReader::new(vec![
        row("05", "extra-first"),
        row("10", "ten"),
        row("15", "extra-second"),
        row("20", "twenty"),
    ]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "chunk-deletes".to_string(),
            run_scope: "chunk-deletes-scope".to_string(),
            chunk_size: 1,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("all target extras reconcile");

    assert_eq!(
        repair_target.deletes.borrow().as_slice(),
        &[vec!["05".to_string()], vec!["15".to_string()]]
    );
    let saved = progress_store.saved.borrow();
    assert!(saved.iter().any(|progress| {
        progress.last_primary_key == Some(vec!["10".to_string()]) && progress.extra_target_rows == 1
    }));
    let completed = saved.last().expect("completed progress");
    assert_eq!(completed.last_primary_key, Some(vec!["20".to_string()]));
    assert_eq!(completed.extra_target_rows, 2);
    assert_eq!(completed.status, progress::SyncProgressStatus::Complete);
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
        delete_preflight_complete: false,
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
        delete_preflight_complete: false,
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
