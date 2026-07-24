use super::tests_support::*;
use super::*;
use crate::snapshot::SnapshotRow;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
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
fn apply_retries_recoverable_connection_and_constraint_failures() {
    for first_error in [
        TableSyncError::Read("connection reset".to_string()),
        TableSyncError::Repair("MySqlError { ERROR 1213 (40001): deadlock }".to_string()),
    ] {
        let attempts = Cell::new(0);
        let mut first_error = Some(first_error);
        let report = retry_sync_table_operation(SyncMode::Apply, 2, Duration::ZERO, || {
            attempts.set(attempts.get() + 1);
            if let Some(error) = first_error.take() {
                Err(error)
            } else {
                Ok(SyncTableReport::default())
            }
        })
        .expect("recoverable apply failure retries");

        assert_eq!(report, SyncTableReport::default());
        assert_eq!(attempts.get(), 2);
    }
}

#[test]
fn recoverable_failure_preserves_running_progress_without_advancing() {
    struct ConnectionFailureReader;

    impl SyncTableReader for ConnectionFailureReader {
        fn read_rows(
            &self,
            _request: &SyncChunkRequest,
        ) -> Result<Vec<SnapshotRow>, TableSyncError> {
            Err(TableSyncError::Read("connection reset".to_string()))
        }
    }

    let source = ConnectionFailureReader;
    let target = FakeReader::new(Vec::new());
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "recoverable-running".to_string(),
            run_scope: "recoverable-running-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("connection failure is returned for outer retry");

    assert!(progress_store.errors.borrow().is_empty());
    assert!(progress_store.saved.borrow().iter().all(|progress| {
        progress.status == progress::SyncProgressStatus::Running
            && progress.last_primary_key.is_none()
            && progress.chunks == 0
            && progress.inserts == 0
            && progress.updates == 0
    }));
}

#[test]
fn recoverable_constraint_preserves_running_progress_without_advancing() {
    struct DeadlockingRepairTarget;

    impl SyncRepairTarget for DeadlockingRepairTarget {
        fn insert_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Err(TableSyncError::Repair(
                "MySqlError { ERROR 1213 (40001): deadlock }".to_string(),
            ))
        }

        fn insert_rows(&mut self, _rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
            Err(TableSyncError::Repair(
                "MySqlError { ERROR 1213 (40001): deadlock }".to_string(),
            ))
        }

        fn update_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn delete_row(&mut self, _primary_key: &[String]) -> Result<(), TableSyncError> {
            Ok(())
        }
    }

    let source = FakeReader::new(vec![row("1", "source")]);
    let target = FakeReader::new(Vec::new());
    let mut repair_target = DeadlockingRepairTarget;
    let mut progress_store = RecordingProgressStore::default();

    sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "recoverable-constraint-running".to_string(),
            run_scope: "recoverable-constraint-running-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("constraint failure is returned for outer retry");

    assert!(progress_store.errors.borrow().is_empty());
    assert!(progress_store.saved.borrow().iter().all(|progress| {
        progress.status == progress::SyncProgressStatus::Running
            && progress.last_primary_key.is_none()
            && progress.chunks == 0
            && progress.inserts == 0
            && progress.updates == 0
    }));
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
        updated_since: None,
        plan_hash: None,
    };

    let target = target_connection_config(&config);

    assert_eq!(target.host, "target");
    assert_eq!(target.port, 25060);
    assert_eq!(target.database, "globalcomix");
}

#[test]
fn apply_uses_strict_inserts_so_constraint_failures_are_observable() {
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
        mode: SyncMode::Apply,
        progress_table: "cdc.table_sync_runs".to_string(),
        run_id: "strict-insert".to_string(),
        start_after: None,
        end_at: None,
        updated_since: None,
        plan_hash: None,
    };

    assert_eq!(
        sync_insert_mode(&config),
        crate::target::SnapshotInsertMode::Insert
    );
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

struct ConcurrentDuplicateRepairTarget {
    target_rows: RefCell<Vec<SnapshotRow>>,
    divergent: bool,
}

impl ConcurrentDuplicateRepairTarget {
    fn exact() -> Self {
        Self {
            target_rows: RefCell::new(Vec::new()),
            divergent: false,
        }
    }

    fn divergent() -> Self {
        Self {
            target_rows: RefCell::new(Vec::new()),
            divergent: true,
        }
    }
}

impl SyncRepairTarget for ConcurrentDuplicateRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        self.insert_rows(&[row])
    }

    fn insert_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        let concurrent_rows = rows
            .iter()
            .map(|row| {
                if self.divergent {
                    super::tests_support::row(&row.primary_key[0], "divergent-owner")
                } else {
                    (*row).clone()
                }
            })
            .collect();
        self.target_rows.replace(concurrent_rows);
        Err(TableSyncError::Duplicate(
            "mysql error 1062 from concurrent insert".to_string(),
        ))
    }

    fn update_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn verify_rows(&self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        let target_rows = self.target_rows.borrow();
        let exact = rows.len() == target_rows.len()
            && rows
                .iter()
                .zip(target_rows.iter())
                .all(|(source, target)| **source == *target);
        if exact {
            Ok(())
        } else {
            Err(TableSyncError::Repair(
                "concurrent duplicate owner diverges from source".to_string(),
            ))
        }
    }

    fn delete_row(&mut self, _primary_key: &[String]) -> Result<(), TableSyncError> {
        Ok(())
    }
}

#[test]
fn fk_parent_repair_then_exact_child_duplicate_advances_after_verification() {
    struct FkThenDuplicateTarget {
        target_rows: RefCell<Vec<SnapshotRow>>,
        divergent: bool,
        operations: RefCell<Vec<&'static str>>,
    }

    impl SyncRepairTarget for FkThenDuplicateTarget {
        fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
            self.insert_rows(&[row])
        }

        fn insert_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
            self.operations
                .borrow_mut()
                .extend(["child-1452", "parent-repaired", "child-1062"]);
            self.target_rows.replace(
                rows.iter()
                    .map(|row| {
                        if self.divergent {
                            super::tests_support::row(&row.primary_key[0], "divergent-owner")
                        } else {
                            (*row).clone()
                        }
                    })
                    .collect(),
            );
            Err(TableSyncError::Duplicate("mysql error 1062".to_string()))
        }

        fn update_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn verify_rows(&self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
            let target_rows = self.target_rows.borrow();
            if rows.len() == target_rows.len()
                && rows
                    .iter()
                    .zip(target_rows.iter())
                    .all(|(source, target)| **source == *target)
            {
                Ok(())
            } else {
                Err(TableSyncError::Repair(
                    "concurrent duplicate owner diverges from source".to_string(),
                ))
            }
        }

        fn delete_row(&mut self, _primary_key: &[String]) -> Result<(), TableSyncError> {
            Ok(())
        }
    }

    for divergent in [false, true] {
        let source = FakeReader::new(vec![row("1", "source")]);
        let target = FakeReader::new(Vec::new());
        let mut repair_target = FkThenDuplicateTarget {
            target_rows: RefCell::new(Vec::new()),
            divergent,
            operations: RefCell::new(Vec::new()),
        };
        let mut progress_store = RecordingProgressStore::default();
        let result = sync_table_with_progress_range(
            &account_table(),
            SyncRunOptions {
                run_id: format!("fk-then-duplicate-{divergent}"),
                run_scope: "fk-then-duplicate-scope".to_string(),
                chunk_size: 10,
                mode: SyncMode::Apply,
                start_after: None,
                end_at: None,
            },
            &source,
            &target,
            &mut repair_target,
            &mut progress_store,
        );

        assert_eq!(
            repair_target.operations.borrow().as_slice(),
            &["child-1452", "parent-repaired", "child-1062"]
        );
        if divergent {
            assert!(result.unwrap_err().to_string().contains("diverges"));
            assert!(
                progress_store.saved.borrow().iter().all(|progress| {
                    progress.last_primary_key.is_none() && progress.inserts == 0
                })
            );
        } else {
            assert_eq!(result.unwrap().inserts, 1);
            assert!(progress_store.saved.borrow().iter().any(|progress| {
                progress.last_primary_key == Some(vec!["1".to_string()]) && progress.inserts == 1
            }));
        }
    }
}

#[test]
fn concurrent_exact_child_duplicate_advances_only_after_verification() {
    let source = FakeReader::new(vec![row("1", "source")]);
    let target = FakeReader::new(Vec::new());
    let mut repair_target = ConcurrentDuplicateRepairTarget::exact();
    let mut progress_store = RecordingProgressStore::default();

    let report = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "concurrent-exact-child".to_string(),
            run_scope: "concurrent-exact-child-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("identical concurrent child is benign");

    assert_eq!(report.inserts, 1);
    assert!(progress_store.saved.borrow().iter().any(|progress| {
        progress.last_primary_key == Some(vec!["1".to_string()]) && progress.inserts == 1
    }));
}

#[test]
fn concurrent_divergent_child_duplicate_rejects_progress() {
    let source = FakeReader::new(vec![row("1", "source")]);
    let target = FakeReader::new(Vec::new());
    let mut repair_target = ConcurrentDuplicateRepairTarget::divergent();
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "concurrent-divergent-child".to_string(),
            run_scope: "concurrent-divergent-child-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("divergent concurrent owner must fail");

    assert!(error.to_string().contains("diverges from source"));
    assert!(progress_store.saved.borrow().iter().all(|progress| {
        progress.last_primary_key.is_none() && progress.inserts == 0 && progress.chunks == 0
    }));
}

#[test]
fn apply_batches_missing_rows_before_checkpointing_the_chunk() {
    let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
    let target = FakeReader::new(Vec::new());
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let report = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "batched-inserts".to_string(),
            run_scope: "batched-inserts-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("sync report");

    assert_eq!(report.inserts, 2);
    assert_eq!(
        repair_target.operations.borrow().as_slice(),
        &["insert-batch:1,2"]
    );
    let saved = progress_store.saved.borrow();
    assert_eq!(
        saved.last().expect("saved progress").last_primary_key,
        Some(vec!["2".to_string()])
    );
}

#[test]
fn missing_fk_parent_is_repaired_before_child_retry_and_progress_advance() {
    use super::fk_parent_repair::{
        ForeignKeyColumn, ForeignKeyEdge, ParentIdentity, ParentRepairRow, ParentRepairStore,
        repair_fk_parents_and_retry,
    };

    struct SharedReader(Rc<RefCell<Vec<crate::snapshot::SnapshotRow>>>);
    impl SyncTableReader for SharedReader {
        fn read_rows(
            &self,
            request: &SyncChunkRequest,
        ) -> Result<Vec<crate::snapshot::SnapshotRow>, TableSyncError> {
            Ok(self
                .0
                .borrow()
                .iter()
                .filter(|row| {
                    let after_start = request
                        .start_after
                        .as_ref()
                        .is_none_or(|start| row.primary_key > *start);
                    let before_end = request
                        .end_at
                        .as_ref()
                        .is_none_or(|end| row.primary_key <= *end);
                    after_start && before_end
                })
                .take(request.limit)
                .cloned()
                .collect())
        }
    }

    struct ParentStore<'a> {
        source_parent: ParentRepairRow,
        target_parents: &'a mut BTreeMap<ParentIdentity, ParentRepairRow>,
        operations: &'a mut Vec<String>,
    }
    impl ParentRepairStore for ParentStore<'_> {
        fn read_source_parent(
            &mut self,
            _identity: &ParentIdentity,
        ) -> Result<Option<ParentRepairRow>, String> {
            Ok(Some(self.source_parent.clone()))
        }

        fn read_target_parent(
            &mut self,
            identity: &ParentIdentity,
        ) -> Result<Option<ParentRepairRow>, String> {
            Ok(self.target_parents.get(identity).cloned())
        }

        fn repair_parent(&mut self, row: &ParentRepairRow) -> Result<(), String> {
            self.operations
                .push("insert-parent:utms:184041".to_string());
            self.target_parents.insert(
                ParentIdentity {
                    table: "utms".to_string(),
                    values: vec![("id".to_string(), "184041".to_string())],
                },
                row.clone(),
            );
            Ok(())
        }
    }

    struct RepairTarget {
        target_children: Rc<RefCell<Vec<crate::snapshot::SnapshotRow>>>,
        target_parents: BTreeMap<ParentIdentity, ParentRepairRow>,
        operations: Vec<String>,
    }
    impl SyncRepairTarget for RepairTarget {
        fn insert_row(&mut self, row: &crate::snapshot::SnapshotRow) -> Result<(), TableSyncError> {
            self.insert_rows(&[row])
        }

        fn insert_rows(
            &mut self,
            rows: &[&crate::snapshot::SnapshotRow],
        ) -> Result<(), TableSyncError> {
            self.operations
                .push("insert-child-batch:guests".to_string());
            let child_rows = rows.iter().map(|row| (*row).clone()).collect::<Vec<_>>();
            let repair_rows = child_rows
                .iter()
                .map(|row| ParentRepairRow {
                    table: "guests".to_string(),
                    values: row.values.clone(),
                })
                .collect::<Vec<_>>();
            let source_parent = ParentRepairRow {
                table: "utms".to_string(),
                values: BTreeMap::from([
                    ("id".to_string(), Some("184041".to_string())),
                    ("utm_hash".to_string(), Some("hash".to_string())),
                ]),
            };
            let mut store = ParentStore {
                source_parent,
                target_parents: &mut self.target_parents,
                operations: &mut self.operations,
            };
            repair_fk_parents_and_retry(
                "guests",
                &repair_rows,
                &[ForeignKeyEdge {
                    child_table: "guests".to_string(),
                    parent_table: "utms".to_string(),
                    columns: vec![ForeignKeyColumn {
                        child: "utm_id".to_string(),
                        parent: "id".to_string(),
                    }],
                }],
                &mut store,
            )
            .map_err(|error| TableSyncError::Repair(error.to_string()))?;
            self.operations.push("retry-child-batch:guests".to_string());
            self.target_children.borrow_mut().extend(child_rows);
            Ok(())
        }

        fn update_row(
            &mut self,
            _row: &crate::snapshot::SnapshotRow,
        ) -> Result<(), TableSyncError> {
            unreachable!()
        }

        fn delete_row(&mut self, _primary_key: &[String]) -> Result<(), TableSyncError> {
            unreachable!()
        }
    }

    let table = SyncTable {
        name: "guests".to_string(),
        primary_key: vec!["guest_id".to_string()],
        columns: vec![
            "guest_id".to_string(),
            "guest_hash".to_string(),
            "utm_id".to_string(),
        ],
    };
    let child = crate::snapshot::SnapshotRow {
        primary_key: vec!["87308589".to_string()],
        values: BTreeMap::from([
            ("guest_id".to_string(), Some("87308589".to_string())),
            ("guest_hash".to_string(), Some("guest-hash".to_string())),
            ("utm_id".to_string(), Some("184041".to_string())),
        ]),
    };
    let source = FakeReader::new(vec![child.clone()]);
    let target_rows = Rc::new(RefCell::new(Vec::new()));
    let target = SharedReader(target_rows.clone());
    let mut repair_target = RepairTarget {
        target_children: target_rows.clone(),
        target_parents: BTreeMap::new(),
        operations: Vec::new(),
    };
    let mut progress_store = RecordingProgressStore::default();

    let report = sync_table_with_progress_range(
        &table,
        SyncRunOptions {
            run_id: "guest-fk-parent".to_string(),
            run_scope: "guest-fk-parent-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("parent-first repair");

    assert_eq!(report.inserts, 1);
    assert_eq!(target_rows.borrow().as_slice(), &[child]);
    assert_eq!(
        repair_target.operations,
        [
            "insert-child-batch:guests",
            "insert-parent:utms:184041",
            "retry-child-batch:guests",
        ]
    );
    let progressed_keys = progress_store
        .saved
        .borrow()
        .iter()
        .filter_map(|progress| progress.last_primary_key.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        progressed_keys,
        BTreeSet::from([vec!["87308589".to_string()]])
    );
}

#[test]
fn failed_post_update_verification_does_not_advance_counters_or_cursor() {
    struct UpdateSucceedsWithoutConvergence;

    impl SyncRepairTarget for UpdateSucceedsWithoutConvergence {
        fn insert_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn update_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn update_rows(&mut self, _rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn verify_rows(&self, _rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
            Err(TableSyncError::Repair(
                "post-update verification failed for `accounts` identity [(\"id\", \"1\")]"
                    .to_string(),
            ))
        }

        fn delete_row(&mut self, _primary_key: &[String]) -> Result<(), TableSyncError> {
            Ok(())
        }
    }

    let source = FakeReader::new(vec![row("1", "source")]);
    let target = FakeReader::new(vec![row("1", "target")]);
    let mut repair_target = UpdateSucceedsWithoutConvergence;
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "failed-update-verification".to_string(),
            run_scope: "failed-update-verification-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("divergent post-update row must fail");

    assert!(
        error
            .to_string()
            .contains("post-update verification failed")
    );
    assert!(progress_store.saved.borrow().iter().all(|progress| {
        progress.last_primary_key.is_none() && progress.updates == 0 && progress.chunks == 0
    }));
}

#[test]
fn apply_batches_divergent_rows_before_checkpointing_the_chunk() {
    let source = FakeReader::new(vec![row("1", "alpha"), row("2", "bravo")]);
    let target = FakeReader::new(vec![row("1", "old-alpha"), row("2", "old-bravo")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    let report = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "batched-updates".to_string(),
            run_scope: "batched-updates-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("sync report");

    assert_eq!(report.updates, 2);
    assert_eq!(
        repair_target.operations.borrow().as_slice(),
        &["update-batch:1,2"]
    );
    let saved = progress_store.saved.borrow();
    assert_eq!(
        saved.last().expect("saved progress").last_primary_key,
        Some(vec!["2".to_string()])
    );
}

#[test]
fn later_update_statement_repair_does_not_replay_committed_subbatch() {
    struct StatementSizedUpdateTarget {
        operations: RefCell<Vec<String>>,
    }

    impl SyncRepairTarget for StatementSizedUpdateTarget {
        fn insert_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn update_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn update_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
            if rows.len() > 128 {
                return Err(TableSyncError::Repair(
                    "table sync replayed more than one writer statement".to_string(),
                ));
            }
            let first = rows.first().expect("update rows").primary_key[0].clone();
            let last = rows.last().expect("update rows").primary_key[0].clone();
            self.operations
                .borrow_mut()
                .push(format!("update:{first}-{last}"));
            if first == "128" {
                self.operations.borrow_mut().extend([
                    "fk-1452:128-129".to_string(),
                    "repair-parent:128-129".to_string(),
                    "retry-update:128-129".to_string(),
                ]);
            }
            Ok(())
        }

        fn update_batch_size(&self) -> usize {
            crate::target::update_statement_capacity(1, 256)
        }

        fn delete_row(&mut self, _primary_key: &[String]) -> Result<(), TableSyncError> {
            Ok(())
        }
    }

    let source_rows = (1..=129)
        .map(|id| row(&format!("{id:03}"), "source"))
        .collect::<Vec<_>>();
    let target_rows = (1..=129)
        .map(|id| row(&format!("{id:03}"), "target"))
        .collect::<Vec<_>>();
    let source = FakeReader::new(source_rows);
    let target = FakeReader::new(target_rows);
    let mut repair_target = StatementSizedUpdateTarget {
        operations: RefCell::new(Vec::new()),
    };
    let mut progress_store = RecordingProgressStore::default();
    let wide_table = SyncTable {
        name: "wide_accounts".to_string(),
        primary_key: vec!["id".to_string()],
        columns: std::iter::once("id".to_string())
            .chain((1..=256).map(|index| format!("value_{index}")))
            .collect(),
    };

    let report = sync_table_with_progress_range(
        &wide_table,
        SyncRunOptions {
            run_id: "statement-sized-update-retry".to_string(),
            run_scope: "statement-sized-update-retry-scope".to_string(),
            chunk_size: 129,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("later statement repairs without replaying first statement");

    assert_eq!(report.updates, 129);
    assert_eq!(
        repair_target.operations.borrow().as_slice(),
        &[
            "update:001-127",
            "update:128-129",
            "fk-1452:128-129",
            "repair-parent:128-129",
            "retry-update:128-129",
        ]
    );
    assert_eq!(
        progress_store
            .saved
            .borrow()
            .last()
            .expect("progress")
            .last_primary_key,
        Some(vec!["129".to_string()])
    );
}

#[test]
fn apply_batches_divergent_rows_in_source_primary_key_order() {
    let source = FakeReader::new(vec![row("99", "ninety-nine"), row("100", "one-hundred")]);
    let target = FakeReader::new(vec![row("99", "old-99"), row("100", "old-100")]);
    let mut repair_target = RecordingRepairTarget::default();

    let mut report = SyncTableReport::default();
    repair_chunk(
        &source.rows,
        &target.rows,
        SyncMode::Apply,
        &mut repair_target,
        &mut report,
        SyncPhase::All,
    )
    .expect("repair chunk");

    assert_eq!(
        repair_target.operations.borrow().as_slice(),
        &["update-batch:99,100"]
    );
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
        delete_preflight_complete: false,
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
fn delete_verification_failure_does_not_persist_chunk_progress() {
    struct FailedDeleteVerificationTarget;

    impl SyncRepairTarget for FailedDeleteVerificationTarget {
        fn insert_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }
        fn update_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }
        fn delete_row(&mut self, _primary_key: &[String]) -> Result<(), TableSyncError> {
            Ok(())
        }
        fn verify_deleted_rows(&self, _primary_keys: &[Vec<String>]) -> Result<(), TableSyncError> {
            Err(TableSyncError::Repair(
                "post-delete verification failed".to_string(),
            ))
        }
    }

    let source = FakeReader::new(vec![row("10", "ten")]);
    let target = FakeReader::new(vec![row("05", "extra"), row("10", "ten")]);
    let mut repair_target = FailedDeleteVerificationTarget;
    let mut progress_store = RecordingProgressStore::default();

    let error = sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "delete-verification".to_string(),
            run_scope: "delete-verification-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("delete verification failure");

    assert_eq!(
        error.to_string(),
        "sync repair failed: post-delete verification failed"
    );
    assert!(
        progress_store
            .saved
            .borrow()
            .iter()
            .all(|progress| progress.last_primary_key.is_none())
    );
}

#[test]
fn target_tail_delete_counters_are_persisted_before_completion() {
    let source = FakeReader::new(vec![row("10", "ten")]);
    let target = FakeReader::new(vec![row("10", "ten"), row("20", "extra")]);
    let mut repair_target = RecordingRepairTarget::default();
    let mut progress_store = RecordingProgressStore::default();

    sync_table_with_progress_range(
        &account_table(),
        SyncRunOptions {
            run_id: "tail-delete-progress".to_string(),
            run_scope: "tail-delete-progress-scope".to_string(),
            chunk_size: 10,
            mode: SyncMode::Apply,
            start_after: None,
            end_at: None,
        },
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("tail delete sync");

    let completed = progress_store
        .saved
        .borrow()
        .last()
        .cloned()
        .expect("completed progress");
    assert_eq!(completed.extra_target_rows, 1);
    assert_eq!(completed.last_primary_key, Some(vec!["10".to_string()]));
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
            "update-batch:1",
            "insert-batch:2",
            "delete:4",
            "delete:5",
            "insert-batch:3,6",
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
        &["update-batch:20".to_string(), "insert-batch:10".to_string(),]
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
fn apply_completes_only_after_a_subsequent_zero_drift_pass() {
    struct VerificationTargetReader {
        converged: Rc<Cell<bool>>,
    }

    impl SyncTableReader for VerificationTargetReader {
        fn read_rows(
            &self,
            request: &SyncChunkRequest,
        ) -> Result<Vec<SnapshotRow>, TableSyncError> {
            if self.converged.get() {
                FakeReader::new(vec![row("1", "source")]).read_rows(request)
            } else {
                Ok(Vec::new())
            }
        }
    }

    struct TerminalVerificationRepairTarget;

    impl SyncRepairTarget for TerminalVerificationRepairTarget {
        fn insert_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn update_row(&mut self, _row: &SnapshotRow) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn delete_row(&mut self, _primary_key: &[String]) -> Result<(), TableSyncError> {
            Ok(())
        }

        fn requires_terminal_verification(&self) -> bool {
            true
        }
    }

    let converged = Rc::new(Cell::new(false));
    let source = FakeReader::new(vec![row("1", "source")]);
    let target = VerificationTargetReader {
        converged: Rc::clone(&converged),
    };
    let mut repair_target = TerminalVerificationRepairTarget;
    let mut progress_store = RecordingProgressStore::default();
    let options = || SyncRunOptions {
        run_id: "terminal-zero-drift".to_string(),
        run_scope: "terminal-zero-drift-scope".to_string(),
        chunk_size: 10,
        mode: SyncMode::Apply,
        start_after: None,
        end_at: None,
    };

    let first_error = sync_table_with_progress_range(
        &account_table(),
        options(),
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect_err("missing row after apply must block completion");

    assert!(first_error.to_string().contains("missing_rows=1"));
    assert!(progress_store.errors.borrow().is_empty());
    let pending = progress_store
        .saved
        .borrow()
        .last()
        .cloned()
        .expect("progress");
    assert_eq!(pending.status, progress::SyncProgressStatus::Running);
    assert_eq!(pending.last_primary_key, Some(vec!["1".to_string()]));
    assert_eq!(pending.inserts, 1);

    converged.set(true);
    progress_store.loaded = Some(pending);
    let report = sync_table_with_progress_range(
        &account_table(),
        options(),
        &source,
        &target,
        &mut repair_target,
        &mut progress_store,
    )
    .expect("subsequent zero-drift verification completes run");

    assert_eq!(report.inserts, 1);
    assert_eq!(
        progress_store
            .saved
            .borrow()
            .last()
            .expect("completed")
            .status,
        progress::SyncProgressStatus::Complete
    );
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
        &["insert-batch:1".to_string()]
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
