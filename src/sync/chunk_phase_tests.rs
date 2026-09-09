use super::*;
use crate::sync::model::{SyncChunkPage, SyncPrimaryKeyOrdering};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Default)]
struct Fixture {
    rows: Vec<DatabaseRow>,
    durable: Vec<DatabaseRow>,
    progress: Option<SyncChunkProgress>,
    events: Rc<RefCell<Vec<&'static str>>>,
    unique_values: bool,
    fail_verify: bool,
    verified_rows: Vec<DatabaseRow>,
}

impl Fixture {
    fn unique_owner(&self, intended: &DatabaseRow) -> Option<&DatabaseRow> {
        self.rows.iter().find(|owner| {
            owner.primary_key != intended.primary_key
                && owner.values["value"] == intended.values["value"]
        })
    }

    fn check_unique_values(&self, rows: &[DatabaseRow]) -> Result<(), SyncMutationFailure> {
        if self.unique_values && rows.iter().any(|row| self.unique_owner(row).is_some()) {
            return Err(SyncMutationFailure {
                mysql_code: Some(1062),
                message: "duplicate fixture".into(),
                failed_batch: rows.to_vec(),
                remaining_rows: vec![],
            });
        }
        Ok(())
    }
}

fn row(id: &str, value: &str) -> DatabaseRow {
    DatabaseRow {
        primary_key: vec![id.into()],
        values: BTreeMap::from([
            ("id".into(), Some(id.into())),
            ("value".into(), Some(value.into())),
        ]),
    }
}

fn page(rows: &[DatabaseRow], request: &SyncChunkReadRequest) -> SyncChunkPage {
    let mut rows: Vec<_> = rows
        .iter()
        .filter(|row| {
            request
                .start_after
                .as_ref()
                .is_none_or(|key| row.primary_key > *key)
                && request
                    .end_at
                    .as_ref()
                    .is_none_or(|key| row.primary_key <= *key)
        })
        .cloned()
        .collect();
    rows.sort_by(|a, b| a.primary_key.cmp(&b.primary_key));
    let has_more = rows.len() > request.limit;
    rows.truncate(request.limit);
    SyncChunkPage { rows, has_more }
}

impl SyncChunkSource for Fixture {
    fn read_row_by_primary_key(&mut self, key: &[String]) -> Result<Option<DatabaseRow>, String> {
        Ok(self.rows.iter().find(|row| row.primary_key == key).cloned())
    }
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<SyncChunkPage, String> {
        self.events.borrow_mut().push("source-read");
        Ok(page(&self.rows, request))
    }
}
impl SyncChunkTargetSession for Fixture {
    fn set_autocommit(&mut self, _: bool) -> Result<(), String> {
        self.events.borrow_mut().push("autocommit");
        Ok(())
    }
    fn lock_table_write(&mut self, _: &str, _: &str) -> Result<(), String> {
        self.events.borrow_mut().push("lock");
        Ok(())
    }
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<SyncChunkPage, String> {
        self.events.borrow_mut().push("target-read");
        Ok(page(&self.rows, request))
    }
    fn delete_rows(&mut self, keys: &[Vec<String>]) -> Result<(), String> {
        self.events.borrow_mut().push("delete");
        self.rows.retain(|r| !keys.contains(&r.primary_key));
        Ok(())
    }
    fn update_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), SyncMutationFailure> {
        self.events.borrow_mut().push("update");
        self.check_unique_values(rows)?;
        for row in rows {
            *self
                .rows
                .iter_mut()
                .find(|r| r.primary_key == row.primary_key)
                .unwrap() = row.clone();
        }
        Ok(())
    }
    fn insert_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), SyncMutationFailure> {
        self.events.borrow_mut().push("insert");
        self.check_unique_values(rows)?;
        self.rows.extend_from_slice(rows);
        Ok(())
    }
    fn inspect_unique_owner_conflicts(
        &mut self,
        failure: &SyncMutationFailure,
    ) -> Result<Vec<SyncUniqueOwnerConflict>, String> {
        Ok(failure
            .failed_batch
            .iter()
            .filter_map(|intended| {
                self.unique_owner(intended)
                    .map(|owner| SyncUniqueOwnerConflict {
                        index: crate::sync::model::SyncUniqueIndex {
                            name: "value".into(),
                            columns: vec!["value".into()],
                        },
                        intended: intended.clone(),
                        owner: owner.clone(),
                    })
            })
            .collect())
    }
    fn reconcile_unique_owner(
        &mut self,
        conflict: &SyncUniqueOwnerConflict,
        action: &SyncUniqueOwnerAction,
    ) -> Result<(), String> {
        match action {
            SyncUniqueOwnerAction::Update(row) => self
                .update_rows(std::slice::from_ref(row))
                .map_err(|e| e.to_string()),
            SyncUniqueOwnerAction::Delete => {
                self.delete_rows(std::slice::from_ref(&conflict.owner.primary_key))
            }
        }
    }
    fn verify_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), String> {
        self.verified_rows.extend_from_slice(rows);
        if self.fail_verify {
            return Err("injected post-repair verification failure".into());
        }
        if rows.iter().all(|row| self.rows.contains(row)) {
            Ok(())
        } else {
            Err("repaired row differs".into())
        }
    }
    fn commit(&mut self) -> Result<(), String> {
        self.events.borrow_mut().push("commit");
        self.durable = self.rows.clone();
        Ok(())
    }
    fn rollback(&mut self) -> Result<(), String> {
        self.events.borrow_mut().push("rollback");
        self.rows = self.durable.clone();
        Ok(())
    }
    fn unlock_tables(&mut self) -> Result<(), String> {
        self.events.borrow_mut().push("unlock");
        Ok(())
    }
}
impl SyncChunkProgressStore for Fixture {
    fn load(&mut self, _: &str, _: &str) -> Result<Option<SyncChunkProgress>, String> {
        self.events.borrow_mut().push("load");
        Ok(self.progress.clone())
    }
    fn save(&mut self, progress: &SyncChunkProgress) -> Result<(), String> {
        self.events.borrow_mut().push("save");
        self.progress = Some(progress.clone());
        Ok(())
    }
}
fn config() -> SyncChunkConfig {
    SyncChunkConfig {
        run_id: "phase-test".into(),
        target_database: "target".into(),
        chunk_size: 2,
        table: SyncTable {
            name: "items".into(),
            primary_key: vec!["id".into()],
            primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
            columns: vec!["id".into(), "value".into()],
            bit_columns: vec![],
            enum_columns: BTreeMap::new(),
            mediumblob_columns: vec![],
        },
    }
}

#[test]
fn selected_phase_preserves_windows_and_advances_retained_tail() {
    for (phase, expected, counts) in [
        (
            SyncMutationPhase::All,
            vec![row("04", "new"), row("05", "missing")],
            (1, 1, 6),
        ),
        (
            SyncMutationPhase::InsertMissing,
            vec![
                row("01", "extra"),
                row("02", "extra"),
                row("03", "extra"),
                row("04", "old"),
                row("05", "missing"),
                row("06", "tail"),
                row("07", "tail"),
                row("08", "tail"),
            ],
            (1, 0, 0),
        ),
        (
            SyncMutationPhase::UpdateDivergent,
            vec![
                row("01", "extra"),
                row("02", "extra"),
                row("03", "extra"),
                row("04", "new"),
                row("06", "tail"),
                row("07", "tail"),
                row("08", "tail"),
            ],
            (0, 1, 0),
        ),
        (
            SyncMutationPhase::DeleteExtras,
            vec![row("04", "old")],
            (0, 0, 6),
        ),
    ] {
        let events = Rc::new(RefCell::new(vec![]));
        let mut source = Fixture {
            rows: vec![row("04", "new"), row("05", "missing")],
            events: events.clone(),
            ..Default::default()
        };
        let initial = vec![
            row("01", "extra"),
            row("02", "extra"),
            row("03", "extra"),
            row("04", "old"),
            row("06", "tail"),
            row("07", "tail"),
            row("08", "tail"),
        ];
        let mut target = Fixture {
            rows: initial.clone(),
            durable: initial,
            events: events.clone(),
            ..Default::default()
        };
        let mut store = Fixture {
            events: events.clone(),
            ..Default::default()
        };
        for _ in 0..4 {
            let progress =
                sync_next_chunk_with_phase(&config(), &mut source, &mut target, &mut store, phase)
                    .unwrap();
            if progress.complete {
                break;
            }
        }
        let progress = store.progress.unwrap();
        assert!(progress.complete, "{phase:?} must finish retained tail");
        assert_eq!(
            (progress.inserts, progress.updates, progress.deletes),
            counts,
            "{phase:?}"
        );
        assert_eq!(progress.rows_scanned, 2);
        target
            .durable
            .sort_by(|a, b| a.primary_key.cmp(&b.primary_key));
        assert_eq!(target.durable, expected, "{phase:?}");
        let events = events.borrow();
        let reads: Vec<_> = events
            .iter()
            .copied()
            .filter(|event| !matches!(*event, "insert" | "update" | "delete"))
            .collect();
        assert_eq!(
            &reads[..6],
            &[
                "load",
                "autocommit",
                "lock",
                "source-read",
                "target-read",
                "target-read"
            ],
            "{phase:?}"
        );
        for chunk in events.split_inclusive(|event| *event == "unlock") {
            assert_eq!(&chunk[chunk.len() - 3..], &["commit", "save", "unlock"]);
        }
    }
}

#[test]
fn explicit_phases_repair_stale_unique_owner_and_roll_back_failed_verification() {
    for phase in [
        SyncMutationPhase::InsertMissing,
        SyncMutationPhase::UpdateDivergent,
    ] {
        for fail_verify in [false, true] {
            // Insert reproduces the harness ordering; update reaches the conflict before its owner.
            let (owner_id, intended_id) = if phase == SyncMutationPhase::InsertMissing {
                ("01", "02")
            } else {
                ("02", "01")
            };
            let intended = row(intended_id, "live@example.test");
            let corrected_owner = row(owner_id, "deleted@example.test");
            let mut source = Fixture {
                rows: vec![intended.clone(), corrected_owner.clone()],
                ..Default::default()
            };
            let mut initial = vec![row(owner_id, "live@example.test")];
            if phase == SyncMutationPhase::UpdateDivergent {
                initial.insert(0, row("01", "old@example.test"));
            }
            let mut target = Fixture {
                rows: initial.clone(),
                durable: initial.clone(),
                unique_values: true,
                fail_verify,
                ..Default::default()
            };
            let mut store = Fixture::default();
            let result =
                sync_next_chunk_with_phase(&config(), &mut source, &mut target, &mut store, phase);
            assert_eq!(target.verified_rows, vec![intended.clone()]);
            if fail_verify {
                let error = result.unwrap_err();
                assert!(
                    error.contains("injected post-repair verification failure"),
                    "{error}"
                );
                assert_eq!(target.rows, initial);
                assert_eq!(target.durable, initial);
                assert!(store.progress.is_none());
                assert!(target.events.borrow().ends_with(&["rollback", "unlock"]));
            } else {
                let progress = result.unwrap();
                target
                    .durable
                    .sort_by(|a, b| a.primary_key.cmp(&b.primary_key));
                let mut expected = vec![intended, corrected_owner];
                expected.sort_by(|a, b| a.primary_key.cmp(&b.primary_key));
                assert_eq!(target.durable, expected);
                let counts = if phase == SyncMutationPhase::InsertMissing {
                    (1, 0, 0)
                } else {
                    (0, 2, 0)
                };
                assert_eq!(
                    (progress.inserts, progress.updates, progress.deletes),
                    counts
                );
                assert_eq!(store.progress, Some(progress));
            }
        }
    }
}
