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
    conflict: bool,
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
        if self.conflict {
            return Err(SyncMutationFailure {
                mysql_code: Some(1062),
                message: "duplicate fixture".into(),
                failed_batch: rows.to_vec(),
                remaining_rows: vec![],
            });
        }
        self.rows.extend_from_slice(rows);
        Ok(())
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
fn phased_insert_conflict_rolls_back_without_owner_repair() {
    let mut source = Fixture {
        rows: vec![row("04", "new")],
        ..Default::default()
    };
    let mut target = Fixture {
        conflict: true,
        ..Default::default()
    };
    let mut store = Fixture::default();
    let error = sync_next_chunk_with_phase(
        &config(),
        &mut source,
        &mut target,
        &mut store,
        SyncMutationPhase::InsertMissing,
    )
    .unwrap_err();
    assert!(error.contains("duplicate fixture"), "{error}");
    assert!(target.durable.is_empty());
    assert!(store.progress.is_none());
    assert!(target.events.borrow().ends_with(&["rollback", "unlock"]));
}
