use crate::snapshot::SnapshotRow;
use crate::sync::{
    SyncChunkConfig, SyncChunkProgress, SyncChunkProgressStore, SyncChunkReadRequest,
    SyncChunkSource, SyncChunkTargetSession, SyncTable, sync_next_chunk,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    ProgressLoad {
        run_id: String,
        table: String,
    },
    SetAutocommit(bool),
    LockWrite {
        database: String,
        table: String,
    },
    SourceRead {
        start_after: Option<Vec<String>>,
        end_at: Option<Vec<String>>,
        limit: usize,
    },
    TargetRead {
        start_after: Option<Vec<String>>,
        end_at: Option<Vec<String>>,
        limit: usize,
    },
    Delete(Vec<Vec<String>>),
    Update(Vec<Vec<String>>),
    Insert(Vec<Vec<String>>),
    Commit,
    ProgressSave {
        last_primary_key: Option<Vec<String>>,
        complete: bool,
        chunks: u64,
        rows_scanned: u64,
        inserts: u64,
        updates: u64,
        deletes: u64,
    },
    Rollback,
    Unlock,
}

type Events = Rc<RefCell<Vec<Event>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailurePoint {
    SetAutocommit,
    LockTable,
    SourceRead,
    TargetRead,
    Delete,
    Update,
    Insert,
    Commit,
    ProgressSave,
    Unlock,
}

struct RecordingSource {
    reads: VecDeque<Vec<SnapshotRow>>,
    failure: Option<FailurePoint>,
    events: Events,
}

impl RecordingSource {
    fn new(rows: Vec<SnapshotRow>, events: Events) -> Self {
        Self::scripted([rows], events)
    }

    fn scripted(
        reads: impl IntoIterator<Item = Vec<SnapshotRow>>,
        events: Events,
    ) -> Self {
        Self {
            reads: reads.into_iter().collect(),
            failure: None,
            events,
        }
    }

    fn fail_at(mut self, failure: FailurePoint) -> Self {
        self.failure = Some(failure);
        self
    }
}

impl SyncChunkSource for RecordingSource {
    fn read_rows(
        &mut self,
        request: &SyncChunkReadRequest,
    ) -> Result<Vec<SnapshotRow>, String> {
        self.events.borrow_mut().push(Event::SourceRead {
            start_after: request.start_after.clone(),
            end_at: request.end_at.clone(),
            limit: request.limit,
        });
        if self.failure == Some(FailurePoint::SourceRead) {
            return Err("injected source read failure".to_string());
        }
        Ok(self.reads.pop_front().unwrap_or_default())
    }
}

struct RecordingTargetSession {
    visible_rows: Vec<SnapshotRow>,
    pending_rows: Vec<SnapshotRow>,
    read_batches: VecDeque<Vec<SnapshotRow>>,
    failure: Option<FailurePoint>,
    events: Events,
}

impl RecordingTargetSession {
    fn new(rows: Vec<SnapshotRow>, events: Events) -> Self {
        Self {
            visible_rows: rows.clone(),
            pending_rows: rows,
            read_batches: VecDeque::new(),
            failure: None,
            events,
        }
    }

    fn with_read_batches(
        mut self,
        reads: impl IntoIterator<Item = Vec<SnapshotRow>>,
    ) -> Self {
        self.read_batches = reads.into_iter().collect();
        self
    }

    fn fail_at(mut self, failure: FailurePoint) -> Self {
        self.failure = Some(failure);
        self
    }
}

impl SyncChunkTargetSession for RecordingTargetSession {
    fn set_autocommit(&mut self, enabled: bool) -> Result<(), String> {
        self.events
            .borrow_mut()
            .push(Event::SetAutocommit(enabled));
        if self.failure == Some(FailurePoint::SetAutocommit) {
            return Err("injected autocommit setup failure".to_string());
        }
        self.pending_rows = self.visible_rows.clone();
        Ok(())
    }

    fn lock_table_write(&mut self, database: &str, table: &str) -> Result<(), String> {
        self.events.borrow_mut().push(Event::LockWrite {
            database: database.to_string(),
            table: table.to_string(),
        });
        if self.failure == Some(FailurePoint::LockTable) {
            return Err("injected table lock failure".to_string());
        }
        Ok(())
    }

    fn read_rows(
        &mut self,
        request: &SyncChunkReadRequest,
    ) -> Result<Vec<SnapshotRow>, String> {
        self.events.borrow_mut().push(Event::TargetRead {
            start_after: request.start_after.clone(),
            end_at: request.end_at.clone(),
            limit: request.limit,
        });
        if self.failure == Some(FailurePoint::TargetRead) {
            return Err("injected target read failure".to_string());
        }
        Ok(self
            .read_batches
            .pop_front()
            .unwrap_or_else(|| self.pending_rows.clone()))
    }

    fn delete_rows(&mut self, primary_keys: &[Vec<String>]) -> Result<(), String> {
        self.events
            .borrow_mut()
            .push(Event::Delete(primary_keys.to_vec()));
        if self.failure == Some(FailurePoint::Delete) {
            return Err("injected delete failure".to_string());
        }
        self.pending_rows
            .retain(|row| !primary_keys.contains(&row.primary_key));
        Ok(())
    }

    fn update_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), String> {
        self.events
            .borrow_mut()
            .push(Event::Update(primary_keys(rows)));
        if self.failure == Some(FailurePoint::Update) {
            return Err("injected update failure".to_string());
        }
        for row in rows {
            let target = self
                .pending_rows
                .iter_mut()
                .find(|target| target.primary_key == row.primary_key)
                .expect("divergent target row exists");
            *target = row.clone();
        }
        Ok(())
    }

    fn insert_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), String> {
        self.events
            .borrow_mut()
            .push(Event::Insert(primary_keys(rows)));
        if self.failure == Some(FailurePoint::Insert) {
            return Err("injected insert failure".to_string());
        }
        self.pending_rows.extend_from_slice(rows);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), String> {
        self.events.borrow_mut().push(Event::Commit);
        if self.failure == Some(FailurePoint::Commit) {
            return Err("injected commit failure".to_string());
        }
        self.visible_rows = self.pending_rows.clone();
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), String> {
        self.events.borrow_mut().push(Event::Rollback);
        self.pending_rows = self.visible_rows.clone();
        Ok(())
    }

    fn unlock_tables(&mut self) -> Result<(), String> {
        self.events.borrow_mut().push(Event::Unlock);
        if self.failure == Some(FailurePoint::Unlock) {
            return Err("injected unlock failure".to_string());
        }
        Ok(())
    }
}

struct RecordingProgressStore {
    durable: Option<SyncChunkProgress>,
    failure: Option<FailurePoint>,
    events: Events,
}

impl RecordingProgressStore {
    fn new(events: Events) -> Self {
        Self {
            durable: None,
            failure: None,
            events,
        }
    }
}

impl SyncChunkProgressStore for RecordingProgressStore {
    fn load(&mut self, run_id: &str, table: &str) -> Result<Option<SyncChunkProgress>, String> {
        self.events.borrow_mut().push(Event::ProgressLoad {
            run_id: run_id.to_string(),
            table: table.to_string(),
        });
        Ok(self.durable.clone())
    }

    fn save(&mut self, progress: &SyncChunkProgress) -> Result<(), String> {
        self.events.borrow_mut().push(Event::ProgressSave {
            last_primary_key: progress.last_primary_key.clone(),
            complete: progress.complete,
            chunks: progress.chunks,
            rows_scanned: progress.rows_scanned,
            inserts: progress.inserts,
            updates: progress.updates,
            deletes: progress.deletes,
        });
        if self.failure == Some(FailurePoint::ProgressSave) {
            return Err("injected progress save failure".to_string());
        }
        self.durable = Some(progress.clone());
        Ok(())
    }
}

#[test]
fn locks_before_reads_and_saves_progress_after_commit_before_unlock() {
    let events = events();
    let source_rows = source_rows();
    let mut source = RecordingSource::new(source_rows.clone(), Rc::clone(&events));
    let mut target = RecordingTargetSession::new(target_rows(), Rc::clone(&events));
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));

    let outcome = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    )
    .expect("synchronize one chunk");

    assert_eq!(outcome.last_primary_key, Some(keys(["4"])));
    assert!(!outcome.complete);
    assert_rows_equal(&target.visible_rows, &source_rows);
    assert_eq!(
        events.borrow().as_slice(),
        [
            Event::ProgressLoad {
                run_id: "sync-run-1".to_string(),
                table: "widgets".to_string(),
            },
            Event::SetAutocommit(false),
            Event::LockWrite {
                database: "target_db".to_string(),
                table: "widgets".to_string(),
            },
            Event::SourceRead {
                start_after: None,
                end_at: None,
                limit: 10,
            },
            Event::TargetRead {
                start_after: None,
                end_at: Some(keys(["4"])),
                limit: 10,
            },
            Event::Delete(vec![keys(["3"])]),
            Event::Update(vec![keys(["2"])]),
            Event::Insert(vec![keys(["4"])]),
            Event::Commit,
            Event::ProgressSave {
                last_primary_key: Some(keys(["4"])),
                complete: false,
                chunks: 1,
                rows_scanned: 3,
                inserts: 1,
                updates: 1,
                deletes: 1,
            },
            Event::Unlock,
        ]
    );
}

#[test]
fn a_short_source_chunk_requires_a_later_locked_empty_source_tail_chunk() {
    let events = events();
    let source_rows = source_rows();
    let target_tail = vec![row("10", "numeric tail ten"), row("11", "numeric tail eleven")];
    let mut initial_target = target_rows();
    initial_target.extend(target_tail.clone());
    let mut source = RecordingSource::scripted(
        [source_rows.clone(), Vec::new()],
        Rc::clone(&events),
    );
    let mut target = RecordingTargetSession::new(initial_target, Rc::clone(&events))
        .with_read_batches([target_rows(), target_tail.clone()]);
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));

    let first = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    )
    .expect("synchronize short source chunk");

    assert!(!first.complete, "a non-empty source chunk is never terminal");
    assert_eq!(first.last_primary_key, Some(keys(["4"])));
    let mut expected_after_first = source_rows.clone();
    expected_after_first.extend(target_tail.clone());
    assert_rows_equal(&target.visible_rows, &expected_after_first);

    events.borrow_mut().clear();
    let completed = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    )
    .expect("synchronize target-only tail");

    assert!(completed.complete);
    assert_eq!(completed.last_primary_key, Some(keys(["4"])));
    assert_rows_equal(&target.visible_rows, &source_rows);
    assert_eq!(
        events.borrow().as_slice(),
        [
            Event::ProgressLoad {
                run_id: "sync-run-1".to_string(),
                table: "widgets".to_string(),
            },
            Event::SetAutocommit(false),
            Event::LockWrite {
                database: "target_db".to_string(),
                table: "widgets".to_string(),
            },
            Event::SourceRead {
                start_after: Some(keys(["4"])),
                end_at: None,
                limit: 10,
            },
            Event::TargetRead {
                start_after: Some(keys(["4"])),
                end_at: None,
                limit: 10,
            },
            Event::Delete(vec![keys(["10"]), keys(["11"])]),
            Event::Commit,
            Event::ProgressSave {
                last_primary_key: Some(keys(["4"])),
                complete: true,
                chunks: 2,
                rows_scanned: 3,
                inserts: 1,
                updates: 1,
                deletes: 3,
            },
            Event::Unlock,
        ]
    );
}

#[test]
fn lock_setup_failures_do_not_read_source_or_save_progress() {
    for failure in [FailurePoint::SetAutocommit, FailurePoint::LockTable] {
        let events = events();
        let mut source = RecordingSource::new(source_rows(), Rc::clone(&events));
        let mut target = RecordingTargetSession::new(target_rows(), Rc::clone(&events))
            .fail_at(failure);
        let mut progress = RecordingProgressStore::new(Rc::clone(&events));

        let result = sync_next_chunk(
            &config(10),
            &mut source,
            &mut target,
            &mut progress,
        );

        assert!(result.is_err(), "{failure:?} must fail the chunk");
        assert_eq!(progress.durable, None);
        let recorded = events.borrow();
        assert!(!recorded.iter().any(|event| matches!(
            event,
            Event::SourceRead { .. }
                | Event::TargetRead { .. }
                | Event::ProgressSave { .. }
                | Event::Commit
                | Event::Rollback
                | Event::Unlock
        )));
        assert_eq!(
            recorded.first(),
            Some(&Event::ProgressLoad {
                run_id: "sync-run-1".to_string(),
                table: "widgets".to_string(),
            })
        );
        assert!(recorded.contains(&Event::SetAutocommit(false)));
        assert_eq!(
            recorded.contains(&Event::LockWrite {
                database: "target_db".to_string(),
                table: "widgets".to_string(),
            }),
            failure == FailurePoint::LockTable
        );
    }
}

#[test]
fn source_read_failure_rolls_back_unlocks_and_does_not_save_progress() {
    let events = events();
    let initial_target = target_rows();
    let mut source = RecordingSource::new(source_rows(), Rc::clone(&events))
        .fail_at(FailurePoint::SourceRead);
    let mut target = RecordingTargetSession::new(initial_target.clone(), Rc::clone(&events));
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));

    let result = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    );

    assert!(result.is_err());
    assert_eq!(progress.durable, None);
    assert_rows_equal(&target.visible_rows, &initial_target);
    assert_eq!(
        events.borrow().as_slice(),
        [
            Event::ProgressLoad {
                run_id: "sync-run-1".to_string(),
                table: "widgets".to_string(),
            },
            Event::SetAutocommit(false),
            Event::LockWrite {
                database: "target_db".to_string(),
                table: "widgets".to_string(),
            },
            Event::SourceRead {
                start_after: None,
                end_at: None,
                limit: 10,
            },
            Event::Rollback,
            Event::Unlock,
        ]
    );
}

#[test]
fn target_read_failure_rolls_back_unlocks_and_does_not_save_progress() {
    let events = events();
    let initial_target = target_rows();
    let mut source = RecordingSource::new(source_rows(), Rc::clone(&events));
    let mut target = RecordingTargetSession::new(initial_target.clone(), Rc::clone(&events))
        .fail_at(FailurePoint::TargetRead);
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));

    let result = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    );

    assert!(result.is_err());
    assert_eq!(progress.durable, None);
    assert_rows_equal(&target.visible_rows, &initial_target);
    let recorded = events.borrow();
    assert_eq!(
        &recorded[recorded.len() - 3..],
        [
            Event::TargetRead {
                start_after: None,
                end_at: Some(keys(["4"])),
                limit: 10,
            },
            Event::Rollback,
            Event::Unlock,
        ]
    );
    assert!(!recorded.contains(&Event::Commit));
    assert!(!recorded
        .iter()
        .any(|event| matches!(event, Event::ProgressSave { .. })));
}

#[test]
fn every_strict_write_failure_rolls_back_unlocks_and_does_not_save_progress() {
    for failure in [
        FailurePoint::Delete,
        FailurePoint::Update,
        FailurePoint::Insert,
    ] {
        let events = events();
        let initial_target = target_rows();
        let mut source = RecordingSource::new(source_rows(), Rc::clone(&events));
        let mut target = RecordingTargetSession::new(initial_target.clone(), Rc::clone(&events))
            .fail_at(failure);
        let mut progress = RecordingProgressStore::new(Rc::clone(&events));

        let result = sync_next_chunk(
            &config(10),
            &mut source,
            &mut target,
            &mut progress,
        );

        assert!(result.is_err(), "{failure:?} must fail the chunk");
        assert_eq!(progress.durable, None, "{failure:?} saved progress");
        assert_rows_equal(&target.visible_rows, &initial_target);
        let recorded = events.borrow();
        assert_eq!(
            &recorded[recorded.len() - 2..],
            [Event::Rollback, Event::Unlock],
            "{failure:?} cleanup order"
        );
        assert!(!recorded.contains(&Event::Commit));
        assert!(!recorded
            .iter()
            .any(|event| matches!(event, Event::ProgressSave { .. })));
    }
}

#[test]
fn commit_failure_rolls_back_unlocks_and_never_saves_progress() {
    let events = events();
    let initial_target = target_rows();
    let mut source = RecordingSource::new(source_rows(), Rc::clone(&events));
    let mut target = RecordingTargetSession::new(initial_target.clone(), Rc::clone(&events))
        .fail_at(FailurePoint::Commit);
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));

    let result = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    );

    assert!(result.is_err());
    assert_eq!(progress.durable, None);
    assert_rows_equal(&target.visible_rows, &initial_target);
    let recorded = events.borrow();
    assert_eq!(
        &recorded[recorded.len() - 3..],
        [Event::Commit, Event::Rollback, Event::Unlock]
    );
    assert!(!recorded
        .iter()
        .any(|event| matches!(event, Event::ProgressSave { .. })));
}

#[test]
fn progress_failure_after_commit_leaves_cursor_unsaved_and_restart_replays_safely() {
    let events = events();
    let source_rows = source_rows();
    let mut source = RecordingSource::scripted(
        [source_rows.clone(), source_rows.clone()],
        Rc::clone(&events),
    );
    let mut target = RecordingTargetSession::new(target_rows(), Rc::clone(&events));
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));
    progress.failure = Some(FailurePoint::ProgressSave);

    let first = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    );

    assert!(first.is_err());
    assert_rows_equal(&target.visible_rows, &source_rows);
    assert_eq!(progress.durable, None);
    assert_eq!(
        event_names_after_commit(&events),
        vec!["commit", "progress_save", "unlock"]
    );
    assert!(!events.borrow().contains(&Event::Rollback));

    events.borrow_mut().clear();
    progress.failure = None;
    let resumed = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    )
    .expect("restart safely replays the committed unsaved cursor");

    assert!(!resumed.complete);
    assert_eq!(resumed.last_primary_key, Some(keys(["4"])));
    assert!(events.borrow().contains(&Event::SourceRead {
        start_after: None,
        end_at: None,
        limit: 10,
    }));
    assert!(!events.borrow().iter().any(|event| matches!(
        event,
        Event::Delete(_) | Event::Update(_) | Event::Insert(_)
    )));
}

#[test]
fn unlock_failure_returns_error_after_durable_progress_without_reporting_completion() {
    let events = events();
    let mut source = RecordingSource::new(source_rows(), Rc::clone(&events));
    let mut target = RecordingTargetSession::new(target_rows(), Rc::clone(&events))
        .fail_at(FailurePoint::Unlock);
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));

    let result = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    );

    assert!(result.is_err(), "unlock failure cannot report chunk completion");
    let durable = progress
        .durable
        .as_ref()
        .expect("progress is durable before unlock");
    assert!(!durable.complete);
    assert_eq!(durable.last_primary_key, Some(keys(["4"])));
    assert_eq!(
        event_names_after_commit(&events),
        vec!["commit", "progress_save", "unlock"]
    );
}

#[test]
fn restart_reads_from_the_saved_cursor() {
    let events = events();
    let mut source = RecordingSource::new(
        vec![row("21", "twenty-one")],
        Rc::clone(&events),
    );
    let mut target = RecordingTargetSession::new(Vec::new(), Rc::clone(&events));
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));
    progress.durable = Some(SyncChunkProgress {
        run_id: "sync-run-1".to_string(),
        table: "widgets".to_string(),
        last_primary_key: Some(keys(["20"])),
        complete: false,
        chunks: 2,
        rows_scanned: 20,
        inserts: 4,
        updates: 3,
        deletes: 2,
    });

    let resumed = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    )
    .expect("resume from durable cursor");

    assert_eq!(resumed.last_primary_key, Some(keys(["21"])));
    assert!(!resumed.complete);
    assert!(events.borrow().contains(&Event::SourceRead {
        start_after: Some(keys(["20"])),
        end_at: None,
        limit: 10,
    }));
    assert!(events.borrow().contains(&Event::TargetRead {
        start_after: Some(keys(["20"])),
        end_at: Some(keys(["21"])),
        limit: 10,
    }));
}

#[test]
fn completed_progress_returns_without_reopening_a_chunk() {
    let events = events();
    let completed = SyncChunkProgress {
        run_id: "sync-run-1".to_string(),
        table: "widgets".to_string(),
        last_primary_key: Some(keys(["99"])),
        complete: true,
        chunks: 7,
        rows_scanned: 64,
        inserts: 5,
        updates: 4,
        deletes: 3,
    };
    let mut source = RecordingSource::new(
        vec![row("100", "must not be read")],
        Rc::clone(&events),
    );
    let mut target = RecordingTargetSession::new(target_rows(), Rc::clone(&events));
    let mut progress = RecordingProgressStore::new(Rc::clone(&events));
    progress.durable = Some(completed.clone());

    let outcome = sync_next_chunk(
        &config(10),
        &mut source,
        &mut target,
        &mut progress,
    )
    .expect("completed progress is terminal");

    assert!(outcome.complete);
    assert_eq!(outcome.last_primary_key, completed.last_primary_key);
    assert_eq!(outcome.chunks, completed.chunks);
    assert_eq!(outcome.rows_scanned, completed.rows_scanned);
    assert_eq!(outcome.inserts, completed.inserts);
    assert_eq!(outcome.updates, completed.updates);
    assert_eq!(outcome.deletes, completed.deletes);
    assert_eq!(
        events.borrow().as_slice(),
        [Event::ProgressLoad {
            run_id: "sync-run-1".to_string(),
            table: "widgets".to_string(),
        }]
    );
}

fn config(chunk_size: usize) -> SyncChunkConfig {
    SyncChunkConfig {
        run_id: "sync-run-1".to_string(),
        target_database: "target_db".to_string(),
        table: SyncTable {
            name: "widgets".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        },
        chunk_size,
    }
}

fn source_rows() -> Vec<SnapshotRow> {
    vec![row("1", "same"), row("2", "new"), row("4", "missing")]
}

fn target_rows() -> Vec<SnapshotRow> {
    vec![row("1", "same"), row("2", "old"), row("3", "extra")]
}

fn row(id: &str, name: &str) -> SnapshotRow {
    SnapshotRow {
        primary_key: keys([id]),
        values: BTreeMap::from([
            ("id".to_string(), Some(id.to_string())),
            ("name".to_string(), Some(name.to_string())),
        ]),
    }
}

fn keys<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}

fn primary_keys(rows: &[SnapshotRow]) -> Vec<Vec<String>> {
    rows.iter().map(|row| row.primary_key.clone()).collect()
}

fn assert_rows_equal(actual: &[SnapshotRow], expected: &[SnapshotRow]) {
    assert_eq!(actual.len(), expected.len(), "row count differs");
    for expected_row in expected {
        assert!(
            actual.contains(expected_row),
            "missing expected row: {expected_row:?}; actual: {actual:?}"
        );
    }
}

fn events() -> Events {
    Rc::new(RefCell::new(Vec::new()))
}

fn event_names_after_commit(events: &Events) -> Vec<&'static str> {
    let events = events.borrow();
    let commit = events
        .iter()
        .position(|event| event == &Event::Commit)
        .expect("commit event");
    events[commit..]
        .iter()
        .map(|event| match event {
            Event::Commit => "commit",
            Event::ProgressSave { .. } => "progress_save",
            Event::Unlock => "unlock",
            other => panic!("unexpected post-commit event: {other:?}"),
        })
        .collect()
}
