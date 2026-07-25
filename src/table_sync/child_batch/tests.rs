//! The `guests` chunk at `guest_id > 87524743` inserted 4791 rows while the live stream inserted
//! the same rows concurrently. Retrying the whole batch after each duplicate re-read every row of
//! the batch, so the chunk took 42 minutes. These tests pin the round-trip cost.

use super::*;
use std::collections::BTreeMap;

fn row(primary_key: &str) -> SnapshotRow {
    SnapshotRow {
        primary_key: vec![primary_key.to_string()],
        values: BTreeMap::from([("guest_id".to_string(), Some(primary_key.to_string()))]),
    }
}

fn batch(primary_keys: &[&str]) -> Vec<SnapshotRow> {
    primary_keys.iter().map(|key| row(key)).collect()
}

#[derive(Debug, Default)]
struct RecordedInserter {
    /// Row counts of every insert call, in order.
    insert_sizes: Vec<usize>,
    /// Row counts of every duplicate reconciliation call, in order.
    reconcile_sizes: Vec<usize>,
    repair_parent_calls: usize,
    /// Primary keys the target already holds. Reconciliation reports the rest as absent.
    present: Vec<String>,
    /// MySQL text passed to each reconciliation call, in order.
    reconcile_errors: Vec<String>,
    /// Primary keys that collide on a secondary unique key owned by another identity.
    foreign_owned: Vec<String>,
    /// Insert calls that report a missing parent before any parent repair runs.
    missing_parent_until_repair: bool,
}

impl RecordedInserter {
    fn collides(&self, rows: &[SnapshotRow]) -> bool {
        rows.iter().any(|row| {
            self.present.contains(&row.primary_key[0])
                || self.foreign_owned.contains(&row.primary_key[0])
        })
    }
}

impl ChildBatchInserter for RecordedInserter {
    fn table_name(&self) -> &str {
        "guests"
    }

    fn insert(&mut self, rows: &[SnapshotRow]) -> Result<ChildInsertOutcome, TableSyncError> {
        self.insert_sizes.push(rows.len());
        if self.missing_parent_until_repair && self.repair_parent_calls == 0 {
            return Ok(ChildInsertOutcome::MissingParent);
        }
        if self.collides(rows) {
            return Ok(ChildInsertOutcome::DuplicateKey(
                "Duplicate entry 'x' for key 'guests.idx_guest_hash'".to_string(),
            ));
        }
        Ok(ChildInsertOutcome::Applied)
    }

    fn repair_parents(&mut self, _rows: &[SnapshotRow]) -> Result<(), TableSyncError> {
        self.repair_parent_calls += 1;
        Ok(())
    }

    fn reconcile_duplicates(
        &mut self,
        rows: &[SnapshotRow],
        duplicate_error: &str,
    ) -> Result<Vec<SnapshotRow>, TableSyncError> {
        self.reconcile_sizes.push(rows.len());
        self.reconcile_errors.push(duplicate_error.to_string());
        let absent = rows
            .iter()
            .filter(|row| !self.present.contains(&row.primary_key[0]))
            .cloned()
            .collect::<Vec<_>>();
        if absent.len() == rows.len() {
            return Err(TableSyncError::Repair(format!(
                "duplicate key for `{}` is owned by a different target identity",
                self.table_name()
            )));
        }
        Ok(absent)
    }
}

#[test]
fn inserts_a_clean_batch_in_one_round_trip() {
    let mut inserter = RecordedInserter::default();

    insert_child_batch_with_reconciliation(&mut inserter, &batch(&["1", "2", "3"]))
        .expect("insert");

    assert_eq!(inserter.insert_sizes, vec![3]);
    assert!(inserter.reconcile_sizes.is_empty());
}

/// Reconciliation must run once for the batch, not once per concurrently inserted row.
#[test]
fn reconciles_the_batch_once_when_the_stream_inserted_rows_concurrently() {
    let mut inserter = RecordedInserter {
        present: vec!["2".to_string(), "4".to_string()],
        ..RecordedInserter::default()
    };

    insert_child_batch_with_reconciliation(&mut inserter, &batch(&["1", "2", "3", "4", "5"]))
        .expect("insert");

    assert_eq!(inserter.reconcile_sizes, vec![5]);
    // One failed batch insert, then one insert per proven-absent row.
    assert_eq!(inserter.insert_sizes, vec![5, 1, 1, 1]);
}

/// Round trips must stay linear in the batch size. The previous full-batch retry re-read every
/// row on every cycle, which is what made one 4791-row chunk take 42 minutes.
#[test]
fn keeps_round_trips_linear_when_every_retry_hits_another_duplicate() {
    let keys = (0..64).map(|index| index.to_string()).collect::<Vec<_>>();
    let present = keys.iter().skip(1).cloned().collect::<Vec<_>>();
    let mut inserter = RecordedInserter {
        present,
        ..RecordedInserter::default()
    };
    let rows = keys.iter().map(|key| row(key)).collect::<Vec<_>>();

    insert_child_batch_with_reconciliation(&mut inserter, &rows).expect("insert");

    let reconciled_rows: usize = inserter.reconcile_sizes.iter().sum();
    assert_eq!(inserter.reconcile_sizes, vec![64]);
    assert_eq!(reconciled_rows, 64);
    assert_eq!(inserter.insert_sizes, vec![64, 1]);
}

#[test]
fn returns_ok_when_reconciliation_proves_every_row_already_present() {
    let mut inserter = RecordedInserter {
        present: vec!["1".to_string(), "2".to_string()],
        ..RecordedInserter::default()
    };

    insert_child_batch_with_reconciliation(&mut inserter, &batch(&["1", "2"])).expect("insert");

    assert_eq!(inserter.insert_sizes, vec![2]);
    assert_eq!(inserter.reconcile_sizes, vec![2]);
}

/// A secondary unique key owned by another identity can never be inserted, so it must fail closed.
#[test]
fn fails_closed_when_a_duplicate_is_owned_by_another_identity() {
    let mut inserter = RecordedInserter {
        foreign_owned: vec!["7".to_string()],
        ..RecordedInserter::default()
    };

    let error = insert_child_batch_with_reconciliation(&mut inserter, &batch(&["7"]))
        .expect_err("owned duplicate");

    assert!(
        error
            .to_string()
            .contains("owned by a different target identity")
    );
}

/// A single foreign-owned row inside an otherwise clean batch still fails closed instead of
/// looping, and it is detected during the individual inserts.
#[test]
fn fails_closed_for_a_foreign_owned_row_inside_a_larger_batch() {
    let mut inserter = RecordedInserter {
        present: vec!["1".to_string()],
        foreign_owned: vec!["3".to_string()],
        ..RecordedInserter::default()
    };

    let error = insert_child_batch_with_reconciliation(&mut inserter, &batch(&["1", "2", "3"]))
        .expect_err("owned duplicate");

    assert!(
        error
            .to_string()
            .contains("owned by a different target identity")
    );
    assert_eq!(inserter.insert_sizes, vec![3, 1, 1]);
}

#[test]
fn repairs_missing_parents_once_then_retries_the_batch() {
    let mut inserter = RecordedInserter {
        missing_parent_until_repair: true,
        ..RecordedInserter::default()
    };

    insert_child_batch_with_reconciliation(&mut inserter, &batch(&["1", "2"])).expect("insert");

    assert_eq!(inserter.repair_parent_calls, 1);
    assert_eq!(inserter.insert_sizes, vec![2, 2]);
}
