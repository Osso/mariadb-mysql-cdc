use super::*;
use crate::snapshot::SnapshotRow;
use std::cell::RefCell;
use std::collections::BTreeMap;

pub(crate) fn account_table() -> SyncTable {
    SyncTable {
        name: "accounts".to_string(),
        primary_key: vec!["id".to_string()],
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns: vec!["id".to_string(), "name".to_string()],
    }
}

pub(crate) fn account_table_with_updated_at() -> SyncTable {
    SyncTable {
        name: "accounts".to_string(),
        primary_key: vec!["id".to_string()],
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns: vec![
            "id".to_string(),
            "name".to_string(),
            "updated_at".to_string(),
        ],
    }
}

pub(crate) fn row(id: &str, name: &str) -> SnapshotRow {
    snapshot_row(id, &[("name", name)])
}

pub(crate) fn row_with_updated_at(id: &str, name: &str, updated_at: &str) -> SnapshotRow {
    snapshot_row(id, &[("name", name), ("updated_at", updated_at)])
}

fn snapshot_row(id: &str, fields: &[(&str, &str)]) -> SnapshotRow {
    let mut values = BTreeMap::from([("id".to_string(), Some(id.to_string()))]);
    for (column, value) in fields {
        values.insert((*column).to_string(), Some((*value).to_string()));
    }
    SnapshotRow {
        primary_key: vec![id.to_string()],
        values,
    }
}

pub(crate) struct FakeReader {
    pub(crate) rows: Vec<SnapshotRow>,
    pub(crate) requests: RefCell<Vec<SyncChunkRequest>>,
}

impl FakeReader {
    pub(crate) fn new(rows: Vec<SnapshotRow>) -> Self {
        Self {
            rows,
            requests: RefCell::new(Vec::new()),
        }
    }
}

impl SyncTableReader for FakeReader {
    fn read_rows(&self, request: &SyncChunkRequest) -> Result<Vec<SnapshotRow>, TableSyncError> {
        self.requests.borrow_mut().push(request.clone());
        Ok(self
            .rows
            .iter()
            .filter(|row| row_in_window(row, request))
            .take(request.limit)
            .cloned()
            .collect())
    }
}

fn row_in_window(row: &SnapshotRow, request: &SyncChunkRequest) -> bool {
    let after_start = request
        .start_after
        .as_ref()
        .is_none_or(|start| row.primary_key > *start);
    let before_end = request
        .end_at
        .as_ref()
        .is_none_or(|end| row.primary_key <= *end);
    let after_update = request.updated_since.as_ref().is_none_or(|updated_since| {
        row.values
            .get(&updated_since.column)
            .and_then(Option::as_deref)
            .is_some_and(|value| value >= updated_since.value.as_str())
    });
    after_start && before_end && after_update
}

#[derive(Default)]
pub(crate) struct RecordingRepairTarget {
    pub(crate) inserts: RefCell<Vec<SnapshotRow>>,
    pub(crate) updates: RefCell<Vec<SnapshotRow>>,
    pub(crate) deletes: RefCell<Vec<Vec<String>>>,
    pub(crate) operations: RefCell<Vec<String>>,
}

impl SyncRepairTarget for RecordingRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        self.inserts.borrow_mut().push(row.clone());
        self.operations
            .borrow_mut()
            .push(format!("insert:{}", row.primary_key.join(",")));
        Ok(())
    }

    fn insert_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        self.inserts
            .borrow_mut()
            .extend(rows.iter().map(|row| (*row).clone()));
        self.operations.borrow_mut().push(format!(
            "insert-batch:{}",
            rows.iter()
                .map(|row| row.primary_key.join(","))
                .collect::<Vec<_>>()
                .join(",")
        ));
        Ok(())
    }

    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        self.updates.borrow_mut().push(row.clone());
        self.operations
            .borrow_mut()
            .push(format!("update:{}", row.primary_key.join(",")));
        Ok(())
    }

    fn update_rows(&mut self, rows: &[&SnapshotRow]) -> Result<(), TableSyncError> {
        self.updates
            .borrow_mut()
            .extend(rows.iter().map(|row| (*row).clone()));
        self.operations.borrow_mut().push(format!(
            "update-batch:{}",
            rows.iter()
                .map(|row| row.primary_key.join(","))
                .collect::<Vec<_>>()
                .join(",")
        ));
        Ok(())
    }

    fn delete_row(&mut self, primary_key: &[String]) -> Result<(), TableSyncError> {
        self.deletes.borrow_mut().push(primary_key.to_vec());
        self.operations
            .borrow_mut()
            .push(format!("delete:{}", primary_key.join(",")));
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct RecordingProgressStore {
    pub(crate) loaded: Option<SyncTableProgress>,
    pub(crate) saved: RefCell<Vec<SyncTableProgress>>,
    pub(crate) acquired_run_ids: RefCell<Vec<String>>,
    pub(crate) released_run_ids: RefCell<Vec<String>>,
    pub(crate) errors: RefCell<Vec<String>>,
    pub(crate) release_error: Option<String>,
}

impl RecordingProgressStore {
    pub(crate) fn with_progress(progress: SyncTableProgress) -> Self {
        Self {
            loaded: Some(progress),
            saved: RefCell::new(Vec::new()),
            acquired_run_ids: RefCell::new(Vec::new()),
            released_run_ids: RefCell::new(Vec::new()),
            errors: RefCell::new(Vec::new()),
            release_error: None,
        }
    }
}

impl SyncProgressStore for RecordingProgressStore {
    fn ensure(&mut self) -> Result<(), TableSyncError> {
        Ok(())
    }

    fn acquire_run(&self, run_id: &str) -> Result<(), TableSyncError> {
        self.acquired_run_ids.borrow_mut().push(run_id.to_string());
        Ok(())
    }

    fn release_run(&self, run_id: &str) -> Result<(), TableSyncError> {
        self.released_run_ids.borrow_mut().push(run_id.to_string());
        if let Some(error) = &self.release_error {
            return Err(TableSyncError::Progress(error.clone()));
        }
        Ok(())
    }

    fn load(&self, _table: &str) -> Result<Option<SyncTableProgress>, TableSyncError> {
        Ok(self.loaded.clone())
    }

    fn save(&mut self, progress: &SyncTableProgress) -> Result<(), TableSyncError> {
        self.saved.borrow_mut().push(progress.clone());
        Ok(())
    }

    fn save_error(&mut self, _table: &str, error: &TableSyncError) -> Result<(), TableSyncError> {
        self.errors.borrow_mut().push(error.to_string());
        Ok(())
    }
}
