use super::TableSyncError;
use crate::snapshot::SnapshotRow;

pub trait SyncRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
}

impl<E> SyncRepairTarget for crate::target::TargetMySqlWriter<E>
where
    E: crate::target::TargetExecutor,
{
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        self.insert_rows(std::slice::from_ref(row))
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }

    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError> {
        crate::target::TargetMySqlWriter::update_row(self, row)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }
}
