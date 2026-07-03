use super::TableSyncError;
use crate::snapshot::SnapshotRow;
use crate::target::PrimaryKey;
use mysql::Value;

pub trait SyncRepairTarget {
    fn insert_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
    fn update_row(&mut self, row: &SnapshotRow) -> Result<(), TableSyncError>;
    fn delete_row(&mut self, primary_key: &[String]) -> Result<(), TableSyncError>;
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

    fn delete_row(&mut self, primary_key: &[String]) -> Result<(), TableSyncError> {
        let primary_key = PrimaryKey::new(primary_key.iter().cloned().map(Value::from).collect());
        crate::target::TargetMySqlWriter::delete_row(self, &primary_key)
            .map_err(|error| TableSyncError::Repair(error.to_string()))
    }
}
