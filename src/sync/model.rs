use crate::database_row::DatabaseRow;
pub(crate) use crate::primary_key_ordering::PrimaryKeyOrdering as SyncPrimaryKeyOrdering;
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SyncTable {
    pub(crate) name: String,
    pub(crate) primary_key: Vec<String>,
    pub(crate) primary_key_ordering: Vec<SyncPrimaryKeyOrdering>,
    pub(crate) columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncChunkConfig {
    pub(crate) run_id: String,
    pub(crate) run_spec_json: String,
    pub(crate) target_database: String,
    pub(crate) table: SyncTable,
    pub(crate) chunk_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncChunkReadRequest {
    pub(crate) start_after: Option<Vec<String>>,
    pub(crate) end_at: Option<Vec<String>>,
    pub(crate) limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncChunkProgress {
    pub(crate) run_id: String,
    pub(crate) table: String,
    pub(crate) run_spec_json: String,
    pub(crate) last_primary_key: Option<Vec<String>>,
    pub(crate) complete: bool,
    pub(crate) chunks: u64,
    pub(crate) rows_scanned: u64,
    pub(crate) inserts: u64,
    pub(crate) updates: u64,
    pub(crate) deletes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncStage {
    PrerequisiteSchema,
    Rows,
    FinalConstraints,
}

impl SyncStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PrerequisiteSchema => "prerequisite_schema",
            Self::Rows => "rows",
            Self::FinalConstraints => "final_constraints",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "prerequisite_schema" => Ok(Self::PrerequisiteSchema),
            "rows" => Ok(Self::Rows),
            "final_constraints" => Ok(Self::FinalConstraints),
            _ => Err(format!("invalid sync progress stage `{value}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SyncProgressStatus {
    Running,
    Complete,
    Error,
}

impl SyncProgressStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Error => "error",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "running" => Ok(Self::Running),
            "complete" => Ok(Self::Complete),
            "error" => Ok(Self::Error),
            _ => Err(format!("invalid sync progress status `{value}`")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncProgressRow {
    pub(crate) run_id: String,
    pub(crate) stage: SyncStage,
    pub(crate) table_name: String,
    pub(crate) run_spec_json: String,
    pub(crate) last_primary_key: Option<Vec<String>>,
    pub(crate) chunks: u64,
    pub(crate) rows_scanned: u64,
    pub(crate) inserts: u64,
    pub(crate) updates: u64,
    pub(crate) deletes: u64,
    pub(crate) status: SyncProgressStatus,
    pub(crate) last_error: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) completed_at: Option<String>,
}

pub(crate) trait SyncChunkSource {
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<DatabaseRow>, String>;
}

pub(crate) trait SyncChunkTargetSession {
    fn set_autocommit(&mut self, enabled: bool) -> Result<(), String>;
    fn lock_table_write(&mut self, database: &str, table: &str) -> Result<(), String>;
    fn read_rows(&mut self, request: &SyncChunkReadRequest) -> Result<Vec<DatabaseRow>, String>;
    fn delete_rows(&mut self, primary_keys: &[Vec<String>]) -> Result<(), String>;
    fn update_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), String>;
    fn insert_rows(&mut self, rows: &[DatabaseRow]) -> Result<(), String>;
    fn commit(&mut self) -> Result<(), String>;
    fn rollback(&mut self) -> Result<(), String>;
    fn unlock_tables(&mut self) -> Result<(), String>;
}

pub(crate) trait SyncChunkProgressStore {
    fn load(&mut self, run_id: &str, table: &str) -> Result<Option<SyncChunkProgress>, String>;
    fn save(&mut self, progress: &SyncChunkProgress) -> Result<(), String>;
}

pub(crate) trait SyncRunProgressStore {
    fn load_stage(
        &mut self,
        run_id: &str,
        stage: SyncStage,
        table_name: &str,
    ) -> Result<Option<SyncProgressRow>, String>;

    fn save_stage(&mut self, row: &SyncProgressRow) -> Result<(), String>;
}
