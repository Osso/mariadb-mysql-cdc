use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotTable {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<String>,
}

impl From<&crate::inventory::TableInventory> for SnapshotTable {
    fn from(table: &crate::inventory::TableInventory) -> Self {
        Self {
            name: table.name.clone(),
            primary_key: table.primary_key.clone(),
            columns: table
                .columns
                .iter()
                .filter(|column| column.generated.is_none())
                .map(|column| column.name.clone())
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotRow {
    pub primary_key: Vec<String>,
    pub values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRequest {
    pub table: String,
    pub primary_key: Vec<String>,
    pub selected_columns: Vec<String>,
    pub start_after: Option<Vec<String>>,
    pub limit: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotProgress {
    pub tables: BTreeMap<String, TableSnapshotProgress>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TableSnapshotProgress {
    pub last_primary_key: Option<Vec<String>>,
    pub rows_copied: u64,
    pub complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotResult {
    pub table: String,
    pub rows_copied: u64,
}

pub trait SnapshotProgressStore {
    fn load(&self) -> Result<SnapshotProgress, SnapshotError>;
    fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError>;
}

pub trait SnapshotSource {
    fn read_chunk(&self, request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError>;
}

pub trait SnapshotTarget {
    fn write_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), SnapshotError>;
}

#[derive(Clone, Debug)]
pub struct FileSnapshotProgressStore {
    path: PathBuf,
}

impl FileSnapshotProgressStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl SnapshotProgressStore for FileSnapshotProgressStore {
    fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => decode_progress(&contents),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(SnapshotProgress::default())
            }
            Err(error) => Err(SnapshotError::Read {
                path: self.path.clone(),
                source: error,
            }),
        }
    }

    fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
        let encoded = encode_progress(progress)?;
        let temp_path = temp_progress_path(&self.path);

        fs::write(&temp_path, encoded).map_err(|error| SnapshotError::Write {
            path: temp_path.clone(),
            source: error,
        })?;

        fs::rename(&temp_path, &self.path).map_err(|error| SnapshotError::Rename {
            from: temp_path,
            to: self.path.clone(),
            source: error,
        })
    }
}

#[derive(Debug)]
pub enum SnapshotError {
    Decode(serde_json::Error),
    Encode(serde_json::Error),
    InvalidTable(String),
    Read {
        path: PathBuf,
        source: io::Error,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
        source: io::Error,
    },
    Write {
        path: PathBuf,
        source: io::Error,
    },
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(source) => {
                write!(formatter, "failed to decode snapshot progress: {source}")
            }
            Self::Encode(source) => {
                write!(formatter, "failed to encode snapshot progress: {source}")
            }
            Self::InvalidTable(message) => formatter.write_str(message),
            Self::Read { path, source } => {
                write!(formatter, "failed to read {}: {source}", path.display())
            }
            Self::Rename { from, to, source } => write!(
                formatter,
                "failed to move {} to {}: {source}",
                from.display(),
                to.display()
            ),
            Self::Write { path, source } => {
                write!(formatter, "failed to write {}: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for SnapshotError {}

impl SnapshotProgress {
    pub fn table(&self, table: &str) -> Option<&TableSnapshotProgress> {
        self.tables.get(table)
    }

    pub fn mark_chunk(&mut self, table: &str, last_primary_key: Vec<String>, row_count: u64) {
        let progress = self.tables.entry(table.to_string()).or_default();
        progress.last_primary_key = Some(last_primary_key);
        progress.rows_copied += row_count;
        progress.complete = false;
    }

    pub fn mark_complete(&mut self, table: &str) {
        self.tables.entry(table.to_string()).or_default().complete = true;
    }
}

pub fn build_chunk_request(
    table: &SnapshotTable,
    limit: usize,
    progress: &SnapshotProgress,
) -> Result<ChunkRequest, SnapshotError> {
    validate_table(table)?;
    let table_progress = progress.table(&table.name);
    let start_after = table_progress.and_then(|progress| progress.last_primary_key.clone());

    Ok(ChunkRequest {
        table: table.name.clone(),
        primary_key: table.primary_key.clone(),
        selected_columns: table.columns.clone(),
        start_after,
        limit,
    })
}

pub fn snapshot_table(
    table: &SnapshotTable,
    chunk_size: usize,
    progress_store: &impl SnapshotProgressStore,
    source: &impl SnapshotSource,
    target: &mut impl SnapshotTarget,
) -> Result<SnapshotResult, SnapshotError> {
    validate_table(table)?;
    let mut progress = progress_store.load()?;
    let starting_rows_copied = rows_copied_for_table(&progress, &table.name);
    if is_table_complete(&progress, &table.name) {
        return Ok(snapshot_result(table, 0));
    }

    copy_table_chunks(
        table,
        chunk_size,
        &mut progress,
        progress_store,
        source,
        target,
    )?;
    let rows_copied = rows_copied_for_table(&progress, &table.name) - starting_rows_copied;

    Ok(snapshot_result(table, rows_copied))
}

fn copy_table_chunks(
    table: &SnapshotTable,
    chunk_size: usize,
    progress: &mut SnapshotProgress,
    progress_store: &impl SnapshotProgressStore,
    source: &impl SnapshotSource,
    target: &mut impl SnapshotTarget,
) -> Result<(), SnapshotError> {
    loop {
        let request = build_chunk_request(table, chunk_size, progress)?;
        let rows = source.read_chunk(&request)?;

        if rows.is_empty() {
            progress.mark_complete(&table.name);
            progress_store.save(progress)?;
            return Ok(());
        }

        target.write_rows(&rows)?;
        let last_primary_key = last_primary_key(&rows)?;
        progress.mark_chunk(&table.name, last_primary_key, rows.len() as u64);

        if rows.len() < chunk_size {
            progress.mark_complete(&table.name);
            progress_store.save(progress)?;
            return Ok(());
        }

        progress_store.save(progress)?;
    }
}

fn is_table_complete(progress: &SnapshotProgress, table: &str) -> bool {
    progress.table(table).is_some_and(|table| table.complete)
}

fn rows_copied_for_table(progress: &SnapshotProgress, table: &str) -> u64 {
    progress
        .table(table)
        .map_or(0, |table_progress| table_progress.rows_copied)
}

fn snapshot_result(table: &SnapshotTable, rows_copied: u64) -> SnapshotResult {
    SnapshotResult {
        table: table.name.clone(),
        rows_copied,
    }
}

pub fn format_progress(progress: &SnapshotProgress) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "snapshot_progress tables={}",
        progress.tables.len()
    ));

    for (table, table_progress) in &progress.tables {
        let last_primary_key = table_progress
            .last_primary_key
            .as_ref()
            .map(|values| values.join(","))
            .unwrap_or_default();
        lines.push(format!(
            "snapshot_table_progress table={} rows_copied={} complete={} last_primary_key={}",
            table, table_progress.rows_copied, table_progress.complete, last_primary_key
        ));
    }

    lines.join("\n")
}

pub fn encode_progress(progress: &SnapshotProgress) -> Result<String, SnapshotError> {
    serde_json::to_string_pretty(progress)
        .map(|json| format!("{json}\n"))
        .map_err(SnapshotError::Encode)
}

fn decode_progress(contents: &str) -> Result<SnapshotProgress, SnapshotError> {
    serde_json::from_str(contents).map_err(SnapshotError::Decode)
}

fn validate_table(table: &SnapshotTable) -> Result<(), SnapshotError> {
    if table.name.is_empty() {
        return Err(SnapshotError::InvalidTable(
            "table name is required".to_string(),
        ));
    }

    if table.primary_key.is_empty() {
        return Err(SnapshotError::InvalidTable(format!(
            "{} needs a primary key for deterministic snapshot chunks",
            table.name
        )));
    }

    Ok(())
}

fn last_primary_key(rows: &[SnapshotRow]) -> Result<Vec<String>, SnapshotError> {
    rows.last()
        .map(|row| row.primary_key.clone())
        .ok_or_else(|| SnapshotError::InvalidTable("chunk had no rows".to_string()))
}

fn temp_progress_path(path: &Path) -> PathBuf {
    let mut temp_path = path.to_path_buf();
    let extension = match path.extension().and_then(|value| value.to_str()) {
        Some(extension) => format!("{extension}.tmp"),
        None => "tmp".to_string(),
    };
    temp_path.set_extension(extension);
    temp_path
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, VecDeque};

    #[test]
    fn builds_first_chunk_request_from_table_metadata() {
        let table = accounts_table();
        let progress = SnapshotProgress::default();

        let request = build_chunk_request(&table, 500, &progress).expect("request");

        assert_eq!(request.table, "accounts");
        assert_eq!(request.primary_key, vec!["id"]);
        assert_eq!(request.selected_columns, vec!["id", "name"]);
        assert_eq!(request.start_after, None);
        assert_eq!(request.limit, 500);
    }

    #[test]
    fn resumes_chunk_request_after_last_primary_key() {
        let table = accounts_table();
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts", vec!["42".to_string()], 42);

        let request = build_chunk_request(&table, 100, &progress).expect("request");

        assert_eq!(request.start_after, Some(vec!["42".to_string()]));
    }

    #[test]
    fn snapshots_table_in_chunks_and_saves_progress() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let source = FakeSnapshotSource::new(vec![
            vec![row("1", "alpha"), row("2", "beta")],
            vec![row("3", "gamma")],
        ]);
        let mut target = FakeSnapshotTarget::default();

        let result =
            snapshot_table(&table, 2, &progress_store, &source, &mut target).expect("snapshot");

        assert_eq!(result.rows_copied, 3);
        assert_eq!(target.rows.len(), 3);

        let saved = progress_store.load().expect("load progress");
        let table_progress = saved.table("accounts").expect("table progress");
        assert_eq!(table_progress.last_primary_key, Some(vec!["3".to_string()]));
        assert_eq!(table_progress.rows_copied, 3);
        assert!(table_progress.complete);
    }

    #[test]
    fn skips_completed_table_on_rerun() {
        let table = accounts_table();
        let progress_store = MemoryProgressStore::default();
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts", vec!["3".to_string()], 3);
        progress.mark_complete("accounts");
        progress_store.save(&progress).expect("save progress");
        let source = FakeSnapshotSource::new(vec![vec![row("4", "delta")]]);
        let mut target = FakeSnapshotTarget::default();

        let result =
            snapshot_table(&table, 2, &progress_store, &source, &mut target).expect("snapshot");

        assert_eq!(result.rows_copied, 0);
        assert!(target.rows.is_empty());
    }

    #[test]
    fn formats_snapshot_progress_for_operators() {
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts", vec!["42".to_string()], 42);

        assert_eq!(
            format_progress(&progress),
            "snapshot_progress tables=1\nsnapshot_table_progress table=accounts rows_copied=42 complete=false last_primary_key=42"
        );
    }

    #[test]
    fn file_progress_store_round_trips_table_progress() {
        let path = unique_path("snapshot-progress.json");
        let store = FileSnapshotProgressStore::new(path.clone());
        let mut progress = SnapshotProgress::default();
        progress.mark_chunk("accounts", vec!["9".to_string()], 9);

        store.save(&progress).expect("save progress");
        let loaded = store.load().expect("load progress");

        assert_eq!(loaded, progress);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn builds_snapshot_table_from_inventory_table() {
        let inventory_table = crate::inventory::TableInventory {
            name: "accounts".to_string(),
            table_type: "BASE TABLE".to_string(),
            engine: Some("InnoDB".to_string()),
            collation: None,
            primary_key: vec!["id".to_string()],
            columns: vec![
                inventory_column("id"),
                inventory_column("name"),
                generated_inventory_column("name_length"),
            ],
        };

        let table = SnapshotTable::from(&inventory_table);

        assert_eq!(table.name, "accounts");
        assert_eq!(table.primary_key, vec!["id"]);
        assert_eq!(table.columns, vec!["id", "name"]);
    }

    fn accounts_table() -> SnapshotTable {
        SnapshotTable {
            name: "accounts".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec!["id".to_string(), "name".to_string()],
        }
    }

    fn row(id: &str, name: &str) -> SnapshotRow {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), id.to_string());
        values.insert("name".to_string(), name.to_string());

        SnapshotRow {
            primary_key: vec![id.to_string()],
            values,
        }
    }

    fn inventory_column(name: &str) -> crate::inventory::ColumnInventory {
        crate::inventory::ColumnInventory {
            name: name.to_string(),
            ordinal_position: 1,
            column_type: "varchar(64)".to_string(),
            data_type: "varchar".to_string(),
            is_nullable: false,
            default_value: None,
            extra: String::new(),
            generated: None,
        }
    }

    fn generated_inventory_column(name: &str) -> crate::inventory::ColumnInventory {
        crate::inventory::ColumnInventory {
            generated: Some(crate::inventory::GeneratedColumn {
                expression: "`name`".to_string(),
                generation_kind: "VIRTUAL".to_string(),
            }),
            ..inventory_column(name)
        }
    }

    fn unique_path(file_name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        path.push(format!("mariadb-mysql-cdc-{nanos}-{file_name}"));
        path
    }

    #[derive(Default)]
    struct MemoryProgressStore {
        progress: RefCell<SnapshotProgress>,
    }

    impl SnapshotProgressStore for MemoryProgressStore {
        fn load(&self) -> Result<SnapshotProgress, SnapshotError> {
            Ok(self.progress.borrow().clone())
        }

        fn save(&self, progress: &SnapshotProgress) -> Result<(), SnapshotError> {
            *self.progress.borrow_mut() = progress.clone();
            Ok(())
        }
    }

    struct FakeSnapshotSource {
        chunks: RefCell<VecDeque<Vec<SnapshotRow>>>,
    }

    impl FakeSnapshotSource {
        fn new(chunks: Vec<Vec<SnapshotRow>>) -> Self {
            Self {
                chunks: RefCell::new(chunks.into()),
            }
        }
    }

    impl SnapshotSource for FakeSnapshotSource {
        fn read_chunk(&self, _request: &ChunkRequest) -> Result<Vec<SnapshotRow>, SnapshotError> {
            Ok(self.chunks.borrow_mut().pop_front().unwrap_or_default())
        }
    }

    #[derive(Default)]
    struct FakeSnapshotTarget {
        rows: Vec<SnapshotRow>,
    }

    impl SnapshotTarget for FakeSnapshotTarget {
        fn write_rows(&mut self, rows: &[SnapshotRow]) -> Result<(), SnapshotError> {
            self.rows.extend_from_slice(rows);
            Ok(())
        }
    }
}
