use crate::conflict_repair::CanonicalForeignKeyRow;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SchemaInventory {
    pub schema: String,
    pub tables: Vec<TableInventory>,
    #[serde(default)]
    pub indexes: Vec<IndexInventory>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyInventory>,
    pub views: Vec<ViewInventory>,
    pub triggers: Vec<TriggerInventory>,
    pub routines: Vec<RoutineInventory>,
    pub events: Vec<EventInventory>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TableInventory {
    pub name: String,
    pub table_type: String,
    pub engine: Option<String>,
    pub collation: Option<String>,
    pub primary_key: Vec<String>,
    pub columns: Vec<ColumnInventory>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ColumnInventory {
    pub name: String,
    pub ordinal_position: u32,
    pub column_type: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub default_value: Option<String>,
    pub extra: String,
    #[serde(default)]
    pub comment: String,
    pub generated: Option<GeneratedColumn>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GeneratedColumn {
    pub expression: String,
    pub generation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IndexInventory {
    pub table: String,
    pub name: String,
    pub unique: bool,
    pub index_type: String,
    #[serde(default)]
    pub visible: bool,
    #[serde(default)]
    pub comment: Option<String>,
    pub columns: Vec<IndexColumnInventory>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IndexColumnInventory {
    pub name: String,
    pub sequence: u32,
    pub prefix_length: Option<u32>,
    pub collation: Option<String>,
    #[serde(default)]
    pub order: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ForeignKeyInventory {
    pub table: String,
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ViewInventory {
    pub name: String,
    pub definition: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TriggerInventory {
    pub name: String,
    pub table: String,
    pub timing: String,
    pub event: String,
    pub statement: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutineInventory {
    pub name: String,
    pub routine_type: String,
    pub definition: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventInventory {
    pub name: String,
    pub status: String,
    pub definition: String,
}

pub trait InventoryReader {
    fn read_tables(&self, schema: &str) -> Result<Vec<TableRow>, InventoryError>;
    fn read_columns(&self, schema: &str) -> Result<Vec<ColumnRow>, InventoryError>;
    fn read_primary_keys(&self, schema: &str) -> Result<Vec<PrimaryKeyRow>, InventoryError>;
    fn read_indexes(&self, schema: &str) -> Result<Vec<IndexRow>, InventoryError>;
    fn read_foreign_keys(&self, _schema: &str) -> Result<Vec<ForeignKeyRow>, InventoryError> {
        Ok(Vec::new())
    }
    fn read_canonical_foreign_keys(
        &self,
        _schema: &str,
    ) -> Result<Vec<CanonicalForeignKeyRow>, InventoryError> {
        Ok(Vec::new())
    }
    fn read_views(&self, schema: &str) -> Result<Vec<ViewRow>, InventoryError>;
    fn read_triggers(&self, schema: &str) -> Result<Vec<TriggerRow>, InventoryError>;
    fn read_routines(&self, schema: &str) -> Result<Vec<RoutineRow>, InventoryError>;
    fn read_events(&self, schema: &str) -> Result<Vec<EventRow>, InventoryError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryEndpointRole {
    Source,
    Target,
}

impl InventoryEndpointRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Target => "target",
        }
    }
}

#[derive(Clone, Debug)]
pub struct InventoryConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub endpoint_role: InventoryEndpointRole,
    pub use_tls: bool,
    pub tls_ca_file: Option<String>,
    pub max_connection_age: Duration,
}

impl Default for InventoryConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3306,
            user: String::new(),
            password: String::new(),
            endpoint_role: InventoryEndpointRole::Source,
            use_tls: false,
            tls_ca_file: None,
            max_connection_age: Duration::from_secs(300),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceBinlogSettings {
    pub format: String,
    pub row_image: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableRuntimeMetadata {
    pub row_count: u64,
    pub auto_increment: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceMasterCoordinate {
    pub file: String,
    pub position: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaDefaults {
    pub character_set: String,
    pub collation: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableRow {
    pub table_name: String,
    pub table_type: String,
    pub engine: Option<String>,
    pub table_collation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnRow {
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: u32,
    pub column_type: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
    pub extra: String,
    pub column_comment: String,
    pub generation_expression: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryKeyRow {
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRow {
    pub table_name: String,
    pub index_name: String,
    pub non_unique: bool,
    pub index_type: String,
    pub sequence: u32,
    pub column_name: Option<String>,
    pub prefix_length: Option<u32>,
    pub collation: Option<String>,
    pub visible: bool,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForeignKeyRow {
    pub table_name: String,
    pub constraint_name: String,
    pub column_name: String,
    pub sequence: u32,
    pub referenced_table: String,
    pub referenced_column: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewRow {
    pub table_name: String,
    pub view_definition: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TriggerRow {
    pub trigger_name: String,
    pub event_manipulation: String,
    pub action_timing: String,
    pub event_object_table: String,
    pub action_statement: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutineRow {
    pub routine_name: String,
    pub routine_type: String,
    pub routine_definition: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventRow {
    pub event_name: String,
    pub status: String,
    pub event_definition: String,
}

#[derive(Debug)]
pub struct InventoryError {
    message: String,
}

impl InventoryError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for InventoryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for InventoryError {}
