use super::ddl_replay_journal::DdlFamily;
use crate::inventory::{
    InventoryConfig, MariaDbInventoryReader, SourceMasterCoordinate, build_inventory,
};
use model::{DdlOperation, SemanticSchemaSnapshot, TableRuntimeState};

mod canonical;
mod model;
mod parser;
#[cfg(test)]
mod tests;
mod tokenizer;
mod transform;

#[cfg(test)]
pub(super) use canonical::canonical_absent_state;
pub use canonical::{
    build_semantic_evidence, observe_operation_state, supports_automatic_semantic_recovery,
};
pub use model::{DdlObjectKind, DdlSemanticEvidence};
#[cfg(test)]
pub(super) use parser::parse_simple_index_ddl;
pub use parser::{parse_ddl_operation, supports_automatic_index_ddl};
#[cfg(test)]
pub(super) use tokenizer::tokenize_ddl;
pub use transform::{
    DdlTransformation, supports_rename_columns_if_exists, transform_rename_columns_if_exists,
};

pub trait DdlSemanticInventory {
    fn transform_sql(&self, sql: &str) -> Result<DdlTransformation, String>;

    fn capture_evidence(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
    ) -> Result<DdlSemanticEvidence, String>;
    fn observe_target_state(&self, sql: &str) -> Result<String, String>;
}

pub struct LiveDdlSemanticInventory {
    source: MariaDbInventoryReader,
    target: MariaDbInventoryReader,
    source_schema: String,
    target_schema: String,
}

impl LiveDdlSemanticInventory {
    pub fn new(
        source: InventoryConfig,
        target: InventoryConfig,
        source_schema: String,
        target_schema: String,
    ) -> Self {
        Self {
            source: MariaDbInventoryReader::new(source),
            target: MariaDbInventoryReader::new(target),
            source_schema,
            target_schema,
        }
    }

    fn snapshot(
        reader: &MariaDbInventoryReader,
        schema: &str,
        operation: &DdlOperation,
    ) -> Result<SemanticSchemaSnapshot, String> {
        let inventory = build_inventory(schema, reader)
            .map_err(|error| format!("failed to build semantic inventory for {schema}: {error}"))?;
        let table_runtime = read_affected_runtime(reader, schema, operation, &inventory)?;
        Ok(SemanticSchemaSnapshot {
            inventory,
            table_runtime,
        })
    }
}

fn read_affected_runtime(
    reader: &MariaDbInventoryReader,
    schema: &str,
    operation: &DdlOperation,
    inventory: &crate::inventory::SchemaInventory,
) -> Result<std::collections::BTreeMap<String, TableRuntimeState>, String> {
    let mut runtime = std::collections::BTreeMap::new();
    for table in affected_tables(operation) {
        if inventory.tables.iter().any(|item| item.name == table) {
            let value = reader.read_table_runtime(schema, table).map_err(|error| {
                format!("failed to read semantic runtime for {schema}.{table}: {error}")
            })?;
            runtime.insert(
                table.to_string(),
                TableRuntimeState {
                    row_count: value.row_count,
                    auto_increment: value.auto_increment,
                },
            );
        }
    }
    Ok(runtime)
}

impl DdlSemanticInventory for LiveDdlSemanticInventory {
    fn transform_sql(&self, sql: &str) -> Result<DdlTransformation, String> {
        if supports_automatic_index_ddl(sql) {
            return Ok(DdlTransformation {
                version: transform::DDL_TRANSFORMATION_VERSION,
                target_sql: Some(sql.trim().trim_end_matches(';').trim().to_string()),
            });
        }
        if !supports_rename_columns_if_exists(sql) {
            return Err("MariaDB DDL translator does not support this statement".to_string());
        }
        let operation = parse_ddl_operation(sql)?;
        let inventory = build_inventory(&self.target_schema, &self.target).map_err(|error| {
            format!(
                "failed to build target inventory for DDL transformation in {}: {error}",
                self.target_schema
            )
        })?;
        let table = inventory
            .tables
            .iter()
            .find(|table| table.name == operation.primary_object)
            .ok_or_else(|| {
                format!(
                    "target table {}.{} is missing for DDL transformation",
                    self.target_schema, operation.primary_object
                )
            })?;
        let columns = table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        transform_rename_columns_if_exists(sql, &columns)
    }

    fn capture_evidence(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
    ) -> Result<DdlSemanticEvidence, String> {
        let operation = parse_ddl_operation(sql)?;
        let target_before = Self::snapshot(&self.target, &self.target_schema, &operation)?;
        if operation.object_kind == DdlObjectKind::Index {
            return capture_index_evidence(self, &operation, &target_before);
        }
        capture_source_evidence(
            self,
            &operation,
            &target_before,
            source_file,
            event_end_position,
        )
    }

    fn observe_target_state(&self, sql: &str) -> Result<String, String> {
        let operation = parse_ddl_operation(sql)?;
        let before = Self::snapshot(&self.target, &self.target_schema, &operation)?;
        let after = Self::snapshot(&self.target, &self.target_schema, &operation)?;
        validate_target_snapshot_consistency(&before, &after)?;
        observe_operation_state(&before, &operation)
    }
}

fn capture_index_evidence(
    inventory: &LiveDdlSemanticInventory,
    operation: &DdlOperation,
    target_before: &SemanticSchemaSnapshot,
) -> Result<DdlSemanticEvidence, String> {
    let target_after =
        LiveDdlSemanticInventory::snapshot(&inventory.target, &inventory.target_schema, operation)?;
    validate_target_snapshot_consistency(target_before, &target_after)?;
    build_semantic_evidence(operation, target_before, target_before)
}

fn capture_source_evidence(
    inventory: &LiveDdlSemanticInventory,
    operation: &DdlOperation,
    target_before: &SemanticSchemaSnapshot,
    source_file: &str,
    event_end_position: u64,
) -> Result<DdlSemanticEvidence, String> {
    let before = inventory
        .source
        .read_source_master_coordinate()
        .map_err(|error| {
            format!("failed to read source coordinate before semantic inventory: {error}")
        })?;
    let source =
        LiveDdlSemanticInventory::snapshot(&inventory.source, &inventory.source_schema, operation)?;
    let after = inventory
        .source
        .read_source_master_coordinate()
        .map_err(|error| {
            format!("failed to read source coordinate after semantic inventory: {error}")
        })?;
    validate_source_snapshot_coordinate(source_file, event_end_position, &before, &after)?;
    let target_after =
        LiveDdlSemanticInventory::snapshot(&inventory.target, &inventory.target_schema, operation)?;
    validate_target_snapshot_consistency(target_before, &target_after)?;
    build_semantic_evidence(operation, target_before, &source)
}

fn affected_tables(operation: &DdlOperation) -> Vec<&str> {
    match operation.family {
        DdlFamily::Rename => vec![
            operation.primary_object.as_str(),
            operation.secondary_object.as_deref().unwrap_or_default(),
        ],
        DdlFamily::Index => operation.secondary_object.as_deref().into_iter().collect(),
        _ if operation.object_kind == DdlObjectKind::Table => {
            vec![operation.primary_object.as_str()]
        }
        _ => Vec::new(),
    }
}

pub fn validate_target_snapshot_consistency(
    before: &SemanticSchemaSnapshot,
    after: &SemanticSchemaSnapshot,
) -> Result<(), String> {
    if before == after {
        return Ok(());
    }
    Err("target semantic inventory changed during evidence capture".to_string())
}

pub fn validate_source_snapshot_coordinate(
    expected_file: &str,
    expected_position: u64,
    before: &SourceMasterCoordinate,
    after: &SourceMasterCoordinate,
) -> Result<(), String> {
    if before.file == expected_file && before.position == expected_position && after == before {
        return Ok(());
    }
    Err(format!(
        "source semantic inventory is not event-position consistent: expected {}:{} before={}:{} after={}:{}",
        expected_file, expected_position, before.file, before.position, after.file, after.position,
    ))
}
