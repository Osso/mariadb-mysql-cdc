use super::ddl_replay_journal::DdlFamily;
use super::query_charset_context::{SourceSqlMode, decode_query_charset_context};
use crate::inventory::{
    InventoryConfig, MariaDbInventoryReader, SourceMasterCoordinate, build_inventory,
};
use model::{DdlOperation, TableRuntimeState};
use std::collections::BTreeSet;

mod canonical;
mod model;
mod parser;
mod table_operations;
#[cfg(test)]
mod tests;
mod tokenizer;
mod transform;

#[cfg(test)]
pub(super) use canonical::canonical_absent_state;
pub use canonical::{
    build_assistant_reply_reports_create_evidence, build_semantic_evidence,
    observe_operation_state, supports_automatic_semantic_recovery,
};
#[cfg(test)]
pub(super) use canonical::{
    build_fenced_create_table_evidence, build_source_only_procedure_create_evidence,
};
pub(super) use model::SemanticSchemaSnapshot;
pub use model::{DdlObjectKind, DdlSemanticEvidence};
use parser::parse_modeled_index_ddl;
#[cfg(test)]
pub(super) use parser::parse_simple_index_ddl;
pub use parser::{
    parse_ddl_operation, parse_ddl_operation_with_mode, supports_automatic_index_ddl,
};
#[cfg(test)]
pub(super) use tokenizer::tokenize_ddl;
pub use transform::{
    DDL_TRANSFORMATION_VERSION, DdlTransformation, parse_fixture_create_table_with_mode,
    parse_production_alter_table_ast_with_mode, render_modeled_index_ddl,
    supports_assistant_reply_reports_create, supports_drop_columns_if_exists,
    supports_drop_procedure, supports_drop_trigger_if_exists, supports_fixture_create_table,
    supports_production_alter_table, supports_rename_columns_if_exists,
    supports_source_only_release_move_procedure_create, transform_assistant_reply_reports_create,
    transform_drop_columns_if_exists, transform_drop_procedure, transform_drop_trigger_if_exists,
    transform_generated_schema_ddl, transform_production_alter_table,
    transform_rename_columns_if_exists, transform_source_only_release_move_procedure_create,
};

pub trait DdlSemanticInventory {
    fn transform_sql(&self, sql: &str) -> Result<DdlTransformation, String>;
    fn transform_sql_with_query_context(
        &self,
        sql: &str,
        _status_variables: &[u8],
    ) -> Result<DdlTransformation, String> {
        self.transform_sql(sql)
    }

    fn capture_evidence(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
    ) -> Result<DdlSemanticEvidence, String>;
    fn capture_evidence_with_query_context(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
        _status_variables: &[u8],
    ) -> Result<DdlSemanticEvidence, String> {
        self.capture_evidence(sql, source_file, event_end_position)
    }

    fn observe_target_state(&self, sql: &str) -> Result<String, String>;
    fn observe_target_state_with_evidence(
        &self,
        sql: &str,
        _evidence: &DdlSemanticEvidence,
    ) -> Result<String, String> {
        self.observe_target_state(sql)
    }
    fn expected_target_state(&self, sql: &str) -> Result<String, String>;
    fn expected_target_state_with_evidence(
        &self,
        sql: &str,
        _evidence: &DdlSemanticEvidence,
    ) -> Result<String, String> {
        self.expected_target_state(sql)
    }
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

    fn read_target_inventory(&self) -> Result<crate::inventory::SchemaInventory, String> {
        build_inventory(&self.target_schema, &self.target).map_err(|error| {
            format!(
                "failed to build target inventory for DDL transformation in {}: {error}",
                self.target_schema
            )
        })
    }

    fn read_target_procedure_names(&self) -> Result<Vec<String>, String> {
        Ok(self
            .read_target_inventory()?
            .routines
            .into_iter()
            .filter(|routine| routine.routine_type == "PROCEDURE")
            .map(|routine| routine.name)
            .collect())
    }

    fn read_target_column_names(&self, table_name: &str) -> Result<Vec<String>, String> {
        self.read_target_inventory()?
            .tables
            .into_iter()
            .find(|table| table.name == table_name)
            .ok_or_else(|| {
                format!(
                    "target table {}.{table_name} is missing for DDL transformation",
                    self.target_schema
                )
            })
            .map(|table| {
                table
                    .columns
                    .into_iter()
                    .map(|column| column.name)
                    .collect()
            })
    }

    fn read_target_trigger_names(&self) -> Result<Vec<String>, String> {
        Ok(self
            .read_target_inventory()?
            .triggers
            .into_iter()
            .map(|trigger| trigger.name)
            .collect())
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DdlTranslationProvenance {
    Streamed,
    ModeledPlanner,
}

pub(crate) fn translate_ddl(
    sql: &str,
    target_columns: &[String],
) -> Result<DdlTransformation, String> {
    translate_ddl_with_provenance(sql, target_columns, DdlTranslationProvenance::Streamed)
}

pub(crate) fn translate_modeled_ddl(
    sql: &str,
    target_columns: &[String],
) -> Result<DdlTransformation, String> {
    translate_ddl_with_provenance(
        sql,
        target_columns,
        DdlTranslationProvenance::ModeledPlanner,
    )
}

fn translate_ddl_with_provenance(
    sql: &str,
    target_columns: &[String],
    provenance: DdlTranslationProvenance,
) -> Result<DdlTransformation, String> {
    if let Some(transformation) = translate_index_ddl(sql, provenance) {
        return transformation;
    }
    if let Ok(operation) = parse_ddl_operation(sql)
        && operation.table_operation_ast.is_some()
    {
        return table_operations::render(&operation);
    }
    let target_objects = target_columns.iter().cloned().collect();
    if let Some(transformation) = translate_create_or_routine_ddl(sql, &target_objects) {
        return transformation;
    }
    if let Some(transformation) = translate_alter_ddl(sql, &target_objects) {
        return transformation;
    }
    match provenance {
        DdlTranslationProvenance::ModeledPlanner => transform_generated_schema_ddl(sql),
        DdlTranslationProvenance::Streamed => {
            Err("streamed DDL family is unsupported without an existing parsed model".to_string())
        }
    }
}

fn translate_index_ddl(
    sql: &str,
    provenance: DdlTranslationProvenance,
) -> Option<Result<DdlTransformation, String>> {
    let parsed = match provenance {
        DdlTranslationProvenance::Streamed => parser::parse_simple_index_ddl(sql),
        DdlTranslationProvenance::ModeledPlanner => parse_modeled_index_ddl(sql),
    };
    let Ok(index) = parsed else {
        return None;
    };
    if provenance == DdlTranslationProvenance::ModeledPlanner && index.unique {
        return Some(render_modeled_index_ddl(&index, sql));
    }
    Some(Ok(DdlTransformation {
        version: transform::DDL_TRANSFORMATION_VERSION,
        target_sql: Some(sql.trim().trim_end_matches(';').trim().to_string()),
    }))
}

fn translate_create_or_routine_ddl(
    sql: &str,
    target_objects: &BTreeSet<String>,
) -> Option<Result<DdlTransformation, String>> {
    if supports_assistant_reply_reports_create(sql) {
        return Some(transform_assistant_reply_reports_create(sql));
    }
    if supports_fixture_create_table(sql) {
        return Some(transform::transform_fixture_create_table(sql));
    }
    if supports_source_only_release_move_procedure_create(sql) {
        return Some(transform_source_only_release_move_procedure_create(sql));
    }
    if supports_drop_procedure(sql) {
        return Some(transform_drop_procedure(sql, target_objects));
    }
    supports_drop_trigger_if_exists(sql)
        .then(|| transform_drop_trigger_if_exists(sql, target_objects))
}

fn translate_alter_ddl(
    sql: &str,
    target_objects: &BTreeSet<String>,
) -> Option<Result<DdlTransformation, String>> {
    if supports_production_alter_table(sql) {
        return Some(transform_production_alter_table(sql));
    }
    if supports_drop_columns_if_exists(sql) {
        return Some(transform_drop_columns_if_exists(sql, target_objects));
    }
    supports_rename_columns_if_exists(sql)
        .then(|| transform_rename_columns_if_exists(sql, target_objects))
}

fn parse_semantic_operation(sql: &str) -> Result<DdlOperation, String> {
    parse_semantic_operation_with_mode(sql, SourceSqlMode(None))
}

fn parse_semantic_operation_with_mode(
    sql: &str,
    mode: SourceSqlMode,
) -> Result<DdlOperation, String> {
    if supports_assistant_reply_reports_create(sql) {
        return Ok(DdlOperation {
            family: DdlFamily::Table,
            object_kind: DdlObjectKind::Table,
            primary_object: "assistant_reply_reports".to_string(),
            secondary_object: None,
            index_ast: None,
            create_table_ast: None,
            alter_table_ast: None,
            table_operation_ast: None,
        });
    }
    if supports_source_only_release_move_procedure_create(sql) {
        return Ok(DdlOperation {
            family: DdlFamily::Procedure,
            object_kind: DdlObjectKind::Procedure,
            primary_object: "apply_release_move_purchase_repair".to_string(),
            secondary_object: None,
            index_ast: None,
            create_table_ast: None,
            alter_table_ast: None,
            table_operation_ast: None,
        });
    }
    parse_ddl_operation_with_mode(sql, mode)
}

fn capture_specialized_evidence(
    inventory: &LiveDdlSemanticInventory,
    sql: &str,
    operation: &DdlOperation,
    target_before: &SemanticSchemaSnapshot,
) -> Option<Result<DdlSemanticEvidence, String>> {
    if supports_assistant_reply_reports_create(sql) {
        return Some(capture_assistant_reply_reports_create_evidence(
            inventory,
            operation,
            target_before,
        ));
    }
    supports_source_only_release_move_procedure_create(sql)
        .then(|| capture_source_only_procedure_create_evidence(inventory, operation, target_before))
}

fn capture_early_evidence(
    inventory: &LiveDdlSemanticInventory,
    sql: &str,
    operation: &DdlOperation,
    target_before: &SemanticSchemaSnapshot,
    source_file: &str,
    event_end_position: u64,
) -> Option<Result<DdlSemanticEvidence, String>> {
    if let Some(evidence) = capture_specialized_evidence(inventory, sql, operation, target_before) {
        return Some(evidence);
    }
    operation.create_table_ast.as_ref()?;
    Some(capture_fenced_create_table_evidence(
        inventory,
        operation,
        target_before,
        source_file,
        event_end_position,
    ))
}

fn requires_translated_evidence(sql: &str, operation: &DdlOperation) -> bool {
    operation.object_kind == DdlObjectKind::Index
        || operation.alter_table_ast.is_some()
        || operation.table_operation_ast.is_some()
        || supports_drop_procedure(sql)
        || supports_drop_trigger_if_exists(sql)
}

impl DdlSemanticInventory for LiveDdlSemanticInventory {
    fn transform_sql(&self, sql: &str) -> Result<DdlTransformation, String> {
        self.transform_sql_mode(sql, SourceSqlMode(None))
    }

    fn transform_sql_with_query_context(
        &self,
        sql: &str,
        status_variables: &[u8],
    ) -> Result<DdlTransformation, String> {
        let mode = SourceSqlMode(
            decode_query_charset_context(status_variables)
                .map_err(|error| format!("DDL SQL mode context: {error}"))?
                .sql_mode,
        );
        self.transform_sql_mode(sql, mode)
    }

    fn capture_evidence(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
    ) -> Result<DdlSemanticEvidence, String> {
        let operation = parse_semantic_operation(sql)?;
        self.capture_evidence_for_operation(sql, source_file, event_end_position, &operation)
    }

    fn capture_evidence_with_query_context(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
        status_variables: &[u8],
    ) -> Result<DdlSemanticEvidence, String> {
        let context = decode_query_charset_context(status_variables)
            .map_err(|error| format!("DDL SQL mode context: {error}"))?;
        let mode = SourceSqlMode(context.sql_mode);
        let operation = parse_semantic_operation_with_mode(sql, mode)?;
        let mut evidence =
            self.capture_query_evidence(sql, source_file, event_end_position, &operation, context)?;
        record_source_sql_mode(&mut evidence, mode)?;
        Ok(evidence)
    }

    fn observe_target_state(&self, sql: &str) -> Result<String, String> {
        let operation = parse_semantic_operation(sql)?;
        self.observe_operation(&operation)
    }

    fn observe_target_state_with_evidence(
        &self,
        sql: &str,
        evidence: &DdlSemanticEvidence,
    ) -> Result<String, String> {
        let mode = source_mode_from_evidence(evidence)?;
        let operation = parse_semantic_operation_with_mode(sql, mode)?;
        self.observe_operation(&operation)
    }

    fn expected_target_state(&self, sql: &str) -> Result<String, String> {
        self.expected_create_state(&parse_semantic_operation(sql)?)
    }

    fn expected_target_state_with_evidence(
        &self,
        sql: &str,
        evidence: &DdlSemanticEvidence,
    ) -> Result<String, String> {
        let mode = source_mode_from_evidence(evidence)?;
        self.expected_create_state(&parse_semantic_operation_with_mode(sql, mode)?)
    }
}

impl LiveDdlSemanticInventory {
    fn expected_create_state(&self, operation: &DdlOperation) -> Result<String, String> {
        let ast = operation
            .create_table_ast
            .as_ref()
            .ok_or_else(|| "blocked recovery requires modeled CREATE TABLE".to_string())?;
        let defaults = canonical::explicit_create_table_defaults(ast).ok_or_else(|| {
            "blocked recovery requires explicit CREATE TABLE defaults".to_string()
        })?;
        canonical::expected_create_table_post_state(ast, &defaults, &self.target_schema)
    }

    fn transform_sql_mode(
        &self,
        sql: &str,
        mode: SourceSqlMode,
    ) -> Result<DdlTransformation, String> {
        if let Ok(ast) = transform::parse_production_alter_table_ast_with_mode(sql, mode) {
            let alters_default = ast.clauses.iter().any(|clause| {
                matches!(clause, model::ParsedAlterClause::AlterColumnDefault { .. })
            });
            if alters_default {
                let operation = parse_semantic_operation_with_mode(sql, mode)?;
                let before = Self::snapshot(&self.target, &self.target_schema, &operation)?;
                let after = Self::snapshot(&self.target, &self.target_schema, &operation)?;
                validate_target_snapshot_consistency(&before, &after)?;
                return transform::transform_production_alter_table_with_target_mode(
                    sql, &before, mode,
                );
            }
            return transform::transform_production_alter_table_with_mode(sql, mode);
        }
        let target_objects = if supports_drop_procedure(sql) {
            self.read_target_procedure_names()?
        } else if supports_drop_trigger_if_exists(sql) {
            self.read_target_trigger_names()?
        } else if supports_drop_columns_if_exists(sql) || supports_rename_columns_if_exists(sql) {
            let operation = parse_ddl_operation(sql)?;
            self.read_target_column_names(&operation.primary_object)?
        } else {
            Vec::new()
        };
        translate_ddl(sql, &target_objects)
    }

    fn capture_evidence_for_operation(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
        operation: &DdlOperation,
    ) -> Result<DdlSemanticEvidence, String> {
        let target_before = Self::snapshot(&self.target, &self.target_schema, operation)?;
        if let Some(evidence) = capture_early_evidence(
            self,
            sql,
            operation,
            &target_before,
            source_file,
            event_end_position,
        ) {
            return evidence;
        }
        if requires_translated_evidence(sql, operation) {
            return capture_translated_evidence(self, operation, &target_before);
        }
        capture_source_evidence(
            self,
            operation,
            &target_before,
            source_file,
            event_end_position,
        )
    }

    fn capture_query_evidence(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
        operation: &DdlOperation,
        context: super::query_charset_context::QueryCharsetContext,
    ) -> Result<DdlSemanticEvidence, String> {
        let Some(ast) = operation.create_table_ast.as_ref() else {
            return self.capture_evidence_for_operation(
                sql,
                source_file,
                event_end_position,
                operation,
            );
        };
        if ast.character_set.is_none() && ast.collation.is_none() {
            return self.capture_database_default_create(&operation);
        }
        if ast.character_set.as_deref() != Some("utf8mb4") || ast.collation.is_some() {
            return self.capture_evidence_for_operation(
                sql,
                source_file,
                event_end_position,
                operation,
            );
        }
        let overrides = context
            .character_set_collations
            .ok_or_else(|| "historical CREATE charset override map is absent".to_string())?;
        // MariaDB identifies utf8mb4 by its primary collation ID, 45.
        let collation_id = overrides
            .iter()
            .find(|(from, _)| *from == 45)
            .map(|(_, to)| *to)
            .unwrap_or(45);
        let mut defaults = self
            .source
            .read_collation_identity(collation_id)
            .map_err(|error| format!("historical collation identity: {error}"))?;
        if defaults.character_set != "utf8mb4" {
            return Err("historical collation does not belong to utf8mb4".to_string());
        }
        if defaults.collation.starts_with("uca1400_") {
            defaults.collation = format!("utf8mb4_{}", defaults.collation);
        }
        let source_collation = defaults.collation.clone();
        defaults.collation = crate::sync_schema::canonical_collation(&defaults.collation);
        let before = Self::snapshot(&self.target, &self.target_schema, &operation)?;
        let after = Self::snapshot(&self.target, &self.target_schema, &operation)?;
        validate_target_snapshot_consistency(&before, &after)?;
        let mut evidence =
            canonical::build_resolved_create_table_evidence(&operation, &before, &defaults)?;
        record_query_charset_context(&mut evidence, overrides, collation_id, source_collation)?;
        Ok(evidence)
    }

    fn observe_operation(&self, operation: &DdlOperation) -> Result<String, String> {
        let before = Self::snapshot(&self.target, &self.target_schema, operation)?;
        let after = Self::snapshot(&self.target, &self.target_schema, operation)?;
        validate_target_snapshot_consistency(&before, &after)?;
        if let Some(ast) = operation.create_table_ast.as_ref() {
            self.validate_observed_create_foreign_keys(ast, &before)?;
        }
        if let Some(ast) = operation.alter_table_ast.as_ref() {
            self.validate_observed_json_alias_checks(ast, &before)?;
            self.validate_observed_alter_foreign_keys(ast)?;
        }
        observe_operation_state(&before, operation)
    }
}

impl LiveDdlSemanticInventory {
    fn capture_database_default_create(
        &self,
        operation: &DdlOperation,
    ) -> Result<DdlSemanticEvidence, String> {
        // MariaDB resolves omitted CREATE charset against the table's database, not
        // Q_CHARSET.server or a later source-head schema. Replay uses the target pre-state.
        let before = Self::snapshot(&self.target, &self.target_schema, operation)?;
        let defaults = self
            .target
            .read_schema_defaults(&self.target_schema)
            .map_err(|error| format!("read target CREATE database defaults: {error}"))?;
        let repeated_defaults = self
            .target
            .read_schema_defaults(&self.target_schema)
            .map_err(|error| format!("reread target CREATE database defaults: {error}"))?;
        let after = Self::snapshot(&self.target, &self.target_schema, operation)?;
        validate_target_snapshot_consistency(&before, &after)?;
        if defaults != repeated_defaults {
            return Err("target CREATE database defaults changed during evidence capture".into());
        }
        let mut evidence =
            canonical::build_resolved_create_table_evidence(operation, &before, &defaults)?;
        let mut ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast)
            .map_err(|error| format!("CREATE evidence JSON: {error}"))?;
        ast["inherited_database_defaults"] = serde_json::json!({
            "character_set": defaults.character_set, "collation": defaults.collation,
        });
        evidence.canonical_ast = serde_json::to_string(&ast).map_err(|error| error.to_string())?;
        Ok(evidence)
    }

    fn validate_observed_json_alias_checks(
        &self,
        ast: &model::ParsedAlterTableAst,
        snapshot: &SemanticSchemaSnapshot,
    ) -> Result<(), String> {
        let Some(table) = snapshot
            .inventory
            .tables
            .iter()
            .find(|table| table.name == ast.table)
        else {
            return Ok(());
        };
        let columns = ast
            .clauses
            .iter()
            .filter_map(|clause| match clause {
                model::ParsedAlterClause::AddColumn(column)
                    if column.data_type == "json"
                        && table
                            .columns
                            .iter()
                            .any(|existing| existing.name == column.name) =>
                {
                    Some(column)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Ok(());
        }
        let before = self
            .target
            .read_table_check_constraints(&self.target_schema, &ast.table)
            .map_err(|error| format!("failed to read JSON alias CHECK constraints: {error}"))?;
        let after = self
            .target
            .read_table_check_constraints(&self.target_schema, &ast.table)
            .map_err(|error| format!("failed to reread JSON alias CHECK constraints: {error}"))?;
        if before != after {
            return Err("JSON alias CHECK constraints changed during observation".into());
        }
        canonical::validate_json_alias_checks(&columns, &before)
    }

    fn validate_observed_alter_foreign_keys(
        &self,
        ast: &model::ParsedAlterTableAst,
    ) -> Result<(), String> {
        let adds_foreign_key = ast
            .clauses
            .iter()
            .any(|clause| matches!(clause, model::ParsedAlterClause::AddForeignKey(_)));
        if !adds_foreign_key {
            return Ok(());
        }
        let keys = crate::inventory::build_canonical_foreign_key_inventory(
            &self.target_schema,
            &self.target,
        )
        .map_err(|error| format!("failed to read ALTER foreign-key actions: {error}"))?;
        canonical::validate_alter_foreign_keys(ast, &self.target_schema, &keys)
    }

    fn validate_observed_create_foreign_keys(
        &self,
        ast: &model::ParsedCreateTableAst,
        snapshot: &SemanticSchemaSnapshot,
    ) -> Result<(), String> {
        let exists = snapshot
            .inventory
            .tables
            .iter()
            .any(|table| table.name == ast.name);
        if ast.foreign_keys.is_empty() || !exists {
            return Ok(());
        }
        let keys = crate::inventory::build_canonical_foreign_key_inventory(
            &self.target_schema,
            &self.target,
        )
        .map_err(|error| format!("failed to read CREATE foreign-key actions: {error}"))?;
        canonical::validate_create_foreign_keys(ast, &self.target_schema, &keys)
    }
}

#[cfg(test)]
mod source_mode_evidence_tests {
    use super::*;

    #[test]
    fn evidence_keeps_immutable_mode_and_rejects_missing_or_malformed_mode() {
        let mut evidence = DdlSemanticEvidence {
            transformation_version: "v1".into(),
            generated_sql: None,
            canonical_ast: "{}".into(),
            pre_state: "before".into(),
            expected_post_state: "after".into(),
        };
        record_source_sql_mode(&mut evidence, SourceSqlMode(Some(1 << 20))).unwrap();
        assert_eq!(
            source_mode_from_evidence(&evidence).unwrap(),
            SourceSqlMode(Some(1 << 20))
        );
        evidence.canonical_ast = "{}".into();
        assert_eq!(
            source_mode_from_evidence(&evidence).unwrap(),
            SourceSqlMode(None)
        );
        evidence.canonical_ast = r#"{"source_sql_mode":"invalid"}"#.into();
        assert!(source_mode_from_evidence(&evidence).is_err());
        record_source_sql_mode(&mut evidence, SourceSqlMode(Some(0))).unwrap();
        assert!(validate_replayed_source_mode(&evidence, &[1, 0, 0, 16, 0, 0, 0, 0, 0]).is_err());
        assert!(validate_replayed_source_mode(&evidence, &[]).is_err());
        assert!(validate_replayed_source_mode(&evidence, &[1, 0, 0, 0, 0, 0, 0, 0, 0]).is_ok());
    }
}

pub(crate) fn validate_replayed_source_mode(
    evidence: &DdlSemanticEvidence,
    status_variables: &[u8],
) -> Result<(), String> {
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast)
        .map_err(|error| format!("DDL evidence JSON: {error}"))?;
    // Pre-SQL_MODE journal rows have no persisted mode. Their parser still rejects
    // backslash-bearing literals without source context.
    if ast.get("source_sql_mode").is_none() {
        return Ok(());
    }
    let expected = source_mode_from_evidence(evidence)?;
    let actual = SourceSqlMode(
        decode_query_charset_context(status_variables)
            .map_err(|error| format!("replayed DDL SQL_MODE: {error}"))?
            .sql_mode,
    );
    if expected != actual {
        return Err("replayed DDL source SQL_MODE differs from prepared evidence".into());
    }
    Ok(())
}

fn record_source_sql_mode(
    evidence: &mut DdlSemanticEvidence,
    mode: SourceSqlMode,
) -> Result<(), String> {
    let mut ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast)
        .map_err(|error| format!("DDL evidence JSON: {error}"))?;
    ast["source_sql_mode"] = serde_json::json!(mode.0);
    evidence.canonical_ast =
        serde_json::to_string(&ast).map_err(|error| format!("DDL evidence JSON: {error}"))?;
    Ok(())
}

fn source_mode_from_evidence(evidence: &DdlSemanticEvidence) -> Result<SourceSqlMode, String> {
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast)
        .map_err(|error| format!("DDL evidence JSON: {error}"))?;
    let Some(mode) = ast.get("source_sql_mode") else {
        return Ok(SourceSqlMode(None));
    };
    if mode.is_null() {
        return Ok(SourceSqlMode(None));
    }
    mode.as_u64()
        .map(|bits| SourceSqlMode(Some(bits)))
        .ok_or_else(|| "DDL evidence has invalid source SQL_MODE".to_string())
}

fn record_query_charset_context(
    evidence: &mut DdlSemanticEvidence,
    overrides: Vec<(u16, u16)>,
    collation_id: u16,
    source_collation: String,
) -> Result<(), String> {
    let mut ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast)
        .map_err(|error| format!("CREATE evidence JSON: {error}"))?;
    ast["query_charset_context"] = serde_json::json!({
        "character_set_collations": overrides,
        "source_collation_id": collation_id,
        "source_collation": source_collation,
    });
    evidence.canonical_ast =
        serde_json::to_string(&ast).map_err(|error| format!("CREATE evidence JSON: {error}"))?;
    Ok(())
}

fn capture_assistant_reply_reports_create_evidence(
    inventory: &LiveDdlSemanticInventory,
    operation: &DdlOperation,
    target_before: &SemanticSchemaSnapshot,
) -> Result<DdlSemanticEvidence, String> {
    let source_before = inventory
        .source
        .read_source_master_coordinate()
        .map_err(|error| format!("failed to fence source schema: {error}"))?;
    let source =
        LiveDdlSemanticInventory::snapshot(&inventory.source, &inventory.source_schema, operation)?;
    let source_after = inventory
        .source
        .read_source_master_coordinate()
        .map_err(|error| format!("failed to fence source schema: {error}"))?;
    if source_before != source_after {
        return Err(
            "source schema changed during assistant_reply_reports convergence proof".to_string(),
        );
    }
    let target_after =
        LiveDdlSemanticInventory::snapshot(&inventory.target, &inventory.target_schema, operation)?;
    validate_target_snapshot_consistency(target_before, &target_after)?;
    validate_assistant_reply_reports_convergence(&source, target_before)?;
    build_assistant_reply_reports_create_evidence(operation, target_before)
}

fn validate_assistant_reply_reports_convergence(
    source: &SemanticSchemaSnapshot,
    target: &SemanticSchemaSnapshot,
) -> Result<(), String> {
    let table = "assistant_reply_reports";
    let source_table = source
        .inventory
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .ok_or_else(|| format!("source table `{table}` is missing"))?;
    let target_table = target
        .inventory
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .ok_or_else(|| format!("target table `{table}` is missing"))?;
    let expected = crate::sync_schema::expected_target_table_fingerprint(source_table)?;
    let observed = crate::sync_schema::observed_target_table_fingerprint(target_table)?;
    if expected != observed {
        return Err("assistant_reply_reports table definition does not converge".to_string());
    }
    validate_assistant_reply_reports_indexes(source, target, table)?;
    validate_assistant_reply_reports_foreign_keys(source, target, table)
}

fn validate_assistant_reply_reports_indexes(
    source: &SemanticSchemaSnapshot,
    target: &SemanticSchemaSnapshot,
    table: &str,
) -> Result<(), String> {
    let mut source_indexes = source
        .inventory
        .indexes
        .iter()
        .filter(|index| index.table == table)
        .cloned()
        .collect::<Vec<_>>();
    let mut target_indexes = target
        .inventory
        .indexes
        .iter()
        .filter(|index| index.table == table)
        .cloned()
        .collect::<Vec<_>>();
    source_indexes.sort_by(|left, right| left.name.cmp(&right.name));
    target_indexes.sort_by(|left, right| left.name.cmp(&right.name));
    if source_indexes == target_indexes {
        Ok(())
    } else {
        Err("assistant_reply_reports indexes do not converge".to_string())
    }
}

fn validate_assistant_reply_reports_foreign_keys(
    source: &SemanticSchemaSnapshot,
    target: &SemanticSchemaSnapshot,
    table: &str,
) -> Result<(), String> {
    let mut source_foreign_keys = source
        .inventory
        .foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.table == table)
        .cloned()
        .collect::<Vec<_>>();
    let mut target_foreign_keys = target
        .inventory
        .foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.table == table)
        .cloned()
        .collect::<Vec<_>>();
    source_foreign_keys.sort_by(|left, right| left.name.cmp(&right.name));
    target_foreign_keys.sort_by(|left, right| left.name.cmp(&right.name));
    if source_foreign_keys == target_foreign_keys {
        Ok(())
    } else {
        Err("assistant_reply_reports foreign keys do not converge".to_string())
    }
}

fn capture_source_only_procedure_create_evidence(
    inventory: &LiveDdlSemanticInventory,
    operation: &DdlOperation,
    target_before: &SemanticSchemaSnapshot,
) -> Result<DdlSemanticEvidence, String> {
    let target_after =
        LiveDdlSemanticInventory::snapshot(&inventory.target, &inventory.target_schema, operation)?;
    validate_target_snapshot_consistency(target_before, &target_after)?;
    canonical::build_source_only_procedure_create_evidence(operation, target_before)
}

fn capture_fenced_create_table_evidence(
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
            format!("failed to read source coordinate before schema defaults: {error}")
        })?;
    let defaults = inventory
        .source
        .read_schema_defaults(&inventory.source_schema)
        .map_err(|error| {
            format!(
                "failed to read source schema defaults for {}: {error}",
                inventory.source_schema
            )
        })?;
    let after = inventory
        .source
        .read_source_master_coordinate()
        .map_err(|error| {
            format!("failed to read source coordinate after schema defaults: {error}")
        })?;
    let target_after =
        LiveDdlSemanticInventory::snapshot(&inventory.target, &inventory.target_schema, operation)?;
    validate_target_snapshot_consistency(target_before, &target_after)?;
    canonical::build_fenced_create_table_evidence(
        operation,
        target_before,
        &defaults,
        source_file,
        event_end_position,
        &before,
        &after,
    )
}

fn capture_translated_evidence(
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
