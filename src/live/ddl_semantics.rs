use super::ddl_replay_journal::DdlFamily;
use crate::inventory::{
    InventoryConfig, MariaDbInventoryReader, SourceMasterCoordinate, build_inventory,
};
use model::{DdlOperation, TableRuntimeState};

mod canonical;
mod model;
mod parser;
#[cfg(test)]
mod tests;
mod tokenizer;
mod transform;

#[cfg(test)]
pub(super) use canonical::build_fenced_create_table_evidence;
#[cfg(test)]
pub(super) use canonical::canonical_absent_state;
pub use canonical::{
    build_semantic_evidence, observe_operation_state, supports_automatic_semantic_recovery,
};
pub(super) use model::SemanticSchemaSnapshot;
pub use model::{DdlObjectKind, DdlSemanticEvidence};
use parser::parse_modeled_index_ddl;
#[cfg(test)]
pub(super) use parser::parse_simple_index_ddl;
pub use parser::{parse_ddl_operation, supports_automatic_index_ddl};
#[cfg(test)]
pub(super) use tokenizer::tokenize_ddl;
pub use transform::{
    DDL_TRANSFORMATION_VERSION, DdlTransformation, render_modeled_index_ddl,
    supports_drop_columns_if_exists, supports_fixture_create_table,
    supports_production_alter_table, supports_rename_columns_if_exists,
    transform_drop_columns_if_exists, transform_generated_schema_ddl,
    transform_production_alter_table, transform_rename_columns_if_exists,
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
    let normalized = translate_extended_timestamp(sql);
    let parsed_index = match provenance {
        DdlTranslationProvenance::Streamed => parser::parse_simple_index_ddl(&normalized),
        DdlTranslationProvenance::ModeledPlanner => parse_modeled_index_ddl(&normalized),
    };
    if let Ok(index) = parsed_index {
        if provenance == DdlTranslationProvenance::ModeledPlanner && index.unique {
            return render_modeled_index_ddl(&index, &normalized);
        }
        return Ok(DdlTransformation {
            version: transform::DDL_TRANSFORMATION_VERSION,
            target_sql: Some(normalized.trim().trim_end_matches(';').trim().to_string()),
        });
    }
    if supports_fixture_create_table(&normalized) {
        return transform::transform_fixture_create_table(&normalized);
    }
    if supports_production_alter_table(&normalized) {
        return transform_production_alter_table(&normalized);
    }
    if supports_drop_columns_if_exists(&normalized) {
        return transform_drop_columns_if_exists(
            &normalized,
            &target_columns.iter().cloned().collect(),
        );
    }
    if supports_rename_columns_if_exists(&normalized) {
        return transform_rename_columns_if_exists(
            &normalized,
            &target_columns.iter().cloned().collect(),
        );
    }
    match provenance {
        DdlTranslationProvenance::ModeledPlanner => transform_generated_schema_ddl(&normalized),
        DdlTranslationProvenance::Streamed => {
            Err("streamed DDL family is unsupported without an existing parsed model".to_string())
        }
    }
}

fn translate_extended_timestamp(sql: &str) -> String {
    let characters = sql.chars().collect::<Vec<_>>();
    let mut translated = String::with_capacity(sql.len());
    let mut index = 0;
    while index < characters.len() {
        if let Some(next) = copy_quoted_or_commented_sql(&characters, index, &mut translated) {
            index = next;
            continue;
        }
        if is_timestamp_token_at(&characters, index) && timestamp_is_column_type(&characters, index)
        {
            translated.push_str("DATETIME");
            index += "timestamp".len();
            continue;
        }
        translated.push(characters[index]);
        index += 1;
    }
    translated
}

fn copy_quoted_or_commented_sql(
    characters: &[char],
    index: usize,
    translated: &mut String,
) -> Option<usize> {
    let character = characters[index];
    if matches!(character, '\'' | '"' | '`') {
        return Some(copy_quoted_sql(characters, index, character, translated));
    }
    if character == '#' {
        return Some(copy_line_comment(characters, index, translated));
    }
    if is_mysql_line_comment_start(characters, index) {
        return Some(copy_line_comment(characters, index, translated));
    }
    if character == '/' && characters.get(index + 1) == Some(&'*') {
        return Some(copy_block_comment(characters, index, translated));
    }
    None
}

fn copy_quoted_sql(
    characters: &[char],
    start: usize,
    quote: char,
    translated: &mut String,
) -> usize {
    let mut index = start;
    while index < characters.len() {
        let character = characters[index];
        translated.push(character);
        index += 1;
        if character == '\\' && quote != '`' {
            if let Some(escaped) = characters.get(index) {
                translated.push(*escaped);
                index += 1;
            }
            continue;
        }
        if character == quote && index > start + 1 {
            if characters.get(index) == Some(&quote) {
                translated.push(quote);
                index += 1;
                continue;
            }
            return index;
        }
    }
    index
}

fn copy_line_comment(characters: &[char], start: usize, translated: &mut String) -> usize {
    let mut index = start;
    while let Some(character) = characters.get(index) {
        translated.push(*character);
        index += 1;
        if *character == '\n' {
            break;
        }
    }
    index
}

fn copy_block_comment(characters: &[char], start: usize, translated: &mut String) -> usize {
    let mut index = start;
    while index < characters.len() {
        let character = characters[index];
        translated.push(character);
        index += 1;
        if character == '*' && characters.get(index) == Some(&'/') {
            translated.push('/');
            return index + 1;
        }
    }
    index
}

fn is_mysql_line_comment_start(characters: &[char], index: usize) -> bool {
    characters.get(index) == Some(&'-')
        && characters.get(index + 1) == Some(&'-')
        && match characters.get(index + 2) {
            None => true,
            Some(character) => character.is_whitespace() || character.is_control(),
        }
}

fn is_timestamp_token_at(characters: &[char], index: usize) -> bool {
    let end = index + "timestamp".len();
    let candidate = characters.get(index..end);
    candidate.is_some_and(|candidate| {
        candidate
            .iter()
            .copied()
            .zip("timestamp".chars())
            .all(|(actual, expected)| actual.eq_ignore_ascii_case(&expected))
    }) && identifier_boundary(
        index
            .checked_sub(1)
            .and_then(|before| characters.get(before)),
    ) && identifier_boundary(characters.get(end))
}

fn identifier_boundary(character: Option<&char>) -> bool {
    character.is_none_or(|character| !(character.is_ascii_alphanumeric() || *character == '_'))
}

fn timestamp_is_column_type(characters: &[char], index: usize) -> bool {
    let prefix = characters[..index].iter().collect::<String>();
    let Ok(tokens) = tokenizer::tokenize_ddl(&prefix) else {
        return false;
    };
    let Some(column_name) = tokens.last() else {
        return false;
    };
    if !is_identifier_token(column_name) || is_create_definition_keyword(column_name) {
        return false;
    }
    let before_name = tokens
        .len()
        .checked_sub(2)
        .and_then(|position| tokens.get(position))
        .map(String::as_str);
    if matches!(before_name, Some("(") | Some(",")) {
        return true;
    }
    let before_before_name = tokens
        .len()
        .checked_sub(3)
        .and_then(|position| tokens.get(position))
        .map(String::as_str);
    matches!(before_name, Some(keyword) if keyword.eq_ignore_ascii_case("COLUMN"))
        && matches!(
            before_before_name,
            Some(keyword)
                if keyword.eq_ignore_ascii_case("ADD")
                    || keyword.eq_ignore_ascii_case("MODIFY")
        )
}

fn is_identifier_token(token: &str) -> bool {
    !matches!(token, "(" | ")" | "," | "." | "=" | "<string>")
}

fn is_create_definition_keyword(token: &str) -> bool {
    matches!(
        token.to_ascii_uppercase().as_str(),
        "CONSTRAINT" | "KEY" | "INDEX" | "PRIMARY" | "UNIQUE" | "FOREIGN" | "CHECK"
    )
}

impl DdlSemanticInventory for LiveDdlSemanticInventory {
    fn transform_sql(&self, sql: &str) -> Result<DdlTransformation, String> {
        let operation = parse_ddl_operation(sql).ok();
        let target_columns =
            if supports_drop_columns_if_exists(sql) || supports_rename_columns_if_exists(sql) {
                let operation =
                    operation.ok_or_else(|| "failed to parse target-aware DDL".to_string())?;
                let inventory = build_inventory(&self.target_schema, &self.target).map_err(|error| {
                format!(
                    "failed to build target inventory for DDL transformation in {}: {error}",
                    self.target_schema
                )
            })?;
                inventory
                    .tables
                    .iter()
                    .find(|table| table.name == operation.primary_object)
                    .ok_or_else(|| {
                        format!(
                            "target table {}.{} is missing for DDL transformation",
                            self.target_schema, operation.primary_object
                        )
                    })?
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
        translate_ddl(sql, &target_columns)
    }

    fn capture_evidence(
        &self,
        sql: &str,
        source_file: &str,
        event_end_position: u64,
    ) -> Result<DdlSemanticEvidence, String> {
        let operation = parse_ddl_operation(sql)?;
        let target_before = Self::snapshot(&self.target, &self.target_schema, &operation)?;
        if operation.create_table_ast.is_some() {
            return capture_fenced_create_table_evidence(
                self,
                &operation,
                &target_before,
                source_file,
                event_end_position,
            );
        }
        if operation.object_kind == DdlObjectKind::Index || operation.alter_table_ast.is_some() {
            return capture_translated_evidence(self, &operation, &target_before);
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
