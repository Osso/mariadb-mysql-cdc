use super::super::ddl_replay_journal::DdlFamily;
use super::model::{
    DdlObjectKind, DdlOperation, DdlSemanticEvidence, ParsedIndexAst, ParsedIndexKeyPart,
    SemanticSchemaSnapshot,
};
use serde_json::json;

pub fn build_semantic_evidence(
    operation: &DdlOperation,
    target: &SemanticSchemaSnapshot,
    source: &SemanticSchemaSnapshot,
) -> Result<DdlSemanticEvidence, String> {
    let canonical_ast = canonical_ast(operation)?;
    let pre_state = canonical_pre_state(operation, target)?;
    let expected_post_state = canonical_post_state(operation, target, source)?;
    Ok(DdlSemanticEvidence {
        canonical_ast,
        pre_state,
        expected_post_state,
    })
}

fn canonical_ast(operation: &DdlOperation) -> Result<String, String> {
    serde_json::to_string(&json!({
        "family": operation.family.as_str(),
        "object_kind": operation.object_kind.as_str(),
        "primary_object": operation.primary_object,
        "secondary_object": operation.secondary_object,
        "parsed_index": operation.index_ast.as_ref().map(canonical_index_ast_value),
    }))
    .map_err(|error| format!("failed to encode canonical DDL AST: {error}"))
}

fn canonical_pre_state(
    operation: &DdlOperation,
    target: &SemanticSchemaSnapshot,
) -> Result<String, String> {
    match operation.family {
        DdlFamily::Rename => canonical_rename_observed_state(target, operation),
        DdlFamily::Truncate => canonical_table_state(target, &operation.primary_object),
        _ => canonical_operation_state(target, operation),
    }
}

fn canonical_post_state(
    operation: &DdlOperation,
    target: &SemanticSchemaSnapshot,
    source: &SemanticSchemaSnapshot,
) -> Result<String, String> {
    match operation.family {
        DdlFamily::Index => translated_index_post_state(target, operation),
        DdlFamily::Drop => Ok(canonical_absent_state()),
        DdlFamily::Rename => canonical_rename_post_state(source, operation),
        DdlFamily::Truncate => canonical_truncate_post_state(target, operation),
        _ => canonical_operation_state(source, operation),
    }
}
fn canonical_operation_state(
    snapshot: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let name = &operation.primary_object;
    match operation.object_kind {
        DdlObjectKind::Table => canonical_table_structure_state(snapshot, name),
        DdlObjectKind::Index => canonical_index_state(snapshot, operation),
        DdlObjectKind::View => canonical_view_state(snapshot, name),
        DdlObjectKind::Procedure | DdlObjectKind::Function => {
            canonical_routine_state(snapshot, operation)
        }
        DdlObjectKind::Event => canonical_event_state(snapshot, name),
        DdlObjectKind::Trigger => canonical_trigger_state(snapshot, name),
    }
}

fn canonical_view_state(snapshot: &SemanticSchemaSnapshot, name: &str) -> Result<String, String> {
    canonical_named_state(
        "view",
        snapshot
            .inventory
            .views
            .iter()
            .find(|item| item.name == name),
    )
}

fn canonical_routine_state(
    snapshot: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let routine_type = operation.object_kind.as_str().to_ascii_uppercase();
    let routine =
        snapshot.inventory.routines.iter().find(|item| {
            item.name == operation.primary_object && item.routine_type == routine_type
        });
    canonical_named_state(operation.object_kind.as_str(), routine)
}

fn canonical_event_state(snapshot: &SemanticSchemaSnapshot, name: &str) -> Result<String, String> {
    canonical_named_state(
        "event",
        snapshot
            .inventory
            .events
            .iter()
            .find(|item| item.name == name),
    )
}

fn canonical_trigger_state(
    snapshot: &SemanticSchemaSnapshot,
    name: &str,
) -> Result<String, String> {
    canonical_named_state(
        "trigger",
        snapshot
            .inventory
            .triggers
            .iter()
            .find(|item| item.name == name),
    )
}
fn canonical_table_structure_state(
    snapshot: &SemanticSchemaSnapshot,
    table_name: &str,
) -> Result<String, String> {
    let Some(table) = find_table(snapshot, table_name) else {
        return Ok(canonical_absent_state());
    };
    serde_json::to_string(&json!({
        "kind": "table",
        "name": table_name,
        "definition": table,
        "indexes": sorted_table_indexes(snapshot, table_name),
        "foreign_keys": sorted_table_foreign_keys(snapshot, table_name),
    }))
    .map_err(|error| format!("failed to encode table structure: {error}"))
}

fn find_table<'a>(
    snapshot: &'a SemanticSchemaSnapshot,
    name: &str,
) -> Option<&'a crate::inventory::TableInventory> {
    snapshot
        .inventory
        .tables
        .iter()
        .find(|table| table.name == name)
}

fn sorted_table_indexes<'a>(
    snapshot: &'a SemanticSchemaSnapshot,
    table_name: &str,
) -> Vec<&'a crate::inventory::IndexInventory> {
    let mut indexes = snapshot
        .inventory
        .indexes
        .iter()
        .filter(|index| index.table == table_name)
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    indexes
}

fn sorted_table_foreign_keys<'a>(
    snapshot: &'a SemanticSchemaSnapshot,
    table_name: &str,
) -> Vec<&'a crate::inventory::ForeignKeyInventory> {
    let mut foreign_keys = snapshot
        .inventory
        .foreign_keys
        .iter()
        .filter(|item| item.table == table_name)
        .collect::<Vec<_>>();
    foreign_keys.sort_by(|left, right| left.name.cmp(&right.name));
    foreign_keys
}
fn canonical_table_state(
    snapshot: &SemanticSchemaSnapshot,
    table_name: &str,
) -> Result<String, String> {
    let Some(table) = find_table(snapshot, table_name) else {
        return Ok(canonical_absent_state());
    };
    let runtime = snapshot
        .table_runtime
        .get(table_name)
        .ok_or_else(|| format!("exact runtime metadata missing for table `{table_name}`"))?;
    serde_json::to_string(&json!({
        "kind": "table",
        "name": table_name,
        "definition": table,
        "indexes": table_indexes(snapshot, table_name),
        "row_count": runtime.row_count,
        "auto_increment": runtime.auto_increment,
    }))
    .map_err(|error| format!("failed to encode table state: {error}"))
}

fn table_indexes<'a>(
    snapshot: &'a SemanticSchemaSnapshot,
    table_name: &str,
) -> Vec<&'a crate::inventory::IndexInventory> {
    snapshot
        .inventory
        .indexes
        .iter()
        .filter(|index| index.table == table_name)
        .collect()
}
fn canonical_index_state(
    snapshot: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let table = operation
        .secondary_object
        .as_deref()
        .ok_or_else(|| "index DDL table is missing".to_string())?;
    canonical_table_structure_state(snapshot, table)
}

fn canonical_index_ast_value(ast: &ParsedIndexAst) -> serde_json::Value {
    json!({
        "create": ast.create,
        "name": ast.name,
        "table": ast.table,
        "unique": ast.unique,
        "index_type": ast.index_type,
        "visible": ast.visible,
        "comment": ast.comment,
        "key_parts": ast.key_parts.iter().map(|part| json!({
            "column": part.column,
            "prefix_length": part.prefix_length,
            "order": part.order,
            "collation": part.collation,
        })).collect::<Vec<_>>(),
    })
}

fn translated_index_post_state(
    target: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    validate_index_operation(target, operation)?;
    let ast = operation
        .index_ast
        .as_ref()
        .ok_or_else(|| "index DDL lacks parsed AST".to_string())?;
    let mut expected = target.clone();
    match ast.create {
        true => expected
            .inventory
            .indexes
            .push(index_inventory_from_ast(ast)),
        false => expected
            .inventory
            .indexes
            .retain(|index| !(index.table == ast.table && index.name == ast.name)),
    }
    canonical_table_structure_state(&expected, &ast.table)
}

fn index_inventory_from_ast(ast: &ParsedIndexAst) -> crate::inventory::IndexInventory {
    crate::inventory::IndexInventory {
        table: ast.table.clone(),
        name: ast.name.clone(),
        unique: ast.unique,
        index_type: ast.index_type.clone(),
        visible: ast.visible,
        comment: ast.comment.clone(),
        columns: ast
            .key_parts
            .iter()
            .enumerate()
            .map(|(index, part)| crate::inventory::IndexColumnInventory {
                name: part.column.clone(),
                sequence: (index + 1) as u32,
                prefix_length: part.prefix_length,
                collation: part
                    .collation
                    .clone()
                    .or_else(|| Some(if part.order == "DESC" { "D" } else { "A" }.to_string())),
                order: part.order.clone(),
            })
            .collect(),
    }
}

fn validate_index_operation(
    target: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<(), String> {
    let ast = operation
        .index_ast
        .as_ref()
        .ok_or_else(|| "index DDL lacks parsed AST".to_string())?;
    let (table, indexes) = validate_index_table(target, ast)?;
    if ast.create {
        validate_create_index(ast, table, &indexes, &target.inventory.foreign_keys)
    } else {
        validate_drop_index(ast, &indexes, &target.inventory.foreign_keys)
    }
}

fn validate_index_table<'a>(
    target: &'a SemanticSchemaSnapshot,
    ast: &ParsedIndexAst,
) -> Result<
    (
        &'a crate::inventory::TableInventory,
        Vec<&'a crate::inventory::IndexInventory>,
    ),
    String,
> {
    let table = find_table(target, &ast.table).ok_or_else(|| {
        format!(
            "index table `{}` is missing from fenced target pre-state",
            ast.table
        )
    })?;
    Ok((table, table_indexes(target, &ast.table)))
}

fn validate_create_index(
    ast: &ParsedIndexAst,
    table: &crate::inventory::TableInventory,
    indexes: &[&crate::inventory::IndexInventory],
    foreign_keys: &[crate::inventory::ForeignKeyInventory],
) -> Result<(), String> {
    validate_parsed_index_ast(ast, table, foreign_keys)?;
    if indexes.iter().any(|index| index.name == ast.name) {
        return Err(format!(
            "index `{}` already exists in fenced target pre-state",
            ast.name
        ));
    }
    Ok(())
}

fn validate_drop_index(
    ast: &ParsedIndexAst,
    indexes: &[&crate::inventory::IndexInventory],
    foreign_keys: &[crate::inventory::ForeignKeyInventory],
) -> Result<(), String> {
    let index = indexes
        .iter()
        .find(|index| index.name == ast.name)
        .ok_or_else(|| {
            format!(
                "index `{}` is absent from fenced target pre-state",
                ast.name
            )
        })?;
    validate_recorded_index(index)?;
    if index_supports_foreign_key(index, foreign_keys) {
        return Err(format!("index `{}` is required by a foreign key", ast.name));
    }
    Ok(())
}
fn validate_parsed_index_ast(
    ast: &ParsedIndexAst,
    table: &crate::inventory::TableInventory,
    foreign_keys: &[crate::inventory::ForeignKeyInventory],
) -> Result<(), String> {
    validate_index_ast_shape(ast)?;
    let columns = validate_index_key_parts(ast, table)?;
    validate_index_foreign_key_dependencies(ast, &columns, foreign_keys)
}

fn validate_index_ast_shape(ast: &ParsedIndexAst) -> Result<(), String> {
    if !ast.create
        || ast.unique
        || ast.index_type != "BTREE"
        || !ast.visible
        || ast.comment.is_some()
    {
        return Err("index DDL is not a simple visible non-unique BTREE index".to_string());
    }
    if ast.name.is_empty() || ast.table.is_empty() || ast.key_parts.is_empty() {
        return Err("index DDL is incomplete".to_string());
    }
    Ok(())
}

fn validate_index_key_parts<'a>(
    ast: &'a ParsedIndexAst,
    table: &crate::inventory::TableInventory,
) -> Result<Vec<&'a str>, String> {
    for part in &ast.key_parts {
        validate_index_key_part(part)?;
        let column = table
            .columns
            .iter()
            .find(|column| column.name == part.column)
            .ok_or_else(|| {
                format!(
                    "index column `{}` is absent from fenced target pre-state",
                    part.column
                )
            })?;
        if column.generated.is_some() {
            return Err(format!("index column `{}` is generated", part.column));
        }
    }
    Ok(ast
        .key_parts
        .iter()
        .map(|part| part.column.as_str())
        .collect())
}

fn validate_index_key_part(part: &ParsedIndexKeyPart) -> Result<(), String> {
    if part.column.is_empty() || !matches!(part.order.as_str(), "ASC" | "DESC") {
        return Err("index key part is incomplete".to_string());
    }
    if part.prefix_length == Some(0) {
        return Err("index key prefix must be positive".to_string());
    }
    Ok(())
}

fn validate_index_foreign_key_dependencies(
    ast: &ParsedIndexAst,
    columns: &[&str],
    foreign_keys: &[crate::inventory::ForeignKeyInventory],
) -> Result<(), String> {
    let supports_foreign_key = foreign_keys
        .iter()
        .any(|foreign_key| index_matches_foreign_key(ast, columns, foreign_key));
    if supports_foreign_key {
        return Err(format!(
            "index `{}` supports or depends on a foreign key",
            ast.name
        ));
    }
    Ok(())
}

fn index_matches_foreign_key(
    ast: &ParsedIndexAst,
    columns: &[&str],
    foreign_key: &crate::inventory::ForeignKeyInventory,
) -> bool {
    let child_columns = foreign_key
        .columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let parent_columns = foreign_key
        .referenced_columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    (foreign_key.table == ast.table && columns.starts_with(&child_columns))
        || (foreign_key.referenced_table == ast.table && columns.starts_with(&parent_columns))
}

fn validate_recorded_index(index: &crate::inventory::IndexInventory) -> Result<(), String> {
    validate_recorded_index_shape(index)?;
    for (expected, column) in index.columns.iter().enumerate() {
        validate_recorded_index_column(index, expected, column)?;
    }
    Ok(())
}

fn validate_recorded_index_shape(index: &crate::inventory::IndexInventory) -> Result<(), String> {
    if index.unique
        || !index.visible
        || index.comment.is_some()
        || index.index_type != "BTREE"
        || index.columns.is_empty()
    {
        return Err(format!(
            "recorded index `{}` is not a simple visible non-unique BTREE index",
            index.name
        ));
    }
    Ok(())
}

fn validate_recorded_index_column(
    index: &crate::inventory::IndexInventory,
    expected: usize,
    column: &crate::inventory::IndexColumnInventory,
) -> Result<(), String> {
    if column.name.is_empty()
        || column.sequence != (expected + 1) as u32
        || !matches!(column.order.as_str(), "ASC" | "DESC")
    {
        return Err(format!(
            "recorded index `{}` has incomplete key metadata",
            index.name
        ));
    }
    if column.prefix_length == Some(0) {
        return Err(format!(
            "recorded index `{}` has an invalid prefix",
            index.name
        ));
    }
    Ok(())
}
fn index_supports_foreign_key(
    index: &crate::inventory::IndexInventory,
    foreign_keys: &[crate::inventory::ForeignKeyInventory],
) -> bool {
    let columns = index
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    foreign_keys.iter().any(|foreign_key| {
        (foreign_key.table == index.table
            && columns.starts_with(
                &foreign_key
                    .columns
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            ))
            || (foreign_key.referenced_table == index.table
                && columns.starts_with(
                    &foreign_key
                        .referenced_columns
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                ))
    })
}

fn canonical_named_state<T: serde::Serialize>(
    kind: &str,
    value: Option<&T>,
) -> Result<String, String> {
    match value {
        Some(value) => serde_json::to_string(&json!({"kind": kind, "definition": value}))
            .map_err(|error| format!("failed to encode {kind} state: {error}")),
        None => Ok(canonical_absent_state()),
    }
}

fn canonical_rename_post_state(
    source: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    canonical_rename_observed_state(source, operation)
}

fn canonical_truncate_post_state(
    target: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let mut expected = target.clone();
    let runtime = expected
        .table_runtime
        .get_mut(&operation.primary_object)
        .ok_or_else(|| {
            format!(
                "exact runtime metadata missing for table `{}`",
                operation.primary_object
            )
        })?;
    runtime.row_count = 0;
    if runtime.auto_increment.is_some() {
        runtime.auto_increment = Some(1);
    }
    canonical_table_state(&expected, &operation.primary_object)
}

pub fn canonical_absent_state() -> String {
    "{\"state\":\"absent\"}".to_string()
}

pub fn observe_operation_state(
    snapshot: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    if operation.family == DdlFamily::Rename {
        return canonical_rename_observed_state(snapshot, operation);
    }
    canonical_operation_state(snapshot, operation)
}

fn canonical_rename_observed_state(
    snapshot: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let destination = operation
        .secondary_object
        .as_deref()
        .ok_or_else(|| "rename destination is missing".to_string())?;
    let source_state = canonical_table_state(snapshot, &operation.primary_object)?;
    let destination_state = canonical_table_state(snapshot, destination)?;
    serde_json::to_string(&json!({
        "source": {"name": operation.primary_object, "state": serde_json::from_str::<serde_json::Value>(&source_state).map_err(|error| format!("invalid rename source state JSON: {error}"))?},
        "destination": {"name": destination, "state": serde_json::from_str::<serde_json::Value>(&destination_state).map_err(|error| format!("invalid rename destination state JSON: {error}"))?},
    }))
    .map_err(|error| format!("failed to encode observed rename state: {error}"))
}

pub fn supports_automatic_semantic_recovery(operation: &DdlOperation) -> bool {
    operation.family == DdlFamily::Index
        && operation.object_kind == DdlObjectKind::Index
        && operation.index_ast.is_some()
}
