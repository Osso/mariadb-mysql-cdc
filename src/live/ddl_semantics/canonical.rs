use super::super::ddl_replay_journal::DdlFamily;
use super::model::{
    DdlObjectKind, DdlOperation, DdlSemanticEvidence, ParsedAddColumnAst, ParsedAlterClause,
    ParsedAlterTableAst, ParsedCreateTableAst, ParsedIndexAst, ParsedIndexKeyPart,
    SemanticSchemaSnapshot,
};
use serde_json::json;

pub fn build_fenced_create_table_evidence(
    operation: &DdlOperation,
    target: &SemanticSchemaSnapshot,
    defaults: &crate::inventory::SchemaDefaults,
    expected_file: &str,
    expected_position: u64,
    before: &crate::inventory::SourceMasterCoordinate,
    after: &crate::inventory::SourceMasterCoordinate,
) -> Result<DdlSemanticEvidence, String> {
    let ast = operation
        .create_table_ast
        .as_ref()
        .ok_or_else(|| "typed fixture CREATE TABLE AST is missing".to_string())?;
    let explicit_defaults = explicit_create_table_defaults(ast);
    if explicit_defaults.is_none() {
        super::validate_source_snapshot_coordinate(
            expected_file,
            expected_position,
            before,
            after,
        )?;
    }
    let defaults = explicit_defaults.as_ref().unwrap_or(defaults);
    let pre_state = canonical_pre_state(operation, target)?;
    if pre_state != canonical_absent_state() {
        return Err(format!(
            "target table `{}` already exists before CREATE TABLE",
            operation.primary_object
        ));
    }
    let transformation =
        super::transform::transform_fixture_create_table_with_defaults(ast, defaults)?;
    let mut ast_value: serde_json::Value = serde_json::from_str(&canonical_ast(operation)?)
        .map_err(|error| format!("failed to decode canonical CREATE TABLE AST: {error}"))?;
    ast_value["source_schema_defaults"] = json!({
        "character_set": defaults.character_set,
        "collation": defaults.collation,
    });
    let canonical_ast = serde_json::to_string(&ast_value)
        .map_err(|error| format!("failed to encode canonical CREATE TABLE AST: {error}"))?;
    Ok(DdlSemanticEvidence {
        transformation_version: transformation.version.to_string(),
        generated_sql: transformation.target_sql,
        canonical_ast,
        pre_state,
        expected_post_state: expected_create_table_post_state(ast, defaults)?,
    })
}

pub(crate) fn explicit_create_table_defaults(
    ast: &ParsedCreateTableAst,
) -> Option<crate::inventory::SchemaDefaults> {
    Some(crate::inventory::SchemaDefaults {
        character_set: ast.character_set.clone()?,
        collation: ast.collation.clone()?,
    })
}

pub fn build_assistant_reply_reports_create_evidence(
    operation: &DdlOperation,
    target: &SemanticSchemaSnapshot,
) -> Result<DdlSemanticEvidence, String> {
    build_semantic_evidence(operation, target, target)
}
pub fn build_source_only_procedure_create_evidence(
    operation: &DdlOperation,
    target: &SemanticSchemaSnapshot,
) -> Result<DdlSemanticEvidence, String> {
    let pre_state = canonical_pre_state(operation, target)?;
    if pre_state != canonical_absent_state() {
        return Err(format!(
            "target procedure `{}` already exists before source-only CREATE PROCEDURE",
            operation.primary_object
        ));
    }
    build_semantic_evidence(operation, target, target)
}

pub fn build_semantic_evidence(
    operation: &DdlOperation,
    target: &SemanticSchemaSnapshot,
    source: &SemanticSchemaSnapshot,
) -> Result<DdlSemanticEvidence, String> {
    let canonical_ast = canonical_ast(operation)?;
    let pre_state = canonical_pre_state(operation, target)?;
    let expected_post_state = canonical_post_state(operation, target, source)?;
    Ok(DdlSemanticEvidence {
        transformation_version: String::new(),
        generated_sql: None,
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
        "parsed_create_table": operation.create_table_ast.as_ref().map(canonical_create_table_ast_value),
        "parsed_alter_table": operation.alter_table_ast.as_ref().map(canonical_alter_table_ast_value),
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
    if operation.alter_table_ast.is_some() {
        return translated_alter_table_post_state(target, operation);
    }
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

pub(crate) fn expected_create_table_post_state(
    ast: &ParsedCreateTableAst,
    defaults: &crate::inventory::SchemaDefaults,
) -> Result<String, String> {
    let table = crate::inventory::TableInventory {
        name: ast.name.clone(),
        table_type: "BASE TABLE".to_string(),
        engine: Some(ast.engine.clone()),
        collation: Some(defaults.collation.clone()),
        primary_key: ast.primary_key.clone(),
        columns: ast
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let data_type = column
                    .column_type
                    .split(['(', ' '])
                    .next()
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let (character_set, collation) = column_default_encoding(
                    &data_type,
                    &defaults.character_set,
                    &defaults.collation,
                );
                crate::inventory::ColumnInventory {
                    name: column.name.clone(),
                    ordinal_position: (index + 1) as u32,
                    column_type: column.column_type.to_ascii_lowercase(),
                    data_type,
                    is_nullable: column.nullable,
                    character_set,
                    collation,
                    default_value: column
                        .default_sql
                        .as_ref()
                        .filter(|value| !value.eq_ignore_ascii_case("NULL"))
                        .cloned(),
                    extra: expected_create_column_extra(column),
                    comment: String::new(),
                    generated: None,
                }
            })
            .collect(),
    };
    let mut indexes = ast
        .indexes
        .iter()
        .map(|index| crate::inventory::IndexInventory {
            table: ast.name.clone(),
            name: index.name.clone(),
            unique: index.unique,
            index_type: index.index_type.clone(),
            visible: index.visible,
            comment: index.comment.clone(),
            columns: index
                .key_parts
                .iter()
                .enumerate()
                .map(
                    |(part_index, part)| crate::inventory::IndexColumnInventory {
                        name: part.column.clone(),
                        sequence: (part_index + 1) as u32,
                        prefix_length: part.prefix_length,
                        collation: Some("A".to_string()),
                        order: part.order.clone(),
                    },
                )
                .collect(),
        })
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    serde_json::to_string(&json!({
        "kind": "table",
        "name": ast.name,
        "definition": table,
        "indexes": indexes,
        "foreign_keys": [],
    }))
    .map_err(|error| format!("failed to encode expected CREATE TABLE state: {error}"))
}

fn expected_create_column_extra(column: &super::model::ParsedCreateColumnAst) -> String {
    if column.auto_increment {
        return "auto_increment".to_string();
    }
    if column
        .default_sql
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("CURRENT_TIMESTAMP"))
    {
        return "DEFAULT_GENERATED".to_string();
    }
    String::new()
}

fn canonical_create_table_ast_value(ast: &ParsedCreateTableAst) -> serde_json::Value {
    json!({
        "name": ast.name,
        "columns": ast.columns.iter().map(|column| json!({
            "name": column.name,
            "column_type": column.column_type,
            "nullable": column.nullable,
            "default_sql": column.default_sql,
            "auto_increment": column.auto_increment,
        })).collect::<Vec<_>>(),
        "primary_key": ast.primary_key,
        "indexes": ast.indexes.iter().map(canonical_index_ast_value).collect::<Vec<_>>(),
        "engine": ast.engine,
        "character_set": ast.character_set,
        "collation": ast.collation,
    })
}

fn canonical_alter_table_ast_value(ast: &ParsedAlterTableAst) -> serde_json::Value {
    json!({
        "table": ast.table,
        "clauses": ast.clauses.iter().map(|clause| match clause {
            ParsedAlterClause::AddColumn(column) => json!({
                "kind": "add_column",
                "name": column.name,
                "column_type": column.column_type,
                "data_type": column.data_type,
                "nullable": column.nullable,
                "default_value": column.default_value,
                "comment": column.comment,
                "after": column.after,
            }),
            ParsedAlterClause::AddKey(index) => json!({
                "kind": "add_key",
                "index": canonical_index_ast_value(index),
            }),
            ParsedAlterClause::DropColumn(column) => json!({
                "kind": "drop_column",
                "name": column.name,
                "if_exists": column.if_exists,
            }),
        }).collect::<Vec<_>>(),
    })
}

fn translated_alter_table_post_state(
    target: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let ast = operation
        .alter_table_ast
        .as_ref()
        .ok_or_else(|| "ALTER TABLE DDL lacks parsed AST".to_string())?;
    let mut expected = target.clone();
    for clause in &ast.clauses {
        apply_alter_clause(&mut expected, ast, clause)?;
    }
    canonical_table_structure_state(&expected, &ast.table)
}

fn apply_alter_clause(
    expected: &mut SemanticSchemaSnapshot,
    ast: &ParsedAlterTableAst,
    clause: &ParsedAlterClause,
) -> Result<(), String> {
    match clause {
        ParsedAlterClause::AddColumn(column) => apply_add_column(expected, &ast.table, column),
        ParsedAlterClause::AddKey(index) => apply_add_key(expected, index),
        ParsedAlterClause::DropColumn(column) => apply_drop_column(expected, &ast.table, column),
    }
}

fn apply_add_column(
    expected: &mut SemanticSchemaSnapshot,
    table_name: &str,
    column: &ParsedAddColumnAst,
) -> Result<(), String> {
    let table = expected
        .inventory
        .tables
        .iter_mut()
        .find(|table| table.name == table_name)
        .ok_or_else(|| format!("ALTER TABLE target `{table_name}` is missing"))?;
    if table.columns.iter().any(|item| item.name == column.name) {
        return Err(format!(
            "ADD COLUMN target `{table_name}` already contains `{}`",
            column.name
        ));
    }
    let insertion = match &column.after {
        Some(after) => table
            .columns
            .iter()
            .position(|item| item.name == *after)
            .map(|position| position + 1)
            .ok_or_else(|| {
                format!("ADD COLUMN AFTER target `{table_name}`.`{after}` is missing")
            })?,
        None => table.columns.len(),
    };
    let table_collation = table
        .collation
        .as_deref()
        .ok_or_else(|| format!("ALTER TABLE target `{table_name}` has no default collation"))?;
    let table_character_set = table_collation.split('_').next().unwrap_or(table_collation);
    let (character_set, collation) =
        column_default_encoding(&column.data_type, table_character_set, table_collation);
    table.columns.insert(
        insertion,
        crate::inventory::ColumnInventory {
            name: column.name.clone(),
            ordinal_position: 0,
            column_type: column.column_type.clone(),
            data_type: column.data_type.clone(),
            is_nullable: column.nullable,
            character_set,
            collation,
            default_value: column.default_value.clone(),
            extra: String::new(),
            comment: column.comment.clone(),
            generated: None,
        },
    );
    for (index, item) in table.columns.iter_mut().enumerate() {
        item.ordinal_position = (index + 1) as u32;
    }
    Ok(())
}

fn column_default_encoding(
    data_type: &str,
    character_set: &str,
    collation: &str,
) -> (Option<String>, Option<String>) {
    if matches!(
        data_type,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
    ) {
        (Some(character_set.to_string()), Some(collation.to_string()))
    } else {
        (None, None)
    }
}

fn apply_drop_column(
    expected: &mut SemanticSchemaSnapshot,
    table_name: &str,
    column: &super::model::ParsedDropColumnAst,
) -> Result<(), String> {
    if expected.inventory.indexes.iter().any(|index| {
        index.table.eq_ignore_ascii_case(table_name)
            && index
                .columns
                .iter()
                .any(|part| part.name.eq_ignore_ascii_case(&column.name))
    }) {
        return Err(format!(
            "DROP COLUMN target `{table_name}`.`{}` has an index dependency",
            column.name
        ));
    }
    if expected.inventory.foreign_keys.iter().any(|foreign_key| {
        foreign_key.table.eq_ignore_ascii_case(table_name)
            && foreign_key
                .columns
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&column.name))
    }) {
        return Err(format!(
            "DROP COLUMN target `{table_name}`.`{}` has a foreign-key dependency",
            column.name
        ));
    }
    let table = expected
        .inventory
        .tables
        .iter_mut()
        .find(|table| table.name.eq_ignore_ascii_case(table_name))
        .ok_or_else(|| format!("ALTER TABLE target `{table_name}` is missing"))?;
    if table
        .primary_key
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&column.name))
    {
        return Err(format!(
            "DROP COLUMN target `{table_name}`.`{}` is part of the primary key",
            column.name
        ));
    }
    let Some(position) = table
        .columns
        .iter()
        .position(|item| item.name.eq_ignore_ascii_case(&column.name))
    else {
        if column.if_exists {
            return Ok(());
        }
        return Err(format!(
            "DROP COLUMN target `{table_name}` lacks `{}`",
            column.name
        ));
    };
    table.columns.remove(position);
    for (index, item) in table.columns.iter_mut().enumerate() {
        item.ordinal_position = (index + 1) as u32;
    }
    Ok(())
}

fn apply_add_key(
    expected: &mut SemanticSchemaSnapshot,
    ast: &ParsedIndexAst,
) -> Result<(), String> {
    let (table, indexes) = validate_index_table(expected, ast)?;
    validate_create_index(ast, table, &indexes, &expected.inventory.foreign_keys, true)?;
    expected
        .inventory
        .indexes
        .push(index_inventory_from_ast(ast));
    Ok(())
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
        validate_create_index(ast, table, &indexes, &target.inventory.foreign_keys, false)
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
    allow_unique: bool,
) -> Result<(), String> {
    validate_parsed_index_ast(ast, table, foreign_keys, allow_unique)?;
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
    allow_unique: bool,
) -> Result<(), String> {
    validate_index_ast_shape(ast, allow_unique)?;
    let columns = validate_index_key_parts(ast, table)?;
    validate_index_foreign_key_dependencies(ast, &columns, foreign_keys)
}

fn validate_index_ast_shape(ast: &ParsedIndexAst, allow_unique: bool) -> Result<(), String> {
    if !ast.create
        || (ast.unique && !allow_unique)
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
