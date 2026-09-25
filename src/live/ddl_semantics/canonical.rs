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
    build_resolved_create_table_evidence(operation, target, defaults)
}

pub(crate) fn build_resolved_create_table_evidence(
    operation: &DdlOperation,
    target: &SemanticSchemaSnapshot,
    defaults: &crate::inventory::SchemaDefaults,
) -> Result<DdlSemanticEvidence, String> {
    let ast = operation
        .create_table_ast
        .as_ref()
        .ok_or_else(|| "typed CREATE TABLE AST is missing".to_string())?;
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
        expected_post_state: expected_create_table_post_state(
            ast,
            defaults,
            &target.inventory.schema,
        )?,
    })
}

/// Verifies the action and identity of each foreign key an ALTER adds once the target
/// reports it; an absent key is ordinary pre-state.
pub(crate) fn validate_alter_foreign_keys(
    ast: &ParsedAlterTableAst,
    target_schema: &str,
    observed: &[crate::canonical_foreign_key::CanonicalForeignKey],
) -> Result<(), String> {
    for clause in &ast.clauses {
        let ParsedAlterClause::AddForeignKey(key) = clause else {
            continue;
        };
        let Some(actual) = observed
            .iter()
            .find(|item| item.constraint_name.eq_ignore_ascii_case(&key.name))
        else {
            continue;
        };
        if *actual != expected_canonical_foreign_key(key, &ast.table, target_schema) {
            return Err(format!(
                "ADD FOREIGN KEY `{}` definition or actions differ",
                key.name
            ));
        }
    }
    Ok(())
}

fn expected_canonical_foreign_key(
    key: &super::model::ParsedCreateForeignKeyAst,
    child_table: &str,
    target_schema: &str,
) -> crate::canonical_foreign_key::CanonicalForeignKey {
    crate::canonical_foreign_key::CanonicalForeignKey {
        constraint_schema: target_schema.into(),
        constraint_name: key.name.clone(),
        child_schema: target_schema.into(),
        child_table: child_table.into(),
        child_columns: key.columns.clone(),
        parent_schema: target_schema.into(),
        parent_table: key.referenced_table.clone(),
        parent_columns: key.referenced_columns.clone(),
        update_rule: "RESTRICT".into(),
        delete_rule: key.delete_rule.clone(),
        match_option: "NONE".into(),
        enforced: true,
    }
}

pub(crate) fn validate_create_foreign_keys(
    ast: &ParsedCreateTableAst,
    target_schema: &str,
    observed: &[crate::canonical_foreign_key::CanonicalForeignKey],
) -> Result<(), String> {
    let mut expected = ast
        .foreign_keys
        .iter()
        .map(|key| expected_canonical_foreign_key(key, &ast.name, target_schema))
        .collect::<Vec<_>>();
    expected.sort();
    let mut actual = observed
        .iter()
        .filter(|key| key.child_table == ast.name)
        .cloned()
        .collect::<Vec<_>>();
    actual.sort();
    if actual != expected {
        return Err(format!(
            "CREATE foreign-key definitions or actions differ for `{}`",
            ast.name
        ));
    }
    Ok(())
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
    target_schema: &str,
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
                let (character_set, collation) = match (&column.character_set, &column.collation) {
                    (Some(character_set), Some(collation)) => {
                        (Some(character_set.clone()), Some(collation.clone()))
                    }
                    _ => column_default_encoding(
                        &data_type,
                        &defaults.character_set,
                        &defaults.collation,
                    ),
                };
                let (default_value, generated_default) =
                    expected_create_column_default(column.default_sql.as_deref());
                crate::inventory::ColumnInventory {
                    name: column.name.clone(),
                    ordinal_position: (index + 1) as u32,
                    column_type: if data_type == "enum" {
                        format!("enum{}", &column.column_type[4..])
                    } else {
                        column.column_type.to_ascii_lowercase()
                    },
                    data_type,
                    is_nullable: column.nullable,
                    character_set,
                    collation,
                    default_value,
                    extra: expected_create_column_extra(column, generated_default),
                    comment: column.comment.clone(),
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
    let mut foreign_keys = ast
        .foreign_keys
        .iter()
        .map(|key| crate::inventory::ForeignKeyInventory {
            table: ast.name.clone(),
            name: key.name.clone(),
            columns: key.columns.clone(),
            referenced_schema: target_schema.to_string(),
            referenced_table: key.referenced_table.clone(),
            referenced_columns: key.referenced_columns.clone(),
        })
        .collect::<Vec<_>>();
    foreign_keys.sort_by(|left, right| left.name.cmp(&right.name));
    serde_json::to_string(&json!({
        "kind": "table",
        "name": ast.name,
        "definition": table,
        "indexes": indexes,
        "foreign_keys": foreign_keys,
    }))
    .map_err(|error| format!("failed to encode expected CREATE TABLE state: {error}"))
}

/// The `COLUMN_DEFAULT` MySQL 8 reports for a rendered CREATE default, and whether MySQL marks
/// it `DEFAULT_GENERATED`: `CURRENT_TIMESTAMP[(6)]` and the TEXT expression default
/// `(_utf8mb4'...')` are generated; quoted literals are reported bare.
fn expected_create_column_default(default_sql: Option<&str>) -> (Option<String>, bool) {
    let Some(default_sql) = default_sql else {
        return (None, false);
    };
    if default_sql.eq_ignore_ascii_case("NULL") {
        return (None, false);
    }
    if default_sql
        .get(..17)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CURRENT_TIMESTAMP"))
    {
        return (Some(default_sql.to_string()), true);
    }
    if let Some(literal) = default_sql
        .strip_prefix("(_utf8mb4'")
        .and_then(|rest| rest.strip_suffix("')"))
    {
        return (Some(format!("_utf8mb4\\'{literal}\\'")), true);
    }
    if let Some(literal) = default_sql
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return (Some(literal.replace("''", "'")), false);
    }
    (Some(default_sql.to_string()), false)
}

fn expected_create_column_extra(
    column: &super::model::ParsedCreateColumnAst,
    generated_default: bool,
) -> String {
    if column.auto_increment {
        return "auto_increment".to_string();
    }
    let on_update = super::transform::current_timestamp_for(&column.column_type);
    match (generated_default, column.on_update_current_timestamp) {
        (true, true) => format!("DEFAULT_GENERATED on update {on_update}"),
        (true, false) => "DEFAULT_GENERATED".to_string(),
        (false, true) => format!("on update {on_update}"),
        (false, false) => String::new(),
    }
}

/// The `COLUMN_DEFAULT`/`DEFAULT_GENERATED` pair MySQL 8 reports for a translated ADD COLUMN.
fn expected_added_column_default(
    data_type: &str,
    default_value: Option<&str>,
) -> (Option<String>, String) {
    match default_value {
        Some(value) if super::transform::is_text_type(data_type) => (
            Some(format!("_utf8mb4\\'{value}\\'")),
            "DEFAULT_GENERATED".to_string(),
        ),
        other => (other.map(str::to_string), String::new()),
    }
}

fn canonical_create_table_ast_value(ast: &ParsedCreateTableAst) -> serde_json::Value {
    let mut value = json!({
        "name": ast.name,
        "if_not_exists": ast.if_not_exists,
        "columns": ast.columns.iter().map(|column| {
            let mut value = json!({
                "name": column.name,
                "column_type": column.column_type,
                "nullable": column.nullable,
                "default_sql": column.default_sql,
                "auto_increment": column.auto_increment,
                "on_update_current_timestamp": column.on_update_current_timestamp,
            });
            if let (Some(character_set), Some(collation)) = (&column.character_set, &column.collation) {
                value["character_set"] = json!(character_set);
                value["collation"] = json!(collation);
            }
            if !column.comment.is_empty() {
                value["comment"] = json!(column.comment);
            }
            value
        }).collect::<Vec<_>>(),
        "primary_key": ast.primary_key,
        "indexes": ast.indexes.iter().map(canonical_index_ast_value).collect::<Vec<_>>(),
        "engine": ast.engine,
        "character_set": ast.character_set,
        "collation": ast.collation,
    });
    if !ast.foreign_keys.is_empty() {
        value["foreign_keys"] = json!(
            ast.foreign_keys
                .iter()
                .map(|key| json!({
                    "name": key.name,
                    "columns": key.columns,
                    "referenced_table": key.referenced_table,
                    "referenced_columns": key.referenced_columns,
                    "delete_rule": key.delete_rule,
                    "update_rule": "RESTRICT",
                }))
                .collect::<Vec<_>>()
        );
    }
    if !ast.check_constraints.is_empty() {
        value["check_constraints"] = json!(
            ast.check_constraints
                .iter()
                .map(super::transform::canonical_check_constraint_value)
                .collect::<Vec<_>>()
        );
    }
    value
}

fn canonical_alter_table_ast_value(ast: &ParsedAlterTableAst) -> serde_json::Value {
    let clauses = ast
        .clauses
        .iter()
        .map(|clause| match clause {
            ParsedAlterClause::AddColumn(column) => canonical_add_column_ast_value(column),
            ParsedAlterClause::ModifyColumn(column) => {
                let mut value = canonical_add_column_ast_value(column);
                value["kind"] = json!("modify_column");
                value
            }
            ParsedAlterClause::RenameColumn { old_name, new_name } => json!({
                "kind": "rename_column", "old_name": old_name, "new_name": new_name,
            }),
            ParsedAlterClause::AddKey {
                index,
                if_not_exists,
            } => {
                let mut value = json!({
                    "kind": "add_key",
                    "index": canonical_index_ast_value(index),
                });
                if *if_not_exists {
                    value["if_not_exists"] = json!(true);
                }
                value
            }
            ParsedAlterClause::AddCheck(constraint) => json!({
                "kind": "add_check",
                "constraint": super::transform::canonical_check_constraint_value(constraint),
            }),
            ParsedAlterClause::AddForeignKey(key) => json!({
                "kind": "add_foreign_key",
                "name": key.name,
                "columns": key.columns,
                "referenced_table": key.referenced_table,
                "referenced_columns": key.referenced_columns,
                "delete_rule": key.delete_rule,
                "update_rule": "RESTRICT",
            }),
            ParsedAlterClause::DropColumn(column) => json!({
                "kind": "drop_column",
                "name": column.name,
                "if_exists": column.if_exists,
            }),
            ParsedAlterClause::DropIndex(index) => json!({
                "kind": "drop_index",
                "name": index.name,
            }),
        })
        .collect::<Vec<_>>();
    let mut value = json!({
        "table": ast.table,
        "clauses": clauses,
    });
    if let Some(algorithm) = ast.algorithm {
        value["algorithm"] = json!(algorithm.as_str());
    }
    if let Some(lock) = ast.lock {
        value["lock"] = json!(lock.as_str());
    }
    value
}

fn canonical_add_column_ast_value(column: &ParsedAddColumnAst) -> serde_json::Value {
    let mut value = json!({
        "kind": "add_column",
        "name": column.name,
        "column_type": column.column_type,
        "data_type": column.data_type,
        "nullable": column.nullable,
        "default_value": column.default_value,
        "comment": column.comment,
        "after": column.after,
    });
    if column.if_not_exists {
        value["if_not_exists"] = json!(true);
    }
    if let (Some(character_set), Some(collation)) = (&column.character_set, &column.collation) {
        value["character_set"] = json!(character_set);
        value["collation"] = json!(collation);
    }
    if let Some(expression) = &column.generated {
        value["generated"] = json!({
            "expression": super::transform::mysql_generation_expression(expression),
            "generation_kind": "STORED",
        });
    }
    value
}

fn translated_alter_table_post_state(
    target: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let ast = operation
        .alter_table_ast
        .as_ref()
        .ok_or_else(|| "ALTER TABLE DDL lacks parsed AST".to_string())?;
    validate_guarded_clause_pre_state(target, ast)?;
    let mut expected = target.clone();
    for clause in &ast.clauses {
        apply_alter_clause(&mut expected, ast, clause)?;
    }
    canonical_table_structure_state(&expected, &ast.table)
}

/// MySQL 8 has no `IF NOT EXISTS` for ADD COLUMN/INDEX, so guarded clauses execute unguarded
/// only when every guarded object is absent, or prove a no-op when every one already exists
/// with its exact definition. Partial presence fails closed.
fn validate_guarded_clause_pre_state(
    target: &SemanticSchemaSnapshot,
    ast: &ParsedAlterTableAst,
) -> Result<(), String> {
    let mut guarded = 0;
    let mut present = 0;
    for clause in &ast.clauses {
        match clause {
            ParsedAlterClause::AddColumn(column) if column.if_not_exists => {
                guarded += 1;
                let table = find_table(target, &ast.table)
                    .ok_or_else(|| format!("ALTER TABLE target `{}` is missing", ast.table))?;
                if table.columns.iter().any(|item| item.name == column.name) {
                    present += 1;
                }
            }
            ParsedAlterClause::AddKey {
                index,
                if_not_exists: true,
            } => {
                guarded += 1;
                if table_indexes(target, &ast.table)
                    .iter()
                    .any(|item| item.name == index.name)
                {
                    present += 1;
                }
            }
            _ => {}
        }
    }
    if present == 0 || present == guarded {
        return Ok(());
    }
    Err(format!(
        "guarded ALTER TABLE target `{}` has partial pre-state",
        ast.table
    ))
}

fn apply_alter_clause(
    expected: &mut SemanticSchemaSnapshot,
    ast: &ParsedAlterTableAst,
    clause: &ParsedAlterClause,
) -> Result<(), String> {
    match clause {
        ParsedAlterClause::AddColumn(column) => apply_add_column(expected, &ast.table, column),
        ParsedAlterClause::ModifyColumn(column) => {
            apply_modify_column(expected, &ast.table, column)
        }
        ParsedAlterClause::RenameColumn { old_name, new_name } => {
            apply_rename_column(expected, &ast.table, old_name, new_name)
        }
        ParsedAlterClause::AddKey {
            index,
            if_not_exists,
        } => apply_add_key(expected, index, *if_not_exists),
        ParsedAlterClause::AddCheck(constraint) => {
            validate_add_check(expected, &ast.table, constraint)
        }
        ParsedAlterClause::AddForeignKey(key) => apply_add_foreign_key(expected, &ast.table, key),
        ParsedAlterClause::DropColumn(column) => apply_drop_column(expected, &ast.table, column),
        ParsedAlterClause::DropIndex(index) => apply_drop_index(expected, &ast.table, index),
    }
}

/// CHECK constraints are outside the inventory the post-state compares, so the expected state
/// only proves every referenced column exists once preceding clauses have applied.
fn validate_add_check(
    expected: &SemanticSchemaSnapshot,
    table_name: &str,
    constraint: &super::model::ParsedCheckConstraintAst,
) -> Result<(), String> {
    let table = find_table(expected, table_name)
        .ok_or_else(|| format!("ADD CONSTRAINT target `{table_name}` is missing"))?;
    for column in super::transform::referenced_columns(constraint) {
        if !table
            .columns
            .iter()
            .any(|item| item.name.eq_ignore_ascii_case(column))
        {
            return Err(format!(
                "CHECK constraint `{}` references missing column `{table_name}`.`{column}`",
                constraint.name
            ));
        }
    }
    Ok(())
}

/// MySQL silently creates an index for an unsupported foreign key, which the expected state
/// cannot model, so the child column must already lead the primary key or an unprefixed index.
fn apply_add_foreign_key(
    expected: &mut SemanticSchemaSnapshot,
    table_name: &str,
    key: &super::model::ParsedCreateForeignKeyAst,
) -> Result<(), String> {
    let table = find_table(expected, table_name)
        .ok_or_else(|| format!("ADD FOREIGN KEY target `{table_name}` is missing"))?;
    if !table
        .columns
        .iter()
        .any(|column| column.name == key.columns[0])
    {
        return Err(format!(
            "ADD FOREIGN KEY column `{table_name}`.`{}` is missing",
            key.columns[0]
        ));
    }
    let supported = table.primary_key.starts_with(&key.columns)
        || table_indexes(expected, table_name).iter().any(|index| {
            index
                .columns
                .first()
                .is_some_and(|part| part.name == key.columns[0] && part.prefix_length.is_none())
        });
    if !supported {
        return Err(format!(
            "ADD FOREIGN KEY `{}` lacks an explicit supporting index",
            key.name
        ));
    }
    if expected
        .inventory
        .foreign_keys
        .iter()
        .any(|existing| existing.name.eq_ignore_ascii_case(&key.name))
    {
        return Err(format!("foreign key `{}` already exists", key.name));
    }
    let referenced_schema = expected.inventory.schema.clone();
    expected
        .inventory
        .foreign_keys
        .push(crate::inventory::ForeignKeyInventory {
            table: table_name.to_string(),
            name: key.name.clone(),
            columns: key.columns.clone(),
            referenced_schema,
            referenced_table: key.referenced_table.clone(),
            referenced_columns: key.referenced_columns.clone(),
        });
    Ok(())
}

fn apply_modify_column(
    expected: &mut SemanticSchemaSnapshot,
    table_name: &str,
    column: &ParsedAddColumnAst,
) -> Result<(), String> {
    let table = expected
        .inventory
        .tables
        .iter_mut()
        .find(|table| table.name == table_name)
        .ok_or_else(|| format!("MODIFY table `{table_name}` is missing"))?;
    let previous = table
        .columns
        .iter()
        .position(|item| item.name.eq_ignore_ascii_case(&column.name))
        .ok_or_else(|| format!("MODIFY column `{}` is missing", column.name))?;
    let original = &table.columns[previous];
    if original.generated.is_some() || original.extra.contains("auto_increment") {
        return Err("MODIFY of generated or AUTO_INCREMENT columns is not modeled".into());
    }
    let mut replacement = column.clone();
    replacement.name = original.name.clone();
    table.columns.remove(previous);
    let insertion = match &column.after {
        None => previous,
        Some(_) => add_column_insertion_index(table, table_name, column, None)?,
    };
    let replacement = expected_added_column(table, table_name, &replacement, insertion)?;
    table.columns.insert(insertion, replacement);
    for (index, item) in table.columns.iter_mut().enumerate() {
        item.ordinal_position = (index + 1) as u32;
    }
    Ok(())
}

fn apply_rename_column(
    expected: &mut SemanticSchemaSnapshot,
    table_name: &str,
    old_name: &str,
    new_name: &str,
) -> Result<(), String> {
    let table = expected
        .inventory
        .tables
        .iter_mut()
        .find(|table| table.name == table_name)
        .ok_or_else(|| format!("RENAME table `{table_name}` is missing"))?;
    if table
        .columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(new_name))
    {
        return Err(format!("RENAME destination `{new_name}` already exists"));
    }
    if table
        .columns
        .iter()
        .any(|column| column.generated.is_some())
    {
        return Err("RENAME with generated-column dependencies is not modeled".into());
    }
    let column = table
        .columns
        .iter_mut()
        .find(|column| column.name.eq_ignore_ascii_case(old_name))
        .ok_or_else(|| format!("RENAME source `{old_name}` is missing"))?;
    column.name = new_name.to_string();
    rename_column_references(&mut table.primary_key, old_name, new_name);
    for index in &mut expected.inventory.indexes {
        if index.table == table_name {
            for part in &mut index.columns {
                if part.name.eq_ignore_ascii_case(old_name) {
                    part.name = new_name.to_string();
                }
            }
        }
    }
    for key in &mut expected.inventory.foreign_keys {
        if key.table == table_name {
            rename_column_references(&mut key.columns, old_name, new_name);
        }
        if key.referenced_table == table_name && key.referenced_schema == expected.inventory.schema
        {
            rename_column_references(&mut key.referenced_columns, old_name, new_name);
        }
    }
    Ok(())
}

fn rename_column_references(names: &mut [String], old_name: &str, new_name: &str) {
    for name in names {
        if name.eq_ignore_ascii_case(old_name) {
            *name = new_name.to_string();
        }
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
    let existing_index = table
        .columns
        .iter()
        .position(|item| item.name == column.name);
    validate_generated_references(table, table_name, column)?;
    let insertion = add_column_insertion_index(table, table_name, column, existing_index)?;
    let expected_column = expected_added_column(table, table_name, column, insertion)?;
    if let Some(index) = existing_index {
        if index == insertion && table.columns[index] == expected_column {
            return Ok(());
        }
        return Err(format!(
            "ADD COLUMN target `{table_name}` already contains divergent `{}`",
            column.name
        ));
    }
    table.columns.insert(insertion, expected_column);
    for (index, item) in table.columns.iter_mut().enumerate() {
        item.ordinal_position = (index + 1) as u32;
    }
    Ok(())
}

fn add_column_insertion_index(
    table: &crate::inventory::TableInventory,
    table_name: &str,
    column: &ParsedAddColumnAst,
    existing_index: Option<usize>,
) -> Result<usize, String> {
    if column.if_not_exists
        && let Some(index) = existing_index
    {
        return Ok(index);
    }
    match &column.after {
        Some(after) => table
            .columns
            .iter()
            .position(|item| item.name == *after)
            .map(|position| position + 1)
            .ok_or_else(|| format!("ADD COLUMN AFTER target `{table_name}`.`{after}` is missing")),
        None => match existing_index {
            Some(_) => Ok(table.columns.len() - 1),
            None => Ok(table.columns.len()),
        },
    }
}

fn expected_added_column(
    table: &crate::inventory::TableInventory,
    table_name: &str,
    column: &ParsedAddColumnAst,
    insertion: usize,
) -> Result<crate::inventory::ColumnInventory, String> {
    let table_collation = table
        .collation
        .as_deref()
        .ok_or_else(|| format!("ALTER TABLE target `{table_name}` has no default collation"))?;
    let table_character_set = table_collation.split('_').next().unwrap_or(table_collation);
    let (character_set, collation) = match (&column.character_set, &column.collation) {
        (Some(character_set), Some(collation)) => {
            (Some(character_set.clone()), Some(collation.clone()))
        }
        _ => column_default_encoding(&column.data_type, table_character_set, table_collation),
    };
    let (default_value, extra) = match &column.generated {
        Some(_) => (None, "STORED GENERATED".to_string()),
        None => expected_added_column_default(&column.data_type, column.default_value.as_deref()),
    };
    let (column_type, data_type) = if column.data_type == "json" {
        ("longtext".to_string(), "longtext".to_string())
    } else {
        (column.column_type.clone(), column.data_type.clone())
    };
    Ok(crate::inventory::ColumnInventory {
        name: column.name.clone(),
        ordinal_position: (insertion + 1) as u32,
        column_type,
        data_type,
        is_nullable: column.nullable,
        character_set,
        collation,
        default_value,
        extra,
        comment: column.comment.clone(),
        generated: column
            .generated
            .as_ref()
            .map(|expression| crate::inventory::GeneratedColumn {
                expression: super::transform::mysql_generation_expression(expression),
                generation_kind: "STORED".to_string(),
            }),
    })
}

/// MySQL rejects generation expressions over AUTO_INCREMENT columns and this grammar admits
/// only ordinary existing columns, so every reference must name one.
fn validate_generated_references(
    table: &crate::inventory::TableInventory,
    table_name: &str,
    column: &ParsedAddColumnAst,
) -> Result<(), String> {
    let Some(expression) = &column.generated else {
        return Ok(());
    };
    for name in super::transform::generated_referenced_columns(expression) {
        let ordinary = table.columns.iter().any(|item| {
            item.name == name && item.generated.is_none() && !item.extra.contains("auto_increment")
        });
        if !ordinary {
            return Err(format!(
                "generated column `{table_name}`.`{}` references non-ordinary column `{name}`",
                column.name
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_json_alias_checks(
    columns: &[&ParsedAddColumnAst],
    checks: &[(String, String, bool)],
) -> Result<(), String> {
    for column in columns {
        let valid = checks.iter().any(|(_, clause, enforced)| {
            *enforced && json_valid_check_matches(clause, &column.name)
        });
        if !valid {
            return Err(format!(
                "JSON alias `{}` lacks its enforced JSON_VALID CHECK",
                column.name
            ));
        }
    }
    Ok(())
}

pub(crate) fn json_valid_check_matches(clause: &str, column: &str) -> bool {
    let Ok((tokens, quoted)) = super::tokenizer::tokenize_ddl_with_quoted_flags(clause) else {
        return false;
    };
    let (tokens, quoted) = strip_check_parentheses(&tokens, &quoted);
    if tokens.len() < 4 || quoted[0] || !tokens[0].eq_ignore_ascii_case("JSON_VALID") {
        return false;
    }
    if tokens[1] != "("
        || quoted[1]
        || tokens.last().map(String::as_str) != Some(")")
        || quoted[tokens.len() - 1]
    {
        return false;
    }
    let (argument, _) =
        strip_check_parentheses(&tokens[2..tokens.len() - 1], &quoted[2..quoted.len() - 1]);
    argument.len() == 1 && argument[0].eq_ignore_ascii_case(column)
}

fn strip_check_parentheses<'a>(
    mut tokens: &'a [String],
    mut quoted: &'a [bool],
) -> (&'a [String], &'a [bool]) {
    while check_has_outer_parentheses(tokens, quoted) {
        tokens = &tokens[1..tokens.len() - 1];
        quoted = &quoted[1..quoted.len() - 1];
    }
    (tokens, quoted)
}

fn check_has_outer_parentheses(tokens: &[String], quoted: &[bool]) -> bool {
    if tokens.len() < 2 || tokens[0] != "(" || quoted[0] {
        return false;
    }
    let mut depth = 0_i32;
    for (index, (token, quoted)) in tokens.iter().zip(quoted).enumerate() {
        if !quoted {
            match token.as_str() {
                "(" => depth += 1,
                ")" => depth -= 1,
                _ => {}
            }
        }
        if depth <= 0 {
            return depth == 0 && index == tokens.len() - 1;
        }
    }
    false
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

fn apply_drop_index(
    expected: &mut SemanticSchemaSnapshot,
    table_name: &str,
    index: &super::model::ParsedDropIndexAst,
) -> Result<(), String> {
    let before = expected.inventory.indexes.len();
    expected.inventory.indexes.retain(|target_index| {
        !target_index.table.eq_ignore_ascii_case(table_name)
            || !target_index.name.eq_ignore_ascii_case(&index.name)
    });
    if expected.inventory.indexes.len() == before {
        return Err(format!(
            "DROP INDEX target `{table_name}` lacks `{}`",
            index.name
        ));
    }
    Ok(())
}

fn apply_add_key(
    expected: &mut SemanticSchemaSnapshot,
    ast: &ParsedIndexAst,
    if_not_exists: bool,
) -> Result<(), String> {
    let (table, indexes) = validate_index_table(expected, ast)?;
    if if_not_exists && let Some(existing) = indexes.iter().find(|index| index.name == ast.name) {
        if **existing == index_inventory_from_ast(ast) {
            return Ok(());
        }
        return Err(format!(
            "guarded ADD INDEX target `{}`.`{}` already exists with a divergent definition",
            ast.table, ast.name
        ));
    }
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
        if column
            .generated
            .as_ref()
            .is_some_and(|generated| generated.generation_kind != "STORED")
        {
            return Err(format!(
                "index column `{}` is virtual generated",
                part.column
            ));
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
    (operation.family == DdlFamily::Index
        && operation.object_kind == DdlObjectKind::Index
        && operation.index_ast.is_some())
        || (operation.family == DdlFamily::Table && operation.create_table_ast.is_some())
}

#[cfg(test)]
mod json_alias_check_tests {
    use super::*;

    #[test]
    fn json_alias_check_requires_exact_column_and_enforcement() {
        for clause in [
            "json_valid(`source_layout`)",
            "((JSON_VALID((`source_layout`))))",
        ] {
            assert!(json_valid_check_matches(clause, "source_layout"));
        }
        for clause in [
            "JSON_VALID(other)",
            "JSON_VALID(source_layout) OR 1",
            "JSON_VALID('source_layout')",
            "`JSON_VALID`(source_layout)",
            "(JSON_VALID(source_layout)) OR (1)",
        ] {
            assert!(!json_valid_check_matches(clause, "source_layout"));
        }
        let column = ParsedAddColumnAst {
            name: "source_layout".into(),
            if_not_exists: true,
            column_type: "json".into(),
            data_type: "json".into(),
            nullable: true,
            default_value: None,
            comment: String::new(),
            after: None,
            character_set: Some("utf8mb4".into()),
            collation: Some("utf8mb4_bin".into()),
            generated: None,
        };
        assert!(validate_json_alias_checks(&[&column], &[]).is_err());
        assert!(
            validate_json_alias_checks(
                &[&column],
                &[(
                    "source_layout".into(),
                    "JSON_VALID(source_layout)".into(),
                    false
                )]
            )
            .is_err()
        );
        assert!(
            validate_json_alias_checks(
                &[&column],
                &[(
                    "releases_pages_chk_1".into(),
                    "JSON_VALID(source_layout)".into(),
                    true
                )]
            )
            .is_ok()
        );
    }
}
