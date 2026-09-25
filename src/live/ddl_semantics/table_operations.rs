use super::model::{DdlOperation, SemanticSchemaSnapshot};
use super::tokenizer::tokenize_ddl_with_quoted_flags;
use super::transform::{DDL_TRANSFORMATION_VERSION, DdlTransformation};
use serde::Serialize;
use serde_json::json;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TableOperation {
    Drop { if_exists: bool },
    Rename,
    Truncate,
}

pub(super) fn parse(sql: &str) -> Result<TableOperation, String> {
    let active_comment = ["/*!", "/*+", "/*M!", "/*m!"]
        .iter()
        .any(|marker| sql.contains(marker));
    if active_comment || sql.contains('"') {
        return Err(
            "table operation requires ordinary comments and backtick/unquoted names".into(),
        );
    }
    let (mut tokens, mut quoted) = tokenize_ddl_with_quoted_flags(sql)?;
    if tokens.last().map(String::as_str) == Some(";") {
        tokens.pop();
        quoted.pop();
    }
    let kind = tokens
        .first()
        .map(|word| word.to_ascii_uppercase())
        .ok_or("empty table DDL")?;
    match kind.as_str() {
        "DROP" => parse_drop(&tokens, &quoted),
        "RENAME" => {
            require_keywords(&tokens, &quoted, &[(0, "RENAME"), (1, "TABLE"), (3, "TO")])?;
            require_names(&tokens, &[2, 4], 5)?;
            Ok(TableOperation::Rename)
        }
        "TRUNCATE" => parse_truncate(&tokens, &quoted),
        _ => Err("not a modeled table lifecycle operation".into()),
    }
}

fn parse_drop(tokens: &[String], quoted: &[bool]) -> Result<TableOperation, String> {
    require_keywords(tokens, quoted, &[(0, "DROP"), (1, "TABLE")])?;
    let if_exists = tokens
        .get(2)
        .is_some_and(|word| word.eq_ignore_ascii_case("IF"));
    let name = if if_exists {
        require_keywords(tokens, quoted, &[(2, "IF"), (3, "EXISTS")])?;
        4
    } else {
        2
    };
    require_names(tokens, &[name], name + 1)?;
    Ok(TableOperation::Drop { if_exists })
}

fn parse_truncate(tokens: &[String], quoted: &[bool]) -> Result<TableOperation, String> {
    require_keywords(tokens, quoted, &[(0, "TRUNCATE")])?;
    let name = if tokens
        .get(1)
        .is_some_and(|word| word.eq_ignore_ascii_case("TABLE"))
    {
        require_keywords(tokens, quoted, &[(1, "TABLE")])?;
        2
    } else {
        1
    };
    require_names(tokens, &[name], name + 1)?;
    Ok(TableOperation::Truncate)
}

fn require_keywords(
    tokens: &[String],
    quoted: &[bool],
    words: &[(usize, &str)],
) -> Result<(), String> {
    for (index, word) in words {
        if quoted.get(*index) != Some(&false)
            || !tokens
                .get(*index)
                .is_some_and(|token| token.eq_ignore_ascii_case(word))
        {
            return Err(format!("expected unquoted table DDL keyword {word}"));
        }
    }
    Ok(())
}

fn require_names(tokens: &[String], names: &[usize], length: usize) -> Result<(), String> {
    if tokens.len() != length {
        return Err("table operation must contain exactly one unqualified table/pair".into());
    }
    for index in names {
        let name = &tokens[*index];
        let valid_start =
            name.starts_with(|character: char| character.is_ascii_alphabetic() || character == '_');
        if !valid_start
            || !name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(format!("unsupported table identifier {name:?}"));
        }
    }
    Ok(())
}

pub(super) fn render(operation: &DdlOperation) -> Result<DdlTransformation, String> {
    let name = &operation.primary_object;
    let sql = match operation
        .table_operation_ast
        .as_ref()
        .ok_or("missing table operation AST")?
    {
        TableOperation::Drop { if_exists } => {
            let guard = if *if_exists { "IF EXISTS " } else { "" };
            format!("DROP TABLE {guard}`{name}`")
        }
        TableOperation::Rename => format!(
            "RENAME TABLE `{name}` TO `{}`",
            operation
                .secondary_object
                .as_deref()
                .ok_or("missing rename destination")?
        ),
        TableOperation::Truncate => format!("TRUNCATE TABLE `{name}`"),
    };
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: Some(sql),
    })
}

pub(super) fn observe(
    snapshot: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let names = super::affected_tables(operation);
    let tables = names
        .iter()
        .map(|name| table_definition_runtime(snapshot, name))
        .collect::<Vec<_>>();
    let (indexes, keys, triggers) = related_metadata(snapshot, &names);
    serde_json::to_string(
        &json!({"tables":tables,"indexes":indexes,"foreign_keys":keys,"triggers":triggers}),
    )
    .map_err(|error| format!("table operation state: {error}"))
}

fn table_definition_runtime(snapshot: &SemanticSchemaSnapshot, name: &str) -> serde_json::Value {
    let definition = snapshot
        .inventory
        .tables
        .iter()
        .find(|table| table.name == name);
    let runtime = snapshot.table_runtime.get(name).map(
        |runtime| json!({"row_count":runtime.row_count, "auto_increment":runtime.auto_increment}),
    );
    json!({"name":name, "definition":definition, "runtime":runtime})
}

fn related_metadata(
    snapshot: &SemanticSchemaSnapshot,
    names: &[&str],
) -> (serde_json::Value, serde_json::Value, serde_json::Value) {
    let mut indexes = snapshot
        .inventory
        .indexes
        .iter()
        .filter(|index| names.contains(&index.table.as_str()))
        .collect::<Vec<_>>();
    indexes.sort_by_key(|index| (&index.table, &index.name));
    let mut keys = snapshot
        .inventory
        .foreign_keys
        .iter()
        .filter(|key| {
            names.contains(&key.table.as_str())
                || (key.referenced_schema == snapshot.inventory.schema
                    && names.contains(&key.referenced_table.as_str()))
        })
        .collect::<Vec<_>>();
    keys.sort_by_key(|key| (&key.table, &key.name));
    let mut triggers = snapshot
        .inventory
        .triggers
        .iter()
        .filter(|trigger| names.contains(&trigger.table.as_str()))
        .collect::<Vec<_>>();
    triggers.sort_by_key(|trigger| (&trigger.table, &trigger.name));
    (json!(indexes), json!(keys), json!(triggers))
}

pub(super) fn expected(
    target: &SemanticSchemaSnapshot,
    operation: &DdlOperation,
) -> Result<String, String> {
    let ast = operation
        .table_operation_ast
        .as_ref()
        .ok_or("missing table operation AST")?;
    let name = &operation.primary_object;
    if !target
        .inventory
        .tables
        .iter()
        .any(|table| table.name == *name)
    {
        return match ast {
            TableOperation::Drop { if_exists: true } => observe(target, operation),
            _ => Err(format!("table operation source `{name}` is missing")),
        };
    }
    validate_source_table(target, name)?;
    let mut expected = target.clone();
    apply_operation(&mut expected, operation, ast)?;
    observe(&expected, operation)
}

fn validate_source_table(snapshot: &SemanticSchemaSnapshot, name: &str) -> Result<(), String> {
    let table = snapshot
        .inventory
        .tables
        .iter()
        .find(|table| table.name == name)
        .ok_or_else(|| format!("table operation source `{name}` is missing"))?;
    if table.table_type != "BASE TABLE" || table.engine.as_deref() != Some("InnoDB") {
        return Err("table lifecycle requires an InnoDB base table".into());
    }
    if !snapshot.table_runtime.contains_key(name) {
        return Err(format!("table `{name}` lacks exact runtime metadata"));
    }
    Ok(())
}

fn apply_operation(
    snapshot: &mut SemanticSchemaSnapshot,
    operation: &DdlOperation,
    ast: &TableOperation,
) -> Result<(), String> {
    let name = &operation.primary_object;
    match ast {
        TableOperation::Drop { .. } => drop_table(snapshot, name),
        TableOperation::Rename => rename_table(
            snapshot,
            name,
            operation
                .secondary_object
                .as_deref()
                .ok_or("missing rename destination")?,
        ),
        TableOperation::Truncate => truncate_table(snapshot, name),
    }
}

fn ensure_no_external_references(
    snapshot: &SemanticSchemaSnapshot,
    name: &str,
) -> Result<(), String> {
    let referenced = snapshot.inventory.foreign_keys.iter().any(|key| {
        key.table != name
            && key.referenced_schema == snapshot.inventory.schema
            && key.referenced_table == name
    });
    if referenced {
        return Err(format!(
            "table `{name}` is referenced by another table's foreign key"
        ));
    }
    Ok(())
}

fn drop_table(snapshot: &mut SemanticSchemaSnapshot, name: &str) -> Result<(), String> {
    ensure_no_external_references(snapshot, name)?;
    snapshot.inventory.tables.retain(|table| table.name != name);
    snapshot
        .inventory
        .indexes
        .retain(|index| index.table != name);
    snapshot
        .inventory
        .foreign_keys
        .retain(|key| key.table != name);
    snapshot
        .inventory
        .triggers
        .retain(|trigger| trigger.table != name);
    snapshot.table_runtime.remove(name);
    Ok(())
}

fn truncate_table(snapshot: &mut SemanticSchemaSnapshot, name: &str) -> Result<(), String> {
    ensure_no_external_references(snapshot, name)?;
    let runtime = snapshot
        .table_runtime
        .get_mut(name)
        .ok_or("missing truncate runtime")?;
    runtime.row_count = 0;
    if runtime.auto_increment.is_some() {
        runtime.auto_increment = Some(1);
    }
    Ok(())
}

fn rename_table(snapshot: &mut SemanticSchemaSnapshot, old: &str, new: &str) -> Result<(), String> {
    if snapshot
        .inventory
        .tables
        .iter()
        .any(|table| table.name.eq_ignore_ascii_case(new))
        || snapshot
            .inventory
            .views
            .iter()
            .any(|view| view.name.eq_ignore_ascii_case(new))
    {
        return Err(format!("rename destination `{new}` already exists"));
    }
    let table = snapshot
        .inventory
        .tables
        .iter_mut()
        .find(|table| table.name == old)
        .ok_or("missing rename table")?;
    table.name = new.to_string();
    let runtime = snapshot
        .table_runtime
        .remove(old)
        .ok_or("missing rename runtime")?;
    snapshot.table_runtime.insert(new.to_string(), runtime);
    rename_dependent_metadata(snapshot, old, new);
    Ok(())
}

fn rename_dependent_metadata(snapshot: &mut SemanticSchemaSnapshot, old: &str, new: &str) {
    for index in &mut snapshot.inventory.indexes {
        if index.table == old {
            index.table = new.into();
        }
    }
    for trigger in &mut snapshot.inventory.triggers {
        if trigger.table == old {
            trigger.table = new.into();
        }
    }
    for key in &mut snapshot.inventory.foreign_keys {
        if key.table == old {
            key.table = new.into();
            if let Some(suffix) = key.name.strip_prefix(&format!("{old}_ibfk_")) {
                key.name = format!("{new}_ibfk_{suffix}");
            }
        }
        if key.referenced_schema == snapshot.inventory.schema && key.referenced_table == old {
            key.referenced_table = new.into();
        }
    }
}
