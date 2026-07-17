use super::tokenizer::tokenize_ddl;
use std::collections::BTreeSet;

pub const DDL_TRANSFORMATION_VERSION: &str = "mariadb-mysql8-v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlTransformation {
    pub version: &'static str,
    pub target_sql: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RenameColumnClause {
    old_name: String,
    new_name: String,
}

pub fn supports_rename_columns_if_exists(source_sql: &str) -> bool {
    tokenize_ddl(source_sql)
        .ok()
        .and_then(|tokens| parse_rename_columns_if_exists(&tokens).ok())
        .is_some()
}

pub fn transform_rename_columns_if_exists(
    source_sql: &str,
    target_columns: &BTreeSet<String>,
) -> Result<DdlTransformation, String> {
    let tokens = tokenize_ddl(source_sql)?;
    let (table, clauses) = parse_rename_columns_if_exists(&tokens)?;
    let executable_clauses = select_executable_renames(&table, clauses, target_columns)?;
    let target_sql = emit_rename_columns(&table, &executable_clauses);
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql,
    })
}

fn parse_rename_columns_if_exists(
    tokens: &[String],
) -> Result<(String, Vec<RenameColumnClause>), String> {
    require_keyword(tokens, 0, "ALTER")?;
    require_keyword(tokens, 1, "TABLE")?;
    let table = require_identifier(tokens, 2, "ALTER TABLE name")?;
    let mut clauses = Vec::new();
    let mut index = 3;
    while index < tokens.len() {
        require_keyword(tokens, index, "RENAME")?;
        require_keyword(tokens, index + 1, "COLUMN")?;
        require_keyword(tokens, index + 2, "IF")?;
        require_keyword(tokens, index + 3, "EXISTS")?;
        let old_name = require_identifier(tokens, index + 4, "renamed source column")?;
        require_keyword(tokens, index + 5, "TO")?;
        let new_name = require_identifier(tokens, index + 6, "renamed target column")?;
        clauses.push(RenameColumnClause { old_name, new_name });
        index += 7;
        if index == tokens.len() {
            break;
        }
        if tokens.get(index).map(String::as_str) != Some(",") {
            return Err(format!(
                "expected comma between RENAME COLUMN clauses, found {:?}",
                tokens.get(index)
            ));
        }
        index += 1;
    }
    if clauses.is_empty() {
        return Err("ALTER TABLE has no RENAME COLUMN IF EXISTS clauses".to_string());
    }
    Ok((table, clauses))
}

fn select_executable_renames(
    table: &str,
    clauses: Vec<RenameColumnClause>,
    target_columns: &BTreeSet<String>,
) -> Result<Vec<RenameColumnClause>, String> {
    let mut simulated_columns = target_columns.clone();
    let mut executable = Vec::new();
    for clause in clauses {
        if !simulated_columns.contains(&clause.old_name) {
            continue;
        }
        if simulated_columns.contains(&clause.new_name) {
            return Err(format!(
                "cannot transform ALTER TABLE {table}: old column {} and new column {} both exist",
                clause.old_name, clause.new_name
            ));
        }
        simulated_columns.remove(&clause.old_name);
        simulated_columns.insert(clause.new_name.clone());
        executable.push(clause);
    }
    Ok(executable)
}

fn emit_rename_columns(table: &str, clauses: &[RenameColumnClause]) -> Option<String> {
    if clauses.is_empty() {
        return None;
    }
    let clauses = clauses
        .iter()
        .map(|clause| {
            format!(
                "RENAME COLUMN {} TO {}",
                quote_identifier(&clause.old_name),
                quote_identifier(&clause.new_name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("ALTER TABLE {} {clauses}", quote_identifier(table)))
}

fn require_keyword(tokens: &[String], index: usize, expected: &str) -> Result<(), String> {
    match tokens.get(index) {
        Some(actual) if actual.eq_ignore_ascii_case(expected) => Ok(()),
        actual => Err(format!(
            "expected {expected} at token {index}, found {actual:?}"
        )),
    }
}

fn require_identifier(tokens: &[String], index: usize, context: &str) -> Result<String, String> {
    let value = tokens
        .get(index)
        .ok_or_else(|| format!("missing {context}"))?;
    if matches!(value.as_str(), "." | "," | "(" | ")" | "<string>") {
        return Err(format!("invalid {context}: {value}"));
    }
    Ok(value.clone())
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}
