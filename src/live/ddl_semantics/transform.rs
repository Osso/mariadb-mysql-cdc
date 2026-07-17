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

pub fn supports_add_columns(source_sql: &str) -> bool {
    tokenize_ddl(source_sql)
        .ok()
        .is_some_and(|tokens| parse_add_columns(&tokens).is_ok())
}

pub fn transform_add_columns(source_sql: &str) -> Result<DdlTransformation, String> {
    let tokens = tokenize_ddl(source_sql)?;
    parse_add_columns(&tokens)?;
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: Some(normalize_ddl_sql(source_sql)?),
    })
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

fn parse_add_columns(tokens: &[String]) -> Result<(), String> {
    require_keyword(tokens, 0, "ALTER")?;
    require_keyword(tokens, 1, "TABLE")?;
    require_identifier(tokens, 2, "ALTER TABLE name")?;
    let mut index = 3;
    let mut clause_count = 0;
    while index < tokens.len() {
        require_keyword(tokens, index, "ADD")?;
        require_keyword(tokens, index + 1, "COLUMN")?;
        require_identifier(tokens, index + 2, "added column")?;
        index += 3;
        index = parse_observed_column_type(tokens, index)?;
        index = parse_observed_column_options(tokens, index)?;
        clause_count += 1;
        if index == tokens.len() {
            break;
        }
        if tokens.get(index).map(String::as_str) != Some(",") {
            return Err(format!(
                "expected comma between ADD COLUMN clauses, found {:?}",
                tokens.get(index)
            ));
        }
        index += 1;
    }
    if clause_count == 0 {
        return Err("ALTER TABLE has no ADD COLUMN clauses".to_string());
    }
    Ok(())
}

fn parse_observed_column_type(tokens: &[String], mut index: usize) -> Result<usize, String> {
    let column_type = require_identifier(tokens, index, "added column type")?;
    if !matches!(column_type.to_ascii_uppercase().as_str(), "VARCHAR" | "DATETIME" | "SMALLINT") {
        return Err(format!("unsupported production ADD COLUMN type {column_type}"));
    }
    index += 1;
    if tokens.get(index).map(String::as_str) == Some("(") {
        require_identifier(tokens, index + 1, "column type length")?;
        require_keyword(tokens, index + 2, ")")?;
        index += 3;
    }
    if tokens
        .get(index)
        .is_some_and(|token| token.eq_ignore_ascii_case("UNSIGNED"))
    {
        index += 1;
    }
    Ok(index)
}

fn parse_observed_column_options(tokens: &[String], mut index: usize) -> Result<usize, String> {
    while index < tokens.len() && tokens[index] != "," {
        if tokens[index].eq_ignore_ascii_case("NULL") {
            index += 1;
        } else if tokens[index].eq_ignore_ascii_case("DEFAULT") {
            require_keyword(tokens, index + 1, "NULL")?;
            index += 2;
        } else if tokens[index].eq_ignore_ascii_case("COMMENT") {
            require_keyword(tokens, index + 1, "<string>")?;
            index += 2;
        } else if tokens[index].eq_ignore_ascii_case("AFTER") {
            require_identifier(tokens, index + 1, "AFTER column")?;
            index += 2;
        } else {
            return Err(format!(
                "unsupported production ADD COLUMN option {:?}",
                tokens.get(index)
            ));
        }
    }
    Ok(index)
}

fn normalize_ddl_sql(source_sql: &str) -> Result<String, String> {
    let characters = source_sql.chars().collect::<Vec<_>>();
    let mut output = String::new();
    let mut quote = None;
    let mut pending_space = false;
    let mut index = 0;
    while index < characters.len() {
        let character = characters[index];
        if let Some(active_quote) = quote {
            output.push(character);
            if character == active_quote {
                if characters.get(index + 1) == Some(&active_quote) {
                    output.push(active_quote);
                    index += 2;
                    continue;
                }
                quote = None;
            } else if character == '\\' && active_quote == '\'' {
                if let Some(escaped) = characters.get(index + 1) {
                    output.push(*escaped);
                    index += 2;
                    continue;
                }
            }
            index += 1;
            continue;
        }
        if matches!(character, '`' | '\'' | '"') {
            if pending_space && !output.is_empty() && !output.ends_with([' ', '(', '.']) {
                output.push(' ');
            }
            pending_space = false;
            quote = Some(character);
            output.push(character);
            index += 1;
            continue;
        }
        if character == '/' && characters.get(index + 1) == Some(&'*') {
            index += 2;
            while index + 1 < characters.len()
                && !(characters[index] == '*' && characters[index + 1] == '/')
            {
                index += 1;
            }
            if index + 1 >= characters.len() {
                return Err("unterminated DDL block comment".to_string());
            }
            index += 2;
            pending_space = true;
            continue;
        }
        if character == '#'
            || (character == '-'
                && characters.get(index + 1) == Some(&'-')
                && characters
                    .get(index + 2)
                    .is_none_or(|after| after.is_whitespace() || after.is_control()))
        {
            while characters
                .get(index)
                .is_some_and(|current| *current != '\n')
            {
                index += 1;
            }
            pending_space = true;
            continue;
        }
        if character.is_whitespace() {
            pending_space = true;
            index += 1;
            continue;
        }
        if character == ',' {
            while output.ends_with(' ') {
                output.pop();
            }
            output.push(',');
            output.push(' ');
            pending_space = false;
            index += 1;
            continue;
        }
        if pending_space
            && !output.is_empty()
            && !output.ends_with([' ', '(', '.'])
            && !matches!(character, ')' | '.' | ';')
        {
            output.push(' ');
        }
        pending_space = false;
        output.push(character);
        index += 1;
    }
    if quote.is_some() {
        return Err("unterminated DDL quote".to_string());
    }
    Ok(output.trim().trim_end_matches(';').trim().to_string())
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
