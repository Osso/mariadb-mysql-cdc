use super::model::{
    ParsedAddColumnAst, ParsedAlterClause, ParsedAlterTableAst, ParsedDropColumnAst,
    ParsedIndexAst, ParsedIndexKeyPart,
};
#[cfg(test)]
use super::model::{ParsedCreateColumnAst, ParsedCreateTableAst};
use super::tokenizer::{ddl_contains_comments, tokenize_ddl};
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

pub fn supports_production_alter_table(source_sql: &str) -> bool {
    parse_production_alter_table_ast(source_sql).is_ok_and(|ast| {
        ast.clauses.iter().all(|clause| {
            matches!(
                clause,
                ParsedAlterClause::AddColumn(_) | ParsedAlterClause::AddKey(_)
            )
        })
    })
}

pub fn transform_production_alter_table(source_sql: &str) -> Result<DdlTransformation, String> {
    let ast = parse_production_alter_table_ast(source_sql)?;
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: Some(render_production_alter_table(&ast)),
    })
}

fn render_production_alter_table(ast: &ParsedAlterTableAst) -> String {
    let clauses = ast
        .clauses
        .iter()
        .map(render_production_alter_clause)
        .collect::<Vec<_>>()
        .join(", ");
    format!("ALTER TABLE {} {clauses}", quote_identifier(&ast.table))
}

fn render_production_alter_clause(clause: &ParsedAlterClause) -> String {
    match clause {
        ParsedAlterClause::AddColumn(column) => render_add_column(column),
        ParsedAlterClause::AddKey(index) => render_add_key(index),
        ParsedAlterClause::DropColumn(column) => {
            format!("DROP COLUMN {}", quote_identifier(&column.name))
        }
    }
}

fn render_add_column(column: &ParsedAddColumnAst) -> String {
    let mut sql = format!(
        "ADD COLUMN {} {} NULL DEFAULT NULL",
        quote_identifier(&column.name),
        column.column_type.to_ascii_uppercase()
    );
    if !column.comment.is_empty() {
        sql.push_str(&format!(
            " COMMENT {}",
            quote_string_literal(&column.comment)
        ));
    }
    if let Some(after) = &column.after {
        sql.push_str(&format!(" AFTER {}", quote_identifier(after)));
    }
    sql
}

fn render_add_key(index: &ParsedIndexAst) -> String {
    let columns = index
        .key_parts
        .iter()
        .map(|part| quote_identifier(&part.column))
        .collect::<Vec<_>>()
        .join(", ");
    let key_kind = if index.unique { "UNIQUE KEY" } else { "KEY" };
    format!(
        "ADD {key_kind} {} ({columns})",
        quote_identifier(&index.name)
    )
}

fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
pub fn parse_fixture_create_table(source_sql: &str) -> Result<ParsedCreateTableAst, String> {
    if ddl_contains_comments(source_sql) {
        return Err("fixture CREATE TABLE comments are not supported".to_string());
    }
    if source_sql.contains('"') {
        return Err("fixture CREATE TABLE double-quoted identifiers are not supported".to_string());
    }
    let tokens = tokenize_ddl(source_sql)?;
    require_keyword(&tokens, 0, "CREATE")?;
    require_keyword(&tokens, 1, "TABLE")?;
    let name = require_identifier(&tokens, 2, "CREATE TABLE name")?;
    require_keyword(&tokens, 3, "(")?;
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();
    let mut indexes = Vec::new();
    let mut index = 4;
    loop {
        if tokens.get(index).map(String::as_str) == Some(")") {
            index += 1;
            break;
        }
        if tokens
            .get(index)
            .is_some_and(|token| token.eq_ignore_ascii_case("KEY"))
        {
            let (parsed_index, next_index) = parse_fixture_table_key(&tokens, index, &name)?;
            indexes.push(parsed_index);
            index = next_index;
        } else {
            let (column, is_primary, next_index) = parse_fixture_table_column(&tokens, index)?;
            if is_primary {
                primary_key.push(column.name.clone());
            }
            columns.push(column);
            index = next_index;
        }
        match tokens.get(index).map(String::as_str) {
            Some(",") => index += 1,
            Some(")") => {
                index += 1;
                break;
            }
            actual => {
                return Err(format!(
                    "expected comma or closing parenthesis in fixture CREATE TABLE, found {actual:?}"
                ));
            }
        }
    }
    require_keyword(&tokens, index, "ENGINE")?;
    require_keyword(&tokens, index + 1, "=")?;
    let engine = require_identifier(&tokens, index + 2, "CREATE TABLE engine")?;
    if !engine.eq_ignore_ascii_case("InnoDB") {
        return Err(format!("unsupported fixture CREATE TABLE engine {engine}"));
    }
    index += 3;
    if tokens.get(index).map(String::as_str) == Some(";") {
        index += 1;
    }
    if index != tokens.len() {
        return Err(format!(
            "unsupported trailing fixture CREATE TABLE syntax {:?}",
            &tokens[index..]
        ));
    }
    if columns.is_empty() || primary_key.is_empty() {
        return Err("fixture CREATE TABLE requires columns and an inline primary key".to_string());
    }
    Ok(ParsedCreateTableAst {
        name,
        columns,
        primary_key,
        indexes,
        engine: "InnoDB".to_string(),
    })
}

#[cfg(test)]
fn parse_fixture_table_column(
    tokens: &[String],
    index: usize,
) -> Result<(ParsedCreateColumnAst, bool, usize), String> {
    let name = require_identifier(tokens, index, "CREATE TABLE column name")?;
    let data_type = require_identifier(tokens, index + 1, "CREATE TABLE column type")?;
    let (column_type, mut next_index) = if data_type.eq_ignore_ascii_case("BIGINT") {
        ("bigint".to_string(), index + 2)
    } else if data_type.eq_ignore_ascii_case("VARCHAR") {
        require_keyword(tokens, index + 2, "(")?;
        let length = require_identifier(tokens, index + 3, "VARCHAR length")?;
        let parsed_length = length
            .parse::<u32>()
            .map_err(|_| format!("invalid VARCHAR length {length}"))?;
        if parsed_length == 0 || parsed_length.to_string() != length {
            return Err(format!("noncanonical VARCHAR length {length}"));
        }
        require_keyword(tokens, index + 4, ")")?;
        (format!("varchar({parsed_length})"), index + 5)
    } else {
        return Err(format!("unsupported fixture CREATE TABLE type {data_type}"));
    };
    require_keyword(tokens, next_index, "NOT")?;
    require_keyword(tokens, next_index + 1, "NULL")?;
    next_index += 2;
    let primary = tokens
        .get(next_index)
        .is_some_and(|token| token.eq_ignore_ascii_case("PRIMARY"));
    if primary {
        require_keyword(tokens, next_index + 1, "KEY")?;
        next_index += 2;
    }
    Ok((
        ParsedCreateColumnAst {
            name,
            column_type,
            nullable: false,
        },
        primary,
        next_index,
    ))
}

#[cfg(test)]
fn parse_fixture_table_key(
    tokens: &[String],
    index: usize,
    table: &str,
) -> Result<(ParsedIndexAst, usize), String> {
    require_keyword(tokens, index, "KEY")?;
    let name = require_identifier(tokens, index + 1, "CREATE TABLE key name")?;
    require_keyword(tokens, index + 2, "(")?;
    let column = require_identifier(tokens, index + 3, "CREATE TABLE key column")?;
    require_keyword(tokens, index + 4, ")")?;
    Ok((
        ParsedIndexAst {
            create: true,
            name,
            table: table.to_string(),
            unique: false,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: None,
            key_parts: vec![ParsedIndexKeyPart {
                column,
                prefix_length: None,
                order: "ASC".to_string(),
                collation: Some("A".to_string()),
            }],
        },
        index + 5,
    ))
}

#[cfg(test)]
pub fn transform_fixture_create_table(source_sql: &str) -> Result<DdlTransformation, String> {
    let ast = parse_fixture_create_table(source_sql)?;
    let mut definitions = ast
        .columns
        .iter()
        .map(|column| {
            format!(
                "{} {} NOT NULL",
                quote_identifier(&column.name),
                column.column_type.to_ascii_uppercase()
            )
        })
        .collect::<Vec<_>>();
    definitions.push(format!(
        "PRIMARY KEY ({})",
        ast.primary_key
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    definitions.extend(ast.indexes.iter().map(|index| {
        format!(
            "KEY {} ({})",
            quote_identifier(&index.name),
            index
                .key_parts
                .iter()
                .map(|part| quote_identifier(&part.column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }));
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: Some(format!(
            "CREATE TABLE {} ({}) ENGINE={}",
            quote_identifier(&ast.name),
            definitions.join(", "),
            ast.engine
        )),
    })
}

pub fn supports_drop_columns_if_exists(source_sql: &str) -> bool {
    parse_production_alter_table_ast(source_sql).is_ok_and(|ast| {
        ast.clauses
            .iter()
            .all(|clause| matches!(clause, ParsedAlterClause::DropColumn(_)))
    })
}

pub fn transform_drop_columns_if_exists(
    source_sql: &str,
    target_columns: &BTreeSet<String>,
) -> Result<DdlTransformation, String> {
    let ast = parse_production_alter_table_ast(source_sql)?;
    if !ast
        .clauses
        .iter()
        .all(|clause| matches!(clause, ParsedAlterClause::DropColumn(_)))
    {
        return Err("ALTER TABLE mixes DROP COLUMN IF EXISTS with unsupported clauses".to_string());
    }
    let mut remaining_columns = target_columns.clone();
    let mut executable_columns = Vec::new();
    for clause in &ast.clauses {
        let ParsedAlterClause::DropColumn(column) = clause else {
            continue;
        };
        let Some(target_column) = remaining_columns
            .iter()
            .find(|target| target.eq_ignore_ascii_case(&column.name))
            .cloned()
        else {
            continue;
        };
        remaining_columns.remove(&target_column);
        executable_columns.push(target_column);
    }
    let target_sql = if executable_columns.is_empty() {
        None
    } else {
        Some(format!(
            "ALTER TABLE {} {}",
            quote_identifier(&ast.table),
            executable_columns
                .iter()
                .map(|column| format!("DROP COLUMN {}", quote_identifier(column)))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    };
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql,
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

pub fn parse_production_alter_table_ast(source_sql: &str) -> Result<ParsedAlterTableAst, String> {
    if ddl_contains_comments(source_sql) {
        return Err("production ALTER TABLE comments are not supported".to_string());
    }
    let tokens = tokenize_ddl(source_sql)?;
    require_keyword(&tokens, 0, "ALTER")?;
    require_keyword(&tokens, 1, "TABLE")?;
    let table = require_identifier(&tokens, 2, "ALTER TABLE name")?;
    let mut literals = extract_single_quoted_literals(source_sql)?.into_iter();
    let mut clauses = Vec::new();
    let mut index = 3;
    while index < tokens.len() {
        let (clause, next_index) = match tokens
            .get(index)
            .map(|token| token.to_ascii_uppercase())
            .as_deref()
        {
            Some("ADD") => parse_production_add_clause(&tokens, index, &table, &mut literals)?,
            Some("DROP") => parse_drop_column_clause(&tokens, index)?,
            actual => {
                return Err(format!(
                    "unsupported production ALTER TABLE clause {actual:?}"
                ));
            }
        };
        clauses.push(clause);
        index = next_index;
        if index == tokens.len() {
            break;
        }
        require_keyword(&tokens, index, ",")?;
        index += 1;
    }
    if clauses.is_empty() {
        return Err("ALTER TABLE has no supported clauses".to_string());
    }
    Ok(ParsedAlterTableAst { table, clauses })
}

fn parse_production_add_clause(
    tokens: &[String],
    index: usize,
    table: &str,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    match tokens
        .get(index + 1)
        .map(|token| token.to_ascii_uppercase())
    {
        Some(kind) if kind == "COLUMN" => parse_add_column_clause(tokens, index, literals),
        Some(kind) if kind == "KEY" => parse_add_key_clause(tokens, index + 1, table, false),
        Some(kind) if kind == "UNIQUE" => {
            require_keyword(tokens, index + 2, "KEY")?;
            parse_add_key_clause(tokens, index + 2, table, true)
        }
        actual => Err(format!(
            "unsupported production ALTER TABLE clause {actual:?}"
        )),
    }
}

struct ParsedColumnOptions {
    nullable: bool,
    default_value: Option<String>,
    comment: String,
    after: Option<String>,
    next_index: usize,
}

fn parse_add_column_clause(
    tokens: &[String],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    let name = require_identifier(tokens, index + 2, "added column")?;
    let (column_type, data_type, options_start) = parse_observed_column_type(tokens, index + 3)?;
    let options = parse_observed_column_options(tokens, options_start, literals)?;
    Ok((
        ParsedAlterClause::AddColumn(ParsedAddColumnAst {
            name,
            column_type,
            data_type,
            nullable: options.nullable,
            default_value: options.default_value,
            comment: options.comment,
            after: options.after,
        }),
        options.next_index,
    ))
}

fn parse_add_key_clause(
    tokens: &[String],
    key_index: usize,
    table: &str,
    unique: bool,
) -> Result<(ParsedAlterClause, usize), String> {
    let name = require_identifier(tokens, key_index + 1, "added key name")?;
    require_keyword(tokens, key_index + 2, "(")?;
    let mut key_parts = Vec::new();
    let mut column_index = key_index + 3;
    loop {
        let column = require_identifier(tokens, column_index, "added key column")?;
        key_parts.push(ParsedIndexKeyPart {
            column,
            prefix_length: None,
            order: "ASC".to_string(),
            collation: Some("A".to_string()),
        });
        column_index += 1;
        match tokens.get(column_index).map(String::as_str) {
            Some(",") => column_index += 1,
            Some(")") => {
                let ast = ParsedIndexAst {
                    create: true,
                    name,
                    table: table.to_string(),
                    unique,
                    index_type: "BTREE".to_string(),
                    visible: true,
                    comment: None,
                    key_parts,
                };
                return Ok((ParsedAlterClause::AddKey(ast), column_index + 1));
            }
            actual => {
                return Err(format!(
                    "expected comma or closing parenthesis in ADD KEY, found {actual:?}"
                ));
            }
        }
    }
}

fn parse_observed_column_type(
    tokens: &[String],
    mut index: usize,
) -> Result<(String, String, usize), String> {
    let data_type = require_identifier(tokens, index, "added column type")?.to_ascii_lowercase();
    if !matches!(data_type.as_str(), "varchar" | "datetime" | "smallint") {
        return Err(format!(
            "unsupported production ADD COLUMN type {data_type}"
        ));
    }
    index += 1;
    let mut column_type = data_type.clone();
    if tokens.get(index).map(String::as_str) == Some("(") {
        let length = require_identifier(tokens, index + 1, "column type length")?;
        let parsed_length = length
            .parse::<u32>()
            .map_err(|_| format!("invalid column type length {length}"))?;
        if parsed_length == 0 || parsed_length.to_string() != length {
            return Err(format!("noncanonical column type length {length}"));
        }
        require_keyword(tokens, index + 2, ")")?;
        column_type.push_str(&format!("({parsed_length})"));
        index += 3;
    } else if data_type == "varchar" {
        return Err("VARCHAR requires an explicit canonical length".to_string());
    }
    if tokens
        .get(index)
        .is_some_and(|token| token.eq_ignore_ascii_case("UNSIGNED"))
    {
        column_type.push_str(" unsigned");
        index += 1;
    }
    Ok((column_type, data_type, index))
}

fn parse_observed_column_options(
    tokens: &[String],
    mut index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<ParsedColumnOptions, String> {
    let mut nullable = true;
    let mut default_value = None;
    let mut comment = String::new();
    let mut after = None;
    while index < tokens.len() && tokens[index] != "," {
        if tokens[index].eq_ignore_ascii_case("NULL") {
            nullable = true;
            index += 1;
        } else if tokens[index].eq_ignore_ascii_case("DEFAULT") {
            require_keyword(tokens, index + 1, "NULL")?;
            default_value = None;
            index += 2;
        } else if tokens[index].eq_ignore_ascii_case("COMMENT") {
            require_keyword(tokens, index + 1, "<string>")?;
            comment = literals
                .next()
                .ok_or_else(|| "COMMENT literal is missing".to_string())?;
            index += 2;
        } else if tokens[index].eq_ignore_ascii_case("AFTER") {
            after = Some(require_identifier(tokens, index + 1, "AFTER column")?);
            index += 2;
        } else {
            return Err(format!(
                "unsupported production ADD COLUMN option {:?}",
                tokens.get(index)
            ));
        }
    }
    Ok(ParsedColumnOptions {
        nullable,
        default_value,
        comment,
        after,
        next_index: index,
    })
}

fn extract_single_quoted_literals(source_sql: &str) -> Result<Vec<String>, String> {
    let characters = source_sql.chars().collect::<Vec<_>>();
    let mut literals = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        if characters[index] != '\'' {
            index += 1;
            continue;
        }
        let mut literal = String::new();
        index += 1;
        loop {
            let character = *characters
                .get(index)
                .ok_or_else(|| "unterminated DDL string literal".to_string())?;
            if character == '\'' {
                if characters.get(index + 1) == Some(&'\'') {
                    literal.push('\'');
                    index += 2;
                    continue;
                }
                index += 1;
                break;
            }
            if character == '\\' {
                let escaped = *characters
                    .get(index + 1)
                    .ok_or_else(|| "unterminated DDL string escape".to_string())?;
                literal.push(escaped);
                index += 2;
                continue;
            }
            literal.push(character);
            index += 1;
        }
        literals.push(literal);
    }
    Ok(literals)
}

fn parse_drop_column_clause(
    tokens: &[String],
    index: usize,
) -> Result<(ParsedAlterClause, usize), String> {
    require_keyword(tokens, index, "DROP")?;
    require_keyword(tokens, index + 1, "COLUMN")?;
    require_keyword(tokens, index + 2, "IF")?;
    require_keyword(tokens, index + 3, "EXISTS")?;
    let name = require_identifier(tokens, index + 4, "dropped column")?;
    Ok((
        ParsedAlterClause::DropColumn(ParsedDropColumnAst {
            name,
            if_exists: true,
        }),
        index + 5,
    ))
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
