use super::model::{
    ParsedAddColumnAst, ParsedAlterClause, ParsedAlterTableAst, ParsedIndexAst,
    ParsedIndexKeyPart,
};
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

pub fn supports_production_alter_table(source_sql: &str) -> bool {
    parse_production_alter_table_ast(source_sql).is_ok()
}

pub fn transform_production_alter_table(source_sql: &str) -> Result<DdlTransformation, String> {
    parse_production_alter_table_ast(source_sql)?;
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

pub fn parse_production_alter_table_ast(
    source_sql: &str,
) -> Result<ParsedAlterTableAst, String> {
    let tokens = tokenize_ddl(source_sql)?;
    require_keyword(&tokens, 0, "ALTER")?;
    require_keyword(&tokens, 1, "TABLE")?;
    let table = require_identifier(&tokens, 2, "ALTER TABLE name")?;
    let mut literals = extract_single_quoted_literals(source_sql)?.into_iter();
    let mut clauses = Vec::new();
    let mut index = 3;
    while index < tokens.len() {
        require_keyword(&tokens, index, "ADD")?;
        let (clause, next_index) = parse_production_alter_clause(
            &tokens,
            index,
            &table,
            &mut literals,
        )?;
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

fn parse_production_alter_clause(
    tokens: &[String],
    index: usize,
    table: &str,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    match tokens.get(index + 1).map(|token| token.to_ascii_uppercase()) {
        Some(kind) if kind == "COLUMN" => parse_add_column_clause(tokens, index, literals),
        Some(kind) if kind == "KEY" => parse_add_key_clause(tokens, index, table),
        actual => Err(format!(
            "unsupported production ALTER TABLE clause {actual:?}"
        )),
    }
}

fn parse_add_column_clause(
    tokens: &[String],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    let name = require_identifier(tokens, index + 2, "added column")?;
    let (column_type, data_type, options_start) =
        parse_observed_column_type(tokens, index + 3)?;
    let (nullable, default_value, comment, after, next_index) =
        parse_observed_column_options(tokens, options_start, literals)?;
    Ok((
        ParsedAlterClause::AddColumn(ParsedAddColumnAst {
            name,
            column_type,
            data_type,
            nullable,
            default_value,
            comment,
            after,
        }),
        next_index,
    ))
}

fn parse_add_key_clause(
    tokens: &[String],
    index: usize,
    table: &str,
) -> Result<(ParsedAlterClause, usize), String> {
    let name = require_identifier(tokens, index + 2, "added key name")?;
    require_keyword(tokens, index + 3, "(")?;
    let mut key_parts = Vec::new();
    let mut column_index = index + 4;
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
                    unique: false,
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
        return Err(format!("unsupported production ADD COLUMN type {data_type}"));
    }
    index += 1;
    let mut column_type = data_type.clone();
    if tokens.get(index).map(String::as_str) == Some("(") {
        let length = require_identifier(tokens, index + 1, "column type length")?;
        require_keyword(tokens, index + 2, ")")?;
        column_type.push_str(&format!("({length})"));
        index += 3;
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
) -> Result<(bool, Option<String>, String, Option<String>, usize), String> {
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
    Ok((nullable, default_value, comment, after, index))
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

fn normalize_ddl_sql(source_sql: &str) -> Result<String, String> {
    DdlSqlNormalizer::new(source_sql).normalize()
}

struct DdlSqlNormalizer {
    characters: Vec<char>,
    output: String,
    quote: Option<char>,
    pending_space: bool,
    index: usize,
}

impl DdlSqlNormalizer {
    fn new(source_sql: &str) -> Self {
        Self {
            characters: source_sql.chars().collect(),
            output: String::new(),
            quote: None,
            pending_space: false,
            index: 0,
        }
    }

    fn normalize(mut self) -> Result<String, String> {
        while self.index < self.characters.len() {
            self.normalize_next_character()?;
        }
        if self.quote.is_some() {
            return Err("unterminated DDL quote".to_string());
        }
        Ok(self.output.trim().trim_end_matches(';').trim().to_string())
    }

    fn normalize_next_character(&mut self) -> Result<(), String> {
        if self.quote.is_some() {
            self.copy_quoted_character();
        } else if self.starts_quote() {
            self.start_quote();
        } else if self.starts_block_comment() {
            self.skip_block_comment()?;
        } else if self.starts_line_comment() {
            self.skip_line_comment();
        } else {
            self.copy_unquoted_character();
        }
        Ok(())
    }

    fn copy_quoted_character(&mut self) {
        let character = self.characters[self.index];
        let active_quote = self.quote.expect("quoted scanner requires active quote");
        self.output.push(character);
        if character == active_quote && self.characters.get(self.index + 1) == Some(&active_quote) {
            self.output.push(active_quote);
            self.index += 2;
        } else if character == active_quote {
            self.quote = None;
            self.index += 1;
        } else if character == '\\' && active_quote == '\'' {
            self.copy_escaped_character();
        } else {
            self.index += 1;
        }
    }

    fn copy_escaped_character(&mut self) {
        if let Some(escaped) = self.characters.get(self.index + 1) {
            self.output.push(*escaped);
            self.index += 2;
        } else {
            self.index += 1;
        }
    }

    fn starts_quote(&self) -> bool {
        matches!(self.characters[self.index], '`' | '\'' | '"')
    }

    fn start_quote(&mut self) {
        self.append_pending_space(self.characters[self.index]);
        let quote = self.characters[self.index];
        self.quote = Some(quote);
        self.output.push(quote);
        self.index += 1;
    }

    fn starts_block_comment(&self) -> bool {
        self.characters[self.index] == '/'
            && self.characters.get(self.index + 1) == Some(&'*')
    }

    fn skip_block_comment(&mut self) -> Result<(), String> {
        self.index += 2;
        while self.index + 1 < self.characters.len()
            && !(self.characters[self.index] == '*'
                && self.characters[self.index + 1] == '/')
        {
            self.index += 1;
        }
        if self.index + 1 >= self.characters.len() {
            return Err("unterminated DDL block comment".to_string());
        }
        self.index += 2;
        self.pending_space = true;
        Ok(())
    }

    fn starts_line_comment(&self) -> bool {
        let character = self.characters[self.index];
        character == '#'
            || (character == '-'
                && self.characters.get(self.index + 1) == Some(&'-')
                && self
                    .characters
                    .get(self.index + 2)
                    .is_none_or(|after| after.is_whitespace() || after.is_control()))
    }

    fn skip_line_comment(&mut self) {
        while self
            .characters
            .get(self.index)
            .is_some_and(|character| *character != '\n')
        {
            self.index += 1;
        }
        self.pending_space = true;
    }

    fn copy_unquoted_character(&mut self) {
        let character = self.characters[self.index];
        if character.is_whitespace() {
            self.pending_space = true;
        } else if character == ',' {
            self.append_comma();
        } else {
            self.append_pending_space(character);
            self.output.push(character);
        }
        self.index += 1;
    }

    fn append_comma(&mut self) {
        while self.output.ends_with(' ') {
            self.output.pop();
        }
        self.output.push_str(", ");
        self.pending_space = false;
    }

    fn append_pending_space(&mut self, next_character: char) {
        let needs_space = self.pending_space
            && !self.output.is_empty()
            && !self.output.ends_with([' ', '(', '.'])
            && !matches!(next_character, ')' | '.' | ';');
        if needs_space {
            self.output.push(' ');
        }
        self.pending_space = false;
    }
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
