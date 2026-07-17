use super::super::ddl_replay_journal::DdlFamily;
use super::model::{DdlObjectKind, DdlOperation, ParsedIndexAst, ParsedIndexKeyPart};
use super::tokenizer::{ddl_contains_comments, tokenize_ddl};
use super::transform::{parse_fixture_create_table, parse_production_alter_table_ast};

pub fn parse_simple_index_ddl(sql: &str) -> Result<ParsedIndexAst, String> {
    if ddl_contains_comments(sql) {
        return Err("index DDL comments are not automatic".to_string());
    }
    if sql.contains('"') {
        return Err(
            "double-quoted index identifiers depend on uncaptured ANSI_QUOTES mode".to_string(),
        );
    }
    let tokens = tokenize_ddl(sql)?;
    let keywords = tokens
        .iter()
        .map(|token| token.to_ascii_uppercase())
        .collect::<Vec<_>>();
    match keywords.first().map(String::as_str) {
        Some("CREATE") => parse_simple_create_index(&tokens, &keywords),
        Some("DROP") => parse_simple_drop_index(&tokens, &keywords),
        _ => Err("only CREATE INDEX and DROP INDEX are automatic".to_string()),
    }
}

fn parse_simple_create_index(
    tokens: &[String],
    keywords: &[String],
) -> Result<ParsedIndexAst, String> {
    let (name, table, index, index_type) = parse_create_index_header(tokens, keywords)?;
    let (key_parts, index) = parse_create_index_keys(tokens, keywords, index)?;
    let index = parse_create_index_options(keywords, index)?;
    ensure_create_index_complete(tokens, index)?;
    Ok(simple_create_index_ast(name, table, index_type, key_parts))
}

fn simple_create_index_ast(
    name: String,
    table: String,
    index_type: String,
    key_parts: Vec<ParsedIndexKeyPart>,
) -> ParsedIndexAst {
    ParsedIndexAst {
        create: true,
        name,
        table,
        unique: false,
        index_type,
        visible: true,
        comment: None,
        key_parts,
    }
}

fn parse_create_index_header(
    tokens: &[String],
    keywords: &[String],
) -> Result<(String, String, usize, String), String> {
    reject_index_variants(keywords)?;
    require_keyword(keywords, 1, "INDEX")?;
    let name = strict_index_identifier(tokens.get(2), "index name")?;
    if name.eq_ignore_ascii_case("ON") {
        return Err("generated index names are manual".to_string());
    }
    require_keyword(keywords, 3, "ON")?;
    let table = strict_index_identifier(tokens.get(4), "index table")?;
    let (index_type, index) = parse_index_type(tokens, keywords, 5)?;
    if index_type != "BTREE" {
        return Err("only BTREE indexes are automatic".to_string());
    }
    Ok((name, table, index, index_type))
}

fn reject_index_variants(keywords: &[String]) -> Result<(), String> {
    if keywords
        .get(1)
        .is_some_and(|token| matches!(token.as_str(), "UNIQUE" | "FULLTEXT" | "SPATIAL"))
    {
        return Err("unique, fulltext, and spatial indexes are manual".to_string());
    }
    Ok(())
}

fn parse_index_type(
    tokens: &[String],
    keywords: &[String],
    index: usize,
) -> Result<(String, usize), String> {
    if keywords.get(index).map(String::as_str) != Some("USING") {
        return Ok(("BTREE".to_string(), index));
    }
    let value = tokens
        .get(index + 1)
        .ok_or_else(|| "index type is missing".to_string())?;
    Ok((value.to_ascii_uppercase(), index + 2))
}

fn parse_create_index_keys(
    tokens: &[String],
    keywords: &[String],
    index: usize,
) -> Result<(Vec<ParsedIndexKeyPart>, usize), String> {
    require_token(tokens, index, "(")?;
    parse_index_key_parts(tokens, keywords, index + 1)
}

fn parse_create_index_options(keywords: &[String], mut index: usize) -> Result<usize, String> {
    index += 1;
    if keywords.get(index).map(String::as_str) == Some("USING") {
        require_keyword(keywords, index + 1, "BTREE")?;
        index += 2;
    }
    if keywords.get(index).map(String::as_str) == Some("INVISIBLE") {
        return Err("invisible indexes are manual".to_string());
    }
    if keywords.get(index).map(String::as_str) == Some("VISIBLE") {
        index += 1;
    }
    Ok(index)
}

fn ensure_create_index_complete(tokens: &[String], index: usize) -> Result<(), String> {
    if index == tokens.len() {
        Ok(())
    } else {
        Err("unmodeled CREATE INDEX option".to_string())
    }
}
fn parse_simple_drop_index(
    tokens: &[String],
    keywords: &[String],
) -> Result<ParsedIndexAst, String> {
    require_keyword(keywords, 1, "INDEX")?;
    if keywords.get(2).map(String::as_str) == Some("IF") {
        return Err("DROP INDEX IF EXISTS is manual".to_string());
    }
    let name = strict_index_identifier(tokens.get(2), "index name")?;
    require_keyword(keywords, 3, "ON")?;
    let table = strict_index_identifier(tokens.get(4), "index table")?;
    if tokens.len() != 5 {
        return Err("DROP INDEX options are manual".to_string());
    }
    Ok(ParsedIndexAst {
        create: false,
        name,
        table,
        unique: false,
        index_type: "BTREE".to_string(),
        visible: true,
        comment: None,
        key_parts: Vec::new(),
    })
}

fn parse_index_key_parts(
    tokens: &[String],
    keywords: &[String],
    mut index: usize,
) -> Result<(Vec<ParsedIndexKeyPart>, usize), String> {
    let mut key_parts = Vec::new();
    loop {
        let (part, next_index) = parse_index_key_part(tokens, keywords, index)?;
        key_parts.push(part);
        index = next_key_part_index(tokens, next_index)?;
        if index == next_index {
            return Ok((key_parts, index));
        }
    }
}

fn next_key_part_index(tokens: &[String], part_end: usize) -> Result<usize, String> {
    match tokens.get(part_end).map(String::as_str) {
        Some(",") => Ok(part_end + 1),
        Some(")") => Ok(part_end),
        _ => Err("index key part separator is invalid".to_string()),
    }
}

fn parse_index_key_part(
    tokens: &[String],
    keywords: &[String],
    mut index: usize,
) -> Result<(ParsedIndexKeyPart, usize), String> {
    let column = strict_index_identifier(tokens.get(index), "index column")?;
    index += 1;
    let (prefix_length, next_index) = parse_index_prefix(tokens, index)?;
    index = next_index;
    let (order, collation, index) = parse_index_key_options(tokens, keywords, index)?;
    Ok((
        ParsedIndexKeyPart {
            column,
            prefix_length,
            order,
            collation,
        },
        index,
    ))
}

fn parse_index_prefix(tokens: &[String], index: usize) -> Result<(Option<u32>, usize), String> {
    if tokens.get(index).map(String::as_str) != Some("(") {
        return Ok((None, index));
    }
    let value = tokens
        .get(index + 1)
        .ok_or_else(|| "index prefix length is missing".to_string())?
        .parse::<u32>()
        .map_err(|_| "index prefix length is not numeric".to_string())?;
    if value == 0 {
        return Err("index prefix length must be positive".to_string());
    }
    require_token(tokens, index + 2, ")")?;
    Ok((Some(value), index + 3))
}

fn parse_index_key_options(
    tokens: &[String],
    keywords: &[String],
    mut index: usize,
) -> Result<(String, Option<String>, usize), String> {
    let mut order = "ASC".to_string();
    let mut collation = None;
    loop {
        match keywords.get(index).map(String::as_str) {
            Some("ASC") | Some("DESC") => {
                order = keywords[index].clone();
                index += 1;
            }
            Some("COLLATE") => {
                collation = Some(strict_index_identifier(
                    tokens.get(index + 1),
                    "index collation",
                )?);
                index += 2;
            }
            _ => break,
        }
    }
    Ok((order, collation, index))
}
fn require_token(tokens: &[String], index: usize, expected: &str) -> Result<(), String> {
    if tokens.get(index).is_some_and(|token| token == expected) {
        Ok(())
    } else {
        Err(format!("expected index token `{expected}`"))
    }
}

fn strict_index_identifier(token: Option<&String>, kind: &str) -> Result<String, String> {
    let token = token.ok_or_else(|| format!("{kind} is missing"))?;
    if token.is_empty()
        || token.contains('.')
        || matches!(
            token.as_str(),
            "." | "," | "(" | ")" | "=" | ";" | "<string>"
        )
    {
        return Err(format!("qualified or invalid {kind} is manual"));
    }
    Ok(token.clone())
}

pub fn parse_ddl_operation(sql: &str) -> Result<DdlOperation, String> {
    let tokens = tokenize_ddl(sql)?;
    let keywords = tokens
        .iter()
        .map(|token| token.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let Some(command) = keywords.first().map(String::as_str) else {
        return Err("empty DDL statement".to_string());
    };

    let mut operation = match command {
        "CREATE" => parse_create_or_alter(&tokens, &keywords, true),
        "ALTER" => parse_create_or_alter(&tokens, &keywords, false),
        "DROP" => parse_drop(&tokens, &keywords),
        "RENAME" => parse_rename(&tokens, &keywords),
        "TRUNCATE" => parse_truncate(&tokens, &keywords),
        _ => Err(format!("unsupported automatic DDL command `{command}`")),
    }?;
    if operation.object_kind == DdlObjectKind::Index {
        operation.index_ast = Some(parse_simple_index_ddl(sql)?);
    }
    if command == "CREATE" && operation.object_kind == DdlObjectKind::Table {
        operation.create_table_ast = parse_fixture_create_table(sql).ok();
    }
    if command == "ALTER" && operation.object_kind == DdlObjectKind::Table {
        operation.alter_table_ast = parse_production_alter_table_ast(sql).ok();
    }
    Ok(operation)
}

fn parse_create_or_alter(
    tokens: &[String],
    keywords: &[String],
    create: bool,
) -> Result<DdlOperation, String> {
    let kind_index = parse_kind_index(keywords, create)?;
    let kind = keywords
        .get(kind_index)
        .ok_or_else(|| "DDL object type is missing".to_string())?;
    let name_index = parse_name_index(keywords, kind, kind_index + 1);
    let name = object_name(tokens, name_index)?;
    let object_kind = object_kind_for(kind)?;
    let family = family_for_object_kind(kind)?;
    let secondary_object = parse_secondary_object(tokens, keywords, kind, name_index)?;
    Ok(DdlOperation {
        family,
        object_kind,
        primary_object: name,
        secondary_object,
        index_ast: None,
        create_table_ast: None,
        alter_table_ast: None,
    })
}

fn parse_kind_index(keywords: &[String], create: bool) -> Result<usize, String> {
    let mut index = 1;
    if create && keywords.get(index).is_some_and(|token| token == "OR") {
        require_keyword(keywords, index + 1, "REPLACE")?;
        index += 2;
    }
    if create && keywords.get(index).is_some_and(|token| token == "UNIQUE") {
        index += 1;
    }
    Ok(index)
}

fn parse_name_index(keywords: &[String], kind: &str, index: usize) -> usize {
    if matches!(kind, "TABLE" | "VIEW") {
        skip_if_exists(keywords, index)
    } else {
        index
    }
}

fn parse_secondary_object(
    tokens: &[String],
    keywords: &[String],
    kind: &str,
    name_index: usize,
) -> Result<Option<String>, String> {
    if !matches!(kind, "INDEX" | "TRIGGER") {
        return Ok(None);
    }
    let on_index = keyword_position(keywords, "ON", name_index + 1)?;
    Ok(Some(object_name(tokens, on_index + 1)?))
}
fn parse_drop(tokens: &[String], keywords: &[String]) -> Result<DdlOperation, String> {
    let kind_index = parse_drop_kind_index(keywords);
    let kind = keywords
        .get(kind_index)
        .ok_or_else(|| "DROP object type is missing".to_string())?;
    let object_kind = object_kind_for(kind)?;
    let name_index = skip_if_exists(keywords, kind_index + 1);
    let name = object_name(tokens, name_index)?;
    reject_list_separator(tokens, name_index + 1)?;
    let secondary_object = parse_drop_secondary(tokens, keywords, kind, name_index)?;
    let family = if kind == "INDEX" {
        DdlFamily::Index
    } else {
        DdlFamily::Drop
    };
    Ok(DdlOperation {
        family,
        object_kind,
        primary_object: name,
        secondary_object,
        index_ast: None,
        create_table_ast: None,
        alter_table_ast: None,
    })
}

fn parse_drop_kind_index(keywords: &[String]) -> usize {
    if keywords.get(1).is_some_and(|token| token == "TEMPORARY") {
        2
    } else {
        1
    }
}

fn parse_drop_secondary(
    tokens: &[String],
    keywords: &[String],
    kind: &str,
    name_index: usize,
) -> Result<Option<String>, String> {
    if kind != "INDEX" {
        return Ok(None);
    }
    let on_index = keyword_position(keywords, "ON", name_index + 1)?;
    Ok(Some(object_name(tokens, on_index + 1)?))
}
fn parse_rename(tokens: &[String], keywords: &[String]) -> Result<DdlOperation, String> {
    require_keyword(keywords, 1, "TABLE")?;
    let from = object_name(tokens, 2)?;
    require_keyword(keywords, 3, "TO")?;
    let to = object_name(tokens, 4)?;
    reject_list_separator(tokens, 5)?;
    Ok(DdlOperation {
        family: DdlFamily::Rename,
        object_kind: DdlObjectKind::Table,
        primary_object: from,
        secondary_object: Some(to),
        index_ast: None,
        create_table_ast: None,
        alter_table_ast: None,
    })
}

fn parse_truncate(tokens: &[String], keywords: &[String]) -> Result<DdlOperation, String> {
    let name_index = if keywords.get(1).is_some_and(|token| token == "TABLE") {
        2
    } else {
        1
    };
    Ok(DdlOperation {
        family: DdlFamily::Truncate,
        object_kind: DdlObjectKind::Table,
        primary_object: object_name(tokens, name_index)?,
        secondary_object: None,
        index_ast: None,
        create_table_ast: None,
        alter_table_ast: None,
    })
}

fn object_kind_for(kind: &str) -> Result<DdlObjectKind, String> {
    match kind {
        "TABLE" => Ok(DdlObjectKind::Table),
        "INDEX" => Ok(DdlObjectKind::Index),
        "VIEW" => Ok(DdlObjectKind::View),
        "PROCEDURE" => Ok(DdlObjectKind::Procedure),
        "FUNCTION" => Ok(DdlObjectKind::Function),
        "EVENT" => Ok(DdlObjectKind::Event),
        "TRIGGER" => Ok(DdlObjectKind::Trigger),
        other => Err(format!("unsupported automatic DDL object type `{other}`")),
    }
}

fn family_for_object_kind(kind: &str) -> Result<DdlFamily, String> {
    match kind {
        "TABLE" => Ok(DdlFamily::Table),
        "INDEX" => Ok(DdlFamily::Index),
        "VIEW" => Ok(DdlFamily::View),
        "PROCEDURE" => Ok(DdlFamily::Procedure),
        "FUNCTION" => Ok(DdlFamily::Function),
        "EVENT" => Ok(DdlFamily::Event),
        "TRIGGER" => Ok(DdlFamily::Trigger),
        other => Err(format!("unsupported automatic DDL object type `{other}`")),
    }
}

fn skip_if_exists(keywords: &[String], index: usize) -> usize {
    if keywords.get(index).is_some_and(|token| token == "IF")
        && keywords
            .get(index + 1)
            .is_some_and(|token| matches!(token.as_str(), "EXISTS" | "NOT"))
    {
        if keywords.get(index + 1).is_some_and(|token| token == "NOT") {
            return index + 3;
        }
        return index + 2;
    }
    index
}

fn object_name(tokens: &[String], index: usize) -> Result<String, String> {
    let token = tokens
        .get(index)
        .ok_or_else(|| "DDL object name is missing".to_string())?;
    if matches!(token.as_str(), "." | "," | "(" | ")") {
        return Err("DDL object name is invalid".to_string());
    }
    if tokens.get(index + 1).is_some_and(|token| token == ".") {
        return Err(format!("qualified DDL object `{token}` is not automatic"));
    }
    Ok(token.clone())
}

fn keyword_position(keywords: &[String], keyword: &str, start: usize) -> Result<usize, String> {
    keywords[start..]
        .iter()
        .position(|token| token == keyword)
        .map(|offset| start + offset)
        .ok_or_else(|| format!("DDL keyword `{keyword}` is missing"))
}

fn require_keyword(keywords: &[String], index: usize, expected: &str) -> Result<(), String> {
    if keywords.get(index).is_some_and(|token| token == expected) {
        Ok(())
    } else {
        Err(format!("expected DDL keyword `{expected}`"))
    }
}

fn reject_list_separator(tokens: &[String], start: usize) -> Result<(), String> {
    if tokens[start..].iter().any(|token| token == ",") {
        Err("multi-object DDL is not automatic".to_string())
    } else {
        Ok(())
    }
}

pub fn supports_automatic_index_ddl(sql: &str) -> bool {
    parse_simple_index_ddl(sql).is_ok()
}
