use super::model::{
    ParsedAddColumnAst, ParsedAlterClause, ParsedAlterTableAst, ParsedCreateColumnAst,
    ParsedCreateTableAst, ParsedDropColumnAst, ParsedIndexAst, ParsedIndexKeyPart,
};
use super::tokenizer::{
    ddl_contains_comments, strip_leading_ordinary_ddl_comments,
    strip_one_leading_mysql_line_comment, tokenize_ddl, tokenize_ddl_with_quoted_flags,
};
use sha2::{Digest, Sha256};
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
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

pub fn parse_fixture_create_table(source_sql: &str) -> Result<ParsedCreateTableAst, String> {
    let source_sql = strip_leading_ordinary_ddl_comments(source_sql)?;
    if ddl_contains_comments(source_sql) {
        return Err("fixture CREATE TABLE comments are not supported".to_string());
    }
    if source_sql.contains('"') {
        return Err("fixture CREATE TABLE double-quoted identifiers are not supported".to_string());
    }
    let tokens = tokenize_ddl(source_sql)?;
    if let Some(ast) = parse_home_feed_artist_blacklist_create(&tokens)? {
        return Ok(ast);
    }
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
        character_set: None,
        collation: None,
    })
}

const HOME_FEED_ARTIST_BLACKLIST_CREATE: &str = "CREATE TABLE IF NOT EXISTS `home_feed_artist_blacklist` (\
    `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, \
    `artist_id` MEDIUMINT(8) UNSIGNED NOT NULL, \
    `reason` VARCHAR(255) DEFAULT NULL, \
    `creator_id` MEDIUMINT(8) UNSIGNED DEFAULT NULL, \
    `create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, \
    UNIQUE KEY `uidx_hfab_artist` (`artist_id`)\
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

fn parse_home_feed_artist_blacklist_create(
    tokens: &[String],
) -> Result<Option<ParsedCreateTableAst>, String> {
    if tokens != tokenize_ddl(HOME_FEED_ARTIST_BLACKLIST_CREATE)? {
        return Ok(None);
    }
    Ok(Some(ParsedCreateTableAst {
        name: "home_feed_artist_blacklist".to_string(),
        columns: home_feed_artist_blacklist_columns(),
        primary_key: vec!["id".to_string()],
        indexes: vec![home_feed_artist_blacklist_index()],
        engine: "InnoDB".to_string(),
        character_set: Some("utf8mb4".to_string()),
        collation: Some("utf8mb4_unicode_ci".to_string()),
    }))
}

fn home_feed_artist_blacklist_columns() -> Vec<ParsedCreateColumnAst> {
    vec![
        create_column("id", "int unsigned", false, None, true),
        create_column("artist_id", "mediumint unsigned", false, None, false),
        create_column("reason", "varchar(255)", true, Some("NULL"), false),
        create_column(
            "creator_id",
            "mediumint unsigned",
            true,
            Some("NULL"),
            false,
        ),
        create_column(
            "create_time",
            "timestamp",
            false,
            Some("CURRENT_TIMESTAMP"),
            false,
        ),
    ]
}

fn home_feed_artist_blacklist_index() -> ParsedIndexAst {
    ParsedIndexAst {
        create: true,
        name: "uidx_hfab_artist".to_string(),
        table: "home_feed_artist_blacklist".to_string(),
        unique: true,
        index_type: "BTREE".to_string(),
        visible: true,
        comment: None,
        key_parts: vec![ParsedIndexKeyPart {
            column: "artist_id".to_string(),
            prefix_length: None,
            order: "ASC".to_string(),
            collation: Some("A".to_string()),
        }],
    }
}

fn create_column(
    name: &str,
    column_type: &str,
    nullable: bool,
    default_sql: Option<&str>,
    auto_increment: bool,
) -> ParsedCreateColumnAst {
    ParsedCreateColumnAst {
        name: name.to_string(),
        column_type: column_type.to_string(),
        nullable,
        default_sql: default_sql.map(str::to_string),
        auto_increment,
    }
}

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
        let length = tokens
            .get(index + 3)
            .cloned()
            .ok_or_else(|| "missing VARCHAR length".to_string())?;
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
            default_sql: None,
            auto_increment: false,
        },
        primary,
        next_index,
    ))
}

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

pub fn supports_fixture_create_table(source_sql: &str) -> bool {
    parse_fixture_create_table(source_sql).is_ok()
}

pub fn render_modeled_index_ddl(
    index: &super::model::ParsedIndexAst,
    source_sql: &str,
) -> Result<DdlTransformation, String> {
    if !index.create {
        return Err("modeled index renderer requires CREATE INDEX".to_string());
    }
    if index.index_type != "BTREE" {
        return Err(format!(
            "unsupported modeled index type {}",
            index.index_type
        ));
    }
    let unique = if index.unique { " UNIQUE" } else { "" };
    let key_parts = index
        .key_parts
        .iter()
        .map(render_modeled_index_key_part)
        .collect::<Vec<_>>()
        .join(",");
    let tokens = super::tokenizer::tokenize_ddl(source_sql)?;
    let visibility = if tokens
        .iter()
        .any(|token| token.eq_ignore_ascii_case("INVISIBLE"))
    {
        " INVISIBLE"
    } else if tokens
        .iter()
        .any(|token| token.eq_ignore_ascii_case("VISIBLE"))
    {
        " VISIBLE"
    } else {
        ""
    };
    let comment = index
        .comment
        .as_ref()
        .map(|value| format!(" COMMENT {}", quote_string_literal(value)))
        .unwrap_or_default();
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: Some(format!(
            "CREATE{unique} INDEX {} ON {} ({key_parts}) USING BTREE{visibility}{comment}",
            quote_identifier(&index.name),
            quote_identifier(&index.table),
        )),
    })
}

fn render_modeled_index_key_part(part: &super::model::ParsedIndexKeyPart) -> String {
    let mut rendered = quote_identifier(&part.column);
    if let Some(prefix_length) = part.prefix_length {
        rendered.push_str(&format!("({prefix_length})"));
    }
    if part.order != "ASC" {
        rendered.push(' ');
        rendered.push_str(&part.order);
    }
    if let Some(collation) = &part.collation {
        rendered.push_str(" COLLATE ");
        rendered.push_str(&quote_identifier(collation));
    }
    rendered
}

pub fn transform_generated_schema_ddl(source_sql: &str) -> Result<DdlTransformation, String> {
    validate_generated_schema_ddl(source_sql)?;
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: Some(source_sql.trim().trim_end_matches(';').trim().to_string()),
    })
}

fn validate_generated_schema_ddl(source_sql: &str) -> Result<(), String> {
    let tokens = generated_schema_tokens(source_sql).ok_or_else(|| {
        "generated schema DDL has unsupported quoting, comments, or statement shape".to_string()
    })?;
    if is_generated_create_table(&tokens) {
        return validate_generated_create_table(&tokens);
    }
    if is_generated_alter_table(&tokens) {
        return validate_generated_alter_table(&tokens);
    }
    if is_generated_unique_index(&tokens) {
        return validate_generated_unique_index(&tokens);
    }
    Err("generated schema DDL family is unsupported".to_string())
}

fn validate_generated_create_table(tokens: &[String]) -> Result<(), String> {
    let close = matching_parenthesis(tokens, 3)?;
    validate_create_table_definitions(tokens, 4, close)?;
    if close + 3 >= tokens.len()
        || !tokens_match(tokens, close + 1, "ENGINE")
        || tokens.get(close + 2).map(String::as_str) != Some("=")
    {
        return Err("generated CREATE TABLE requires an explicit ENGINE".to_string());
    }
    validate_create_table_tail(tokens, close + 3)
}

fn validate_create_table_tail(tokens: &[String], engine_value: usize) -> Result<(), String> {
    let mut index = engine_value + 1;
    while index < tokens.len() {
        index = consume_create_table_option(tokens, index)?;
    }
    Ok(())
}

fn consume_create_table_option(tokens: &[String], mut index: usize) -> Result<usize, String> {
    if tokens_match(tokens, index, "DEFAULT") {
        index += 1;
    }
    if tokens_match(tokens, index, "CHARACTER") {
        index += 1;
        if tokens_match(tokens, index, "SET") {
            index += 1;
        }
    } else if token_is_one_of(tokens, index, &["CHARSET", "COLLATE"]) {
        index += 1;
    } else {
        return Err("generated CREATE TABLE has an unmodeled option".to_string());
    }
    if tokens.get(index).map(String::as_str) == Some("=") {
        index += 1;
    }
    tokens
        .get(index)
        .map(|_| index + 1)
        .ok_or_else(|| "generated CREATE TABLE option value is missing".to_string())
}

fn validate_generated_alter_table(tokens: &[String]) -> Result<(), String> {
    if has_top_level_comma(tokens, 3) {
        return Err("generated ALTER TABLE must contain exactly one action".to_string());
    }
    let action = tokens.get(3).map(|token| token.to_ascii_uppercase());
    match action.as_deref() {
        Some("ADD") => validate_generated_add(tokens),
        Some("MODIFY") if tokens_match(tokens, 4, "COLUMN") => {
            validate_column_definition(tokens, 5, tokens.len(), true)
        }
        Some("DROP") => validate_generated_drop(tokens),
        _ => Err("generated ALTER TABLE action is unsupported".to_string()),
    }
}

fn validate_generated_add(tokens: &[String]) -> Result<(), String> {
    if tokens_match(tokens, 4, "COLUMN") {
        return validate_column_definition(tokens, 5, tokens.len(), true);
    }
    if tokens_match(tokens, 4, "PRIMARY") {
        return validate_key_definition(tokens, 4, tokens.len(), false);
    }
    if tokens_match(tokens, 4, "CONSTRAINT") {
        return validate_constraint_definition(tokens, 4, tokens.len());
    }
    Err("generated ADD action is unsupported".to_string())
}

fn validate_create_table_definitions(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<(), String> {
    for (definition_start, definition_end) in top_level_ranges(tokens, start, end)? {
        validate_create_table_definition(tokens, definition_start, definition_end)?;
    }
    Ok(())
}

fn top_level_ranges(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<Vec<(usize, usize)>, String> {
    let mut ranges = Vec::new();
    let mut definition_start = start;
    let mut depth = 0_u32;
    for (index, token) in tokens.iter().enumerate().take(end).skip(start) {
        match token.as_str() {
            "(" => depth += 1,
            ")" => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| "generated definition parentheses are unbalanced".to_string())?;
            }
            "," if depth == 0 => {
                if definition_start == index {
                    return Err("generated CREATE TABLE contains an empty definition".to_string());
                }
                ranges.push((definition_start, index));
                definition_start = index + 1;
            }
            _ => {}
        }
    }
    if depth != 0 || definition_start >= end {
        return Err("generated CREATE TABLE definitions are incomplete".to_string());
    }
    ranges.push((definition_start, end));
    Ok(ranges)
}

fn validate_create_table_definition(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<(), String> {
    if tokens_match(tokens, start, "PRIMARY") {
        return validate_key_definition(tokens, start, end, false);
    }
    if tokens_match(tokens, start, "UNIQUE")
        || tokens_match(tokens, start, "KEY")
        || tokens_match(tokens, start, "INDEX")
    {
        return validate_key_definition(tokens, start, end, true);
    }
    if tokens_match(tokens, start, "CONSTRAINT") {
        return validate_constraint_definition(tokens, start, end);
    }
    validate_column_definition(tokens, start, end, false)
}

fn validate_key_definition(
    tokens: &[String],
    start: usize,
    end: usize,
    named: bool,
) -> Result<(), String> {
    let mut index = start;
    if tokens_match(tokens, index, "UNIQUE") {
        index += 1;
    }
    if tokens_match(tokens, index, "PRIMARY") {
        index += 1;
        require_generated_keyword(tokens, index, end, "KEY")?;
        index += 1;
    } else {
        if !token_is_one_of(tokens, index, &["KEY", "INDEX"]) {
            return Err("generated secondary index requires KEY or INDEX".to_string());
        }
        index += 1;
        if named {
            require_generated_identifier(tokens, index, end, "key name")?;
            index += 1;
        }
    }
    let close = validate_parenthesized_definition(tokens, index, end)?;
    validate_generated_index_options(tokens, close + 1, end)
}

fn validate_generated_index_options(
    tokens: &[String],
    mut index: usize,
    end: usize,
) -> Result<(), String> {
    while index < end {
        if tokens_match(tokens, index, "USING") {
            if !tokens_match(tokens, index + 1, "BTREE") {
                return Err("generated index USING type is unsupported".to_string());
            }
            index += 2;
        } else if token_is_one_of(tokens, index, &["VISIBLE", "INVISIBLE"]) {
            index += 1;
        } else if tokens_match(tokens, index, "COMMENT") {
            require_generated_keyword(tokens, index + 1, end, "<string>")?;
            index += 2;
        } else {
            return Err("generated key definition has unknown trailing option".to_string());
        }
    }
    Ok(())
}

fn validate_constraint_definition(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<(), String> {
    require_generated_identifier(tokens, start + 1, end, "constraint name")?;
    if tokens_match(tokens, start + 2, "FOREIGN") {
        require_generated_keyword(tokens, start + 3, end, "KEY")?;
        let child_close = validate_parenthesized_definition(tokens, start + 4, end)?;
        require_generated_keyword(tokens, child_close + 1, end, "REFERENCES")?;
        let parent_columns_open =
            validate_generated_parent_reference(tokens, child_close + 2, end)?;
        let parent_close = validate_parenthesized_definition(tokens, parent_columns_open, end)?;
        return validate_reference_actions(tokens, parent_close + 1, end);
    }
    if tokens_match(tokens, start + 2, "CHECK") {
        let close = validate_parenthesized_definition(tokens, start + 3, end)?;
        return (close + 1 == end)
            .then_some(())
            .ok_or_else(|| "generated CHECK constraint has trailing tokens".to_string());
    }
    Err("generated constraint kind is unsupported".to_string())
}

fn validate_generated_parent_reference(
    tokens: &[String],
    parent_start: usize,
    end: usize,
) -> Result<usize, String> {
    require_generated_identifier(tokens, parent_start, end, "parent table or schema")?;
    if tokens.get(parent_start + 1).map(String::as_str) != Some(".") {
        return Ok(parent_start + 1);
    }
    require_generated_identifier(tokens, parent_start + 2, end, "parent table")?;
    if tokens.get(parent_start + 3).map(String::as_str) == Some(".") {
        return Err("generated parent reference has malformed qualification".to_string());
    }
    Ok(parent_start + 3)
}

fn validate_reference_actions(
    tokens: &[String],
    mut index: usize,
    end: usize,
) -> Result<(), String> {
    while index < end {
        require_generated_keyword(tokens, index, end, "ON")?;
        if !token_is_one_of(tokens, index + 1, &["DELETE", "UPDATE"]) {
            return Err("generated foreign key action is unsupported".to_string());
        }
        index += 2;
        if tokens_match(tokens, index, "SET") {
            require_generated_keyword(tokens, index + 1, end, "NULL")?;
            index += 2;
        } else if tokens_match(tokens, index, "NO") {
            require_generated_keyword(tokens, index + 1, end, "ACTION")?;
            index += 2;
        } else if token_is_one_of(tokens, index, &["CASCADE", "RESTRICT"]) {
            index += 1;
        } else {
            return Err("generated foreign key action value is unsupported".to_string());
        }
    }
    Ok(())
}

fn validate_column_definition(
    tokens: &[String],
    start: usize,
    end: usize,
    allow_position: bool,
) -> Result<(), String> {
    require_generated_identifier(tokens, start, end, "column name")?;
    let mut index = validate_column_type(tokens, start + 1, end)?;
    while index < end {
        index = consume_column_modifier(tokens, index, end, allow_position)?.ok_or_else(|| {
            format!(
                "generated column definition has unsupported modifier {:?}",
                tokens[index]
            )
        })?;
    }
    Ok(())
}

fn consume_column_modifier(
    tokens: &[String],
    index: usize,
    end: usize,
    allow_position: bool,
) -> Result<Option<usize>, String> {
    if let Some(next) = consume_storage_column_modifier(tokens, index, end)? {
        return Ok(Some(next));
    }
    consume_behavior_column_modifier(tokens, index, end, allow_position)
}

fn consume_storage_column_modifier(
    tokens: &[String],
    index: usize,
    end: usize,
) -> Result<Option<usize>, String> {
    if token_is_one_of(
        tokens,
        index,
        &["UNSIGNED", "ZEROFILL", "NULL", "AUTO_INCREMENT"],
    ) {
        return Ok(Some(index + 1));
    }
    if tokens_match(tokens, index, "NOT") {
        return require_following_keyword(tokens, index, end, "NULL");
    }
    if tokens_match(tokens, index, "DEFAULT") {
        return consume_default_expression(tokens, index + 1, end).map(Some);
    }
    if tokens_match(tokens, index, "CHARACTER") {
        return consume_character_set(tokens, index, end).map(Some);
    }
    if tokens_match(tokens, index, "COLLATE") {
        require_generated_identifier(tokens, index + 1, end, "collation")?;
        return Ok(Some(index + 2));
    }
    Ok(None)
}

fn consume_behavior_column_modifier(
    tokens: &[String],
    index: usize,
    end: usize,
    allow_position: bool,
) -> Result<Option<usize>, String> {
    if tokens_match(tokens, index, "ON") {
        require_generated_keyword(tokens, index + 1, end, "UPDATE")?;
        return consume_current_timestamp(tokens, index + 2, end).map(Some);
    }
    if tokens_match(tokens, index, "COMMENT") {
        return require_following_keyword(tokens, index, end, "<string>");
    }
    if tokens_match(tokens, index, "GENERATED") || tokens_match(tokens, index, "AS") {
        return consume_generated_expression(tokens, index, end).map(Some);
    }
    if allow_position {
        return consume_column_position(tokens, index, end);
    }
    consume_inline_key_modifier(tokens, index, end)
}

fn require_following_keyword(
    tokens: &[String],
    index: usize,
    end: usize,
    expected: &str,
) -> Result<Option<usize>, String> {
    require_generated_keyword(tokens, index + 1, end, expected)?;
    Ok(Some(index + 2))
}

fn consume_character_set(tokens: &[String], index: usize, end: usize) -> Result<usize, String> {
    require_generated_keyword(tokens, index + 1, end, "SET")?;
    require_generated_identifier(tokens, index + 2, end, "character set")?;
    Ok(index + 3)
}

fn consume_column_position(
    tokens: &[String],
    index: usize,
    end: usize,
) -> Result<Option<usize>, String> {
    if tokens_match(tokens, index, "AFTER") {
        require_generated_identifier(tokens, index + 1, end, "AFTER column")?;
        return Ok(Some(index + 2));
    }
    if tokens_match(tokens, index, "FIRST") {
        return Ok(Some(index + 1));
    }
    consume_inline_key_modifier(tokens, index, end)
}

fn consume_inline_key_modifier(
    tokens: &[String],
    index: usize,
    end: usize,
) -> Result<Option<usize>, String> {
    if tokens_match(tokens, index, "PRIMARY") {
        return require_following_keyword(tokens, index, end, "KEY");
    }
    if tokens_match(tokens, index, "UNIQUE") {
        let next = index + usize::from(tokens_match(tokens, index + 1, "KEY")) + 1;
        return Ok(Some(next));
    }
    Ok(None)
}

const SUPPORTED_COLUMN_TYPES: &[&str] = &[
    "BIGINT",
    "BINARY",
    "BIT",
    "BLOB",
    "BOOL",
    "BOOLEAN",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DOUBLE",
    "ENUM",
    "FLOAT",
    "GEOMETRY",
    "GEOMETRYCOLLECTION",
    "INT",
    "INTEGER",
    "JSON",
    "LINESTRING",
    "LONGBLOB",
    "LONGTEXT",
    "MEDIUMBLOB",
    "MEDIUMINT",
    "MEDIUMTEXT",
    "MULTILINESTRING",
    "MULTIPOINT",
    "MULTIPOLYGON",
    "NUMERIC",
    "POINT",
    "POLYGON",
    "REAL",
    "SMALLINT",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TINYBLOB",
    "TINYINT",
    "TINYTEXT",
    "VARBINARY",
    "VARCHAR",
    "YEAR",
];

const PARAMETERIZED_COLUMN_TYPES: &[&str] = &[
    "BIGINT",
    "BINARY",
    "BIT",
    "CHAR",
    "DATETIME",
    "DECIMAL",
    "DOUBLE",
    "ENUM",
    "FLOAT",
    "INT",
    "INTEGER",
    "MEDIUMINT",
    "NUMERIC",
    "REAL",
    "SMALLINT",
    "TIME",
    "TIMESTAMP",
    "TINYINT",
    "VARBINARY",
    "VARCHAR",
    "YEAR",
];

fn validate_column_type(tokens: &[String], index: usize, end: usize) -> Result<usize, String> {
    let column_type = tokens
        .get(index)
        .filter(|_| index < end)
        .ok_or_else(|| "generated column type is missing".to_string())?;
    if column_type.eq_ignore_ascii_case("SET") {
        return Err(format!(
            "generated schema DDL does not support {column_type}"
        ));
    }
    if !type_is_supported(column_type, SUPPORTED_COLUMN_TYPES) {
        return Err(format!(
            "generated column type {column_type} is unsupported"
        ));
    }
    validate_optional_type_parameters(tokens, index + 1, end, column_type)
}

fn validate_optional_type_parameters(
    tokens: &[String],
    index: usize,
    end: usize,
    column_type: &str,
) -> Result<usize, String> {
    if tokens.get(index).map(String::as_str) != Some("(") {
        return Ok(index);
    }
    if !type_is_supported(column_type, PARAMETERIZED_COLUMN_TYPES) {
        return Err(format!(
            "generated column type {column_type} does not accept parameters"
        ));
    }
    let close = validate_parenthesized_definition(tokens, index, end)?;
    if column_type.eq_ignore_ascii_case("ENUM") {
        validate_enum_type_parameters(tokens, index + 1, close)?;
    } else {
        validate_numeric_type_parameters(tokens, index + 1, close)?;
    }
    Ok(close + 1)
}

/// MariaDB and MySQL agree on `ENUM` semantics, so the value list only has to be a
/// comma-separated list of string literals.
fn validate_enum_type_parameters(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<(), String> {
    if start >= end {
        return Err("generated ENUM value list is empty".to_string());
    }
    let mut expect_value = true;
    for token in &tokens[start..end] {
        if expect_value {
            if token != "<string>" {
                return Err(format!("generated ENUM value {token:?} is unsupported"));
            }
        } else if token != "," {
            return Err("generated ENUM values require commas".to_string());
        }
        expect_value = !expect_value;
    }
    if expect_value {
        Err("generated ENUM value list is incomplete".to_string())
    } else {
        Ok(())
    }
}

fn type_is_supported(column_type: &str, supported: &[&str]) -> bool {
    supported
        .iter()
        .any(|candidate| column_type.eq_ignore_ascii_case(candidate))
}

fn validate_numeric_type_parameters(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<(), String> {
    if start >= end {
        return Err("generated column type parameters are empty".to_string());
    }
    let mut expect_number = true;
    for token in &tokens[start..end] {
        if expect_number {
            if token.parse::<u32>().is_err() {
                return Err(format!(
                    "generated column type parameter {token:?} is unsupported"
                ));
            }
        } else if token != "," {
            return Err("generated column type parameters require commas".to_string());
        }
        expect_number = !expect_number;
    }
    if expect_number {
        Err("generated column type parameter list is incomplete".to_string())
    } else {
        Ok(())
    }
}

fn consume_default_expression(
    tokens: &[String],
    index: usize,
    end: usize,
) -> Result<usize, String> {
    if tokens_match(tokens, index, "CURRENT_TIMESTAMP") {
        return consume_current_timestamp(tokens, index, end);
    }
    if tokens.get(index).is_some_and(|_| index < end) {
        return Ok(index + 1);
    }
    Err("generated DEFAULT value is missing".to_string())
}

fn consume_current_timestamp(tokens: &[String], index: usize, end: usize) -> Result<usize, String> {
    require_generated_keyword(tokens, index, end, "CURRENT_TIMESTAMP")?;
    if tokens.get(index + 1).map(String::as_str) == Some("(") {
        return Ok(validate_parenthesized_definition(tokens, index + 1, end)? + 1);
    }
    Ok(index + 1)
}

fn consume_generated_expression(
    tokens: &[String],
    mut index: usize,
    end: usize,
) -> Result<usize, String> {
    if tokens_match(tokens, index, "GENERATED") {
        require_generated_keyword(tokens, index + 1, end, "ALWAYS")?;
        index += 2;
    }
    require_generated_keyword(tokens, index, end, "AS")?;
    let close = validate_parenthesized_definition(tokens, index + 1, end)?;
    if !token_is_one_of(tokens, close + 1, &["VIRTUAL", "STORED"]) {
        return Err("generated column requires VIRTUAL or STORED".to_string());
    }
    Ok(close + 2)
}

fn validate_parenthesized_definition(
    tokens: &[String],
    open: usize,
    end: usize,
) -> Result<usize, String> {
    if open >= end || tokens.get(open).map(String::as_str) != Some("(") {
        return Err("generated parenthesized definition is missing".to_string());
    }
    let close = matching_parenthesis(tokens, open)?;
    if close >= end {
        return Err("generated parenthesized definition crosses its boundary".to_string());
    }
    Ok(close)
}

fn require_generated_keyword(
    tokens: &[String],
    index: usize,
    end: usize,
    expected: &str,
) -> Result<(), String> {
    if index < end && tokens_match(tokens, index, expected) {
        Ok(())
    } else {
        Err(format!("generated definition expected {expected}"))
    }
}

fn require_generated_identifier(
    tokens: &[String],
    index: usize,
    end: usize,
    context: &str,
) -> Result<(), String> {
    let token = tokens
        .get(index)
        .filter(|_| index < end)
        .ok_or_else(|| format!("generated {context} is missing"))?;
    if token == "<string>" || matches!(token.as_str(), "(" | ")" | "," | "." | "=") {
        Err(format!("generated {context} is invalid"))
    } else {
        Ok(())
    }
}

fn validate_generated_drop(tokens: &[String]) -> Result<(), String> {
    if tokens_match(tokens, 4, "PRIMARY") {
        return (tokens.len() == 6 && tokens_match(tokens, 5, "KEY"))
            .then_some(())
            .ok_or_else(|| "generated DROP PRIMARY KEY is ambiguous".to_string());
    }
    if tokens_match(tokens, 4, "FOREIGN") {
        return (tokens.len() == 7 && tokens_match(tokens, 5, "KEY"))
            .then_some(())
            .ok_or_else(|| "generated DROP FOREIGN KEY is ambiguous".to_string());
    }
    if tokens_match(tokens, 4, "CHECK") {
        return (tokens.len() == 6)
            .then_some(())
            .ok_or_else(|| "generated DROP CHECK is ambiguous".to_string());
    }
    if tokens_match(tokens, 4, "COLUMN") {
        let offset = if tokens_match(tokens, 5, "IF") { 8 } else { 6 };
        return (tokens.len() == offset)
            .then_some(())
            .ok_or_else(|| "generated DROP COLUMN is ambiguous".to_string());
    }
    Err("generated DROP action is unsupported".to_string())
}

fn is_generated_unique_index(tokens: &[String]) -> bool {
    tokens_match(tokens, 0, "CREATE")
        && tokens_match(tokens, 1, "UNIQUE")
        && tokens_match(tokens, 2, "INDEX")
}

fn validate_generated_unique_index(tokens: &[String]) -> Result<(), String> {
    if tokens.len() < 8 || !tokens_match(tokens, 4, "ON") {
        return Err("generated CREATE UNIQUE INDEX header is invalid".to_string());
    }
    let open = tokens
        .iter()
        .position(|token| token == "(")
        .ok_or_else(|| "generated CREATE UNIQUE INDEX columns are missing".to_string())?;
    let close = matching_parenthesis(tokens, open)?;
    (close + 1 == tokens.len())
        .then_some(())
        .ok_or_else(|| "generated CREATE UNIQUE INDEX has unmodeled options".to_string())
}

fn matching_parenthesis(tokens: &[String], open: usize) -> Result<usize, String> {
    let mut depth = 0_u32;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token.as_str() {
            "(" => depth += 1,
            ")" => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| "generated DDL parentheses are unbalanced".to_string())?;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
    }
    Err("generated DDL parentheses are unbalanced".to_string())
}

fn has_top_level_comma(tokens: &[String], start: usize) -> bool {
    let mut depth = 0_u32;
    for token in tokens.iter().skip(start) {
        match token.as_str() {
            "(" => depth += 1,
            ")" => depth = depth.saturating_sub(1),
            "," if depth == 0 => return true,
            _ => {}
        }
    }
    false
}

fn generated_schema_tokens(source_sql: &str) -> Option<Vec<String>> {
    let source_sql = strip_leading_ordinary_ddl_comments(source_sql).ok()?;
    if ddl_contains_comments(source_sql) || source_sql.contains('"') {
        return None;
    }
    let mut tokens = tokenize_ddl(source_sql).ok()?;
    if tokens.last().is_some_and(|token| token == ";") {
        tokens.pop();
    }
    (!tokens.iter().any(|token| token == ";") && tokens.len() >= 4).then_some(tokens)
}

fn is_generated_create_table(tokens: &[String]) -> bool {
    tokens_match(tokens, 0, "CREATE")
        && tokens_match(tokens, 1, "TABLE")
        && tokens.get(3).is_some_and(|token| token == "(")
        && tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("ENGINE"))
}

fn is_generated_alter_table(tokens: &[String]) -> bool {
    if !tokens_match(tokens, 0, "ALTER") || !tokens_match(tokens, 1, "TABLE") {
        return false;
    }
    match tokens.get(3).map(|token| token.to_ascii_uppercase()) {
        Some(action) if action == "ADD" => {
            token_is_one_of(tokens, 4, &["COLUMN", "PRIMARY", "CONSTRAINT"])
        }
        Some(action) if action == "MODIFY" => tokens_match(tokens, 4, "COLUMN"),
        Some(action) if action == "DROP" => {
            token_is_one_of(tokens, 4, &["PRIMARY", "FOREIGN", "CHECK"])
        }
        _ => false,
    }
}

fn tokens_match(tokens: &[String], index: usize, expected: &str) -> bool {
    tokens
        .get(index)
        .is_some_and(|token| token.eq_ignore_ascii_case(expected))
}

fn token_is_one_of(tokens: &[String], index: usize, expected: &[&str]) -> bool {
    tokens.get(index).is_some_and(|token| {
        expected
            .iter()
            .any(|candidate| token.eq_ignore_ascii_case(candidate))
    })
}

pub fn transform_fixture_create_table(source_sql: &str) -> Result<DdlTransformation, String> {
    let ast = parse_fixture_create_table(source_sql)?;
    transform_fixture_create_table_ast(&ast, None)
}

pub fn transform_fixture_create_table_with_defaults(
    ast: &ParsedCreateTableAst,
    defaults: &crate::inventory::SchemaDefaults,
) -> Result<DdlTransformation, String> {
    validate_schema_default_identifier(&defaults.character_set, "character set")?;
    validate_schema_default_identifier(&defaults.collation, "collation")?;
    transform_fixture_create_table_ast(ast, Some(defaults))
}

fn transform_fixture_create_table_ast(
    ast: &ParsedCreateTableAst,
    defaults: Option<&crate::inventory::SchemaDefaults>,
) -> Result<DdlTransformation, String> {
    let mut definitions = ast
        .columns
        .iter()
        .map(render_create_column)
        .collect::<Vec<_>>();
    definitions.push(format!(
        "PRIMARY KEY ({})",
        ast.primary_key
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    definitions.extend(ast.indexes.iter().map(render_create_index));
    let schema_defaults = render_create_schema_defaults(ast, defaults);
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: Some(format!(
            "CREATE TABLE {} ({}) ENGINE={}{}",
            quote_identifier(&ast.name),
            definitions.join(", "),
            ast.engine,
            schema_defaults,
        )),
    })
}

fn render_create_column(column: &ParsedCreateColumnAst) -> String {
    let nullability = if column.nullable { "NULL" } else { "NOT NULL" };
    let default = column
        .default_sql
        .as_ref()
        .map_or_else(String::new, |value| format!(" DEFAULT {value}"));
    let auto_increment = if column.auto_increment {
        " AUTO_INCREMENT"
    } else {
        ""
    };
    format!(
        "{} {} {nullability}{default}{auto_increment}",
        quote_identifier(&column.name),
        column.column_type.to_ascii_uppercase()
    )
}

fn render_create_index(index: &ParsedIndexAst) -> String {
    let kind = if index.unique { "UNIQUE KEY" } else { "KEY" };
    let columns = index
        .key_parts
        .iter()
        .map(|part| quote_identifier(&part.column))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{kind} {} ({columns})", quote_identifier(&index.name))
}

fn render_create_schema_defaults(
    ast: &ParsedCreateTableAst,
    defaults: Option<&crate::inventory::SchemaDefaults>,
) -> String {
    if let (Some(character_set), Some(collation)) =
        (ast.character_set.as_deref(), ast.collation.as_deref())
    {
        return format!(" DEFAULT CHARACTER SET={character_set} COLLATE={collation}");
    }
    defaults.map_or_else(String::new, |defaults| {
        format!(
            " DEFAULT CHARACTER SET {} COLLATE {}",
            defaults.character_set, defaults.collation
        )
    })
}

fn validate_schema_default_identifier(value: &str, kind: &str) -> Result<(), String> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        Ok(())
    } else {
        Err(format!("invalid source schema {kind} `{value}`"))
    }
}

const SOURCE_ONLY_RELEASE_MOVE_PROCEDURE_HASHES: [&str; 2] = [
    "1326338ea27069ed94e2f1a94f2cfc118465939a2312d7bba0adafb3da3728ec",
    "a3e4b4b54295bd0374965761f3ec3a8bfd7ab857b623d25c9010e8fe6b3449c3",
];

pub(super) fn supports_source_only_release_move_procedure_digest(digest: &str) -> bool {
    SOURCE_ONLY_RELEASE_MOVE_PROCEDURE_HASHES.contains(&digest)
}

pub fn supports_source_only_release_move_procedure_create(source_sql: &str) -> bool {
    let digest = format!("{:x}", Sha256::digest(source_sql.trim_end().as_bytes()));
    supports_source_only_release_move_procedure_digest(&digest)
}

pub(super) fn transform_source_only_release_move_procedure_digest(
    digest: &str,
) -> Result<DdlTransformation, String> {
    if !supports_source_only_release_move_procedure_digest(digest) {
        return Err(
            "source-only release-move CREATE PROCEDURE does not match an admitted body hash"
                .to_string(),
        );
    }
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: None,
    })
}

pub fn transform_source_only_release_move_procedure_create(
    source_sql: &str,
) -> Result<DdlTransformation, String> {
    let digest = format!("{:x}", Sha256::digest(source_sql.trim_end().as_bytes()));
    transform_source_only_release_move_procedure_digest(&digest)
}

pub fn supports_drop_procedure(source_sql: &str) -> bool {
    parse_supported_drop_procedure(source_sql).is_ok()
}

pub fn transform_drop_procedure(
    source_sql: &str,
    target_procedures: &BTreeSet<String>,
) -> Result<DdlTransformation, String> {
    let source_name = parse_supported_drop_procedure(source_sql)?;
    let target_name = target_procedures
        .iter()
        .find(|name| name.eq_ignore_ascii_case(&source_name));
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: target_name.map(|name| format!("DROP PROCEDURE {}", quote_identifier(name))),
    })
}

fn parse_supported_drop_procedure(source_sql: &str) -> Result<String, String> {
    if ddl_contains_comments(source_sql) {
        return Err("DROP PROCEDURE comments are not supported".to_string());
    }
    if source_sql.contains('"') {
        return Err("DROP PROCEDURE double-quoted identifiers are not supported".to_string());
    }
    let (tokens, quoted_flags) = tokenize_ddl_with_quoted_flags(source_sql)?;
    require_keyword(&tokens, 0, "DROP")?;
    require_keyword(&tokens, 1, "PROCEDURE")?;
    let has_if_exists = tokens
        .get(2)
        .is_some_and(|token| token.eq_ignore_ascii_case("IF"));
    let name_index = if has_if_exists {
        require_keyword(&tokens, 3, "EXISTS")?;
        4
    } else {
        2
    };
    if quoted_flags.get(name_index).copied().unwrap_or(false) {
        return Err("quoted DROP PROCEDURE identifiers are not supported".to_string());
    }
    let name = require_identifier(&tokens, name_index, "DROP PROCEDURE name")?;
    if !has_if_exists && name != "apply_release_move_purchase_repair" {
        return Err(
            "plain DROP PROCEDURE is supported only for the release-move repair routine"
                .to_string(),
        );
    }
    let trailing_index = name_index + 1;
    let end = if tokens.get(trailing_index).map(String::as_str) == Some(";") {
        trailing_index + 1
    } else {
        trailing_index
    };
    if tokens.len() != end {
        return Err("DROP PROCEDURE requires one unqualified procedure name".to_string());
    }
    Ok(name)
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

fn production_alter_sql(source_sql: &str) -> Result<&str, String> {
    let source_sql = strip_one_leading_mysql_line_comment(source_sql);
    if ddl_contains_comments(source_sql) {
        return Err("production ALTER TABLE comments are not supported".to_string());
    }
    Ok(source_sql)
}

pub fn parse_production_alter_table_ast(source_sql: &str) -> Result<ParsedAlterTableAst, String> {
    let source_sql = production_alter_sql(source_sql)?;
    let (tokens, quoted_flags) = tokenize_ddl_with_quoted_flags(source_sql)?;
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
            Some("ADD") => {
                parse_production_add_clause(&tokens, &quoted_flags, index, &table, &mut literals)?
            }
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
    quoted_flags: &[bool],
    index: usize,
    table: &str,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    match tokens
        .get(index + 1)
        .map(|token| token.to_ascii_uppercase())
    {
        Some(kind) if kind == "COLUMN" => {
            parse_add_column_clause(tokens, quoted_flags, index, literals)
        }
        Some(kind) if matches!(kind.as_str(), "KEY" | "INDEX") => {
            parse_add_key_clause(tokens, index + 1, table, false)
        }
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
    quoted_flags: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    let name = require_identifier(tokens, index + 2, "added column")?;
    let (column_type, data_type, options_start) =
        parse_observed_column_type(tokens, quoted_flags, index + 3)?;
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
    quoted_flags: &[bool],
    mut index: usize,
) -> Result<(String, String, usize), String> {
    require_unquoted_token(quoted_flags, index, "added column type")?;
    let data_type = require_identifier(tokens, index, "added column type")?.to_ascii_lowercase();
    if !matches!(data_type.as_str(), "varchar" | "datetime" | "smallint") {
        return Err(format!(
            "unsupported production ADD COLUMN type {data_type}"
        ));
    }
    index += 1;
    let column_type = match data_type.as_str() {
        "varchar" => {
            require_unquoted_token(quoted_flags, index, "VARCHAR opening parenthesis")?;
            require_unquoted_token(quoted_flags, index + 1, "VARCHAR length")?;
            require_unquoted_token(quoted_flags, index + 2, "VARCHAR closing parenthesis")?;
            require_keyword(tokens, index, "(")?;
            let length = tokens
                .get(index + 1)
                .cloned()
                .ok_or_else(|| "missing column type length".to_string())?;
            let parsed_length = length
                .parse::<u32>()
                .map_err(|_| format!("invalid column type length {length}"))?;
            if parsed_length == 0 || parsed_length.to_string() != length {
                return Err(format!("noncanonical column type length {length}"));
            }
            require_keyword(tokens, index + 2, ")")?;
            index += 3;
            format!("varchar({parsed_length})")
        }
        "datetime" => {
            if tokens.get(index).map(String::as_str) == Some("(") {
                return Err("DATETIME precision is unsupported".to_string());
            }
            data_type.clone()
        }
        "smallint" => {
            if tokens.get(index).map(String::as_str) == Some("(") {
                return Err("SMALLINT display width is unsupported".to_string());
            }
            require_unquoted_token(quoted_flags, index, "SMALLINT UNSIGNED keyword")?;
            require_keyword(tokens, index, "UNSIGNED")?;
            index += 1;
            "smallint unsigned".to_string()
        }
        _ => unreachable!("supported types were checked above"),
    };
    if tokens
        .get(index)
        .is_some_and(|token| token.eq_ignore_ascii_case("UNSIGNED"))
    {
        return Err(format!("UNSIGNED is unsupported for {data_type}"));
    }
    Ok((column_type, data_type, index))
}

fn require_unquoted_token(
    quoted_flags: &[bool],
    index: usize,
    context: &str,
) -> Result<(), String> {
    if quoted_flags.get(index) == Some(&true) {
        return Err(format!("quoted token is unsupported for {context}"));
    }
    Ok(())
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
    let mut bytes = value.bytes();
    let valid_start = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    let valid_rest = bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if !valid_start || !valid_rest {
        return Err(format!("invalid {context}: {value}"));
    }
    Ok(value.clone())
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}
