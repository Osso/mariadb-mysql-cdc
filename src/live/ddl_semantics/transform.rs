use super::model::{
    ParsedAddColumnAst, ParsedAlterAlgorithm, ParsedAlterClause, ParsedAlterLock,
    ParsedAlterTableAst, ParsedColumnDefault, ParsedCreateColumnAst, ParsedCreateTableAst,
    ParsedDropColumnAst, ParsedDropIndexAst, ParsedIndexAst, ParsedIndexKeyPart,
    ParsedStoredIfExpression,
};
use super::tokenizer::{
    ddl_contains_comments, split_one_leading_mysql_line_comment,
    strip_leading_ordinary_ddl_comments, tokenize_ddl, tokenize_ddl_with_quoted_flags,
    tokenize_ddl_with_quoted_flags_mode,
};
use crate::live::query_charset_context::SourceSqlMode;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

mod basic_types;
mod check_constraint;
mod generated_column;
mod observed_create;

pub(crate) use check_constraint::{canonical_check_constraint_value, referenced_columns};
pub(crate) use generated_column::{
    mysql_generation_expression, referenced_columns as generated_referenced_columns,
};
pub(crate) use observed_create::{current_timestamp_for, is_text_type, text_expression_default};

pub const DDL_TRANSFORMATION_VERSION: &str = "mariadb-mysql8-v1";

type ParsedAlterOptions = (Option<ParsedAlterAlgorithm>, Option<ParsedAlterLock>);
type ParsedAlterBody = (
    Vec<ParsedAlterClause>,
    Option<ParsedAlterAlgorithm>,
    Option<ParsedAlterLock>,
);

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
    parse_production_alter_table_ast(source_sql)
        .is_ok_and(|ast| supports_parsed_production_alter(&ast))
}

fn supports_parsed_production_alter(ast: &ParsedAlterTableAst) -> bool {
    supports_existing_production_alter(ast)
        || supports_content_sections_seen_columns_instant(ast)
        || supports_releases_downloads_sort_rebuild(ast)
}

fn supports_existing_production_alter(ast: &ParsedAlterTableAst) -> bool {
    ast.algorithm.is_none()
        && ast.lock.is_none()
        && ast.clauses.iter().all(|clause| match clause {
            ParsedAlterClause::AddColumn(_) => true,
            ParsedAlterClause::AddKey { .. }
            | ParsedAlterClause::AddCheck(_)
            | ParsedAlterClause::AddForeignKey(_)
            | ParsedAlterClause::ModifyColumn(_)
            | ParsedAlterClause::ChangeColumn { .. }
            | ParsedAlterClause::AlterColumnDefault { .. }
            | ParsedAlterClause::RenameColumn { .. } => true,
            ParsedAlterClause::DropColumn(column) => !column.if_exists,
            ParsedAlterClause::DropIndex(_) => true,
        })
}

fn supports_content_sections_seen_columns_instant(ast: &ParsedAlterTableAst) -> bool {
    if ast.table != "content_sections_events_raw"
        || ast.algorithm != Some(ParsedAlterAlgorithm::Instant)
        || ast.lock.is_some()
    {
        return false;
    }
    let [
        ParsedAlterClause::AddColumn(direct_seen),
        ParsedAlterClause::AddColumn(sync_seen),
    ] = ast.clauses.as_slice()
    else {
        return false;
    };
    is_exact_seen_column(
        direct_seen,
        "direct_seen_at",
        "When CmsEventsBufferManager (direct write) first saw this event",
    ) && is_exact_seen_column(
        sync_seen,
        "sync_seen_at",
        "When ContentSectionsEventSyncService (Mixpanel Export) first saw it",
    )
}

fn supports_releases_downloads_sort_rebuild(ast: &ParsedAlterTableAst) -> bool {
    let [
        ParsedAlterClause::DropIndex(dropped),
        ParsedAlterClause::AddKey {
            index: added,
            if_not_exists: false,
        },
    ] = ast.clauses.as_slice()
    else {
        return false;
    };
    ast.table == "releases"
        && ast.algorithm == Some(ParsedAlterAlgorithm::Inplace)
        && ast.lock == Some(ParsedAlterLock::None)
        && dropped.name == "idx_downloads_sort"
        && added == &releases_downloads_sort_index()
}

fn releases_downloads_sort_index() -> ParsedIndexAst {
    let columns = [
        ("is_deleted", "ASC"),
        ("is_published", "ASC"),
        ("is_visible", "ASC"),
        ("comic_is_visible", "ASC"),
        ("lang_id", "ASC"),
        ("published_time", "DESC"),
        ("comic_id", "ASC"),
        ("id", "ASC"),
    ];
    ParsedIndexAst {
        create: true,
        name: "idx_downloads_sort".to_string(),
        table: "releases".to_string(),
        unique: false,
        index_type: "BTREE".to_string(),
        visible: true,
        comment: None,
        key_parts: columns
            .into_iter()
            .map(|(column, order)| ParsedIndexKeyPart {
                column: column.to_string(),
                prefix_length: None,
                order: order.to_string(),
                collation: Some(if order == "DESC" { "D" } else { "A" }.to_string()),
            })
            .collect(),
    }
}

fn is_exact_seen_column(column: &ParsedAddColumnAst, name: &str, comment: &str) -> bool {
    column
        == &ParsedAddColumnAst {
            name: name.to_string(),
            if_not_exists: true,
            column_type: "timestamp".to_string(),
            data_type: "timestamp".to_string(),
            nullable: true,
            default_value: None,
            comment: comment.to_string(),
            after: None,
            first: false,
            character_set: None,
            collation: None,
            generated: None,
        }
}

pub fn transform_production_alter_table(source_sql: &str) -> Result<DdlTransformation, String> {
    transform_production_alter_table_with_mode(source_sql, SourceSqlMode(None))
}

pub fn transform_production_alter_table_with_mode(
    source_sql: &str,
    mode: SourceSqlMode,
) -> Result<DdlTransformation, String> {
    let (leading_comment, _) = split_one_leading_mysql_line_comment(source_sql);
    let ast = parse_production_alter_table_ast_with_mode(source_sql, mode)?;
    if !supports_parsed_production_alter(&ast) {
        return Err("unsupported production ALTER TABLE shape".to_string());
    }
    if ast
        .clauses
        .iter()
        .any(|clause| matches!(clause, ParsedAlterClause::AlterColumnDefault { .. }))
    {
        return Err("ALTER COLUMN DEFAULT requires target column state".into());
    }
    let rendered_sql = render_production_alter_table(&ast);
    Ok(transformed_alter_sql(leading_comment, rendered_sql))
}

#[cfg(test)]
pub(crate) fn transform_production_alter_table_with_target(
    source_sql: &str,
    target: &super::model::SemanticSchemaSnapshot,
) -> Result<DdlTransformation, String> {
    transform_production_alter_table_with_target_mode(source_sql, target, SourceSqlMode(None))
}

pub(crate) fn transform_production_alter_table_with_target_mode(
    source_sql: &str,
    target: &super::model::SemanticSchemaSnapshot,
    mode: SourceSqlMode,
) -> Result<DdlTransformation, String> {
    let (leading_comment, _) = split_one_leading_mysql_line_comment(source_sql);
    let ast = parse_production_alter_table_ast_with_mode(source_sql, mode)?;
    if !supports_parsed_production_alter(&ast) {
        return Err("unsupported production ALTER TABLE shape".to_string());
    }
    let rendered_sql = render_alter_table_with_target(&ast, target)?;
    Ok(transformed_alter_sql(leading_comment, rendered_sql))
}

fn transformed_alter_sql(leading_comment: Option<&str>, rendered_sql: String) -> DdlTransformation {
    let target_sql = match leading_comment {
        Some(comment) => format!("{comment}{rendered_sql}"),
        None => rendered_sql,
    };
    DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: Some(target_sql),
    }
}

fn render_production_alter_table(ast: &ParsedAlterTableAst) -> String {
    let clauses = ast
        .clauses
        .iter()
        .map(render_production_alter_clause)
        .collect();
    render_alter_table_clauses(ast, clauses)
}

fn render_alter_table_with_target(
    ast: &ParsedAlterTableAst,
    target: &super::model::SemanticSchemaSnapshot,
) -> Result<String, String> {
    let normalized = fold_new_column_defaults(ast, target)?;
    let mut state = target.clone();
    let mut clauses = Vec::with_capacity(normalized.clauses.len());
    for clause in &normalized.clauses {
        let rendered = match clause {
            ParsedAlterClause::AlterColumnDefault { name, default } => {
                render_target_column_default(&state, &ast.table, name, default.as_ref())?
            }
            other => render_production_alter_clause(other),
        };
        super::canonical::apply_alter_clause(&mut state, &normalized, clause)?;
        clauses.push(rendered);
    }
    Ok(render_alter_table_clauses(&normalized, clauses))
}

// MariaDB resolves these defaults before it backfills newly added columns. Keep
// that atomic behavior instead of creating a column and then changing its default.
fn fold_new_column_defaults(
    ast: &ParsedAlterTableAst,
    target: &super::model::SemanticSchemaSnapshot,
) -> Result<ParsedAlterTableAst, String> {
    let mut state = target.clone();
    let mut normalized = ast.clone();
    normalized.clauses.clear();
    let mut added = std::collections::BTreeMap::new();
    for clause in &ast.clauses {
        let new_column = match clause {
            ParsedAlterClause::AddColumn(column)
                if !snapshot_has_column(&state, &ast.table, &column.name) =>
            {
                Some(column.name.to_ascii_lowercase())
            }
            _ => None,
        };
        super::canonical::apply_alter_clause(&mut state, ast, clause)?;
        if let ParsedAlterClause::AlterColumnDefault { name, default } = clause
            && let Some(index) = added.get(&name.to_ascii_lowercase())
        {
            fold_column_default(&mut normalized.clauses, *index, default.as_ref())?;
            continue;
        }
        invalidate_changed_column_origin(&mut added, clause);
        if let Some(name) = new_column {
            added.insert(name, normalized.clauses.len());
        }
        normalized.clauses.push(clause.clone());
    }
    Ok(normalized)
}

fn snapshot_has_column(
    snapshot: &super::model::SemanticSchemaSnapshot,
    table: &str,
    name: &str,
) -> bool {
    snapshot
        .inventory
        .tables
        .iter()
        .find(|item| item.name == table)
        .is_some_and(|table| {
            table
                .columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(name))
        })
}

fn fold_column_default(
    clauses: &mut [ParsedAlterClause],
    index: usize,
    default: Option<&ParsedColumnDefault>,
) -> Result<(), String> {
    let Some(ParsedAlterClause::AddColumn(column)) = clauses.get_mut(index) else {
        return Err("new-column default origin is not an ADD COLUMN".into());
    };
    column.default_value = match default {
        None | Some(ParsedColumnDefault::Null) => None,
        Some(ParsedColumnDefault::String(value)) => Some(value.clone()),
        Some(ParsedColumnDefault::Number(value)) => Some(basic_types::normalize_numeric_default(
            &column.column_type,
            value,
        )?),
    };
    Ok(())
}

fn invalidate_changed_column_origin(
    added: &mut std::collections::BTreeMap<String, usize>,
    clause: &ParsedAlterClause,
) {
    let name = match clause {
        ParsedAlterClause::ModifyColumn(column) => Some(column.name.as_str()),
        ParsedAlterClause::ChangeColumn { old_name, .. }
        | ParsedAlterClause::RenameColumn { old_name, .. } => Some(old_name.as_str()),
        ParsedAlterClause::DropColumn(column) => Some(column.name.as_str()),
        _ => None,
    };
    if let Some(name) = name {
        added.remove(&name.to_ascii_lowercase());
    }
}

fn render_target_column_default(
    target: &super::model::SemanticSchemaSnapshot,
    table: &str,
    name: &str,
    default: Option<&ParsedColumnDefault>,
) -> Result<String, String> {
    let column = target
        .inventory
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .and_then(|table| {
            table
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(name))
        })
        .ok_or_else(|| format!("ALTER COLUMN target `{table}`.`{name}` is missing"))?;
    let (normalized, _) = normalize_alter_column_default(
        &column.column_type,
        &column.data_type,
        column.is_nullable,
        default,
    )?;
    if is_text_type(&column.data_type) {
        let value = match default {
            Some(ParsedColumnDefault::String(value)) => Some(value.as_str()),
            None | Some(ParsedColumnDefault::Null) => None,
            Some(ParsedColumnDefault::Number(_)) => {
                return Err("TEXT defaults require a string or NULL".into());
            }
        };
        return render_text_default_definition(column, value);
    }
    let action = match default {
        // MariaDB retains implicit NULL for a nullable column after DROP DEFAULT.
        // MySQL's DROP can forbid omitted values, so express that default explicitly.
        None if column.is_nullable => "SET DEFAULT NULL".to_string(),
        None => "DROP DEFAULT".to_string(),
        Some(ParsedColumnDefault::Number(_)) => {
            format!(
                "SET DEFAULT {}",
                normalized.ok_or("numeric DEFAULT normalization is missing")?
            )
        }
        Some(value) => format!("SET DEFAULT {}", render_alter_default(value)),
    };
    Ok(format!("ALTER COLUMN {} {action}", quote_identifier(name)))
}

// MySQL 8.4 rejects TEXT SET DEFAULT expression literals with error 1101. Until
// that server path accepts them, MODIFY preserves the complete target definition.
fn render_text_default_definition(
    column: &crate::inventory::ColumnInventory,
    value: Option<&str>,
) -> Result<String, String> {
    if !matches!(column.extra.as_str(), "" | "DEFAULT_GENERATED") || column.generated.is_some() {
        return Err("TEXT default replacement cannot preserve unmodeled column attributes".into());
    }
    let replacement = ParsedAddColumnAst {
        name: column.name.clone(),
        if_not_exists: false,
        column_type: column.column_type.clone(),
        data_type: column.data_type.clone(),
        nullable: column.is_nullable,
        default_value: value.map(str::to_string),
        comment: column.comment.clone(),
        after: None,
        first: false,
        character_set: column.character_set.clone(),
        collation: column.collation.clone(),
        generated: None,
    };
    Ok(render_production_alter_clause(
        &ParsedAlterClause::ModifyColumn(replacement),
    ))
}

fn render_alter_table_clauses(ast: &ParsedAlterTableAst, mut clauses: Vec<String>) -> String {
    if let Some(algorithm) = ast.algorithm {
        clauses.push(format!(
            "ALGORITHM={}",
            algorithm.as_str().to_ascii_uppercase()
        ));
    }
    if let Some(lock) = ast.lock {
        clauses.push(format!("LOCK={}", lock.as_str().to_ascii_uppercase()));
    }
    format!(
        "ALTER TABLE {} {}",
        quote_identifier(&ast.table),
        clauses.join(", ")
    )
}

fn render_production_alter_clause(clause: &ParsedAlterClause) -> String {
    match clause {
        ParsedAlterClause::AddColumn(column) => render_add_column(column),
        ParsedAlterClause::ModifyColumn(column) => {
            render_add_column(column).replacen("ADD COLUMN", "MODIFY COLUMN", 1)
        }
        ParsedAlterClause::ChangeColumn { old_name, column } => render_add_column(column).replacen(
            "ADD COLUMN",
            &format!("CHANGE COLUMN {}", quote_identifier(old_name)),
            1,
        ),
        ParsedAlterClause::AlterColumnDefault { name, default } => {
            let action = match default {
                None => "DROP DEFAULT".to_string(),
                Some(value) => format!("SET DEFAULT {}", render_alter_default(value)),
            };
            format!("ALTER COLUMN {} {action}", quote_identifier(name))
        }
        ParsedAlterClause::RenameColumn { old_name, new_name } => format!(
            "RENAME COLUMN {} TO {}",
            quote_identifier(old_name),
            quote_identifier(new_name)
        ),
        ParsedAlterClause::AddKey { index, .. } => render_add_key(index),
        ParsedAlterClause::AddCheck(constraint) => {
            format!(
                "ADD {}",
                check_constraint::render_check_constraint(constraint)
            )
        }
        ParsedAlterClause::AddForeignKey(key) => format!("ADD {}", render_create_foreign_key(key)),
        ParsedAlterClause::DropColumn(column) => {
            format!("DROP COLUMN {}", quote_identifier(&column.name))
        }
        ParsedAlterClause::DropIndex(index) => {
            format!("DROP INDEX {}", quote_identifier(&index.name))
        }
    }
}

pub(crate) fn normalize_alter_column_default(
    column_type: &str,
    data_type: &str,
    nullable: bool,
    default: Option<&ParsedColumnDefault>,
) -> Result<(Option<String>, bool), String> {
    match default {
        None => Ok((None, false)),
        Some(ParsedColumnDefault::Null) if nullable => Ok((None, false)),
        Some(ParsedColumnDefault::Null) => Err("NOT NULL column cannot have DEFAULT NULL".into()),
        Some(ParsedColumnDefault::String(value)) if is_text_type(data_type) => {
            Ok((Some(mysql_text_default_metadata(value)), true))
        }
        Some(ParsedColumnDefault::String(value))
            if matches!(
                data_type,
                "char"
                    | "varchar"
                    | "binary"
                    | "varbinary"
                    | "date"
                    | "datetime"
                    | "time"
                    | "timestamp"
                    | "year"
            ) =>
        {
            Ok((Some(value.clone()), false))
        }
        Some(ParsedColumnDefault::Number(value))
            if matches!(
                data_type,
                "tinyint"
                    | "smallint"
                    | "mediumint"
                    | "int"
                    | "bigint"
                    | "decimal"
                    | "float"
                    | "double"
            ) =>
        {
            let normalized = basic_types::normalize_numeric_default(column_type, value)?;
            Ok((Some(normalized), false))
        }
        Some(_) => Err(format!("unsupported literal default for {column_type}")),
    }
}

fn render_alter_default(value: &ParsedColumnDefault) -> String {
    match value {
        ParsedColumnDefault::String(value) => quote_string_literal(value),
        ParsedColumnDefault::Number(value) => value.clone(),
        ParsedColumnDefault::Null => "NULL".into(),
    }
}

fn render_add_column(column: &ParsedAddColumnAst) -> String {
    let nullability = if column.nullable { "NULL" } else { "NOT NULL" };
    let data_type = if column.data_type == "json" {
        "longtext"
    } else {
        column.data_type.as_str()
    };
    let default_value = match column.default_value.as_deref() {
        None => "NULL".to_string(),
        Some(value) if is_text_type(data_type) => text_expression_default(value),
        Some(value)
            if matches!(
                data_type,
                "char"
                    | "varchar"
                    | "binary"
                    | "varbinary"
                    | "date"
                    | "datetime"
                    | "time"
                    | "timestamp"
                    | "year"
            ) =>
        {
            quote_string_literal(value)
        }
        Some(value) => value.to_string(),
    };
    let column_type = if column.data_type == "json" {
        "longtext"
    } else {
        &column.column_type
    };
    let attributes = match &column.generated {
        Some(expression) => format!(
            "GENERATED ALWAYS AS ({}) STORED",
            generated_column::render_generation_sql(expression)
        ),
        None if !column.nullable && column.default_value.is_none() => nullability.to_string(),
        None => format!("{nullability} DEFAULT {default_value}"),
    };
    let mut sql = format!(
        "ADD COLUMN {} {}{} {attributes}",
        quote_identifier(&column.name),
        column_type.to_ascii_uppercase(),
        render_column_encoding(column.character_set.as_deref(), column.collation.as_deref()),
    );
    if !column.comment.is_empty() {
        sql.push_str(&format!(
            " COMMENT {}",
            quote_string_literal(&column.comment)
        ));
    }
    if column.first {
        sql.push_str(" FIRST");
    } else if let Some(after) = &column.after {
        sql.push_str(&format!(" AFTER {}", quote_identifier(after)));
    }
    if column.data_type == "json" {
        let name = quote_identifier(&column.name);
        sql.push_str(&format!(", ADD CHECK (JSON_VALID({name}))"));
    }
    sql
}

fn render_add_key(index: &ParsedIndexAst) -> String {
    let columns = index
        .key_parts
        .iter()
        .map(|part| {
            let column = quote_identifier(&part.column);
            if part.order == "DESC" {
                format!("{column} DESC")
            } else {
                column
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let key_kind = if index.unique { "UNIQUE KEY" } else { "KEY" };
    format!(
        "ADD {key_kind} {} ({columns})",
        quote_identifier(&index.name)
    )
}

pub(crate) fn mysql_text_default_metadata(value: &str) -> String {
    let mut metadata = String::from("_utf8mb4\\'");
    for character in value.chars() {
        match character {
            '\0' => metadata.push_str("\\\\0"),
            '\n' => metadata.push_str("\\\\n"),
            '\r' => metadata.push_str("\\\\r"),
            '\u{001a}' => metadata.push_str("\\\\Z"),
            '\\' => metadata.push_str("\\\\\\\\"),
            '\'' => metadata.push_str("\\\\\\'"),
            other => metadata.push(other),
        }
    }
    metadata.push_str("\\'");
    metadata
}

fn quote_string_literal(value: &str) -> String {
    let mut quoted = String::from("'");
    for character in value.chars() {
        match character {
            '\0' => quoted.push_str("\\0"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\u{0008}' => quoted.push_str("\\b"),
            '\u{001a}' => quoted.push_str("\\Z"),
            '\\' => quoted.push_str("\\\\"),
            '\'' => quoted.push_str("''"),
            '"' => quoted.push_str("\\\""),
            other => quoted.push(other),
        }
    }
    quoted.push('\'');
    quoted
}

fn render_column_encoding(character_set: Option<&str>, collation: Option<&str>) -> String {
    match (character_set, collation) {
        (Some(character_set), Some(collation)) => {
            format!(" CHARACTER SET {character_set} COLLATE {collation}")
        }
        _ => String::new(),
    }
}

pub fn parse_fixture_create_table(source_sql: &str) -> Result<ParsedCreateTableAst, String> {
    parse_fixture_create_table_with_mode(source_sql, SourceSqlMode(None))
}

pub fn parse_fixture_create_table_with_mode(
    source_sql: &str,
    mode: SourceSqlMode,
) -> Result<ParsedCreateTableAst, String> {
    let ast = observed_create::parse_with_mode(source_sql, mode)?;
    // This legacy event still has a separate exact-hash evidence contract.
    if ast.name.eq_ignore_ascii_case("assistant_reply_reports") {
        return Err("assistant_reply_reports CREATE is admitted only by exact hash".into());
    }
    Ok(ast)
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
    if ast.collation.is_none()
        && ast
            .character_set
            .as_ref()
            .is_some_and(|charset| !charset.eq_ignore_ascii_case(&defaults.character_set))
    {
        return Err("CREATE charset differs from resolved defaults".to_string());
    }
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
    definitions.extend(
        ast.check_constraints
            .iter()
            .map(check_constraint::render_create_check_constraint),
    );
    definitions.extend(ast.foreign_keys.iter().map(render_create_foreign_key));
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
    let on_update = if column.on_update_current_timestamp {
        format!(" ON UPDATE {}", current_timestamp_for(&column.column_type))
    } else {
        String::new()
    };
    let column_type = match column.column_type.strip_prefix("enum(") {
        Some(labels) => format!("ENUM({labels}"),
        None => column.column_type.to_ascii_uppercase(),
    };
    let encoding =
        render_column_encoding(column.character_set.as_deref(), column.collation.as_deref());
    let comment = if column.comment.is_empty() {
        String::new()
    } else {
        format!(" COMMENT {}", quote_string_literal(&column.comment))
    };
    format!(
        "{} {column_type}{encoding} {nullability}{default}{on_update}{auto_increment}{comment}",
        quote_identifier(&column.name),
    )
}

fn render_create_foreign_key(key: &super::model::ParsedCreateForeignKeyAst) -> String {
    let columns = key
        .columns
        .iter()
        .map(|name| quote_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");
    let referenced = key
        .referenced_columns
        .iter()
        .map(|name| quote_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CONSTRAINT {} FOREIGN KEY ({columns}) REFERENCES {} ({referenced}) ON DELETE {}",
        quote_identifier(&key.name),
        quote_identifier(&key.referenced_table),
        key.delete_rule,
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
    if let Some(character_set) = ast.character_set.as_deref() {
        return defaults.map_or_else(
            || format!(" DEFAULT CHARACTER SET={character_set}"),
            |defaults| {
                format!(
                    " DEFAULT CHARACTER SET={character_set} COLLATE={}",
                    defaults.collation
                )
            },
        );
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

const ASSISTANT_REPLY_REPORTS_CREATE_HASH: &str =
    "f1decd8ad26d7f01f0cea5f3f78fca2ecaa9c97fa40c6a3a1c951e278560cf10";

pub fn supports_assistant_reply_reports_create(source_sql: &str) -> bool {
    let digest = format!("{:x}", Sha256::digest(source_sql.trim_end().as_bytes()));
    digest == ASSISTANT_REPLY_REPORTS_CREATE_HASH
}

pub fn transform_assistant_reply_reports_create(
    source_sql: &str,
) -> Result<DdlTransformation, String> {
    if !supports_assistant_reply_reports_create(source_sql) {
        return Err("unsupported assistant_reply_reports CREATE TABLE statement".to_string());
    }
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: None,
    })
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

pub fn supports_drop_trigger_if_exists(source_sql: &str) -> bool {
    parse_supported_drop_trigger_if_exists(source_sql).is_ok()
}

pub fn transform_drop_trigger_if_exists(
    source_sql: &str,
    target_triggers: &BTreeSet<String>,
) -> Result<DdlTransformation, String> {
    let source_name = parse_supported_drop_trigger_if_exists(source_sql)?;
    let target_name = target_triggers
        .iter()
        .find(|name| name.eq_ignore_ascii_case(&source_name));
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql: target_name.map(|name| format!("DROP TRIGGER {}", quote_identifier(name))),
    })
}

fn parse_supported_drop_trigger_if_exists(source_sql: &str) -> Result<String, String> {
    if ddl_contains_comments(source_sql) {
        return Err("DROP TRIGGER comments are not supported".to_string());
    }
    if source_sql.contains('"') {
        return Err("DROP TRIGGER double-quoted identifiers are not supported".to_string());
    }
    let (tokens, quoted_flags) = tokenize_ddl_with_quoted_flags(source_sql)?;
    require_keyword(&tokens, 0, "DROP")?;
    require_keyword(&tokens, 1, "TRIGGER")?;
    require_keyword(&tokens, 2, "IF")?;
    require_keyword(&tokens, 3, "EXISTS")?;
    parse_exact_drop_trigger_name(&tokens, &quoted_flags)
}

fn parse_exact_drop_trigger_name(
    tokens: &[String],
    quoted_flags: &[bool],
) -> Result<String, String> {
    let name_index = 4;
    if quoted_flags.get(name_index).copied().unwrap_or(false) {
        return Err("quoted DROP TRIGGER identifiers are not supported".to_string());
    }
    let name = require_identifier(tokens, name_index, "DROP TRIGGER name")?;
    if name != "prevent_deactivating_cloned_archives" {
        return Err("DROP TRIGGER name is not admitted".to_string());
    }
    let trailing_index = name_index + 1;
    let expected_len = if tokens.get(trailing_index).map(String::as_str) == Some(";") {
        trailing_index + 1
    } else {
        trailing_index
    };
    if tokens.len() != expected_len {
        return Err("DROP TRIGGER requires one exact unqualified trigger name".to_string());
    }
    Ok(name)
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
        ast.algorithm.is_none()
            && ast.lock.is_none()
            && ast
                .clauses
                .iter()
                .all(|clause| matches!(clause, ParsedAlterClause::DropColumn(column) if column.if_exists))
    })
}

pub fn transform_drop_columns_if_exists(
    source_sql: &str,
    target_columns: &BTreeSet<String>,
) -> Result<DdlTransformation, String> {
    let ast = parse_production_alter_table_ast(source_sql)?;
    if ast.algorithm.is_some()
        || ast.lock.is_some()
        || !ast.clauses.iter().all(
            |clause| matches!(clause, ParsedAlterClause::DropColumn(column) if column.if_exists),
        )
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
    rename_columns_if_exists_sql(source_sql)
        .ok()
        .and_then(|(_, statement_sql)| tokenize_ddl(statement_sql).ok())
        .and_then(|tokens| parse_rename_columns_if_exists(&tokens).ok())
        .is_some()
}

pub fn transform_rename_columns_if_exists(
    source_sql: &str,
    target_columns: &BTreeSet<String>,
) -> Result<DdlTransformation, String> {
    let (leading_comment, statement_sql) = rename_columns_if_exists_sql(source_sql)?;
    let tokens = tokenize_ddl(statement_sql)?;
    let (table, clauses) = parse_rename_columns_if_exists(&tokens)?;
    let executable_clauses = select_executable_renames(&table, clauses, target_columns)?;
    let target_sql =
        emit_rename_columns(&table, &executable_clauses).map(|sql| match leading_comment {
            Some(comment) => format!("{comment}{sql}"),
            None => sql,
        });
    Ok(DdlTransformation {
        version: DDL_TRANSFORMATION_VERSION,
        target_sql,
    })
}

fn rename_columns_if_exists_sql(source_sql: &str) -> Result<(Option<&str>, &str), String> {
    let (leading_comment, statement_sql) = split_one_leading_mysql_line_comment(source_sql);
    if ddl_contains_comments(statement_sql) {
        return Err("RENAME COLUMN IF EXISTS comments are not supported".to_string());
    }
    Ok((leading_comment, statement_sql))
}

pub fn parse_production_alter_table_ast(source_sql: &str) -> Result<ParsedAlterTableAst, String> {
    parse_production_alter_table_ast_with_mode(source_sql, SourceSqlMode(None))
}

pub fn parse_production_alter_table_ast_with_mode(
    source_sql: &str,
    mode: SourceSqlMode,
) -> Result<ParsedAlterTableAst, String> {
    let (_, statement_sql) = split_one_leading_mysql_line_comment(source_sql);
    let ordinary_comments = ddl_contains_comments(statement_sql);
    let leading_comments_only =
        !ddl_contains_comments(strip_leading_ordinary_ddl_comments(statement_sql)?);
    let stripped;
    let source_sql = if ordinary_comments {
        stripped = observed_create::remove_ordinary_comments_with_mode(source_sql, mode)?;
        stripped.as_str()
    } else {
        statement_sql
    };
    let no_escapes = mode.0.is_some_and(|bits| bits & (1 << 20) != 0);
    let (tokens, quoted_flags) = tokenize_ddl_with_quoted_flags_mode(source_sql, no_escapes)?;
    require_keyword(&tokens, 0, "ALTER")?;
    require_keyword(&tokens, 1, "TABLE")?;
    let table = require_identifier(&tokens, 2, "ALTER TABLE name")?;
    let literals = extract_single_quoted_literals_with_mode(source_sql, mode)?;
    let (clauses, algorithm, lock) =
        parse_production_alter_body(&tokens, &quoted_flags, &table, literals)?;
    if ordinary_comments
        && (algorithm.is_some()
            || lock.is_some()
            || !clauses.iter().all(|clause| match clause {
                ParsedAlterClause::ModifyColumn(_)
                | ParsedAlterClause::AddColumn(_)
                | ParsedAlterClause::RenameColumn { .. }
                | ParsedAlterClause::DropIndex(_) => true,
                ParsedAlterClause::DropColumn(_) => leading_comments_only,
                _ => false,
            }))
    {
        return Err(
            "ordinary ALTER comments require modeled ADD COLUMN or MODIFY clauses, or leading-only comments for DROP COLUMN IF EXISTS".to_string(),
        );
    }
    Ok(ParsedAlterTableAst {
        table,
        clauses,
        algorithm,
        lock,
    })
}

fn parse_production_alter_body(
    tokens: &[String],
    quoted_flags: &[bool],
    table: &str,
    literals: Vec<String>,
) -> Result<ParsedAlterBody, String> {
    let mut literals = literals.into_iter();
    let mut clauses = Vec::new();
    let mut index = 3;
    while index < tokens.len() {
        if let Some((algorithm, lock)) = parse_alter_options(tokens, index)? {
            return Ok((require_alter_clauses(clauses)?, algorithm, lock));
        }
        let (clause, next_index) =
            parse_production_alter_clause(tokens, quoted_flags, index, table, &mut literals)?;
        clauses.push(clause);
        index = next_index;
        if index == tokens.len() {
            return Ok((clauses, None, None));
        }
        require_keyword(tokens, index, ",")?;
        index += 1;
    }
    Ok((require_alter_clauses(clauses)?, None, None))
}

fn parse_alter_options(
    tokens: &[String],
    index: usize,
) -> Result<Option<ParsedAlterOptions>, String> {
    if !tokens[index].eq_ignore_ascii_case("ALGORITHM") {
        return Ok(None);
    }
    require_keyword(tokens, index + 1, "=")?;
    let algorithm = match tokens.get(index + 2).map(String::as_str) {
        Some(value) if value.eq_ignore_ascii_case("INSTANT") => ParsedAlterAlgorithm::Instant,
        Some(value) if value.eq_ignore_ascii_case("INPLACE") => ParsedAlterAlgorithm::Inplace,
        actual => return Err(format!("unsupported ALTER TABLE algorithm {actual:?}")),
    };
    let next_index = index + 3;
    if next_index == tokens.len() {
        return Ok(Some((Some(algorithm), None)));
    }
    require_keyword(tokens, next_index, ",")?;
    require_keyword(tokens, next_index + 1, "LOCK")?;
    require_keyword(tokens, next_index + 2, "=")?;
    require_keyword(tokens, next_index + 3, "NONE")?;
    if next_index + 4 != tokens.len() {
        return Err("ALTER TABLE options must be final".to_string());
    }
    Ok(Some((Some(algorithm), Some(ParsedAlterLock::None))))
}

fn require_alter_clauses(
    clauses: Vec<ParsedAlterClause>,
) -> Result<Vec<ParsedAlterClause>, String> {
    if clauses.is_empty() {
        return Err("ALTER TABLE has no supported clauses".to_string());
    }
    Ok(clauses)
}

fn parse_production_alter_clause(
    tokens: &[String],
    quoted_flags: &[bool],
    index: usize,
    table: &str,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    require_unquoted_token(quoted_flags, index, "ALTER clause")?;
    match tokens
        .get(index)
        .map(|token| token.to_ascii_uppercase())
        .as_deref()
    {
        Some("ADD") => parse_production_add_clause(tokens, quoted_flags, index, table, literals),
        Some("DROP") => parse_drop_alter_clause(tokens, index),
        Some("MODIFY") => parse_modify_column_clause(tokens, quoted_flags, index, literals),
        Some("CHANGE") => parse_change_column_clause(tokens, quoted_flags, index, literals),
        Some("ALTER") => parse_alter_column_default_clause(tokens, quoted_flags, index, literals),
        Some("RENAME") => parse_rename_column_clause(tokens, quoted_flags, index),
        actual => Err(format!(
            "unsupported production ALTER TABLE clause {actual:?}"
        )),
    }
}

fn parse_modify_column_clause(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    if tokens_match(tokens, index + 1, "COLUMN") {
        require_unquoted_token(quoted, index + 1, "MODIFY COLUMN")?;
    }
    let (ParsedAlterClause::AddColumn(column), next) =
        parse_add_column_clause(tokens, quoted, index, literals)?
    else {
        unreachable!("column parser returns a column");
    };
    if column.if_not_exists || column.generated.is_some() || column.data_type == "json" {
        return Err("MODIFY requires an ordinary unguarded non-JSON column".into());
    }
    Ok((ParsedAlterClause::ModifyColumn(column), next))
}

fn parse_change_column_clause(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    let old_index = index + 1 + usize::from(tokens_match(tokens, index + 1, "COLUMN"));
    if old_index == index + 2 {
        require_unquoted_token(quoted, index + 1, "CHANGE COLUMN")?;
    }
    let old_name = require_identifier(tokens, old_index, "changed column")?;
    let (ParsedAlterClause::AddColumn(column), next) =
        parse_column_definition_clause(tokens, quoted, old_index + 1, literals)?
    else {
        unreachable!("column parser returns a column");
    };
    if column.if_not_exists || column.generated.is_some() || column.data_type == "json" {
        return Err("CHANGE requires an ordinary unguarded non-JSON column".into());
    }
    Ok((ParsedAlterClause::ChangeColumn { old_name, column }, next))
}

fn parse_alter_column_default_clause(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    require_keyword(tokens, index + 1, "COLUMN")?;
    require_unquoted_token(quoted, index + 1, "ALTER COLUMN")?;
    let name = require_identifier(tokens, index + 2, "altered column")?;
    let action = index + 3;
    let (default, next) = parse_alter_default_action(tokens, quoted, action, literals)?;
    Ok((
        ParsedAlterClause::AlterColumnDefault { name, default },
        next,
    ))
}

fn parse_alter_default_action(
    tokens: &[String],
    quoted: &[bool],
    action: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(Option<ParsedColumnDefault>, usize), String> {
    if tokens_match(tokens, action, "DROP") {
        require_unquoted_token(quoted, action, "DROP DEFAULT")?;
        require_keyword(tokens, action + 1, "DEFAULT")?;
        require_unquoted_token(quoted, action + 1, "DROP DEFAULT")?;
        return Ok((None, action + 2));
    }
    require_keyword(tokens, action, "SET")?;
    require_unquoted_token(quoted, action, "SET DEFAULT")?;
    require_keyword(tokens, action + 1, "DEFAULT")?;
    require_unquoted_token(quoted, action + 1, "SET DEFAULT")?;
    let (value, next) = parse_alter_default_literal(tokens, quoted, action + 2, literals)?;
    Ok((Some(value), next))
}

fn parse_alter_default_literal(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedColumnDefault, usize), String> {
    require_unquoted_token(quoted, index, "DEFAULT literal")?;
    if tokens_match(tokens, index, "NULL") {
        return Ok((ParsedColumnDefault::Null, index + 1));
    }
    if tokens_match(tokens, index, "<string>") {
        let value = parse_string_default_literal(literals)?;
        return Ok((ParsedColumnDefault::String(value), index + 1));
    }
    let (value, next) = numeric_default_literal(tokens, quoted, index)?;
    let numeric = value.chars().any(|digit| digit.is_ascii_digit())
        && value
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, '-' | '+' | '.'));
    if !numeric {
        return Err("DEFAULT must be a numeric or string literal".into());
    }
    Ok((ParsedColumnDefault::Number(value), next))
}

fn parse_rename_column_clause(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
) -> Result<(ParsedAlterClause, usize), String> {
    for (offset, keyword) in [(0, "RENAME"), (1, "COLUMN"), (3, "TO")] {
        require_unquoted_token(quoted, index + offset, "RENAME COLUMN")?;
        require_keyword(tokens, index + offset, keyword)?;
    }
    let old_name = require_identifier(tokens, index + 2, "renamed column")?;
    let new_name = require_identifier(tokens, index + 4, "new column name")?;
    Ok((
        ParsedAlterClause::RenameColumn { old_name, new_name },
        index + 5,
    ))
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
        Some(kind)
            if kind != "KEY" && kind != "INDEX" && kind != "UNIQUE" && kind != "CONSTRAINT" =>
        {
            parse_add_column_clause(tokens, quoted_flags, index, literals)
        }
        Some(kind) if matches!(kind.as_str(), "KEY" | "INDEX") => {
            parse_add_key_clause(tokens, index + 1, table, false)
        }
        Some(kind) if kind == "UNIQUE" => {
            require_keyword(tokens, index + 2, "KEY")?;
            parse_add_key_clause(tokens, index + 2, table, true)
        }
        Some(kind) if kind == "CONSTRAINT" && tokens_match(tokens, index + 3, "FOREIGN") => {
            parse_add_foreign_key_clause(tokens, quoted_flags, index)
        }
        Some(kind) if kind == "CONSTRAINT" => {
            let (constraint, next_index) =
                check_constraint::parse_named_check(tokens, quoted_flags, index + 1, literals)?;
            Ok((ParsedAlterClause::AddCheck(constraint), next_index))
        }
        actual => Err(format!(
            "unsupported production ALTER TABLE clause {actual:?}"
        )),
    }
}

/// Parses `ADD CONSTRAINT <name> FOREIGN KEY (<column>) REFERENCES <table> (<column>)
/// ON DELETE CASCADE|RESTRICT`; update stays the implicit RESTRICT.
fn parse_add_foreign_key_clause(
    tokens: &[String],
    quoted_flags: &[bool],
    index: usize,
) -> Result<(ParsedAlterClause, usize), String> {
    let name = require_identifier(tokens, index + 2, "foreign key name")?;
    let child = require_identifier(tokens, index + 6, "foreign key column")?;
    let parent_table = require_identifier(tokens, index + 9, "referenced table")?;
    let parent = require_identifier(tokens, index + 11, "referenced column")?;
    let keywords = [
        (1, "CONSTRAINT"),
        (3, "FOREIGN"),
        (4, "KEY"),
        (5, "("),
        (7, ")"),
        (8, "REFERENCES"),
        (10, "("),
        (12, ")"),
        (13, "ON"),
        (14, "DELETE"),
    ];
    for (offset, keyword) in keywords {
        require_unquoted_token(quoted_flags, index + offset, keyword)?;
        require_keyword(tokens, index + offset, keyword)?;
    }
    let delete_rule = ["CASCADE", "RESTRICT"]
        .into_iter()
        .find(|rule| tokens_match(tokens, index + 15, rule))
        .ok_or_else(|| "ADD FOREIGN KEY requires ON DELETE CASCADE or RESTRICT".to_string())?;
    require_unquoted_token(quoted_flags, index + 15, "ON DELETE action")?;
    Ok((
        ParsedAlterClause::AddForeignKey(super::model::ParsedCreateForeignKeyAst {
            name,
            columns: vec![child],
            referenced_table: parent_table,
            referenced_columns: vec![parent],
            delete_rule: delete_rule.to_string(),
        }),
        index + 16,
    ))
}

struct ParsedColumnOptions {
    nullable: bool,
    default_value: Option<String>,
    comment: String,
    after: Option<String>,
    first: bool,
    next_index: usize,
}

fn parse_add_column_clause(
    tokens: &[String],
    quoted_flags: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    let name_index = index + 1 + usize::from(tokens_match(tokens, index + 1, "COLUMN"));
    if name_index == index + 2 {
        require_unquoted_token(quoted_flags, index + 1, "ADD COLUMN")?;
    }
    parse_column_definition_clause(tokens, quoted_flags, name_index, literals)
}

fn parse_column_definition_clause(
    tokens: &[String],
    quoted_flags: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedAlterClause, usize), String> {
    let mut name_index = index;
    let if_not_exists = tokens
        .get(name_index)
        .is_some_and(|token| token.eq_ignore_ascii_case("IF"));
    if if_not_exists {
        require_keyword(tokens, name_index + 1, "NOT")?;
        require_keyword(tokens, name_index + 2, "EXISTS")?;
        name_index += 3;
    }
    let name = require_identifier(tokens, name_index, "added column")?;
    let (column_type, data_type, encoding_start) =
        parse_observed_column_type(tokens, quoted_flags, name_index + 1)?;
    let (character_set, collation, generation_start) =
        parse_column_encoding(tokens, quoted_flags, encoding_start, &data_type)?;
    let (generated, options_start) = parse_optional_stored_generation(
        tokens,
        quoted_flags,
        generation_start,
        literals,
        &column_type,
    )?;
    let options =
        parse_observed_column_options(tokens, quoted_flags, options_start, literals, &column_type)?;
    if generated.is_some()
        && tokens[options_start..options.next_index]
            .iter()
            .any(|token| {
                ["NOT", "NULL", "DEFAULT"]
                    .iter()
                    .any(|keyword| token.eq_ignore_ascii_case(keyword))
            })
    {
        return Err("generated ADD COLUMN admits only COMMENT and AFTER options".into());
    }
    let (character_set, collation) = if data_type == "json" {
        (Some("utf8mb4".into()), Some("utf8mb4_bin".into()))
    } else {
        (character_set, collation)
    };
    Ok((
        ParsedAlterClause::AddColumn(ParsedAddColumnAst {
            name,
            if_not_exists,
            column_type,
            data_type,
            nullable: options.nullable,
            default_value: options.default_value,
            comment: options.comment,
            after: options.after,
            first: options.first,
            character_set,
            collation,
            generated,
        }),
        options.next_index,
    ))
}

fn parse_optional_stored_generation(
    tokens: &[String],
    quoted_flags: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
    column_type: &str,
) -> Result<(Option<ParsedStoredIfExpression>, usize), String> {
    if !token_is_one_of(tokens, index, &["AS", "GENERATED"]) {
        return Ok((None, index));
    }
    if column_type != "tinyint unsigned" {
        return Err(format!(
            "generated ADD COLUMN is unsupported for {column_type}"
        ));
    }
    let (expression, next) =
        generated_column::parse_stored_generation(tokens, quoted_flags, index, literals)?;
    Ok((Some(expression), next))
}

/// Parses an optional `CHARACTER SET <charset> COLLATE <collation>` pair after a character type.
fn parse_column_encoding(
    tokens: &[String],
    quoted_flags: &[bool],
    index: usize,
    data_type: &str,
) -> Result<(Option<String>, Option<String>, usize), String> {
    if !tokens_match(tokens, index, "CHARACTER") {
        return Ok((None, None, index));
    }
    if !matches!(data_type, "char" | "varchar") && !is_text_type(data_type) {
        return Err(format!("CHARACTER SET is unsupported for {data_type}"));
    }
    for (offset, keyword) in [(0, "CHARACTER"), (1, "SET"), (3, "COLLATE")] {
        require_unquoted_token(quoted_flags, index + offset, keyword)?;
        require_keyword(tokens, index + offset, keyword)?;
    }
    let character_set = require_identifier(tokens, index + 2, "column character set")?;
    let collation = require_identifier(tokens, index + 4, "column collation")?;
    if !collation.starts_with(&format!("{character_set}_")) {
        return Err(format!(
            "column collation {collation} does not belong to {character_set}"
        ));
    }
    Ok((Some(character_set), Some(collation), index + 5))
}

fn parse_add_key_clause(
    tokens: &[String],
    key_index: usize,
    table: &str,
    unique: bool,
) -> Result<(ParsedAlterClause, usize), String> {
    let mut name_index = key_index + 1;
    let if_not_exists = !unique && tokens_match(tokens, name_index, "IF");
    if if_not_exists {
        require_keyword(tokens, name_index + 1, "NOT")?;
        require_keyword(tokens, name_index + 2, "EXISTS")?;
        name_index += 3;
    }
    let name = require_identifier(tokens, name_index, "added key name")?;
    require_keyword(tokens, name_index + 1, "(")?;
    let mut key_parts = Vec::new();
    let mut column_index = name_index + 2;
    loop {
        let column = require_identifier(tokens, column_index, "added key column")?;
        let (order, next_index) = parse_key_part_order(tokens, column_index + 1);
        column_index = next_index;
        key_parts.push(ParsedIndexKeyPart {
            column,
            prefix_length: None,
            order: order.to_string(),
            collation: Some(if order == "DESC" { "D" } else { "A" }.to_string()),
        });
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
                return Ok((
                    ParsedAlterClause::AddKey {
                        index: ast,
                        if_not_exists,
                    },
                    column_index + 1,
                ));
            }
            actual => {
                return Err(format!(
                    "expected comma or closing parenthesis in ADD KEY, found {actual:?}"
                ));
            }
        }
    }
}

fn parse_key_part_order(tokens: &[String], index: usize) -> (&'static str, usize) {
    match tokens.get(index).map(String::as_str) {
        Some(value) if value.eq_ignore_ascii_case("ASC") => ("ASC", index + 1),
        Some(value) if value.eq_ignore_ascii_case("DESC") => ("DESC", index + 1),
        _ => ("ASC", index),
    }
}

fn parse_observed_column_type(
    tokens: &[String],
    quoted_flags: &[bool],
    index: usize,
) -> Result<(String, String, usize), String> {
    basic_types::parse_column_type(tokens, quoted_flags, index)
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
    quoted: &[bool],
    mut index: usize,
    literals: &mut impl Iterator<Item = String>,
    column_type: &str,
) -> Result<ParsedColumnOptions, String> {
    let data_type = column_type.split(['(', ' ']).next().unwrap_or(column_type);
    let mut options = ParsedColumnOptions {
        nullable: true,
        default_value: None,
        comment: String::new(),
        after: None,
        first: false,
        next_index: index,
    };
    let mut seen = BTreeSet::new();
    let mut explicit_null_default = false;
    while index < tokens.len() && tokens[index] != "," {
        require_unquoted_token(quoted, index, "column option")?;
        let option = tokens[index].to_ascii_uppercase();
        let key = if option == "NOT" { "NULL" } else { &option };
        if !seen.insert(key.to_string()) {
            return Err(format!("duplicate column option {key}"));
        }
        if option == "DEFAULT" {
            explicit_null_default = tokens
                .get(index + 1)
                .is_some_and(|value| value.eq_ignore_ascii_case("NULL"));
        }
        index = parse_observed_column_option(
            tokens,
            quoted,
            index,
            literals,
            column_type,
            &mut options,
        )?;
    }
    if !options.nullable && explicit_null_default {
        return Err("NOT NULL column cannot have DEFAULT NULL".into());
    }
    if data_type == "json" && options.default_value.is_some() {
        return Err("JSON literal defaults are not modeled".into());
    }
    options.next_index = index;
    Ok(options)
}

fn parse_observed_column_option(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
    column_type: &str,
    options: &mut ParsedColumnOptions,
) -> Result<usize, String> {
    let option = tokens[index].to_ascii_uppercase();
    match option.as_str() {
        "NULL" => {
            options.nullable = true;
            Ok(index + 1)
        }
        "NOT" => {
            require_unquoted_token(quoted, index + 1, "NOT NULL")?;
            require_keyword(tokens, index + 1, "NULL")?;
            options.nullable = false;
            Ok(index + 2)
        }
        "DEFAULT" => {
            require_unquoted_token(quoted, index + 1, "DEFAULT value")?;
            let (value, next) =
                parse_basic_default(tokens, quoted, index + 1, literals, column_type)?;
            options.default_value = value;
            Ok(next)
        }
        "COMMENT" => {
            require_keyword(tokens, index + 1, "<string>")?;
            options.comment = literals.next().ok_or("missing COMMENT literal")?;
            Ok(index + 2)
        }
        "AFTER" => {
            if options.first {
                return Err("FIRST and AFTER are mutually exclusive".into());
            }
            options.after = Some(require_identifier(tokens, index + 1, "AFTER column")?);
            Ok(index + 2)
        }
        "FIRST" => {
            if options.after.is_some() {
                return Err("FIRST and AFTER are mutually exclusive".into());
            }
            options.first = true;
            Ok(index + 1)
        }
        _ => Err(format!("unsupported column option {option}")),
    }
}

fn parse_basic_default(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
    column_type: &str,
) -> Result<(Option<String>, usize), String> {
    let value = tokens.get(index).ok_or("missing DEFAULT value")?;
    if value.eq_ignore_ascii_case("NULL") {
        return Ok((None, index + 1));
    }
    let kind = column_type.split(['(', ' ']).next().unwrap_or(column_type);
    if is_text_type(kind) || matches!(kind, "char" | "varchar") {
        require_keyword(tokens, index, "<string>")?;
        return Ok((Some(parse_string_default_literal(literals)?), index + 1));
    }
    let (literal, next) = numeric_default_literal(tokens, quoted, index)?;
    Ok((
        Some(basic_types::normalize_numeric_default(
            column_type,
            &literal,
        )?),
        next,
    ))
}

fn numeric_default_literal(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
) -> Result<(String, usize), String> {
    require_unquoted_token(quoted, index, "numeric default")?;
    let mut next = index;
    let sign = match tokens.get(next).map(String::as_str) {
        Some("-") => {
            next += 1;
            "-"
        }
        Some("+") => {
            next += 1;
            "+"
        }
        _ => "",
    };
    require_unquoted_token(quoted, next, "numeric digits")?;
    let integer = tokens.get(next).ok_or("missing numeric default")?;
    next += 1;
    if tokens.get(next).map(String::as_str) != Some(".") {
        return Ok((format!("{sign}{integer}"), next));
    }
    require_unquoted_token(quoted, next, "decimal point")?;
    require_unquoted_token(quoted, next + 1, "decimal fraction")?;
    let fraction = tokens.get(next + 1).ok_or("missing decimal fraction")?;
    Ok((format!("{sign}{integer}.{fraction}"), next + 2))
}

/// The literal a string `DEFAULT '...'` carries; TEXT columns receive it as a MySQL 8
/// expression default, CHAR/VARCHAR columns as a quoted literal.
fn parse_string_default_literal(
    literals: &mut impl Iterator<Item = String>,
) -> Result<String, String> {
    let value = literals
        .next()
        .ok_or_else(|| "string DEFAULT literal is missing".to_string())?;
    if !value.is_ascii() {
        return Err(format!("unmodeled string default literal {value:?}"));
    }
    Ok(value)
}

#[cfg(test)]
mod sql_mode_literal_tests {
    use super::*;
    use crate::live::query_charset_context::SourceSqlMode;

    #[test]
    fn unescaped_unicode_defaults_remain_unmodeled_but_unicode_comments_are_accepted() {
        let mode = SourceSqlMode(Some(0));
        let create =
            "CREATE TABLE t (id INT PRIMARY KEY, label VARCHAR(40) DEFAULT 'é') ENGINE=InnoDB";
        assert!(parse_fixture_create_table_with_mode(create, mode).is_err());
        let alter = "ALTER TABLE t ADD COLUMN label VARCHAR(40) DEFAULT 'é'";
        assert!(parse_production_alter_table_ast_with_mode(alter, mode).is_err());
        let commented = "/* é */ ALTER TABLE t ADD COLUMN label VARCHAR(40) DEFAULT 'A'";
        assert!(parse_production_alter_table_ast_with_mode(commented, mode).is_ok());
    }

    #[test]
    fn rejects_mixed_non_ascii_and_escaped_source_literal_without_charset_proof() {
        assert!(
            extract_single_quoted_literals_with_mode(r"DEFAULT 'é\n'", SourceSqlMode(Some(0)))
                .is_err()
        );
        assert!(
            extract_single_quoted_literals_with_mode("DEFAULT 'é'", SourceSqlMode(Some(0))).is_ok()
        );
    }

    #[test]
    fn decodes_mysql_escapes_and_preserves_pattern_escapes() {
        let sql =
            r#"ALTER TABLE t ADD COLUMN c VARCHAR(80) DEFAULT 'a\0\n\r\t\b\Z\\\'\"\%\_\q''z'"#;
        assert_eq!(
            extract_single_quoted_literals_with_mode(sql, SourceSqlMode(Some(0))).unwrap(),
            vec!["a\0\n\r\t\u{0008}\u{001a}\\'\"\\%\\_q'z"]
        );
    }

    #[test]
    fn create_ast_uses_source_mode_for_string_default() {
        let sql =
            r"CREATE TABLE t (id INT PRIMARY KEY, label VARCHAR(40) DEFAULT 'a\n\q') ENGINE=InnoDB";
        let escaped = parse_fixture_create_table_with_mode(sql, SourceSqlMode(Some(0))).unwrap();
        let unescaped =
            parse_fixture_create_table_with_mode(sql, SourceSqlMode(Some(1 << 20))).unwrap();
        assert_eq!(escaped.columns[1].default_sql.as_deref(), Some("'a\\nq'"));
        assert_eq!(
            unescaped.columns[1].default_sql.as_deref(),
            Some("'a\\\\n\\\\q'")
        );
        assert!(parse_fixture_create_table(sql).is_err());
    }

    #[test]
    fn rendered_literal_survives_target_backslash_mode() {
        assert_eq!(
            quote_string_literal("\0\n\r\t\u{0008}\u{001a}\\'\""),
            r#"'\0\n\r\t\b\Z\\''\"'"#
        );
    }

    #[test]
    fn no_backslash_mode_preserves_quoted_default_through_ordinary_comments() {
        let sql = r"/* note */ ALTER TABLE t ADD COLUMN c VARCHAR(40) DEFAULT 'a\''b'";
        let ast =
            parse_production_alter_table_ast_with_mode(sql, SourceSqlMode(Some(1 << 20))).unwrap();
        let ParsedAlterClause::AddColumn(column) = &ast.clauses[0] else {
            panic!("expected ADD");
        };
        assert_eq!(column.default_value.as_deref(), Some("a\\'b"));
    }

    #[test]
    fn no_backslash_mode_handles_slash_before_doubled_quote() {
        let sql = r"ALTER TABLE t ADD COLUMN c VARCHAR(40) DEFAULT 'a\''b'";
        let ast =
            parse_production_alter_table_ast_with_mode(sql, SourceSqlMode(Some(1 << 20))).unwrap();
        let ParsedAlterClause::AddColumn(column) = &ast.clauses[0] else {
            panic!("expected ADD");
        };
        assert_eq!(column.default_value.as_deref(), Some("a\\'b"));
        let operation =
            super::super::parser::parse_ddl_operation_with_mode(sql, SourceSqlMode(Some(1 << 20)))
                .unwrap();
        assert!(operation.alter_table_ast.is_some());
    }

    #[test]
    fn alter_ast_decodes_source_mode_before_normalizing_default() {
        let sql = r"ALTER TABLE t ADD COLUMN c VARCHAR(40) DEFAULT 'a\n\q'";
        let escaped =
            parse_production_alter_table_ast_with_mode(sql, SourceSqlMode(Some(0))).unwrap();
        let unescaped =
            parse_production_alter_table_ast_with_mode(sql, SourceSqlMode(Some(1 << 20))).unwrap();
        let default = |ast: ParsedAlterTableAst| match ast.clauses.into_iter().next().unwrap() {
            ParsedAlterClause::AddColumn(column) => column.default_value.unwrap(),
            other => panic!("unexpected clause: {other:?}"),
        };
        assert_eq!(default(escaped), "a\nq");
        assert_eq!(default(unescaped), r"a\n\q");
        assert!(parse_production_alter_table_ast(sql).is_err());
    }

    #[test]
    fn no_backslash_mode_preserves_slashes_and_doubled_quotes() {
        let sql = r"ALTER TABLE t ADD COLUMN c VARCHAR(40) DEFAULT 'a\n\q b''c'";
        assert_eq!(
            extract_single_quoted_literals_with_mode(sql, SourceSqlMode(Some(1 << 20))).unwrap(),
            vec![r"a\n\q b'c"]
        );
        assert!(extract_single_quoted_literals_with_mode(sql, SourceSqlMode(None)).is_err());
    }
}

pub(crate) fn extract_single_quoted_literals_with_mode(
    source_sql: &str,
    mode: crate::live::query_charset_context::SourceSqlMode,
) -> Result<Vec<String>, String> {
    let characters = source_sql.chars().collect::<Vec<_>>();
    let mut literals = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        if characters[index] != '\'' {
            index += 1;
            continue;
        }
        let mut literal = String::new();
        let mut has_source_escape = false;
        let mut has_non_ascii = false;
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
                has_source_escape = true;
                let no_escapes = mode.no_backslash_escapes()?;
                if no_escapes {
                    literal.push('\\');
                    index += 1;
                    continue;
                }
                let escaped = *characters
                    .get(index + 1)
                    .ok_or_else(|| "unterminated DDL string escape".to_string())?;
                match escaped {
                    '0' => literal.push('\0'),
                    'n' => literal.push('\n'),
                    'r' => literal.push('\r'),
                    't' => literal.push('\t'),
                    'b' => literal.push('\u{0008}'),
                    'Z' => literal.push('\u{001a}'),
                    '\\' | '\'' | '"' => literal.push(escaped),
                    '%' | '_' => {
                        literal.push('\\');
                        literal.push(escaped);
                    }
                    other => literal.push(other),
                }
                index += 2;
                continue;
            }
            has_non_ascii |= !character.is_ascii();
            literal.push(character);
            index += 1;
        }
        if has_source_escape && has_non_ascii {
            return Err("escaped non-ASCII source literal requires proven client charset".into());
        }
        literals.push(literal);
    }
    Ok(literals)
}

fn parse_drop_alter_clause(
    tokens: &[String],
    index: usize,
) -> Result<(ParsedAlterClause, usize), String> {
    require_keyword(tokens, index, "DROP")?;
    match tokens.get(index + 1).map(String::as_str) {
        Some(kind) if kind.eq_ignore_ascii_case("COLUMN") => {
            parse_drop_column_clause(tokens, index)
        }
        Some(kind) if kind.eq_ignore_ascii_case("INDEX") => parse_drop_index_clause(tokens, index),
        actual => Err(format!("unsupported DROP ALTER TABLE clause {actual:?}")),
    }
}

fn parse_drop_column_clause(
    tokens: &[String],
    index: usize,
) -> Result<(ParsedAlterClause, usize), String> {
    require_keyword(tokens, index + 1, "COLUMN")?;
    let if_exists = tokens
        .get(index + 2)
        .is_some_and(|token| token.eq_ignore_ascii_case("IF"));
    let name_index = if if_exists {
        require_keyword(tokens, index + 3, "EXISTS")?;
        index + 4
    } else {
        index + 2
    };
    let name = require_identifier(tokens, name_index, "dropped column")?;
    Ok((
        ParsedAlterClause::DropColumn(ParsedDropColumnAst { name, if_exists }),
        name_index + 1,
    ))
}

fn parse_drop_index_clause(
    tokens: &[String],
    index: usize,
) -> Result<(ParsedAlterClause, usize), String> {
    require_keyword(tokens, index + 1, "INDEX")?;
    let name = require_identifier(tokens, index + 2, "dropped index")?;
    Ok((
        ParsedAlterClause::DropIndex(ParsedDropIndexAst { name }),
        index + 3,
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
