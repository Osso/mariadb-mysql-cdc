// Bounded CHECK constraint grammar shared by the observed CREATE and production ALTER parsers.
//
// Admitted atoms, joined by AND/OR with explicit grouping:
//   <column> IS NULL
//   JSON_VALID(<column>)
//   OCTET_LENGTH(<column>) <= <integer>
//   <column> IN ('<literal>', ...)      literals limited to [A-Za-z0-9_]
//   <column> = '<literal>'
//   (<column> = '<literal>') = (<column> IS NULL)
// Boolean equality remains SQL equality, not a two-valued rewrite.
use super::super::model::{CheckPredicate, ParsedCheckConstraintAst};
use super::{quote_identifier, quote_string_literal, require_identifier, tokens_match};

/// Parses `CONSTRAINT <name> CHECK ( <predicate> [OR <predicate>]* )` starting at `index`.
/// Returns the constraint and the index following the closing parenthesis.
pub(super) fn parse_named_check(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedCheckConstraintAst, usize), String> {
    require_unquoted_keyword(tokens, quoted, index, "CONSTRAINT")?;
    let name = require_identifier(tokens, index + 1, "CHECK constraint name")?;
    require_unquoted_keyword(tokens, quoted, index + 2, "CHECK")?;
    require_unquoted_keyword(tokens, quoted, index + 3, "(")?;
    let mut position = index + 4;
    let mut disjuncts = Vec::new();
    loop {
        let (predicate, next) = parse_conjunction(tokens, quoted, position, literals)?;
        disjuncts.push(predicate);
        position = next;
        if tokens_match(tokens, position, ")") && !is_quoted(quoted, position) {
            return Ok((ParsedCheckConstraintAst { name, disjuncts }, position + 1));
        }
        require_unquoted_keyword(tokens, quoted, position, "OR")?;
        position += 1;
    }
}

fn parse_conjunction(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(CheckPredicate, usize), String> {
    let (mut left, mut position) = parse_boolean_term(tokens, quoted, index, literals)?;
    while tokens_match(tokens, position, "AND") && !is_quoted(quoted, position) {
        let (right, next) = parse_boolean_term(tokens, quoted, position + 1, literals)?;
        left = CheckPredicate::And {
            left: Box::new(left),
            right: Box::new(right),
        };
        position = next;
    }
    Ok((left, position))
}

fn parse_boolean_term(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(CheckPredicate, usize), String> {
    let (left, next) = parse_group_or_atom(tokens, quoted, index, literals)?;
    if !tokens_match(tokens, next, "=") || is_quoted(quoted, next) {
        return Ok((left, next));
    }
    require_unquoted_keyword(tokens, quoted, index, "(")?;
    require_unquoted_keyword(tokens, quoted, next + 1, "(")?;
    let (right, end) = parse_group_or_atom(tokens, quoted, next + 1, literals)?;
    let modeled = matches!(
        (&left, &right),
        (
            CheckPredicate::StringEquals { .. },
            CheckPredicate::IsNull { .. }
        ) | (
            CheckPredicate::IsNull { .. },
            CheckPredicate::StringEquals { .. }
        )
    );
    if !modeled {
        return Err("unmodeled CHECK boolean equality operands".to_string());
    }
    Ok((
        CheckPredicate::BooleanEquals {
            left: Box::new(left),
            right: Box::new(right),
        },
        end,
    ))
}

fn parse_group_or_atom(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(CheckPredicate, usize), String> {
    if !tokens_match(tokens, index, "(") || is_quoted(quoted, index) {
        return parse_predicate(tokens, quoted, index, literals);
    }
    let (mut left, mut position) = parse_conjunction(tokens, quoted, index + 1, literals)?;
    while tokens_match(tokens, position, "OR") && !is_quoted(quoted, position) {
        let (right, next) = parse_conjunction(tokens, quoted, position + 1, literals)?;
        left = CheckPredicate::Or {
            left: Box::new(left),
            right: Box::new(right),
        };
        position = next;
    }
    require_unquoted_keyword(tokens, quoted, position, ")")?;
    Ok((left, position + 1))
}

fn parse_predicate(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(CheckPredicate, usize), String> {
    if tokens_match(tokens, index, "JSON_VALID") && !is_quoted(quoted, index) {
        let (column, next) = parse_function_column(tokens, quoted, index)?;
        return Ok((CheckPredicate::JsonValid { column }, next));
    }
    if tokens_match(tokens, index, "OCTET_LENGTH") && !is_quoted(quoted, index) {
        let (column, next) = parse_function_column(tokens, quoted, index)?;
        require_unquoted_keyword(tokens, quoted, next, "<")?;
        require_unquoted_keyword(tokens, quoted, next + 1, "=")?;
        let limit = tokens
            .get(next + 2)
            .ok_or_else(|| "OCTET_LENGTH limit is missing".to_string())?;
        let parsed = limit
            .parse::<u64>()
            .map_err(|_| format!("OCTET_LENGTH limit {limit} is not an integer"))?;
        if parsed == 0 || parsed.to_string() != *limit || is_quoted(quoted, next + 2) {
            return Err(format!("OCTET_LENGTH limit {limit} is not canonical"));
        }
        return Ok((
            CheckPredicate::OctetLengthAtMost {
                column,
                limit: parsed,
            },
            next + 3,
        ));
    }
    let column = require_identifier(tokens, index, "CHECK column")?;
    if tokens_match(tokens, index + 1, "IS") && !is_quoted(quoted, index + 1) {
        require_unquoted_keyword(tokens, quoted, index + 2, "NULL")?;
        return Ok((CheckPredicate::IsNull { column }, index + 3));
    }
    if tokens_match(tokens, index + 1, "=") && !is_quoted(quoted, index + 1) {
        let (value, next) = parse_string(tokens, quoted, index + 2, literals)?;
        return Ok((CheckPredicate::StringEquals { column, value }, next));
    }
    require_unquoted_keyword(tokens, quoted, index + 1, "IN")?;
    require_unquoted_keyword(tokens, quoted, index + 2, "(")?;
    let mut values = Vec::new();
    let mut position = index + 3;
    loop {
        let (value, next) = parse_string(tokens, quoted, position, literals)?;
        values.push(value);
        position = next;
        if tokens_match(tokens, position, ")") && !is_quoted(quoted, position) {
            return Ok((CheckPredicate::InStrings { column, values }, position + 1));
        }
        require_unquoted_keyword(tokens, quoted, position, ",")?;
        position += 1;
    }
}

fn parse_string(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(String, usize), String> {
    require_unquoted_keyword(tokens, quoted, index, "<string>")?;
    let value = literals
        .next()
        .ok_or_else(|| "CHECK literal is missing".to_string())?;
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(format!("unmodeled CHECK literal {value:?}"));
    }
    Ok((value, index + 1))
}

fn parse_function_column(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
) -> Result<(String, usize), String> {
    require_unquoted_keyword(tokens, quoted, index + 1, "(")?;
    let (column, next) = parse_function_argument(tokens, quoted, index + 2)?;
    require_unquoted_keyword(tokens, quoted, next, ")")?;
    Ok((column, next + 1))
}

fn parse_function_argument(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
) -> Result<(String, usize), String> {
    let mut position = index;
    while tokens_match(tokens, position, "(") && !is_quoted(quoted, position) {
        position += 1;
    }
    let column = require_identifier(tokens, position, "CHECK function column")?;
    let parentheses = position - index;
    position += 1;
    for _ in 0..parentheses {
        require_unquoted_keyword(tokens, quoted, position, ")")?;
        position += 1;
    }
    Ok((column, position))
}

fn require_unquoted_keyword(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    keyword: &str,
) -> Result<(), String> {
    if tokens_match(tokens, index, keyword) && !is_quoted(quoted, index) {
        return Ok(());
    }
    Err(format!(
        "expected {keyword} in CHECK constraint, found {:?}",
        tokens.get(index)
    ))
}

fn is_quoted(quoted: &[bool], index: usize) -> bool {
    quoted.get(index) == Some(&true)
}

pub(crate) fn referenced_columns(constraint: &ParsedCheckConstraintAst) -> Vec<&str> {
    let mut columns = Vec::new();
    for predicate in &constraint.disjuncts {
        collect_referenced_columns(predicate, &mut columns);
    }
    columns
}

fn collect_referenced_columns<'a>(predicate: &'a CheckPredicate, columns: &mut Vec<&'a str>) {
    match predicate {
        CheckPredicate::IsNull { column }
        | CheckPredicate::JsonValid { column }
        | CheckPredicate::OctetLengthAtMost { column, .. }
        | CheckPredicate::InStrings { column, .. }
        | CheckPredicate::StringEquals { column, .. } => columns.push(column),
        CheckPredicate::And { left, right }
        | CheckPredicate::Or { left, right }
        | CheckPredicate::BooleanEquals { left, right } => {
            collect_referenced_columns(left, columns);
            collect_referenced_columns(right, columns);
        }
    }
}

pub(super) fn render_check_constraint(constraint: &ParsedCheckConstraintAst) -> String {
    let predicates = constraint
        .disjuncts
        .iter()
        .map(render_predicate)
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "CONSTRAINT {} CHECK ({predicates})",
        quote_identifier(&constraint.name)
    )
}

/// MySQL CHECK names are schema-wide, so a JSON alias CHECK (MariaDB names it after its
/// column) renders anonymously and MySQL assigns a table-specific name.
pub(super) fn render_create_check_constraint(constraint: &ParsedCheckConstraintAst) -> String {
    match constraint.disjuncts.as_slice() {
        [CheckPredicate::JsonValid { column }] if *column == constraint.name => {
            format!("CHECK (JSON_VALID({}))", quote_identifier(column))
        }
        _ => render_check_constraint(constraint),
    }
}

fn render_predicate(predicate: &CheckPredicate) -> String {
    match predicate {
        CheckPredicate::And { left, right } => format!(
            "({}) AND ({})",
            render_predicate(left),
            render_predicate(right)
        ),
        CheckPredicate::Or { left, right } => format!(
            "({}) OR ({})",
            render_predicate(left),
            render_predicate(right)
        ),
        CheckPredicate::BooleanEquals { left, right } => format!(
            "({}) = ({})",
            render_predicate(left),
            render_predicate(right)
        ),
        CheckPredicate::StringEquals { column, value } => format!(
            "{} = {}",
            quote_identifier(column),
            quote_string_literal(value)
        ),
        CheckPredicate::IsNull { column } => format!("{} IS NULL", quote_identifier(column)),
        CheckPredicate::JsonValid { column } => format!("JSON_VALID({})", quote_identifier(column)),
        CheckPredicate::OctetLengthAtMost { column, limit } => {
            format!("OCTET_LENGTH({}) <= {limit}", quote_identifier(column))
        }
        CheckPredicate::InStrings { column, values } => format!(
            "{} IN ({})",
            quote_identifier(column),
            values
                .iter()
                .map(|value| quote_string_literal(value))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

pub(crate) fn canonical_check_constraint_value(
    constraint: &ParsedCheckConstraintAst,
) -> serde_json::Value {
    serde_json::json!({
        "name": constraint.name,
        "disjuncts": constraint.disjuncts.iter().map(canonical_predicate).collect::<Vec<_>>(),
    })
}

/// Structural CHECK metadata value, without its schema-wide constraint name.
/// Redundant predicate parentheses do not change the value; operator grouping does.
pub(crate) fn canonical_check_expression(clause: &str) -> Result<serde_json::Value, String> {
    // MySQL CHECK_CLAUSE escapes quote delimiters; modeled literals contain no quotes.
    let clause = clause.replace("\\'", "'");
    if super::ddl_contains_comments(&clause) {
        return Err("comments are unmodeled in CHECK metadata".to_string());
    }
    let sql = format!("CONSTRAINT metadata_check CHECK ({clause})");
    let (tokens, quoted) = super::tokenize_ddl_with_quoted_flags(&sql)?;
    let (tokens, quoted) = normalize_metadata_tokens(&tokens, &quoted)?;
    let mut literals = super::extract_single_quoted_literals_with_mode(
        &clause,
        crate::live::query_charset_context::SourceSqlMode(Some(0)),
    )?
    .into_iter();
    let (constraint, end) = parse_named_check(&tokens, &quoted, 0, &mut literals)?;
    if end != tokens.len() || literals.next().is_some() {
        return Err("unexpected trailing CHECK metadata".to_string());
    }
    let predicate = constraint
        .disjuncts
        .into_iter()
        .reduce(|left, right| CheckPredicate::Or {
            left: Box::new(left),
            right: Box::new(right),
        })
        .ok_or("empty CHECK metadata")?;
    Ok(canonical_predicate(&predicate))
}

fn normalize_metadata_tokens(
    tokens: &[String],
    quoted: &[bool],
) -> Result<(Vec<String>, Vec<bool>), String> {
    let mut retained = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        let quoted_token = is_quoted(quoted, index);
        if tokens_match(tokens, index, "_utf8mb4") && !quoted_token {
            require_unquoted_keyword(tokens, quoted, index + 1, "<string>")?;
            continue;
        }
        // MySQL reports source OCTET_LENGTH as its byte-counting LENGTH alias.
        let normalized = if !quoted_token
            && token.eq_ignore_ascii_case("LENGTH")
            && tokens_match(tokens, index + 1, "(")
        {
            "OCTET_LENGTH".to_string()
        } else {
            token.clone()
        };
        retained.push((normalized, quoted_token));
    }
    Ok(retained.into_iter().unzip())
}

fn canonical_predicate(predicate: &CheckPredicate) -> serde_json::Value {
    match predicate {
        CheckPredicate::And { left, right } => {
            serde_json::json!({"kind": "and", "left": canonical_predicate(left), "right": canonical_predicate(right)})
        }
        CheckPredicate::Or { left, right } => {
            serde_json::json!({"kind": "or", "left": canonical_predicate(left), "right": canonical_predicate(right)})
        }
        CheckPredicate::BooleanEquals { left, right } => {
            serde_json::json!({"kind": "boolean_equals", "left": canonical_predicate(left), "right": canonical_predicate(right)})
        }
        CheckPredicate::StringEquals { column, value } => {
            serde_json::json!({"kind": "string_equals", "column": column, "value": value})
        }
        CheckPredicate::IsNull { column } => {
            serde_json::json!({"kind": "is_null", "column": column})
        }
        CheckPredicate::JsonValid { column } => {
            serde_json::json!({"kind": "json_valid", "column": column})
        }
        CheckPredicate::OctetLengthAtMost { column, limit } => {
            serde_json::json!({"kind": "octet_length_at_most", "column": column, "limit": limit})
        }
        CheckPredicate::InStrings { column, values } => {
            serde_json::json!({"kind": "in_strings", "column": column, "values": values})
        }
    }
}
