// Bounded CHECK constraint grammar shared by the observed CREATE and production ALTER parsers.
//
// Admitted predicates, joined only by `OR`:
//   <column> IS NULL
//   JSON_VALID(<column>)
//   OCTET_LENGTH(<column>) <= <integer>
//   <column> IN ('<literal>', ...)      literals limited to [A-Za-z0-9_]
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
        let (predicate, next) = parse_predicate(tokens, quoted, position, literals)?;
        disjuncts.push(predicate);
        position = next;
        if tokens_match(tokens, position, ")") && !is_quoted(quoted, position) {
            return Ok((ParsedCheckConstraintAst { name, disjuncts }, position + 1));
        }
        require_unquoted_keyword(tokens, quoted, position, "OR")?;
        position += 1;
    }
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
    require_unquoted_keyword(tokens, quoted, index + 1, "IN")?;
    require_unquoted_keyword(tokens, quoted, index + 2, "(")?;
    let mut values = Vec::new();
    let mut position = index + 3;
    loop {
        require_unquoted_keyword(tokens, quoted, position, "<string>")?;
        let value = literals
            .next()
            .ok_or_else(|| "CHECK IN literal is missing".to_string())?;
        if value.is_empty()
            || !value
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(format!("unmodeled CHECK IN literal {value:?}"));
        }
        values.push(value);
        position += 1;
        if tokens_match(tokens, position, ")") && !is_quoted(quoted, position) {
            return Ok((CheckPredicate::InStrings { column, values }, position + 1));
        }
        require_unquoted_keyword(tokens, quoted, position, ",")?;
        position += 1;
    }
}

fn parse_function_column(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
) -> Result<(String, usize), String> {
    require_unquoted_keyword(tokens, quoted, index + 1, "(")?;
    let column = require_identifier(tokens, index + 2, "CHECK function column")?;
    require_unquoted_keyword(tokens, quoted, index + 3, ")")?;
    Ok((column, index + 4))
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
    constraint
        .disjuncts
        .iter()
        .map(|predicate| match predicate {
            CheckPredicate::IsNull { column }
            | CheckPredicate::JsonValid { column }
            | CheckPredicate::OctetLengthAtMost { column, .. }
            | CheckPredicate::InStrings { column, .. } => column.as_str(),
        })
        .collect()
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
        "disjuncts": constraint.disjuncts.iter().map(|predicate| match predicate {
            CheckPredicate::IsNull { column } => serde_json::json!({"kind": "is_null", "column": column}),
            CheckPredicate::JsonValid { column } => serde_json::json!({"kind": "json_valid", "column": column}),
            CheckPredicate::OctetLengthAtMost { column, limit } => {
                serde_json::json!({"kind": "octet_length_at_most", "column": column, "limit": limit})
            }
            CheckPredicate::InStrings { column, values } => {
                serde_json::json!({"kind": "in_strings", "column": column, "values": values})
            }
        }).collect::<Vec<_>>(),
    })
}
