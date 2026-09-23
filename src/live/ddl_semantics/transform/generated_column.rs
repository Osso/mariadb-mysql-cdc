// Bounded stored generated-column grammar for production ADD COLUMN clauses.
//
// Admitted source form (MariaDB `PERSISTENT` or `STORED`, optional `GENERATED ALWAYS`):
//   AS (IF(<column> = <operand> [AND <column> = <operand>]*, <0..255>, NULL)) PERSISTENT
// Operands are canonical unsigned integers or string literals limited to [A-Za-z0-9_].
use super::super::model::{GeneratedOperand, ParsedStoredIfExpression};
use super::{quote_identifier, require_identifier, tokens_match};

/// Parses the generation clause starting at `index` and returns the index after
/// `PERSISTENT`/`STORED`.
pub(super) fn parse_stored_generation(
    tokens: &[String],
    quoted: &[bool],
    mut index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(ParsedStoredIfExpression, usize), String> {
    if is_keyword(tokens, quoted, index, "GENERATED") {
        require_keyword(tokens, quoted, index + 1, "ALWAYS")?;
        index += 2;
    }
    for keyword in ["AS", "(", "IF", "("] {
        require_keyword(tokens, quoted, index, keyword)?;
        index += 1;
    }
    let mut equalities = Vec::new();
    loop {
        let column = require_identifier(tokens, index, "generated expression column")?;
        require_keyword(tokens, quoted, index + 1, "=")?;
        let (operand, next) = parse_operand(tokens, quoted, index + 2, literals)?;
        equalities.push((column, operand));
        index = next;
        if !is_keyword(tokens, quoted, index, "AND") {
            break;
        }
        index += 1;
    }
    require_keyword(tokens, quoted, index, ",")?;
    let then_value = parse_integer(tokens, quoted, index + 1)?;
    let then_value = u8::try_from(then_value)
        .map_err(|_| format!("generated IF value {then_value} exceeds TINYINT UNSIGNED"))?;
    index += 2;
    for keyword in [",", "NULL", ")", ")"] {
        require_keyword(tokens, quoted, index, keyword)?;
        index += 1;
    }
    if !is_keyword(tokens, quoted, index, "PERSISTENT")
        && !is_keyword(tokens, quoted, index, "STORED")
    {
        return Err("generated column must be PERSISTENT or STORED".into());
    }
    Ok((
        ParsedStoredIfExpression {
            equalities,
            then_value,
        },
        index + 1,
    ))
}

fn parse_operand(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    literals: &mut impl Iterator<Item = String>,
) -> Result<(GeneratedOperand, usize), String> {
    if is_keyword(tokens, quoted, index, "<string>") {
        let value = literals
            .next()
            .ok_or_else(|| "generated expression literal is missing".to_string())?;
        if value.is_empty()
            || !value
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(format!("unmodeled generated expression literal {value:?}"));
        }
        return Ok((GeneratedOperand::String(value), index + 1));
    }
    let value = parse_integer(tokens, quoted, index)?;
    Ok((GeneratedOperand::Integer(value), index + 1))
}

fn parse_integer(tokens: &[String], quoted: &[bool], index: usize) -> Result<u64, String> {
    let token = tokens
        .get(index)
        .ok_or_else(|| "generated expression integer is missing".to_string())?;
    let value = token
        .parse::<u64>()
        .map_err(|_| format!("generated expression operand {token} is not an integer"))?;
    if value.to_string() != *token || quoted.get(index) == Some(&true) {
        return Err(format!(
            "generated expression integer {token} is not canonical"
        ));
    }
    Ok(value)
}

/// MySQL DDL for the expression; string literals carry an explicit `_utf8mb4` introducer so
/// the stored expression does not depend on the connection character set.
pub(super) fn render_generation_sql(expression: &ParsedStoredIfExpression) -> String {
    let condition = expression
        .equalities
        .iter()
        .map(|(column, operand)| {
            let operand = match operand {
                GeneratedOperand::String(value) => format!("_utf8mb4'{value}'"),
                GeneratedOperand::Integer(value) => value.to_string(),
            };
            format!("{} = {operand}", quote_identifier(column))
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("IF({condition}, {}, NULL)", expression.then_value)
}

/// The `GENERATION_EXPRESSION` MySQL 8.4 reports for the rendered expression.
pub(crate) fn mysql_generation_expression(expression: &ParsedStoredIfExpression) -> String {
    let predicates = expression
        .equalities
        .iter()
        .map(|(column, operand)| {
            let operand = match operand {
                GeneratedOperand::String(value) => format!("_utf8mb4\\'{value}\\'"),
                GeneratedOperand::Integer(value) => value.to_string(),
            };
            format!("({} = {operand})", quote_identifier(column))
        })
        .collect::<Vec<_>>();
    let condition = if predicates.len() == 1 {
        predicates[0].clone()
    } else {
        format!("({})", predicates.join(" and "))
    };
    format!("if({condition},{},NULL)", expression.then_value)
}

pub(crate) fn referenced_columns(expression: &ParsedStoredIfExpression) -> Vec<&str> {
    expression
        .equalities
        .iter()
        .map(|(column, _)| column.as_str())
        .collect()
}

fn is_keyword(tokens: &[String], quoted: &[bool], index: usize, keyword: &str) -> bool {
    tokens_match(tokens, index, keyword) && quoted.get(index) != Some(&true)
}

fn require_keyword(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    keyword: &str,
) -> Result<(), String> {
    if is_keyword(tokens, quoted, index, keyword) {
        return Ok(());
    }
    Err(format!(
        "expected {keyword} in generated column, found {:?}",
        tokens.get(index)
    ))
}
