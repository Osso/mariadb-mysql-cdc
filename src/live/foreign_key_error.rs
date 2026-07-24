//! Parses the foreign key identity out of a MySQL `1452` error.
//!
//! Automatic parent recovery previously matched two hardcoded constraint signatures, so every other
//! foreign key became a durable stall that only manual repair could clear. MySQL always names the
//! child table, the constraint, the child columns, the parent table, and the parent columns in the
//! error, so the identity can be read from the error itself instead of being enumerated.
//!
//! Parsing is strict: anything that does not match the exact documented shape yields `None` and the
//! conflict keeps the ordinary durable-abort path.

/// Foreign key identity named by a MySQL `1452` error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForeignKeyViolation {
    pub(crate) child_schema: String,
    pub(crate) child_table: String,
    pub(crate) constraint: String,
    pub(crate) child_columns: Vec<String>,
    /// Present only when MySQL qualifies the parent, which it does for a different schema.
    pub(crate) parent_schema: Option<String>,
    pub(crate) parent_table: String,
    pub(crate) parent_columns: Vec<String>,
}

const FAILURE_MARKER: &str = "a foreign key constraint fails (";
const CONSTRAINT_MARKER: &str = "CONSTRAINT ";
const FOREIGN_KEY_MARKER: &str = "FOREIGN KEY ";
const REFERENCES_MARKER: &str = "REFERENCES ";

pub(crate) fn parse_foreign_key_violation(error_text: &str) -> Option<ForeignKeyViolation> {
    let failure = error_text.find(FAILURE_MARKER)? + FAILURE_MARKER.len();
    let remainder = &error_text[failure..];

    let (child_schema, remainder) = take_quoted_identifier(remainder)?;
    let remainder = remainder.strip_prefix('.')?;
    let (child_table, remainder) = take_quoted_identifier(remainder)?;
    let remainder = remainder.strip_prefix(", ")?;

    let remainder = remainder.strip_prefix(CONSTRAINT_MARKER)?;
    let (constraint, remainder) = take_quoted_identifier(remainder)?;
    let remainder = remainder.strip_prefix(' ')?;

    let remainder = remainder.strip_prefix(FOREIGN_KEY_MARKER)?;
    let (child_columns, remainder) = take_quoted_identifier_list(remainder)?;
    let remainder = remainder.strip_prefix(' ')?;

    let remainder = remainder.strip_prefix(REFERENCES_MARKER)?;
    let (first_identifier, remainder) = take_quoted_identifier(remainder)?;
    let (parent_schema, parent_table, remainder) = match remainder.strip_prefix('.') {
        Some(remainder) => {
            let (parent_table, remainder) = take_quoted_identifier(remainder)?;
            (Some(first_identifier), parent_table, remainder)
        }
        None => (None, first_identifier, remainder),
    };
    let remainder = remainder.strip_prefix(' ')?;
    let (parent_columns, _) = take_quoted_identifier_list(remainder)?;

    if child_columns.len() != parent_columns.len() {
        return None;
    }

    Some(ForeignKeyViolation {
        child_schema,
        child_table,
        constraint,
        child_columns,
        parent_schema,
        parent_table,
        parent_columns,
    })
}

/// Reads one `` `identifier` `` and returns it with the unconsumed remainder. MySQL doubles an
/// embedded backtick, so `` `a``b` `` decodes to `a`b`.
fn take_quoted_identifier(text: &str) -> Option<(String, &str)> {
    let mut characters = text.char_indices();
    if characters.next()?.1 != '`' {
        return None;
    }
    let mut identifier = String::new();
    while let Some((index, character)) = characters.next() {
        if character != '`' {
            identifier.push(character);
            continue;
        }
        match text[index + 1..].starts_with('`') {
            true => {
                identifier.push('`');
                characters.next();
            }
            false => return Some((identifier, &text[index + 1..])),
        }
    }
    None
}

/// Reads a `` (`a`, `b`) `` column list and returns it with the unconsumed remainder.
fn take_quoted_identifier_list(text: &str) -> Option<(Vec<String>, &str)> {
    let mut remainder = text.strip_prefix('(')?;
    let mut identifiers = Vec::new();
    loop {
        let (identifier, next) = take_quoted_identifier(remainder)?;
        identifiers.push(identifier);
        match next.strip_prefix(", ") {
            Some(next) => remainder = next,
            None => {
                let next = next.strip_prefix(')')?;
                if identifiers.is_empty() {
                    return None;
                }
                return Some((identifiers, next));
            }
        }
    }
}

#[cfg(test)]
mod tests;
