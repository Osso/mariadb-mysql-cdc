#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum QueryToken {
    Identifier(String),
    Number,
    Dot,
    String,
    Other,
}

pub(super) fn query_references_schema(sql: &str, schema: &str) -> bool {
    query_tokens(sql).windows(2).any(|tokens| {
        matches!(
            tokens,
            [QueryToken::Identifier(identifier), QueryToken::Dot]
                if identifier.eq_ignore_ascii_case(schema)
        )
    })
}

pub(super) fn query_contains_qualified_identifier(sql: &str) -> bool {
    query_tokens(sql).windows(3).any(|tokens| {
        matches!(
            tokens,
            [
                QueryToken::Identifier(_),
                QueryToken::Dot,
                QueryToken::Identifier(_)
            ]
        )
    })
}

pub(super) fn query_tokens(sql: &str) -> Vec<QueryToken> {
    let characters = sql.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        let (token, next_index) = next_query_token(&characters, index);
        if let Some(token) = token {
            tokens.push(token);
        }
        index = next_index;
    }
    tokens
}

pub(super) fn next_query_token(characters: &[char], index: usize) -> (Option<QueryToken>, usize) {
    let character = characters[index];
    let next = characters.get(index + 1).copied();
    let next_next = characters.get(index + 2).copied();

    if character.is_whitespace() {
        return (None, index + 1);
    }
    if character == '#' || is_mysql_line_comment_start(character, next, next_next) {
        let next_index =
            skip_query_line_comment(characters, index + if character == '#' { 1 } else { 2 });
        return (None, next_index);
    }
    if character == '/' && next == Some('*') {
        return (None, skip_query_block_comment(characters, index + 2));
    }
    if matches!(character, '`' | '"') {
        let (identifier, next_index) = query_quoted_identifier(characters, index, character);
        return (Some(QueryToken::Identifier(identifier)), next_index);
    }
    if character == '\'' {
        return (
            Some(QueryToken::String),
            skip_query_string(characters, index),
        );
    }
    if character == '.' {
        return (Some(QueryToken::Dot), index + 1);
    }
    if character.is_ascii_alphabetic() || matches!(character, '_' | '$') {
        return query_identifier_token(characters, index);
    }
    if character.is_ascii_digit() {
        return query_number_token(characters, index);
    }
    (Some(QueryToken::Other), index + 1)
}

pub(super) fn query_identifier_token(
    characters: &[char],
    start: usize,
) -> (Option<QueryToken>, usize) {
    let mut index = start + 1;
    while characters.get(index).is_some_and(|candidate| {
        candidate.is_ascii_alphanumeric() || matches!(candidate, '_' | '$')
    }) {
        index += 1;
    }
    (
        Some(QueryToken::Identifier(
            characters[start..index].iter().collect(),
        )),
        index,
    )
}

pub(super) fn query_number_token(characters: &[char], start: usize) -> (Option<QueryToken>, usize) {
    let mut index = start + 1;
    while characters
        .get(index)
        .is_some_and(|candidate| candidate.is_ascii_alphanumeric() || *candidate == '_')
    {
        index += 1;
    }
    (Some(QueryToken::Number), index)
}

pub(super) fn is_mysql_line_comment_start(
    character: char,
    next: Option<char>,
    next_next: Option<char>,
) -> bool {
    character == '-'
        && next == Some('-')
        && match next_next {
            None => true,
            Some(after_dash) => after_dash.is_whitespace() || after_dash.is_control(),
        }
}

pub(super) fn skip_query_line_comment(characters: &[char], mut index: usize) -> usize {
    while characters
        .get(index)
        .is_some_and(|character| *character != '\n')
    {
        index += 1;
    }
    index
}

pub(super) fn skip_query_block_comment(characters: &[char], mut index: usize) -> usize {
    while index + 1 < characters.len() {
        if characters[index] == '*' && characters[index + 1] == '/' {
            return index + 2;
        }
        index += 1;
    }
    characters.len()
}

pub(super) fn query_quoted_identifier(
    characters: &[char],
    start: usize,
    quote: char,
) -> (String, usize) {
    let mut value = String::new();
    let mut index = start + 1;
    while index < characters.len() {
        if characters[index] == quote {
            if characters.get(index + 1) == Some(&quote) {
                value.push(quote);
                index += 2;
                continue;
            }
            return (value, index + 1);
        }
        value.push(characters[index]);
        index += 1;
    }
    (value, characters.len())
}

pub(super) fn skip_query_string(characters: &[char], start: usize) -> usize {
    let mut index = start + 1;
    let mut escaped = false;
    while index < characters.len() {
        let character = characters[index];
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '\'' {
            if characters.get(index + 1) == Some(&'\'') {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    characters.len()
}
