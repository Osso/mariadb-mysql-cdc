pub(crate) fn ddl_contains_comments(sql: &str) -> bool {
    let characters = sql.chars().collect::<Vec<_>>();
    ddl_characters_contain_comments(&characters)
}

fn ddl_characters_contain_comments(characters: &[char]) -> bool {
    let mut quote = None;
    let mut index = 0;
    while index < characters.len() {
        if let Some(next_index) = advance_quoted_ddl_scan(characters, index, &mut quote) {
            index = next_index;
            continue;
        }
        if starts_ddl_quote(characters, index) {
            quote = characters.get(index).copied();
            index += 1;
            continue;
        }
        if starts_ddl_comment(characters, index) {
            return true;
        }
        index += 1;
    }
    false
}

fn advance_quoted_ddl_scan(
    characters: &[char],
    index: usize,
    quote: &mut Option<char>,
) -> Option<usize> {
    let quote_character = (*quote)?;
    let character = characters[index];
    if character == quote_character {
        if characters.get(index + 1) == Some(&quote_character) {
            return Some(index + 2);
        }
        *quote = None;
        return Some(index + 1);
    }
    if character == '\\' && quote_character == '\'' {
        return Some(index + 2);
    }
    Some(index + 1)
}

fn starts_ddl_quote(characters: &[char], index: usize) -> bool {
    characters
        .get(index)
        .is_some_and(|character| matches!(character, '`' | '"' | '\''))
}

fn starts_ddl_comment(characters: &[char], index: usize) -> bool {
    let character = characters[index];
    let next = characters.get(index + 1).copied();
    character == '#'
        || is_mysql_line_comment_start(characters, index)
        || (character == '/' && next == Some('*'))
}

pub(crate) fn tokenize_ddl(sql: &str) -> Result<Vec<String>, String> {
    tokenize_ddl_with_quoted_flags(sql).map(|(tokens, _)| tokens)
}

pub(crate) fn tokenize_ddl_with_quoted_flags(
    sql: &str,
) -> Result<(Vec<String>, Vec<bool>), String> {
    let characters = sql.chars().collect::<Vec<_>>();
    tokenize_ddl_characters(&characters)
}

fn tokenize_ddl_characters(characters: &[char]) -> Result<(Vec<String>, Vec<bool>), String> {
    let mut tokens = Vec::new();
    let mut quoted_flags = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        let quoted = characters
            .get(index)
            .is_some_and(|character| matches!(character, '`' | '"'));
        let (token, next_index) = tokenize_ddl_step(characters, index)?;
        if let Some(token) = token {
            tokens.push(token);
            quoted_flags.push(quoted);
        }
        index = next_index;
    }
    Ok((tokens, quoted_flags))
}

fn tokenize_ddl_step(characters: &[char], index: usize) -> Result<(Option<String>, usize), String> {
    let character = characters[index];
    if character.is_whitespace() {
        return Ok((None, index + 1));
    }
    if let Some(next) = tokenize_comment(characters, index)? {
        return Ok((None, next));
    }
    if let Some(token) = tokenize_quoted(characters, index)? {
        return Ok((Some(token.0), token.1));
    }
    if let Some(token) = tokenize_punctuation_or_word(characters, index) {
        return Ok((Some(token.0), token.1));
    }
    Ok((Some(character.to_string()), index + 1))
}

fn tokenize_comment(characters: &[char], index: usize) -> Result<Option<usize>, String> {
    if is_mysql_line_comment_start(characters, index) {
        return Ok(Some(skip_line_comment(characters, index + 2)));
    }
    if characters[index] == '#' {
        return Ok(Some(skip_line_comment(characters, index + 1)));
    }
    if characters[index] == '/' && characters.get(index + 1) == Some(&'*') {
        return Ok(Some(skip_block_comment(characters, index + 2)?));
    }
    Ok(None)
}

fn tokenize_quoted(characters: &[char], index: usize) -> Result<Option<(String, usize)>, String> {
    let character = characters[index];
    if matches!(character, '`' | '"') {
        return Ok(Some(quoted_token(characters, index, character)?));
    }
    if character == '\'' {
        return Ok(Some((
            "<string>".to_string(),
            skip_quoted_string(characters, index, character)?,
        )));
    }
    Ok(None)
}

fn tokenize_punctuation_or_word(characters: &[char], index: usize) -> Option<(String, usize)> {
    let character = characters[index];
    if matches!(character, '.' | ',' | '(' | ')') {
        return Some((character.to_string(), index + 1));
    }
    if character.is_ascii_alphanumeric() || matches!(character, '_' | '$') {
        let next = scan_ddl_word_end(characters, index);
        return Some((characters[index..next].iter().collect(), next));
    }
    None
}
fn scan_ddl_word_end(characters: &[char], start: usize) -> usize {
    let mut index = start + 1;
    while characters.get(index).is_some_and(|candidate| {
        candidate.is_ascii_alphanumeric() || matches!(candidate, '_' | '$')
    }) {
        index += 1;
    }
    index
}

fn is_mysql_line_comment_start(characters: &[char], index: usize) -> bool {
    characters.get(index) == Some(&'-')
        && characters.get(index + 1) == Some(&'-')
        && match characters.get(index + 2) {
            None => true,
            Some(after_dash) => after_dash.is_whitespace() || after_dash.is_control(),
        }
}

fn skip_line_comment(characters: &[char], mut index: usize) -> usize {
    while characters
        .get(index)
        .is_some_and(|character| *character != '\n')
    {
        index += 1;
    }
    index
}

fn skip_block_comment(characters: &[char], mut index: usize) -> Result<usize, String> {
    while index + 1 < characters.len() {
        if characters[index] == '*' && characters[index + 1] == '/' {
            return Ok(index + 2);
        }
        index += 1;
    }
    Err("unterminated DDL block comment".to_string())
}

fn quoted_token(characters: &[char], start: usize, quote: char) -> Result<(String, usize), String> {
    let mut value = String::new();
    let mut index = start + 1;
    while index < characters.len() {
        if characters[index] == quote {
            if characters.get(index + 1) == Some(&quote) {
                value.push(quote);
                index += 2;
                continue;
            }
            return Ok((value, index + 1));
        }
        value.push(characters[index]);
        index += 1;
    }
    Err("unterminated quoted DDL identifier".to_string())
}

fn skip_quoted_string(characters: &[char], start: usize, quote: char) -> Result<usize, String> {
    let mut index = start + 1;
    let mut escaped = false;
    while index < characters.len() {
        let character = characters[index];
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == quote {
            if characters.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }
            return Ok(index + 1);
        }
        index += 1;
    }
    Err("unterminated DDL string literal".to_string())
}
