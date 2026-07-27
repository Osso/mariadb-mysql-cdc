pub(crate) fn parse_enum_column_type(column_type: &str) -> Option<Vec<String>> {
    let values = column_type.strip_prefix("enum(")?.strip_suffix(')')?;
    Some(parse_sql_string_list(values))
}

pub(crate) fn parse_set_column_type(column_type: &str) -> Option<Vec<String>> {
    let values = column_type.strip_prefix("set(")?.strip_suffix(')')?;
    Some(parse_sql_string_list(values))
}

fn parse_sql_string_list(values: &str) -> Vec<String> {
    let mut parsed = Vec::new();
    let mut chars = values.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\'' {
            parsed.push(parse_sql_string_value(&mut chars));
        }
    }
    parsed
}

fn parse_sql_string_value<I>(chars: &mut std::iter::Peekable<I>) -> String
where
    I: Iterator<Item = char>,
{
    let mut value = String::new();
    while let Some(character) = chars.next() {
        match character {
            '\'' if chars.peek() == Some(&'\'') => {
                value.push('\'');
                chars.next();
            }
            '\'' => break,
            '\\' => {
                if let Some(escaped) = chars.next() {
                    value.push(escaped);
                }
            }
            _ => value.push(character),
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_escaped_enum_labels_in_declaration_order() {
        assert_eq!(
            parse_enum_column_type("enum('views','can''t','back\\\\slash')"),
            Some(vec![
                "views".to_string(),
                "can't".to_string(),
                "back\\slash".to_string(),
            ])
        );
    }
}
