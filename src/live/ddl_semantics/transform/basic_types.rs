pub(super) fn parse_column_type(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
) -> Result<(String, String, usize), String> {
    let kind = unquoted_token(tokens, quoted, index)?.to_ascii_lowercase();
    let (data_type, next) = match kind.as_str() {
        "integer" => ("int", index + 1),
        "bool" | "boolean" => ("tinyint", index + 1),
        "numeric" => ("decimal", index + 1),
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" | "decimal" | "float"
        | "double" | "char" | "varchar" | "binary" | "varbinary" | "tinytext" | "text"
        | "mediumtext" | "longtext" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "date"
        | "time" | "datetime" | "timestamp" | "year" | "json" => (kind.as_str(), index + 1),
        _ => return Err(format!("unsupported column type {kind}")),
    };
    let (column_type, next) = parse_type_details(tokens, quoted, &kind, data_type, next)?;
    if let Some(token) = tokens.get(next)
        && ["UNSIGNED", "SIGNED", "ZEROFILL"]
            .iter()
            .any(|word| token.eq_ignore_ascii_case(word))
    {
        return Err(format!("unsupported column type qualifier {token}"));
    }
    Ok((column_type, data_type.into(), next))
}

fn parse_type_details(
    tokens: &[String],
    quoted: &[bool],
    source_kind: &str,
    data_type: &str,
    index: usize,
) -> Result<(String, usize), String> {
    match data_type {
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => {
            if matches!(source_kind, "bool" | "boolean") {
                reject_parameters(tokens, index, source_kind)?;
                return Ok(("tinyint".into(), index));
            }
            parse_integer_type(tokens, quoted, data_type, index)
        }
        "decimal" => {
            let (precision, scale, next) = parse_decimal_size(tokens, quoted, index)?;
            Ok((format!("decimal({precision},{scale})"), next))
        }
        "float" | "double" => parse_float_type(tokens, quoted, data_type, index),
        "char" | "varchar" | "binary" | "varbinary" => {
            parse_length_type(tokens, quoted, data_type, index)
        }
        "time" | "datetime" | "timestamp" => {
            if !at(tokens, index, "(") {
                return Ok((data_type.into(), index));
            }
            let (precision, next) = parse_one_number(tokens, quoted, index, 6)?;
            let suffix = if precision == 0 {
                String::new()
            } else {
                format!("({precision})")
            };
            Ok((format!("{data_type}{suffix}"), next))
        }
        _ => {
            reject_parameters(tokens, index, data_type)?;
            Ok((data_type.into(), index))
        }
    }
}

fn parse_integer_type(
    tokens: &[String],
    quoted: &[bool],
    kind: &str,
    index: usize,
) -> Result<(String, usize), String> {
    let next = if at(tokens, index, "(") {
        let (width, next) = parse_one_number(tokens, quoted, index, 255)?;
        if width == 0 {
            return Err("integer display width must be positive".into());
        }
        next
    } else {
        index
    };
    let (unsigned, next) = parse_signedness(tokens, quoted, next)?;
    let suffix = if unsigned { " unsigned" } else { "" };
    Ok((format!("{kind}{suffix}"), next))
}

fn parse_float_type(
    tokens: &[String],
    quoted: &[bool],
    kind: &str,
    index: usize,
) -> Result<(String, usize), String> {
    let next = if kind == "double" && at(tokens, index, "PRECISION") {
        unquoted_token(tokens, quoted, index)?;
        index + 1
    } else {
        index
    };
    reject_parameters(tokens, next, kind)?;
    let unsigned = at(tokens, next, "UNSIGNED");
    if unsigned {
        unquoted_token(tokens, quoted, next)?;
    }
    let suffix = if unsigned { " unsigned" } else { "" };
    Ok((format!("{kind}{suffix}"), next + usize::from(unsigned)))
}

fn parse_length_type(
    tokens: &[String],
    quoted: &[bool],
    kind: &str,
    index: usize,
) -> Result<(String, usize), String> {
    let maximum = if matches!(kind, "char" | "binary") {
        255
    } else {
        65535
    };
    let (length, next) = parse_one_number(tokens, quoted, index, maximum)?;
    if length == 0 {
        return Err("character or binary length must be positive".into());
    }
    Ok((format!("{kind}({length})"), next))
}

fn unquoted_token<'a>(
    tokens: &'a [String],
    quoted: &[bool],
    index: usize,
) -> Result<&'a str, String> {
    if quoted.get(index) != Some(&false) {
        return Err(format!("missing or quoted type token at {index}"));
    }
    tokens
        .get(index)
        .map(String::as_str)
        .ok_or_else(|| format!("missing type token at {index}"))
}

fn at(tokens: &[String], index: usize, keyword: &str) -> bool {
    tokens
        .get(index)
        .is_some_and(|token| token.eq_ignore_ascii_case(keyword))
}

fn reject_parameters(tokens: &[String], index: usize, kind: &str) -> Result<(), String> {
    if at(tokens, index, "(") {
        return Err(format!("{kind} parameters are unsupported"));
    }
    Ok(())
}

fn parse_one_number(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    max: u32,
) -> Result<(u32, usize), String> {
    if unquoted_token(tokens, quoted, index)? != "(" {
        return Err("expected type length or precision".into());
    }
    let number = parse_canonical_number(tokens, quoted, index + 1, max)?;
    if unquoted_token(tokens, quoted, index + 2)? != ")" {
        return Err("expected closing type parenthesis".into());
    }
    Ok((number, index + 3))
}

fn parse_canonical_number(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
    max: u32,
) -> Result<u32, String> {
    let token = unquoted_token(tokens, quoted, index)?;
    let value = token
        .parse::<u32>()
        .map_err(|_| format!("invalid type number {token}"))?;
    if value > max || token != value.to_string() {
        return Err(format!("invalid type number {token}"));
    }
    Ok(value)
}

fn parse_decimal_size(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
) -> Result<(u32, u32, usize), String> {
    if !at(tokens, index, "(") {
        return Ok((10, 0, index));
    }
    if unquoted_token(tokens, quoted, index)? != "(" {
        return Err("expected decimal precision".into());
    }
    let precision = parse_canonical_number(tokens, quoted, index + 1, 65)?;
    if precision == 0 {
        return Err("decimal precision must be positive".into());
    }
    let separator = unquoted_token(tokens, quoted, index + 2)?;
    let (scale, close) = if separator == "," {
        (
            parse_canonical_number(tokens, quoted, index + 3, 30)?,
            index + 4,
        )
    } else {
        (0, index + 2)
    };
    if scale > precision || unquoted_token(tokens, quoted, close)? != ")" {
        return Err("invalid decimal scale or closing parenthesis".into());
    }
    Ok((precision, scale, close + 1))
}

fn parse_signedness(
    tokens: &[String],
    quoted: &[bool],
    index: usize,
) -> Result<(bool, usize), String> {
    if at(tokens, index, "UNSIGNED") || at(tokens, index, "SIGNED") {
        let unsigned = unquoted_token(tokens, quoted, index)?.eq_ignore_ascii_case("UNSIGNED");
        return Ok((unsigned, index + 1));
    }
    Ok((false, index))
}

pub(super) fn normalize_numeric_default(
    column_type: &str,
    literal: &str,
) -> Result<String, String> {
    let unsigned = column_type.ends_with(" unsigned");
    let kind = column_type.strip_suffix(" unsigned").unwrap_or(column_type);
    match kind {
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => {
            normalize_integer(kind, unsigned, literal)
        }
        "float" | "double" => normalize_float(unsigned, literal),
        _ if kind.starts_with("decimal(") => normalize_decimal(kind, unsigned, literal),
        _ => Err(format!("not a numeric column type: {column_type}")),
    }
}

fn normalize_integer(kind: &str, unsigned: bool, literal: &str) -> Result<String, String> {
    let value = literal
        .parse::<i128>()
        .map_err(|_| format!("invalid integer default {literal}"))?;
    let bits = match kind {
        "tinyint" => 8,
        "smallint" => 16,
        "mediumint" => 24,
        "int" => 32,
        "bigint" => 64,
        _ => return Err(format!("unsupported integer type {kind}")),
    };
    let upper = if unsigned {
        (1_i128 << bits) - 1
    } else {
        (1_i128 << (bits - 1)) - 1
    };
    let lower = if unsigned { 0 } else { -(1_i128 << (bits - 1)) };
    if !(lower..=upper).contains(&value) {
        return Err(format!("integer default {literal} out of range for {kind}"));
    }
    Ok(value.to_string())
}

fn normalize_float(unsigned: bool, literal: &str) -> Result<String, String> {
    let value = literal
        .parse::<f64>()
        .map_err(|_| format!("invalid floating default {literal}"))?;
    if !value.is_finite() || (unsigned && value < 0.0) {
        return Err(format!(
            "floating default {literal} is nonfinite or negative"
        ));
    }
    Ok(value.to_string())
}

fn parse_decimal_dimensions(kind: &str) -> Result<(usize, usize), String> {
    let dimensions = kind
        .strip_prefix("decimal(")
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| format!("invalid decimal type {kind}"))?;
    let (precision, scale) = dimensions
        .split_once(',')
        .ok_or_else(|| format!("invalid decimal type {kind}"))?;
    let precision = precision
        .parse::<usize>()
        .map_err(|_| format!("invalid decimal type {kind}"))?;
    let scale = scale
        .parse::<usize>()
        .map_err(|_| format!("invalid decimal type {kind}"))?;
    if precision == 0 || precision > 65 || scale > 30 || scale > precision {
        return Err(format!("invalid decimal type {kind}"));
    }
    Ok((precision, scale))
}

fn normalize_decimal(kind: &str, unsigned: bool, literal: &str) -> Result<String, String> {
    let (precision, scale) = parse_decimal_dimensions(kind)?;
    let (negative, digits) = match literal.strip_prefix('-') {
        Some(digits) if !unsigned => (true, digits),
        Some(_) => return Err(format!("negative unsigned decimal default {literal}")),
        None => (false, literal.strip_prefix('+').unwrap_or(literal)),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty()
        || digits.ends_with('.')
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("invalid decimal default {literal}"));
    }
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let fraction = fraction.trim_end_matches('0');
    let significant_whole_digits = if whole == "0" { 0 } else { whole.len() };
    if fraction.len() > scale || significant_whole_digits > precision - scale {
        return Err(format!("decimal default {literal} out of range for {kind}"));
    }
    let sign = if negative && (whole != "0" || !fraction.is_empty()) {
        "-"
    } else {
        ""
    };
    if scale == 0 {
        return Ok(format!("{sign}{whole}"));
    }
    Ok(format!("{sign}{whole}.{fraction:0<scale$}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> Result<(String, String, usize), String> {
        let tokens = source
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        parse_column_type(&tokens, &vec![false; tokens.len()], 0)
    }

    #[test]
    fn parses_basic_types_and_consumes_only_type_tokens() {
        let cases = [
            (
                "TINYINT ( 1 ) UNSIGNED NOT NULL",
                "tinyint unsigned",
                "tinyint",
                5,
            ),
            ("SMALLINT SIGNED DEFAULT", "smallint", "smallint", 2),
            ("MEDIUMINT ( 8 )", "mediumint", "mediumint", 4),
            ("INTEGER UNSIGNED", "int unsigned", "int", 2),
            ("BIGINT", "bigint", "bigint", 1),
            ("BOOL DEFAULT", "tinyint", "tinyint", 1),
            ("BOOLEAN", "tinyint", "tinyint", 1),
            ("NUMERIC", "decimal(10,0)", "decimal", 1),
            ("DECIMAL ( 65 , 30 )", "decimal(65,30)", "decimal", 6),
            ("DECIMAL ( 9 )", "decimal(9,0)", "decimal", 4),
            ("FLOAT UNSIGNED", "float unsigned", "float", 2),
            ("DOUBLE PRECISION", "double", "double", 2),
            ("VARCHAR ( 65535 )", "varchar(65535)", "varchar", 4),
            ("CHAR ( 255 )", "char(255)", "char", 4),
            ("BINARY ( 255 )", "binary(255)", "binary", 4),
            ("VARBINARY ( 65535 )", "varbinary(65535)", "varbinary", 4),
            ("TINYTEXT", "tinytext", "tinytext", 1),
            ("TEXT", "text", "text", 1),
            ("MEDIUMTEXT", "mediumtext", "mediumtext", 1),
            ("LONGTEXT", "longtext", "longtext", 1),
            ("TINYBLOB", "tinyblob", "tinyblob", 1),
            ("BLOB", "blob", "blob", 1),
            ("MEDIUMBLOB", "mediumblob", "mediumblob", 1),
            ("LONGBLOB", "longblob", "longblob", 1),
            ("DATE", "date", "date", 1),
            ("TIME ( 0 )", "time", "time", 4),
            ("DATETIME ( 6 )", "datetime(6)", "datetime", 4),
            ("TIMESTAMP ( 3 )", "timestamp(3)", "timestamp", 4),
            ("YEAR", "year", "year", 1),
            ("JSON", "json", "json", 1),
        ];
        for (source, column_type, data_type, next) in cases {
            assert_eq!(
                parse(source),
                Ok((column_type.into(), data_type.into(), next)),
                "{source}"
            );
        }
    }

    #[test]
    fn rejects_invalid_and_unsupported_type_syntax() {
        for source in [
            "INT ( 0 )",
            "BIGINT ( 256 )",
            "INT ( 01 )",
            "INT ( 1.2 )",
            "DECIMAL ( 0 )",
            "DECIMAL ( 66 )",
            "DECIMAL ( 3 , 4 )",
            "DECIMAL ( 40 , 31 )",
            "DECIMAL ( 5 , 01 )",
            "FLOAT ( 8 )",
            "DOUBLE ( 8 , 2 )",
            "CHAR ( 0 )",
            "CHAR ( 256 )",
            "BINARY ( 256 )",
            "VARCHAR ( 65536 )",
            "VARBINARY ( 0 )",
            "DATE ( 2 )",
            "YEAR ( 4 )",
            "TIMESTAMP ( 7 )",
            "TIME ( 01 )",
            "TEXT ( 4 )",
            "BLOB ( 2 )",
            "JSON ( 2 )",
            "REAL",
            "INT ZEROFILL",
            "INT UNSIGNED ZEROFILL",
            "BOOL UNSIGNED",
            "VARCHAR ( 2 ) UNSIGNED",
            "INT UNSIGNED SIGNED",
            "INT (",
            "DECIMAL ( 3 , )",
        ] {
            assert!(parse(source).is_err(), "{source}");
        }
    }

    #[test]
    fn rejects_quoted_type_grammar_tokens() {
        for (source, quoted_index) in [
            ("INT UNSIGNED", 0),
            ("INT UNSIGNED", 1),
            ("CHAR ( 2 )", 1),
            ("CHAR ( 2 )", 2),
            ("DECIMAL ( 5 , 2 )", 4),
            ("DOUBLE PRECISION", 1),
        ] {
            let tokens = source
                .split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut quoted = vec![false; tokens.len()];
            quoted[quoted_index] = true;
            assert!(
                parse_column_type(&tokens, &quoted, 0).is_err(),
                "{source} {quoted_index}"
            );
        }
    }

    #[test]
    fn numeric_defaults_are_exact_and_range_checked() {
        let valid = [
            ("tinyint", "-128", "-128"),
            ("tinyint unsigned", "255", "255"),
            ("smallint", "32767", "32767"),
            ("smallint unsigned", "65535", "65535"),
            ("mediumint", "-8388608", "-8388608"),
            ("mediumint unsigned", "16777215", "16777215"),
            ("int", "-2147483648", "-2147483648"),
            ("int unsigned", "4294967295", "4294967295"),
            ("bigint", "-9223372036854775808", "-9223372036854775808"),
            (
                "bigint unsigned",
                "18446744073709551615",
                "18446744073709551615",
            ),
            ("int", "+0003", "3"),
            ("decimal(5,2)", "-0003.5", "-3.50"),
            ("decimal(5,2)", "0.100", "0.10"),
            (
                "decimal(65,30)",
                "0.123456789012345678901234567890",
                "0.123456789012345678901234567890",
            ),
            ("decimal(4,0)", "-0", "0"),
            ("decimal(2,2)", "0.12", "0.12"),
            ("float", "1.5e2", "150"),
            ("double unsigned", "3.25", "3.25"),
        ];
        for (kind, literal, expected) in valid {
            assert_eq!(
                normalize_numeric_default(kind, literal),
                Ok(expected.into()),
                "{kind} {literal}"
            );
        }
        for (kind, literal) in [
            ("tinyint", "128"),
            ("tinyint unsigned", "-1"),
            ("smallint", "32768"),
            ("smallint unsigned", "65536"),
            ("mediumint", "8388608"),
            ("mediumint unsigned", "16777216"),
            ("int", "2147483648"),
            ("int unsigned", "4294967296"),
            ("bigint", "9223372036854775808"),
            ("bigint unsigned", "18446744073709551616"),
            ("int", "1.0"),
            ("int", "1e2"),
            ("decimal(5,2)", "1000"),
            ("decimal(5,2)", "1.234"),
            ("decimal(5,2)", "NaN"),
            ("decimal(4,0)", "0.1"),
            ("float", "1e999"),
            ("double", "NaN"),
            ("float unsigned", "-0.1"),
            ("varchar(4)", "3"),
        ] {
            assert!(
                normalize_numeric_default(kind, literal).is_err(),
                "{kind} {literal}"
            );
        }
    }
}
