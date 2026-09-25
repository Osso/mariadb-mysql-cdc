// Typed grammar for observed facet, storefront, reader-memory, and assistant-quality CREATE
// statements.
use super::super::model::{
    ParsedCheckConstraintAst, ParsedCreateColumnAst, ParsedCreateForeignKeyAst, ParsedIndexAst,
};
use super::*;

const TABLE_DEFINITION_KEYWORDS: [&str; 4] = ["PRIMARY", "UNIQUE", "KEY", "CONSTRAINT"];

pub(super) fn parse(sql: &str) -> Result<ParsedCreateTableAst, String> {
    let sql = remove_ordinary_comments(sql)?;
    let (tokens, quoted) = tokenize_ddl_with_quoted_flags(&sql)?;
    let mut parser = Parser {
        tokens,
        quoted,
        literals: extract_single_quoted_literals(&sql)?.into_iter(),
        position: 0,
    };
    parser.keyword("CREATE")?;
    parser.keyword("TABLE")?;
    let if_not_exists = parser.at("IF");
    if if_not_exists {
        for keyword in ["IF", "NOT", "EXISTS"] {
            parser.keyword(keyword)?;
        }
    }
    let name = parser.identifier()?;
    parser.keyword("(")?;
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();
    let mut check_constraints = Vec::new();
    while !parser.at_any(&TABLE_DEFINITION_KEYWORDS) {
        let (mut column, inline_primary) = parser.column()?;
        if column.column_type == "json" {
            column.column_type = "longtext".into();
            column.character_set = Some("utf8mb4".into());
            column.collation = Some("utf8mb4_bin".into());
            check_constraints.push(ParsedCheckConstraintAst {
                name: column.name.clone(),
                disjuncts: vec![super::super::model::CheckPredicate::JsonValid {
                    column: column.name.clone(),
                }],
            });
        }
        if inline_primary {
            if !primary_key.is_empty() {
                return Err("CREATE has more than one PRIMARY KEY".into());
            }
            primary_key = vec![column.name.clone()];
        }
        columns.push(column);
        if parser.at(")") {
            break;
        }
        parser.keyword(",")?;
    }
    let mut indexes = Vec::new();
    let mut foreign_keys = Vec::new();
    while !parser.at(")") {
        if parser.at("PRIMARY") {
            parser.keyword("PRIMARY")?;
            parser.keyword("KEY")?;
            if !primary_key.is_empty() {
                return Err("CREATE has more than one PRIMARY KEY".into());
            }
            primary_key = parser.key_columns()?;
        } else if parser.at("CONSTRAINT")
            && parser
                .tokens
                .get(parser.position + 2)
                .is_some_and(|token| token.eq_ignore_ascii_case("FOREIGN"))
        {
            foreign_keys.push(parser.foreign_key()?);
        } else if parser.at("CONSTRAINT") {
            let (constraint, next) = check_constraint::parse_named_check(
                &parser.tokens,
                &parser.quoted,
                parser.position,
                &mut parser.literals,
            )?;
            parser.position = next;
            check_constraints.push(constraint);
        } else {
            indexes.push(parser.index(&name)?);
        }
        if parser.at(")") {
            break;
        }
        parser.keyword(",")?;
    }
    parser.keyword(")")?;
    let (character_set, collation) = parser.table_options()?;
    if parser.at(";") {
        parser.keyword(";")?;
    }
    if parser.position != parser.tokens.len() {
        return Err("unmodeled CREATE tail".into());
    }
    validate_definitions(&columns, &primary_key, &indexes, &check_constraints)?;
    validate_foreign_keys(&foreign_keys, &columns, &primary_key, &indexes)?;
    for column in &mut columns {
        if primary_key
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&column.name))
        {
            column.nullable = false;
        }
    }
    Ok(ParsedCreateTableAst {
        name,
        if_not_exists,
        columns,
        primary_key,
        indexes,
        check_constraints,
        foreign_keys,
        engine: "InnoDB".into(),
        character_set,
        collation,
    })
}

fn validate_definitions(
    columns: &[ParsedCreateColumnAst],
    primary: &[String],
    indexes: &[ParsedIndexAst],
    check_constraints: &[ParsedCheckConstraintAst],
) -> Result<(), String> {
    let names = columns
        .iter()
        .map(|column| column.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let index_names = indexes
        .iter()
        .map(|index| index.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let check_names = check_constraints
        .iter()
        .map(|constraint| constraint.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if columns.is_empty()
        || primary.is_empty()
        || names.len() != columns.len()
        || index_names.len() != indexes.len()
        || check_names.len() != check_constraints.len()
        || indexes
            .iter()
            .any(|index| index.name.eq_ignore_ascii_case("PRIMARY"))
    {
        return Err("empty, missing, or duplicate CREATE definition".into());
    }
    let keys = std::iter::once(primary.to_vec()).chain(indexes.iter().map(|index| {
        index
            .key_parts
            .iter()
            .map(|part| part.column.clone())
            .collect()
    }));
    for key in keys {
        let unique = key
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if unique.len() != key.len() || !unique.is_subset(&names) {
            return Err("duplicate or unknown CREATE key column".into());
        }
    }
    for constraint in check_constraints {
        for column in check_constraint::referenced_columns(constraint) {
            if !names.contains(&column.to_ascii_lowercase()) {
                return Err(format!(
                    "CHECK constraint {} references unknown column {column}",
                    constraint.name
                ));
            }
        }
    }
    Ok(())
}

fn validate_foreign_keys(
    foreign_keys: &[ParsedCreateForeignKeyAst],
    columns: &[ParsedCreateColumnAst],
    primary: &[String],
    indexes: &[ParsedIndexAst],
) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for key in foreign_keys {
        let known_columns = key.columns.iter().all(|name| {
            columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(name))
        });
        let supporting_index = primary.starts_with(&key.columns)
            || indexes.iter().any(|index| {
                let parts = index
                    .key_parts
                    .iter()
                    .map(|part| &part.column)
                    .collect::<Vec<_>>();
                parts.starts_with(&key.columns.iter().collect::<Vec<_>>())
            });
        if !names.insert(key.name.to_ascii_lowercase()) || !known_columns || !supporting_index {
            return Err("CREATE foreign key requires unique name, known columns and explicit supporting index".into());
        }
    }
    Ok(())
}

struct Parser {
    tokens: Vec<String>,
    quoted: Vec<bool>,
    literals: std::vec::IntoIter<String>,
    position: usize,
}

impl Parser {
    fn foreign_key(&mut self) -> Result<ParsedCreateForeignKeyAst, String> {
        self.keyword("CONSTRAINT")?;
        let name = self.identifier()?;
        self.keyword("FOREIGN")?;
        self.keyword("KEY")?;
        let columns = self.key_columns()?;
        self.keyword("REFERENCES")?;
        let referenced_table = self.identifier()?;
        let referenced_columns = self.key_columns()?;
        self.keyword("ON")?;
        self.keyword("DELETE")?;
        let delete_rule = if self.at("RESTRICT") {
            "RESTRICT"
        } else {
            "CASCADE"
        };
        self.keyword(delete_rule)?;
        if columns.len() != 1 || referenced_columns.len() != 1 {
            return Err("observed CREATE foreign key requires one column".into());
        }
        Ok(ParsedCreateForeignKeyAst {
            name,
            columns,
            referenced_table,
            referenced_columns,
            delete_rule: delete_rule.into(),
        })
    }

    fn at(&self, keyword: &str) -> bool {
        self.tokens
            .get(self.position)
            .is_some_and(|token| token.eq_ignore_ascii_case(keyword))
            && !self.quoted[self.position]
    }

    fn at_any(&self, keywords: &[&str]) -> bool {
        keywords.iter().any(|keyword| self.at(keyword))
    }

    fn keyword(&mut self, keyword: &str) -> Result<(), String> {
        if !self.at(keyword) {
            return Err(format!("expected unquoted {keyword} in observed CREATE"));
        }
        self.position += 1;
        Ok(())
    }

    fn identifier(&mut self) -> Result<String, String> {
        let name = require_identifier(&self.tokens, self.position, "observed CREATE identifier")?;
        self.position += 1;
        Ok(name)
    }

    fn key_columns(&mut self) -> Result<Vec<String>, String> {
        self.keyword("(")?;
        let mut columns = vec![self.identifier()?];
        while self.at(",") {
            self.keyword(",")?;
            columns.push(self.identifier()?);
        }
        self.keyword(")")?;
        Ok(columns)
    }

    fn index(&mut self, table: &str) -> Result<ParsedIndexAst, String> {
        let unique = self.at("UNIQUE");
        if unique {
            self.keyword("UNIQUE")?;
        }
        self.keyword("KEY")?;
        let index_name = self.identifier()?;
        let key_parts = self
            .key_columns()?
            .into_iter()
            .map(|column| ParsedIndexKeyPart {
                column,
                prefix_length: None,
                order: "ASC".into(),
                collation: Some("A".into()),
            })
            .collect();
        Ok(ParsedIndexAst {
            create: true,
            name: index_name,
            table: table.to_string(),
            unique,
            index_type: "BTREE".into(),
            visible: true,
            comment: None,
            key_parts,
        })
    }

    fn table_options(&mut self) -> Result<(Option<String>, Option<String>), String> {
        for keyword in ["ENGINE", "=", "InnoDB"] {
            self.keyword(keyword)?;
        }
        let character_set = if self.at("DEFAULT") {
            self.keyword("DEFAULT")?;
            self.keyword("CHARSET")?;
            self.keyword("=")?;
            self.keyword("utf8mb4")?;
            Some("utf8mb4".to_string())
        } else {
            None
        };
        if !self.at("COLLATE") {
            return Ok((character_set, None));
        }
        self.keyword("COLLATE")?;
        self.keyword("=")?;
        let collation = self.identifier()?;
        if !collation.starts_with("utf8mb4_") {
            return Err(format!(
                "CREATE collation {collation} requires utf8mb4 charset"
            ));
        }
        Ok((Some("utf8mb4".into()), Some(collation)))
    }

    /// Parses one column definition; the flag reports an inline `PRIMARY KEY`.
    fn column(&mut self) -> Result<(ParsedCreateColumnAst, bool), String> {
        let name = self.identifier()?;
        let column_type = self.column_type()?;
        let (character_set, collation) = self.column_encoding(&column_type)?;
        let explicit_null = self.at("NULL");
        let mut nullable = self.nullability()?;
        let default_sql = self.column_default(&column_type, nullable)?;
        let incompatible_null = explicit_null || default_sql.as_deref() == Some("NULL");
        let auto_increment = self.auto_increment(&column_type, incompatible_null)?;
        if auto_increment {
            nullable = false;
        }
        let on_update_current_timestamp = self.on_update(&column_type)?;
        let inline_primary = self.at("PRIMARY");
        if inline_primary {
            if explicit_null || default_sql.as_deref() == Some("NULL") {
                return Err("inline PRIMARY KEY cannot be NULL".into());
            }
            nullable = false;
            self.keyword("PRIMARY")?;
            self.keyword("KEY")?;
        }
        let comment = self.column_comment()?;
        Ok((
            ParsedCreateColumnAst {
                name,
                column_type,
                nullable,
                default_sql,
                auto_increment,
                on_update_current_timestamp,
                character_set,
                collation,
                comment,
            },
            inline_primary,
        ))
    }

    /// Consumes an optional trailing `COMMENT '<literal>'` of printable ASCII without quotes.
    fn column_comment(&mut self) -> Result<String, String> {
        if !self.at("COMMENT") {
            return Ok(String::new());
        }
        self.keyword("COMMENT")?;
        self.keyword("<string>")?;
        let value = self.literals.next().ok_or("missing COMMENT literal")?;
        let printable = value
            .chars()
            .all(|character| (' '..='~').contains(&character) && character != '\'');
        if value.is_empty() || value.len() > 1024 || !printable {
            return Err("unmodeled observed CREATE column comment".into());
        }
        Ok(value)
    }

    fn column_encoding(
        &mut self,
        column_type: &str,
    ) -> Result<(Option<String>, Option<String>), String> {
        if !self.at("CHARACTER") {
            return Ok((None, None));
        }
        if !is_character_type(column_type) {
            return Err("CHARACTER SET requires a character column type".into());
        }
        self.keyword("CHARACTER")?;
        self.keyword("SET")?;
        let character_set = self.identifier()?;
        self.keyword("COLLATE")?;
        let collation = self.identifier()?;
        if !collation.starts_with(&format!("{character_set}_")) {
            return Err(format!(
                "column collation {collation} does not belong to {character_set}"
            ));
        }
        Ok((Some(character_set), Some(collation)))
    }

    fn nullability(&mut self) -> Result<bool, String> {
        if self.at("NOT") {
            self.keyword("NOT")?;
            self.keyword("NULL")?;
            return Ok(false);
        }
        if self.at("NULL") {
            self.keyword("NULL")?;
        }
        Ok(true)
    }

    fn column_default(
        &mut self,
        column_type: &str,
        nullable: bool,
    ) -> Result<Option<String>, String> {
        if !self.at("DEFAULT") {
            return Ok(None);
        }
        self.keyword("DEFAULT")?;
        let kind = column_type.split(['(', ' ']).next().unwrap_or_default();
        let integer = matches!(
            kind,
            "int" | "mediumint" | "smallint" | "tinyint" | "bigint"
        );
        if self.at("NULL") {
            if !nullable {
                return Err("NOT NULL column cannot DEFAULT NULL".into());
            }
            self.keyword("NULL")?;
            return Ok(Some("NULL".into()));
        }
        if self.at("CURRENT_TIMESTAMP") && matches!(kind, "timestamp" | "datetime") {
            return self.current_timestamp(column_type).map(Some);
        }
        if integer || kind == "decimal" || matches!(kind, "float" | "double") {
            return self.numeric_default(column_type).map(Some);
        }
        if self.at("<string>") && is_character_type(column_type) {
            return self.string_default(kind).map(Some);
        }
        Err("unmodeled observed CREATE default".into())
    }

    fn string_default(&mut self, kind: &str) -> Result<String, String> {
        self.keyword("<string>")?;
        let value = self.literals.next().ok_or("missing DEFAULT literal")?;
        let printable = value
            .chars()
            .all(|character| (' '..='~').contains(&character) && character != '\'');
        if !printable {
            return Err("unmodeled observed CREATE string default".into());
        }
        Ok(if is_text_type(kind) {
            text_expression_default(&value)
        } else {
            quote_string_literal(&value)
        })
    }

    fn numeric_default(&mut self, column_type: &str) -> Result<String, String> {
        let start = self.position;
        if self.at("+") || self.at("-") {
            self.position += 1;
        }
        let digits = self
            .tokens
            .get(self.position)
            .ok_or("missing numeric default")?;
        if self.quoted[self.position]
            || !digits.starts_with(|character: char| character.is_ascii_digit())
        {
            return Err("invalid numeric default".into());
        }
        self.position += 1;
        if self.at(".") {
            self.position += 1;
            let fraction = self
                .tokens
                .get(self.position)
                .ok_or("missing numeric fraction")?;
            if self.quoted[self.position]
                || !fraction.starts_with(|character: char| character.is_ascii_digit())
            {
                return Err("invalid numeric fraction".into());
            }
            self.position += 1;
        }
        if self.tokens[self.position - 1].ends_with(['e', 'E']) {
            if self.at("+") || self.at("-") {
                self.position += 1;
            }
            let exponent = self
                .tokens
                .get(self.position)
                .ok_or("missing numeric exponent")?;
            if self.quoted[self.position] || !exponent.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("invalid numeric exponent".into());
            }
            self.position += 1;
        }
        let literal = self.tokens[start..self.position].join("");
        super::basic_types::normalize_numeric_default(column_type, &literal)
    }

    /// Consumes `CURRENT_TIMESTAMP` or `CURRENT_TIMESTAMP(6)` matching the column precision.
    fn current_timestamp(&mut self, column_type: &str) -> Result<String, String> {
        self.keyword("CURRENT_TIMESTAMP")?;
        let expected = current_timestamp_for(column_type);
        if let Some(precision) = column_type
            .strip_suffix(')')
            .and_then(|kind| kind.rsplit_once('('))
            .map(|(_, precision)| precision.to_string())
        {
            self.keyword("(")?;
            self.keyword(&precision)?;
            self.keyword(")")?;
        } else if self.at("(") {
            return Err("CURRENT_TIMESTAMP precision does not match the column".into());
        }
        Ok(expected)
    }

    fn auto_increment(
        &mut self,
        column_type: &str,
        incompatible_null: bool,
    ) -> Result<bool, String> {
        if !self.at("AUTO_INCREMENT") {
            return Ok(false);
        }
        let kind = column_type.split(' ').next().unwrap_or_default();
        let integer = matches!(
            kind,
            "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
        );
        if !integer || incompatible_null {
            return Err("AUTO_INCREMENT requires a non-null integer column".into());
        }
        self.keyword("AUTO_INCREMENT")?;
        Ok(true)
    }

    fn on_update(&mut self, column_type: &str) -> Result<bool, String> {
        if !self.at("ON") {
            return Ok(false);
        }
        if !column_type.starts_with("timestamp") && !column_type.starts_with("datetime") {
            return Err("ON UPDATE requires TIMESTAMP or DATETIME".into());
        }
        self.keyword("ON")?;
        self.keyword("UPDATE")?;
        self.current_timestamp(column_type)?;
        Ok(true)
    }

    fn enum_type(&mut self) -> Result<String, String> {
        self.keyword("ENUM")?;
        self.keyword("(")?;
        let mut members = Vec::new();
        loop {
            self.keyword("<string>")?;
            let value = self.literals.next().ok_or("missing ENUM literal")?;
            if value.is_empty()
                || !value
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            {
                return Err("unmodeled ENUM member".into());
            }
            members.push(quote_string_literal(&value));
            if !self.at(",") {
                break;
            }
            self.keyword(",")?;
        }
        self.keyword(")")?;
        Ok(format!("enum({})", members.join(",")))
    }

    fn column_type(&mut self) -> Result<String, String> {
        if self.at("ENUM") {
            return self.enum_type();
        }
        let (column_type, _, next) =
            super::basic_types::parse_column_type(&self.tokens, &self.quoted, self.position)?;
        self.position = next;
        Ok(column_type)
    }
}

pub(crate) fn is_character_type(column_type: &str) -> bool {
    let kind = column_type.split('(').next().unwrap_or_default();
    matches!(kind, "char" | "varchar") || is_text_type(kind)
}

pub(crate) fn is_text_type(data_type: &str) -> bool {
    matches!(data_type, "tinytext" | "text" | "mediumtext" | "longtext")
}

/// MySQL 8 rejects literal TEXT defaults; the expression default with an explicit
/// introducer yields the same stored value on every connection character set.
pub(crate) fn text_expression_default(value: &str) -> String {
    format!("(_utf8mb4{})", quote_string_literal(value))
}

/// The `CURRENT_TIMESTAMP` spelling whose precision matches the column type.
pub(crate) fn current_timestamp_for(column_type: &str) -> String {
    let precision = column_type
        .strip_suffix(')')
        .and_then(|kind| kind.rsplit_once('('));
    match precision {
        Some((_, precision)) => format!("CURRENT_TIMESTAMP({precision})"),
        None => "CURRENT_TIMESTAMP".to_string(),
    }
}

pub(super) fn remove_ordinary_comments(sql: &str) -> Result<String, String> {
    let sql = strip_leading_ordinary_ddl_comments(sql)?;
    let chars = sql.chars().collect::<Vec<_>>();
    let mut result = String::new();
    let mut quote = None;
    let mut position = 0;
    while position < chars.len() {
        let ch = chars[position];
        if let Some(delimiter) = quote {
            if ch == '\\' && delimiter == '\'' {
                return Err("escaped observed CREATE strings are unsupported".into());
            }
            result.push(ch);
            if ch == delimiter {
                quote = None;
            }
            position += 1;
            continue;
        }
        if ch == '`' || ch == '\'' {
            quote = Some(ch);
            result.push(ch);
            position += 1;
            continue;
        }
        if ch == '"' || ch == '#' {
            return Err("unmodeled CREATE quoting/comment".into());
        }
        if ch == '/' && chars.get(position + 1) == Some(&'*') {
            return Err("embedded or executable CREATE block comment".into());
        }
        if ch == '-'
            && chars.get(position + 1) == Some(&'-')
            && chars
                .get(position + 2)
                .is_none_or(|ch| ch.is_whitespace() || ch.is_control())
        {
            while position < chars.len() && chars[position] != '\n' {
                position += 1;
            }
            result.push(' ');
        } else {
            result.push(ch);
            position += 1;
        }
    }
    if quote.is_some() {
        return Err("unclosed CREATE quote".into());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::super::super::model::CheckPredicate;
    use super::*;

    #[test]
    fn ordinary_create_preserves_optional_clauses_and_nullable_defaults() {
        let ast = parse("CREATE TABLE `ordinary` (`id` INTEGER PRIMARY KEY, `title` VARCHAR(30) DEFAULT '', `note` TEXT DEFAULT 'a b', `created` TIMESTAMP(3) NULL, `amount` DECIMAL(7,2) DEFAULT -12.50) ENGINE=InnoDB").unwrap();
        assert!(!ast.if_not_exists);
        assert_eq!(ast.character_set, None);
        assert_eq!(ast.collation, None);
        assert_eq!(ast.primary_key, ["id"]);
        assert!(!ast.columns[0].nullable);
        assert!(ast.columns[1..].iter().all(|column| column.nullable));
        assert_eq!(ast.columns[1].default_sql.as_deref(), Some("''"));
        assert_eq!(
            ast.columns[2].default_sql.as_deref(),
            Some("(_utf8mb4'a b')")
        );
        assert_eq!(ast.columns[3].column_type, "timestamp(3)");
        assert_eq!(ast.columns[4].default_sql.as_deref(), Some("-12.50"));
    }

    #[test]
    fn ordinary_create_accepts_basic_types_and_finite_numeric_defaults() {
        let ast = parse("CREATE TABLE IF NOT EXISTS t (id BIGINT NOT NULL, count SMALLINT SIGNED DEFAULT -32768, ratio DOUBLE DEFAULT 1.25, occurred DATE NULL, elapsed TIME(6), bytes VARBINARY(16), payload BLOB, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci").unwrap();
        assert!(ast.if_not_exists);
        assert_eq!(ast.character_set.as_deref(), Some("utf8mb4"));
        assert_eq!(ast.collation.as_deref(), Some("utf8mb4_unicode_ci"));
        assert_eq!(ast.columns[1].default_sql.as_deref(), Some("-32768"));
        assert_eq!(ast.columns[2].default_sql.as_deref(), Some("1.25"));
        assert_eq!(ast.columns[4].column_type, "time(6)");
        assert_eq!(ast.columns[5].column_type, "varbinary(16)");
        assert_eq!(ast.columns[6].column_type, "blob");
    }

    #[test]
    fn ordinary_create_accepts_finite_scientific_defaults_and_temporal_precision() {
        let ast = parse("CREATE TABLE t (id INT PRIMARY KEY, rate FLOAT DEFAULT 1.5e2, changed DATETIME(2) NULL DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2)) ENGINE=InnoDB").unwrap();
        assert_eq!(ast.columns[1].default_sql.as_deref(), Some("150"));
        assert_eq!(ast.columns[2].column_type, "datetime(2)");
        assert_eq!(
            ast.columns[2].default_sql.as_deref(),
            Some("CURRENT_TIMESTAMP(2)")
        );
        assert!(ast.columns[2].on_update_current_timestamp);
    }

    #[test]
    fn ordinary_create_table_primary_key_implies_not_null() {
        let ast =
            parse("CREATE TABLE t (id BIGINT, label VARCHAR(8), PRIMARY KEY (id)) ENGINE=InnoDB")
                .unwrap();
        assert!(!ast.columns[0].nullable);
        assert!(ast.columns[1].nullable);
    }

    #[test]
    fn ordinary_create_collation_implies_matching_charset() {
        let ast =
            parse("CREATE TABLE t (id INT PRIMARY KEY) ENGINE=InnoDB COLLATE=utf8mb4_unicode_ci")
                .unwrap();
        assert_eq!(ast.character_set.as_deref(), Some("utf8mb4"));
        assert_eq!(ast.collation.as_deref(), Some("utf8mb4_unicode_ci"));
    }

    #[test]
    fn ordinary_create_inline_primary_auto_increment_implies_not_null() {
        let ast = parse(
            "CREATE TABLE t (id INTEGER AUTO_INCREMENT PRIMARY KEY, name VARCHAR(8)) ENGINE=InnoDB",
        )
        .unwrap();
        assert!(!ast.columns[0].nullable);
        assert!(ast.columns[0].auto_increment);
        assert!(ast.columns[1].nullable);
    }

    #[test]
    fn ordinary_create_rejects_contradictory_and_unmodeled_options() {
        for sql in [
            "CREATE TABLE t (id INT NOT NULL DEFAULT NULL PRIMARY KEY) ENGINE=InnoDB",
            "CREATE TABLE t (id INT PRIMARY KEY) ENGINE=InnoDB ROW_FORMAT=COMPRESSED",
            "CREATE TABLE t (id INT PRIMARY KEY) ENGINE=MyISAM",
            "CREATE TABLE t (id INT PRIMARY KEY, n INT DEFAULT 'abc') ENGINE=InnoDB",
            "CREATE TABLE t (id INT PRIMARY KEY, n VARCHAR(5) DEFAULT 'a\\b') ENGINE=InnoDB",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
    }

    const SQL: &str = "/* ordinary */ CREATE TABLE IF NOT EXISTS `facets` (\n`comic_id` MEDIUMINT UNSIGNED NOT NULL, -- identity\n`facet_id` SMALLINT UNSIGNED NOT NULL, `kind` TINYINT UNSIGNED NOT NULL, `label` VARCHAR(80) NOT NULL, `score` DECIMAL(4,3) NOT NULL, `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`comic_id`, `facet_id`), KEY `by_kind` (`kind`, `comic_id`), KEY `by_facet` (`facet_id`, `kind`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    #[test]
    fn sales_placements_create_preserves_columns_defaults_and_indexes() {
        let source = include_str!("../../../../fixtures/ddl/create-sales-placements.sql");
        let ast = parse(source).expect("sales placements observed CREATE AST");
        assert_eq!(parse_fixture_create_table(source).unwrap(), ast);
        assert_eq!(ast.name, "sales_placements");
        assert_eq!(ast.columns.len(), 14);
        assert_eq!(ast.primary_key, ["id"]);
        assert_eq!(ast.columns[8].name, "rule_json");
        assert_eq!(ast.columns[8].column_type, "longtext");
        assert!(!ast.columns[8].nullable);
        assert_eq!(ast.columns[9].column_type, "tinyint unsigned");
        assert_eq!(ast.columns[9].default_sql.as_deref(), Some("1"));
        assert_eq!(ast.columns[13].default_sql.as_deref(), Some("NULL"));
        assert_eq!(ast.indexes.len(), 3);
        assert_eq!(
            ast.indexes[0]
                .key_parts
                .iter()
                .map(|part| part.column.as_str())
                .collect::<Vec<_>>(),
            ["sale_id", "is_active"]
        );
        assert!(ast.indexes.iter().all(|index| !index.unique));

        let result = transform_fixture_create_table(source).expect("sales placements translation");
        assert_eq!(
            result.target_sql.as_deref(),
            Some(
                "CREATE TABLE `sales_placements` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `sale_id` INT UNSIGNED NOT NULL, `target` VARCHAR(80) NOT NULL, `placement_type` VARCHAR(40) NOT NULL, `target_key_id` INT UNSIGNED NULL DEFAULT NULL, `content_section_id` INT UNSIGNED NULL DEFAULT NULL, `custom_card_id` INT UNSIGNED NULL DEFAULT NULL, `comic_id` INT UNSIGNED NULL DEFAULT NULL, `rule_json` LONGTEXT NOT NULL, `is_active` TINYINT UNSIGNED NOT NULL DEFAULT 1, `creator_id` INT UNSIGNED NOT NULL DEFAULT 0, `create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, `updater_id` INT UNSIGNED NULL DEFAULT NULL, `update_time` TIMESTAMP NULL DEFAULT NULL, PRIMARY KEY (`id`), KEY `idx_sp_sale` (`sale_id`, `is_active`), KEY `idx_sp_section` (`content_section_id`), KEY `idx_sp_card` (`custom_card_id`)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
            )
        );
        for unsupported in [
            source.replace("LONGTEXT NOT NULL", "LONGTEXT NOT NULL DEFAULT 7"),
            source.replace("LONGTEXT NOT NULL", "LONGTEXT BINARY NOT NULL"),
        ] {
            assert!(parse_fixture_create_table(&unsupported).is_err());
        }
    }

    #[test]
    fn storefront_create_parses_observed_columns_and_keys() {
        let sql = include_str!("../../../../fixtures/ddl/create-storefront-chips.sql");
        let ast = parse(sql).expect("storefront CREATE");
        assert_eq!(ast.name, "storefront_chips");
        assert_eq!(ast.columns.len(), 10);
        assert_eq!(ast.columns[0].column_type, "int unsigned");
        assert!(ast.columns[0].auto_increment);
        assert_eq!(
            ast.columns[1].column_type,
            "enum('western','manga','webtoon')"
        );
        assert_eq!(ast.columns[5].column_type, "tinyint");
        assert_eq!(ast.columns[5].default_sql.as_deref(), Some("1"));
        assert!(ast.columns[7].nullable);
        assert!(ast.columns[7].on_update_current_timestamp);
        assert_eq!(ast.columns[7].default_sql, None);
        assert_eq!(
            ast.columns[9].default_sql.as_deref(),
            Some("CURRENT_TIMESTAMP")
        );
        assert!(ast.indexes[0].unique);
        assert!(!ast.indexes[1].unique);
        for rejected in [
            sql.replace(
                "ENUM('western','manga','webtoon')",
                "ENUM('west\\\\nern','manga','webtoon')",
            ),
            sql.replace("UNIQUE KEY", "UNIQUE HASH KEY"),
        ] {
            assert!(parse(&rejected).is_err(), "{rejected}");
        }
    }

    #[test]
    fn observed_create_preserves_event_types_and_composite_keys() {
        let ast = parse_fixture_create_table(SQL).unwrap();
        assert_eq!(ast.columns[3].column_type, "varchar(80)");
        assert_eq!(ast.primary_key, ["comic_id", "facet_id"]);
        assert_eq!(ast.indexes[0].key_parts.len(), 2);
        let sql = transform_fixture_create_table(SQL)
            .unwrap()
            .target_sql
            .unwrap();
        assert!(sql.contains("`label` VARCHAR(80) NOT NULL"));
        assert!(sql.contains("DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"));
        assert!(sql.ends_with("DEFAULT CHARACTER SET=utf8mb4"));
    }

    const CURATED_SLIDES: &str =
        include_str!("../../../../fixtures/ddl/create-home-feed-curated-strip-slides.sql");

    #[test]
    fn curated_slides_create_preserves_cascading_foreign_key() {
        let result = transform_fixture_create_table(CURATED_SLIDES)
            .expect("curated slides CREATE")
            .target_sql
            .unwrap();
        assert!(result.contains("CONSTRAINT `fk_hfcss_strip` FOREIGN KEY (`curated_strip_id`) REFERENCES `home_feed_curated_strips` (`id`) ON DELETE CASCADE"));
        for rejected in [
            CURATED_SLIDES.replace("ON DELETE CASCADE", "ON DELETE SET NULL"),
            CURATED_SLIDES.replace("ON DELETE CASCADE", "ON DELETE CASCADE ON UPDATE CASCADE"),
            CURATED_SLIDES.replace(
                "FOREIGN KEY (`curated_strip_id`)",
                "FOREIGN KEY (`missing`)",
            ),
            CURATED_SLIDES.replace(
                "REFERENCES `home_feed_curated_strips`",
                "REFERENCES other.`home_feed_curated_strips`",
            ),
            CURATED_SLIDES.replace(
                "KEY `idx_hfcss_strip` (`curated_strip_id`, `display_order`)",
                "KEY `idx_hfcss_strip` (`display_order`)",
            ),
        ] {
            assert!(parse(&rejected).is_err(), "{rejected}");
        }
    }

    #[test]
    fn curated_slides_rejects_observed_foreign_key_action_drift() {
        let ast = parse(CURATED_SLIDES).unwrap();
        let key = crate::canonical_foreign_key::CanonicalForeignKey {
            constraint_schema: "test".into(),
            constraint_name: "fk_hfcss_strip".into(),
            child_schema: "test".into(),
            child_table: "home_feed_curated_strip_slides".into(),
            child_columns: vec!["curated_strip_id".into()],
            parent_schema: "test".into(),
            parent_table: "home_feed_curated_strips".into(),
            parent_columns: vec!["id".into()],
            update_rule: "RESTRICT".into(),
            delete_rule: "CASCADE".into(),
            match_option: "NONE".into(),
            enforced: true,
        };
        let validate = |keys: &[crate::canonical_foreign_key::CanonicalForeignKey]| {
            crate::live::ddl_semantics::canonical::validate_create_foreign_keys(&ast, "test", keys)
        };
        assert!(validate(std::slice::from_ref(&key)).is_ok());
        assert!(validate(&[]).is_err());
        let mut changed = key.clone();
        changed.delete_rule = "RESTRICT".into();
        assert!(validate(&[changed]).is_err());
        let mut changed = key.clone();
        changed.update_rule = "CASCADE".into();
        assert!(validate(&[changed]).is_err());
        let mut changed = key;
        changed.parent_columns = vec!["other_id".into()];
        assert!(validate(&[changed]).is_err());
    }

    const CURATED_STRIPS: &str =
        include_str!("../../../../fixtures/ddl/create-home-feed-curated-strips.sql");

    #[test]
    fn curated_strips_create_preserves_decimal_default() {
        let ast = parse(CURATED_STRIPS).expect("curated strips CREATE");
        let aspect = &ast.columns[5];
        assert_eq!(aspect.name, "aspect_ratio");
        assert_eq!(aspect.column_type, "decimal(4,3)");
        assert_eq!(aspect.default_sql.as_deref(), Some("0.650"));
        assert!(!aspect.nullable);
        assert_eq!(ast.primary_key, ["id"]);
        assert_eq!(ast.columns.len(), 13);
    }

    #[test]
    fn curated_strips_decimal_defaults_preserve_exact_scale() {
        for value in ["0.000", "1.000", "9.999"] {
            let sql = CURATED_STRIPS.replace("DEFAULT 0.650", &format!("DEFAULT {value}"));
            let ast = parse(&sql).expect(value);
            assert_eq!(ast.columns[5].default_sql.as_deref(), Some(value));
        }
        for (value, normalized) in [("0.65", "0.650"), ("0.6500", "0.650"), ("00.650", "0.650")] {
            let sql = CURATED_STRIPS.replace("DEFAULT 0.650", &format!("DEFAULT {value}"));
            let ast = parse(&sql).expect(value);
            assert_eq!(ast.columns[5].default_sql.as_deref(), Some(normalized));
        }
        for value in ["10.000", "0.65e0", "'0.650'", "(0.650)", "`0`.`650`"] {
            let sql = CURATED_STRIPS.replace("DEFAULT 0.650", &format!("DEFAULT {value}"));
            assert!(parse(&sql).is_err(), "{value}");
        }
    }

    const PROFILES: &str =
        include_str!("../../../../fixtures/ddl/create-reader-memory-profiles.sql");
    const ITEMS: &str = include_str!("../../../../fixtures/ddl/create-reader-memory-items.sql");
    const OPERATIONS: &str =
        include_str!("../../../../fixtures/ddl/create-reader-memory-operations.sql");

    #[test]
    fn reader_memory_profiles_create_parses_inline_primary_key_defaults_and_checks() {
        let ast = parse(PROFILES).expect("profiles CREATE");
        assert_eq!(ast.name, "reader_memory_profiles");
        assert_eq!(ast.primary_key, ["user_id"]);
        assert_eq!(ast.collation.as_deref(), Some("utf8mb4_unicode_ci"));
        let columns = &ast.columns;
        assert_eq!(columns.len(), 10);
        assert_eq!(columns[0].column_type, "int unsigned");
        assert!(!columns[0].nullable);
        assert_eq!(columns[1].column_type, "smallint unsigned");
        assert_eq!(columns[1].default_sql.as_deref(), Some("1"));
        assert_eq!(columns[2].column_type, "tinyint");
        assert_eq!(columns[2].default_sql.as_deref(), Some("0"));
        assert_eq!(columns[4].column_type, "bigint unsigned");
        assert_eq!(columns[4].default_sql.as_deref(), Some("0"));
        assert_eq!(columns[6].column_type, "datetime(6)");
        assert!(columns[6].nullable);
        assert_eq!(columns[6].default_sql, None);
        assert_eq!(columns[8].column_type, "mediumtext");
        assert!(!columns[8].nullable);
        assert_eq!(columns[8].default_sql.as_deref(), Some("(_utf8mb4'{}')"));
        assert_eq!(columns[8].character_set, None);
        assert_eq!(columns[9].column_type, "datetime(6)");
        assert_eq!(
            columns[9].default_sql.as_deref(),
            Some("CURRENT_TIMESTAMP(6)")
        );
        assert!(columns[9].on_update_current_timestamp);
        assert!(ast.indexes.is_empty());
        assert_eq!(
            ast.check_constraints,
            vec![
                ParsedCheckConstraintAst {
                    name: "reader_memory_profile_json".into(),
                    disjuncts: vec![CheckPredicate::JsonValid {
                        column: "prepared_json".into(),
                    }],
                },
                ParsedCheckConstraintAst {
                    name: "reader_memory_profile_size".into(),
                    disjuncts: vec![CheckPredicate::OctetLengthAtMost {
                        column: "prepared_json".into(),
                        limit: 32768,
                    }],
                },
            ]
        );
    }

    #[test]
    fn reader_memory_items_create_parses_ascii_char_text_and_disjunctive_checks() {
        let ast = parse(ITEMS).expect("items CREATE");
        assert_eq!(ast.primary_key, ["uuid"]);
        let uuid = &ast.columns[0];
        assert_eq!(uuid.column_type, "char(36)");
        assert_eq!(uuid.character_set.as_deref(), Some("ascii"));
        assert_eq!(uuid.collation.as_deref(), Some("ascii_bin"));
        assert!(!uuid.nullable);
        let payload = &ast.columns[5];
        assert_eq!(payload.name, "payload_json");
        assert_eq!(payload.column_type, "text");
        assert!(payload.nullable);
        assert_eq!(payload.default_sql, None);
        assert_eq!(ast.columns[8].column_type, "datetime(6)");
        assert!(!ast.columns[8].nullable);
        assert_eq!(ast.indexes.len(), 2);
        assert!(ast.indexes[0].unique);
        assert_eq!(ast.indexes[0].name, "reader_memory_semantic");
        assert_eq!(ast.indexes[0].key_parts.len(), 2);
        assert!(!ast.indexes[1].unique);
        assert_eq!(
            ast.check_constraints,
            vec![
                ParsedCheckConstraintAst {
                    name: "reader_memory_item_json".into(),
                    disjuncts: vec![
                        CheckPredicate::IsNull {
                            column: "payload_json".into(),
                        },
                        CheckPredicate::JsonValid {
                            column: "payload_json".into(),
                        },
                    ],
                },
                ParsedCheckConstraintAst {
                    name: "reader_memory_item_size".into(),
                    disjuncts: vec![
                        CheckPredicate::IsNull {
                            column: "payload_json".into(),
                        },
                        CheckPredicate::OctetLengthAtMost {
                            column: "payload_json".into(),
                            limit: 8192,
                        },
                    ],
                },
                ParsedCheckConstraintAst {
                    name: "reader_memory_item_state".into(),
                    disjuncts: vec![CheckPredicate::InStrings {
                        column: "status".into(),
                        values: vec!["active".into(), "disabled".into(), "forgotten".into()],
                    }],
                },
            ]
        );
    }

    #[test]
    fn reader_memory_operations_create_parses_varchar_default_and_datetime_default() {
        let ast = parse(OPERATIONS).expect("operations CREATE");
        assert_eq!(ast.columns[3].column_type, "varchar(24)");
        assert_eq!(ast.columns[3].default_sql.as_deref(), Some("'pending'"));
        assert_eq!(ast.columns[3].character_set, None);
        assert_eq!(ast.columns[5].name, "lease_token");
        assert_eq!(ast.columns[5].character_set.as_deref(), Some("ascii"));
        assert!(ast.columns[5].nullable);
        let created = &ast.columns[12];
        assert_eq!(created.name, "created_at");
        assert_eq!(created.default_sql.as_deref(), Some("CURRENT_TIMESTAMP(6)"));
        assert!(!created.on_update_current_timestamp);
        assert!(ast.columns[13].on_update_current_timestamp);
        assert_eq!(ast.indexes.len(), 4);
        assert!(ast.indexes[0].unique);
        assert_eq!(ast.indexes[1].key_parts.len(), 3);
        assert!(ast.check_constraints.is_empty());
    }

    #[test]
    fn reader_memory_create_rejects_unmodeled_semantics() {
        for rejected in [
            PROFILES.replace("DATETIME(6)", "DATETIME(3)"),
            PROFILES.replace("CURRENT_TIMESTAMP(6) ON", "CURRENT_TIMESTAMP ON"),
            PROFILES.replace("JSON_VALID(prepared_json)", "CHAR_LENGTH(prepared_json)"),
            PROFILES.replace(
                "OCTET_LENGTH(prepared_json) <=",
                "OCTET_LENGTH(prepared_json) <",
            ),
            PROFILES.replace(
                "CHECK (JSON_VALID(prepared_json))",
                "CHECK (JSON_VALID(missing))",
            ),
            PROFILES.replace("DEFAULT '{}'", "DEFAULT '{\\\\}'"),
            PROFILES.replace("reader_memory_profile_size", "reader_memory_profile_json"),
            PROFILES.replace(
                "DEFAULT 0,\n deletion_epoch",
                "DEFAULT 'x',\n deletion_epoch",
            ),
            PROFILES.replace(
                "CONSTRAINT reader_memory_profile_json",
                "PRIMARY KEY (user_id),\n CONSTRAINT reader_memory_profile_json",
            ),
            ITEMS.replace("'active','disabled'", "'act-ive','disabled'"),
            ITEMS.replace(
                "COLLATE ascii_bin NOT NULL PRIMARY KEY",
                "COLLATE utf8mb4_bin NOT NULL PRIMARY KEY",
            ),
            ITEMS.replace(
                "CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY",
                "CHARACTER SET ascii NOT NULL PRIMARY KEY",
            ),
        ] {
            assert!(parse(&rejected).is_err(), "accepted {rejected}");
        }
    }

    #[test]
    fn observed_create_rejects_unmodeled_semantics() {
        for sql in [
            SQL.replace("/* ordinary */", "/*!99999 invisible */"),
            SQL.replace("/* ordinary */", "/*+ hint */"),
            SQL.replace("DECIMAL(4,3)", "DECIMAL(0,3)"),
            SQL.replace("MEDIUMINT UNSIGNED", "MEDIUMINT ZEROFILL"),
            SQL.replace("VARCHAR(80)", "VARCHAR(`80`)"),
            SQL.replace("TIMESTAMP NOT", "TIMESTAMP(6) NOT"),
            SQL.replace("KEY `by_kind`", "UNIQUE HASH KEY `by_kind`"),
            SQL.replace("ENGINE=InnoDB", "ENGINE=MyISAM"),
        ] {
            assert!(parse_fixture_create_table(&sql).is_err(), "accepted {sql}");
        }
    }
}
