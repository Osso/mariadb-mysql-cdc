// Typed grammar for observed facet, storefront, and reader-memory CREATE statements.
use super::super::model::{ParsedCheckConstraintAst, ParsedCreateColumnAst, ParsedIndexAst};
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
    for keyword in ["CREATE", "TABLE", "IF", "NOT", "EXISTS"] {
        parser.keyword(keyword)?;
    }
    let name = parser.identifier()?;
    parser.keyword("(")?;
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();
    while !parser.at_any(&TABLE_DEFINITION_KEYWORDS) {
        let (column, inline_primary) = parser.column()?;
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
    let mut check_constraints = Vec::new();
    while !parser.at(")") {
        if parser.at("PRIMARY") {
            parser.keyword("PRIMARY")?;
            parser.keyword("KEY")?;
            if !primary_key.is_empty() {
                return Err("CREATE has more than one PRIMARY KEY".into());
            }
            primary_key = parser.key_columns()?;
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
    let collation = parser.table_options()?;
    if parser.at(";") {
        parser.keyword(";")?;
    }
    if parser.position != parser.tokens.len() {
        return Err("unmodeled CREATE tail".into());
    }
    validate_definitions(&columns, &primary_key, &indexes, &check_constraints)?;
    Ok(ParsedCreateTableAst {
        name,
        if_not_exists: true,
        columns,
        primary_key,
        indexes,
        check_constraints,
        engine: "InnoDB".into(),
        character_set: Some("utf8mb4".into()),
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

struct Parser {
    tokens: Vec<String>,
    quoted: Vec<bool>,
    literals: std::vec::IntoIter<String>,
    position: usize,
}

impl Parser {
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

    fn table_options(&mut self) -> Result<Option<String>, String> {
        for keyword in [
            "ENGINE", "=", "InnoDB", "DEFAULT", "CHARSET", "=", "utf8mb4",
        ] {
            self.keyword(keyword)?;
        }
        if !self.at("COLLATE") {
            return Ok(None);
        }
        self.keyword("COLLATE")?;
        self.keyword("=")?;
        let collation = self.identifier()?;
        if !collation.starts_with("utf8mb4_") {
            return Err(format!(
                "CREATE collation {collation} is not a utf8mb4 collation"
            ));
        }
        Ok(Some(collation))
    }

    /// Parses one column definition; the flag reports an inline `PRIMARY KEY`.
    fn column(&mut self) -> Result<(ParsedCreateColumnAst, bool), String> {
        let name = self.identifier()?;
        let column_type = self.column_type()?;
        let (character_set, collation) = self.column_encoding(&column_type)?;
        let nullable = self.nullability()?;
        let default_sql = self.column_default(&column_type, nullable)?;
        let auto_increment = self.auto_increment(&column_type, nullable)?;
        let on_update_current_timestamp = self.on_update(&column_type)?;
        let inline_primary = self.at("PRIMARY");
        if inline_primary {
            if nullable {
                return Err("inline PRIMARY KEY requires NOT NULL".into());
            }
            self.keyword("PRIMARY")?;
            self.keyword("KEY")?;
        }
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
            },
            inline_primary,
        ))
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
        if self.at("NULL") && nullable {
            self.keyword("NULL")?;
            return Ok(Some("NULL".into()));
        }
        if self.at("CURRENT_TIMESTAMP") && matches!(kind, "timestamp" | "datetime") {
            return self.current_timestamp(column_type).map(Some);
        }
        if (self.at("0") || self.at("1")) && integer {
            let value = self.tokens[self.position].clone();
            self.position += 1;
            return Ok(Some(value));
        }
        if self.at("<string>") && is_character_type(column_type) {
            self.keyword("<string>")?;
            let value = self.literals.next().ok_or("missing DEFAULT literal")?;
            if value.is_empty()
                || !value
                    .chars()
                    .all(|character| character.is_ascii_graphic() && character != '\'')
            {
                return Err("unmodeled observed CREATE string default".into());
            }
            return Ok(Some(if is_text_type(kind) {
                text_expression_default(&value)
            } else {
                quote_string_literal(&value)
            }));
        }
        Err("unmodeled observed CREATE default".into())
    }

    /// Consumes `CURRENT_TIMESTAMP` or `CURRENT_TIMESTAMP(6)` matching the column precision.
    fn current_timestamp(&mut self, column_type: &str) -> Result<String, String> {
        self.keyword("CURRENT_TIMESTAMP")?;
        let expected = current_timestamp_for(column_type);
        if expected.ends_with("(6)") {
            for token in ["(", "6", ")"] {
                self.keyword(token)?;
            }
        } else if self.at("(") {
            return Err("CURRENT_TIMESTAMP precision does not match the column".into());
        }
        Ok(expected)
    }

    fn auto_increment(&mut self, column_type: &str, nullable: bool) -> Result<bool, String> {
        if !self.at("AUTO_INCREMENT") {
            return Ok(false);
        }
        if column_type != "int unsigned" || nullable {
            return Err("AUTO_INCREMENT requires observed non-null INT UNSIGNED".into());
        }
        self.keyword("AUTO_INCREMENT")?;
        Ok(true)
    }

    fn on_update(&mut self, column_type: &str) -> Result<bool, String> {
        if !self.at("ON") {
            return Ok(false);
        }
        if !matches!(column_type, "timestamp" | "datetime" | "datetime(6)") {
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
        if self.at("TINYINT") {
            let (column_type, next) =
                parse_tinyint_type(&self.tokens, &self.quoted, self.position + 1)?;
            self.position = next;
            return Ok(column_type);
        }
        for kind in ["INT", "MEDIUMINT", "SMALLINT", "BIGINT"] {
            if self.at(kind) {
                self.keyword(kind)?;
                self.keyword("UNSIGNED")?;
                return Ok(format!("{} unsigned", kind.to_ascii_lowercase()));
            }
        }
        if self.at("TIMESTAMP") {
            self.keyword("TIMESTAMP")?;
            return Ok("timestamp".into());
        }
        if self.at("DATETIME") {
            self.keyword("DATETIME")?;
            if !self.at("(") {
                return Ok("datetime".into());
            }
            for token in ["(", "6", ")"] {
                self.keyword(token)?;
            }
            return Ok("datetime(6)".into());
        }
        for kind in ["TEXT", "MEDIUMTEXT"] {
            if self.at(kind) {
                self.keyword(kind)?;
                return Ok(kind.to_ascii_lowercase());
            }
        }
        if self.at("DECIMAL") {
            for token in ["DECIMAL", "(", "4", ",", "3", ")"] {
                self.keyword(token)?;
            }
            return Ok("decimal(4,3)".into());
        }
        let kind = if self.at("CHAR") { "CHAR" } else { "VARCHAR" };
        self.keyword(kind)?;
        self.keyword("(")?;
        let length = self
            .tokens
            .get(self.position)
            .cloned()
            .ok_or("missing character type length")?;
        let value = length
            .parse::<u32>()
            .map_err(|_| "invalid character type length")?;
        if value == 0 || value.to_string() != length || (kind == "CHAR" && value > 255) {
            return Err("noncanonical character type length".into());
        }
        self.keyword(&length)?;
        self.keyword(")")?;
        Ok(format!("{}({value})", kind.to_ascii_lowercase()))
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
    if column_type.ends_with("(6)") {
        "CURRENT_TIMESTAMP(6)".to_string()
    } else {
        "CURRENT_TIMESTAMP".to_string()
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

    const SQL: &str = "/* ordinary */ CREATE TABLE IF NOT EXISTS `facets` (\n`comic_id` MEDIUMINT UNSIGNED NOT NULL, -- identity\n`facet_id` SMALLINT UNSIGNED NOT NULL, `kind` TINYINT UNSIGNED NOT NULL, `label` VARCHAR(80) NOT NULL, `score` DECIMAL(4,3) NOT NULL, `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`comic_id`, `facet_id`), KEY `by_kind` (`kind`, `comic_id`), KEY `by_facet` (`facet_id`, `kind`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

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
            sql.replace("TINYINT(1)", "TINYINT(2)"),
            sql.replace(
                "ENUM('western','manga','webtoon')",
                "ENUM('west\\\\nern','manga','webtoon')",
            ),
            sql.replace("DEFAULT 1", "DEFAULT 2"),
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
            SQL.replace("DECIMAL(4,3)", "DECIMAL(5,3)"),
            SQL.replace("MEDIUMINT UNSIGNED", "MEDIUMINT"),
            SQL.replace("VARCHAR(80)", "VARCHAR(`80`)"),
            SQL.replace("TIMESTAMP NOT", "TIMESTAMP(6) NOT"),
            SQL.replace("KEY `by_kind`", "UNIQUE HASH KEY `by_kind`"),
            SQL.replace("ENGINE=InnoDB", "ENGINE=MyISAM"),
        ] {
            assert!(parse_fixture_create_table(&sql).is_err(), "accepted {sql}");
        }
    }
}
