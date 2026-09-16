// Typed grammar for observed facet and storefront CREATE statements.
use super::*;

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
    parser.keyword("IF")?;
    parser.keyword("NOT")?;
    parser.keyword("EXISTS")?;
    let name = parser.identifier()?;
    parser.keyword("(")?;
    let mut columns = Vec::new();
    while !parser.at("PRIMARY") {
        columns.push(parser.column()?);
        parser.keyword(",")?;
    }
    parser.keyword("PRIMARY")?;
    parser.keyword("KEY")?;
    let primary_key = parser.key_columns()?;
    let mut indexes = Vec::new();
    while parser.at(",") {
        parser.keyword(",")?;
        let unique = parser.at("UNIQUE");
        if unique {
            parser.keyword("UNIQUE")?;
        }
        parser.keyword("KEY")?;
        let index_name = parser.identifier()?;
        let key_parts = parser
            .key_columns()?
            .into_iter()
            .map(|column| ParsedIndexKeyPart {
                column,
                prefix_length: None,
                order: "ASC".into(),
                collation: Some("A".into()),
            })
            .collect();
        indexes.push(ParsedIndexAst {
            create: true,
            name: index_name,
            table: name.clone(),
            unique,
            index_type: "BTREE".into(),
            visible: true,
            comment: None,
            key_parts,
        });
    }
    parser.keyword(")")?;
    parser.keyword("ENGINE")?;
    parser.keyword("=")?;
    parser.keyword("InnoDB")?;
    parser.keyword("DEFAULT")?;
    parser.keyword("CHARSET")?;
    parser.keyword("=")?;
    parser.keyword("utf8mb4")?;
    if parser.at(";") {
        parser.keyword(";")?;
    }
    if parser.position != parser.tokens.len() {
        return Err("unmodeled CREATE tail".into());
    }
    validate_definitions(&columns, &primary_key, &indexes)?;
    Ok(ParsedCreateTableAst {
        name,
        if_not_exists: true,
        columns,
        primary_key,
        indexes,
        engine: "InnoDB".into(),
        character_set: Some("utf8mb4".into()),
        collation: None,
    })
}

fn validate_definitions(
    columns: &[ParsedCreateColumnAst],
    primary: &[String],
    indexes: &[ParsedIndexAst],
) -> Result<(), String> {
    let names = columns
        .iter()
        .map(|column| column.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let index_names = indexes
        .iter()
        .map(|index| index.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if columns.is_empty()
        || names.len() != columns.len()
        || index_names.len() != indexes.len()
        || indexes
            .iter()
            .any(|index| index.name.eq_ignore_ascii_case("PRIMARY"))
    {
        return Err("empty or duplicate CREATE definition".into());
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

    fn column(&mut self) -> Result<ParsedCreateColumnAst, String> {
        let name = self.identifier()?;
        let column_type = self.column_type()?;
        let nullable = self.nullability()?;
        let default_sql = self.column_default(&column_type, nullable)?;
        let auto_increment = self.auto_increment(&column_type, nullable)?;
        let on_update_current_timestamp = self.on_update(&column_type)?;
        Ok(ParsedCreateColumnAst {
            name,
            column_type,
            nullable,
            default_sql,
            auto_increment,
            on_update_current_timestamp,
        })
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
        let integer = column_type
            .split(' ')
            .next()
            .is_some_and(|kind| matches!(kind, "int" | "mediumint" | "smallint" | "tinyint"));
        let allowed = if self.at("NULL") && nullable {
            "NULL"
        } else if self.at("CURRENT_TIMESTAMP") && column_type == "timestamp" {
            "CURRENT_TIMESTAMP"
        } else if self.at("0") && integer {
            "0"
        } else if self.at("1") && column_type == "tinyint" {
            "1"
        } else {
            return Err("unmodeled observed CREATE default".into());
        };
        self.keyword(allowed)?;
        Ok(Some(allowed.into()))
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
        if column_type != "timestamp" {
            return Err("ON UPDATE requires TIMESTAMP".into());
        }
        self.keyword("ON")?;
        self.keyword("UPDATE")?;
        self.keyword("CURRENT_TIMESTAMP")?;
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
        for kind in ["INT", "MEDIUMINT", "SMALLINT"] {
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
        if self.at("DECIMAL") {
            for token in ["DECIMAL", "(", "4", ",", "3", ")"] {
                self.keyword(token)?;
            }
            return Ok("decimal(4,3)".into());
        }
        self.keyword("VARCHAR")?;
        self.keyword("(")?;
        let length = self
            .tokens
            .get(self.position)
            .cloned()
            .ok_or("missing VARCHAR length")?;
        let value = length
            .parse::<u32>()
            .map_err(|_| "invalid VARCHAR length")?;
        if value == 0 || value.to_string() != length {
            return Err("noncanonical VARCHAR length".into());
        }
        self.keyword(&length)?;
        self.keyword(")")?;
        Ok(format!("varchar({value})"))
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
