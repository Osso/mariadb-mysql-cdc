use super::*;

fn altered(
    sql: &str,
    target: &SemanticSchemaSnapshot,
) -> (serde_json::Value, serde_json::Value, String) {
    let operation = parse_ddl_operation(sql).expect("typed operation");
    let evidence = build_semantic_evidence(&operation, target, target).expect("validated state");
    (
        serde_json::from_str(&evidence.canonical_ast).expect("canonical AST"),
        serde_json::from_str(&evidence.expected_post_state).expect("post-state"),
        super::super::transform::transform_production_alter_table_with_target(sql, target)
            .expect("target SQL")
            .target_sql
            .expect("executable SQL"),
    )
}

#[test]
fn change_column_renames_key_references_and_preserves_position() {
    let target = semantic_snapshot(7, Some(8));
    let sql = "ALTER TABLE accounts CHANGE COLUMN handle alias VARCHAR(128) NOT NULL";
    let (ast, post, rendered) = altered(sql, &target);
    assert_eq!(
        ast["parsed_alter_table"]["clauses"][0]["kind"],
        "change_column"
    );
    assert_eq!(
        ast["parsed_alter_table"]["clauses"][0]["old_name"],
        "handle"
    );
    assert_eq!(
        rendered,
        "ALTER TABLE `accounts` CHANGE COLUMN `handle` `alias` VARCHAR(128) NOT NULL"
    );
    let mut expected = target.clone();
    expected.inventory.tables[0].columns[1].name = "alias".into();
    expected.inventory.tables[0].columns[1].column_type = "varchar(128)".into();
    expected.inventory.tables[0].columns[1].is_nullable = false;
    expected.inventory.tables[0].columns[1].character_set = Some("utf8mb4".into());
    expected.inventory.tables[0].columns[1].collation = Some("utf8mb4_unicode_ci".into());
    for index in &mut expected.inventory.indexes {
        for part in &mut index.columns {
            if part.name == "handle" {
                part.name = "alias".into();
            }
        }
    }
    let operation = parse_ddl_operation(sql).unwrap();
    assert_eq!(
        post,
        serde_json::from_str::<serde_json::Value>(
            &super::super::canonical::observe_operation_state(&expected, &operation).unwrap()
        )
        .unwrap()
    );
}

#[test]
fn change_column_renames_primary_and_foreign_key_references() {
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.tables[0].columns[0].extra.clear();
    target.inventory.foreign_keys.push(ForeignKeyInventory {
        table: "accounts".into(),
        name: "fk_accounts_parent".into(),
        columns: vec!["id".into()],
        referenced_schema: "fixture_cdc".into(),
        referenced_table: "accounts".into(),
        referenced_columns: vec!["id".into()],
    });
    let sql = "ALTER TABLE accounts CHANGE id account_id BIGINT UNSIGNED NOT NULL";
    let (_, post, _) = altered(sql, &target);
    let mut expected = target.clone();
    expected.inventory.tables[0].columns[0].name = "account_id".into();
    expected.inventory.tables[0].primary_key[0] = "account_id".into();
    expected.inventory.foreign_keys[0].columns[0] = "account_id".into();
    expected.inventory.foreign_keys[0].referenced_columns[0] = "account_id".into();
    let operation = parse_ddl_operation(sql).unwrap();
    let state = super::super::canonical::observe_operation_state(&expected, &operation).unwrap();
    assert_eq!(
        post,
        serde_json::from_str::<serde_json::Value>(&state).unwrap()
    );
}

#[test]
fn first_and_after_use_remaining_columns_for_replacement() {
    let mut target = semantic_snapshot(7, Some(8));
    let mut tail = target.inventory.tables[0].columns[1].clone();
    tail.name = "tail".into();
    tail.ordinal_position = 3;
    target.inventory.tables[0].columns.push(tail);
    for (sql, names) in [
        (
            "ALTER TABLE accounts ADD newcomer INT FIRST",
            vec!["newcomer", "id", "handle", "tail"],
        ),
        (
            "ALTER TABLE accounts MODIFY handle VARCHAR(64) FIRST",
            vec!["handle", "id", "tail"],
        ),
        (
            "ALTER TABLE accounts CHANGE handle alias VARCHAR(64) AFTER tail",
            vec!["id", "tail", "alias"],
        ),
    ] {
        let (ast, post, rendered) = altered(sql, &target);
        let columns = post["definition"]["columns"].as_array().expect("columns");
        assert_eq!(
            columns
                .iter()
                .map(|column| column["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            names,
            "{sql}"
        );
        assert_eq!(
            columns
                .iter()
                .map(|column| column["ordinal_position"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            (1..=columns.len() as u64).collect::<Vec<_>>(),
            "{sql}"
        );
        assert!(
            rendered.contains(if sql.contains("FIRST") {
                " FIRST"
            } else {
                " AFTER `tail`"
            }),
            "{rendered}"
        );
        assert_eq!(
            ast["parsed_alter_table"]["clauses"][0]["first"],
            if sql.contains("FIRST") {
                serde_json::json!(true)
            } else {
                serde_json::Value::Null
            }
        );
    }
    for sql in [
        "ALTER TABLE accounts MODIFY handle VARCHAR(64) AFTER handle",
        "ALTER TABLE accounts CHANGE handle alias VARCHAR(64) AFTER handle",
        "ALTER TABLE accounts ADD newcomer INT FIRST AFTER id",
    ] {
        match super::super::transform::parse_production_alter_table_ast(sql) {
            Err(_) => {}
            Ok(_) => {
                let operation = parse_ddl_operation(sql).expect("operation");
                assert!(
                    build_semantic_evidence(&operation, &target, &target).is_err(),
                    "{sql}"
                );
            }
        }
    }
}

#[test]
fn alter_default_preserves_column_metadata_and_distinguishes_null_from_drop() {
    let mut target = semantic_snapshot(7, Some(8));
    let column = &mut target.inventory.tables[0].columns[1];
    column.column_type = "timestamp".into();
    column.data_type = "timestamp".into();
    column.extra = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP".into();
    column.default_value = Some("CURRENT_TIMESTAMP".into());
    for (suffix, value, extra_fragment) in [
        (
            "SET DEFAULT '2026-09-25 12:00:00'",
            Some("2026-09-25 12:00:00"),
            "on update CURRENT_TIMESTAMP",
        ),
        ("SET DEFAULT NULL", None, "on update CURRENT_TIMESTAMP"),
        ("DROP DEFAULT", None, "on update CURRENT_TIMESTAMP"),
    ] {
        let sql = format!("ALTER TABLE accounts ALTER COLUMN handle {suffix}");
        let (ast, post, rendered) = altered(&sql, &target);
        assert_eq!(
            ast["parsed_alter_table"]["clauses"][0]["kind"],
            "alter_column_default"
        );
        assert_eq!(
            rendered,
            format!("ALTER TABLE `accounts` ALTER COLUMN `handle` {suffix}")
        );
        assert_eq!(
            ast["parsed_alter_table"]["clauses"][0]["default"],
            match suffix {
                "SET DEFAULT '2026-09-25 12:00:00'" =>
                    serde_json::json!({"string": "2026-09-25 12:00:00"}),
                "SET DEFAULT NULL" => serde_json::json!({"null": true}),
                _ => serde_json::Value::Null,
            }
        );
        let mut expected = target.clone();
        expected.inventory.tables[0].columns[1].default_value = value.map(str::to_string);
        expected.inventory.tables[0].columns[1].extra = extra_fragment.into();
        let operation = parse_ddl_operation(&sql).unwrap();
        assert_eq!(
            post,
            serde_json::from_str::<serde_json::Value>(
                &super::super::canonical::observe_operation_state(&expected, &operation).unwrap()
            )
            .unwrap(),
            "{sql}"
        );
    }
    assert!(
        super::super::transform::parse_production_alter_table_ast(
            "ALTER TABLE accounts ALTER COLUMN handle SET DEFAULT (UUID())"
        )
        .is_err()
    );
}

#[test]
fn default_literals_normalize_using_existing_target_type_and_reject_mismatches() {
    let mut target = semantic_snapshot(7, Some(8));
    let column = &mut target.inventory.tables[0].columns[1];
    column.column_type = "decimal(6,2)".into();
    column.data_type = "decimal".into();
    column.character_set = None;
    column.collation = None;
    let (_, post, rendered) = altered(
        "ALTER TABLE accounts ALTER COLUMN handle SET DEFAULT -12.50",
        &target,
    );
    assert_eq!(
        rendered,
        "ALTER TABLE `accounts` ALTER COLUMN `handle` SET DEFAULT -12.50"
    );
    assert_eq!(post["definition"]["columns"][1]["default_value"], "-12.50");
    for suffix in [
        "SET DEFAULT 'not-a-number'",
        "SET DEFAULT 1e3000",
        "SET DEFAULT (1+2)",
    ] {
        let sql = format!("ALTER TABLE accounts ALTER COLUMN handle {suffix}");
        if super::super::transform::parse_production_alter_table_ast(&sql).is_ok() {
            let operation = parse_ddl_operation(&sql).expect("operation");
            assert!(
                build_semantic_evidence(&operation, &target, &target).is_err(),
                "{sql}"
            );
        }
    }
    target.inventory.tables[0].columns[1].column_type = "varchar(64)".into();
    target.inventory.tables[0].columns[1].data_type = "varchar".into();
    let operation =
        parse_ddl_operation("ALTER TABLE accounts ALTER COLUMN handle SET DEFAULT 12").unwrap();
    assert!(build_semantic_evidence(&operation, &target, &target).is_err());
}

#[test]
fn text_defaults_get_expression_metadata_and_drop_removes_only_default_marker() {
    let mut target = semantic_snapshot(7, Some(8));
    let column = &mut target.inventory.tables[0].columns[1];
    column.column_type = "text".into();
    column.data_type = "text".into();
    let (_, post, rendered) = altered(
        "ALTER TABLE accounts ALTER COLUMN handle SET DEFAULT '{}'",
        &target,
    );
    assert_eq!(
        rendered,
        "ALTER TABLE `accounts` MODIFY COLUMN `handle` TEXT NULL DEFAULT (_utf8mb4'{}')"
    );
    assert_eq!(
        post["definition"]["columns"][1]["default_value"],
        "_utf8mb4\\'{}\\'"
    );
    assert_eq!(
        post["definition"]["columns"][1]["extra"],
        "DEFAULT_GENERATED"
    );
    target.inventory.tables[0].columns[1].default_value = Some("_utf8mb4\\'{}\\'".into());
    target.inventory.tables[0].columns[1].extra = "DEFAULT_GENERATED".into();
    let (_, post, _) = altered(
        "ALTER TABLE accounts ALTER COLUMN handle DROP DEFAULT",
        &target,
    );
    assert_eq!(
        post["definition"]["columns"][1]["default_value"],
        serde_json::Value::Null
    );
    assert_eq!(post["definition"]["columns"][1]["extra"], "");
}

#[test]
fn text_default_uses_target_type_and_mysql_expression_sql() {
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.tables[0].columns[1].column_type = "text".into();
    target.inventory.tables[0].columns[1].data_type = "text".into();
    let sql = "ALTER TABLE accounts ALTER COLUMN handle SET DEFAULT '{}'";
    let rendered =
        super::super::transform::transform_production_alter_table_with_target(sql, &target)
            .expect("type-aware transformation")
            .target_sql
            .unwrap();
    assert_eq!(
        rendered,
        "ALTER TABLE `accounts` MODIFY COLUMN `handle` TEXT NULL DEFAULT (_utf8mb4'{}')"
    );
    let (_, post, _) = altered(sql, &target);
    assert_eq!(
        post["definition"]["columns"][1]["default_value"],
        "_utf8mb4\\'{}\\'"
    );
}

#[test]
fn text_default_after_add_uses_same_statement_column_state() {
    let target = semantic_snapshot(7, Some(8));
    let sql = "ALTER TABLE accounts ADD COLUMN memo TEXT, ALTER COLUMN memo SET DEFAULT '{}'";
    let rendered =
        super::super::transform::transform_production_alter_table_with_target(sql, &target)
            .expect("sequential transformation")
            .target_sql
            .unwrap();
    assert_eq!(
        rendered,
        "ALTER TABLE `accounts` ADD COLUMN `memo` TEXT NULL DEFAULT NULL, MODIFY COLUMN `memo` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT (_utf8mb4'{}')"
    );
    let (_, post, _) = altered(sql, &target);
    assert_eq!(post["definition"]["columns"][2]["name"], "memo");
    assert_eq!(
        post["definition"]["columns"][2]["default_value"],
        "_utf8mb4\\'{}\\'"
    );
    assert_eq!(
        post["definition"]["columns"][2]["extra"],
        "DEFAULT_GENERATED"
    );
}

#[test]
fn default_null_is_rejected_on_not_null_but_drop_is_valid() {
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.tables[0].columns[1].is_nullable = false;
    let null =
        parse_ddl_operation("ALTER TABLE accounts ALTER COLUMN handle SET DEFAULT NULL").unwrap();
    assert!(build_semantic_evidence(&null, &target, &target).is_err());
    let (_, post, _) = altered(
        "ALTER TABLE accounts ALTER COLUMN handle DROP DEFAULT",
        &target,
    );
    assert_eq!(
        post["definition"]["columns"][1]["default_value"],
        serde_json::Value::Null
    );
}
