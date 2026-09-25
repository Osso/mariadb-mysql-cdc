use super::*;

#[test]
fn basic_ddl_add_scalar_columns_preserves_definitions_and_defaults() {
    for (definition, column_type, data_type, default, nullable) in [
        (
            "INT UNSIGNED DEFAULT NULL",
            "int unsigned",
            "int",
            None,
            true,
        ),
        (
            "BIGINT NOT NULL DEFAULT -9223372036854775808",
            "bigint",
            "bigint",
            Some("-9223372036854775808"),
            false,
        ),
        (
            "DECIMAL(12,2) NOT NULL DEFAULT -12.30",
            "decimal(12,2)",
            "decimal",
            Some("-12.30"),
            false,
        ),
        (
            "BOOLEAN NOT NULL DEFAULT 1",
            "tinyint",
            "tinyint",
            Some("1"),
            false,
        ),
        (
            "VARCHAR(80) NOT NULL DEFAULT ''",
            "varchar(80)",
            "varchar",
            Some(""),
            false,
        ),
        ("DATE DEFAULT NULL", "date", "date", None, true),
        ("TIME(3) NULL", "time(3)", "time", None, true),
        ("DATETIME(3) NULL", "datetime(3)", "datetime", None, true),
        ("TIMESTAMP(6) NULL", "timestamp(6)", "timestamp", None, true),
        (
            "VARBINARY(64) NULL",
            "varbinary(64)",
            "varbinary",
            None,
            true,
        ),
        ("LONGBLOB NULL", "longblob", "longblob", None, true),
        ("LONGTEXT NULL", "longtext", "longtext", None, true),
        ("INT NOT NULL", "int", "int", None, false),
    ] {
        let sql = format!("ALTER TABLE accounts ADD COLUMN basic_value {definition}");
        parse_production_alter_table_ast(&sql)
            .unwrap_or_else(|error| panic!("{definition}: {error}"));
        let operation = parse_ddl_operation(&sql).expect("operation");
        let target = semantic_snapshot(3, None);
        let evidence = build_semantic_evidence(&operation, &target, &target)
            .unwrap_or_else(|error| panic!("{definition}: {error}"));
        let mut expected = target.clone();
        expected.inventory.tables[0].columns.push(ColumnInventory {
            name: "basic_value".into(),
            ordinal_position: 3,
            column_type: column_type.into(),
            data_type: data_type.into(),
            is_nullable: nullable,
            character_set: matches!(data_type, "varchar" | "longtext").then(|| "utf8mb4".into()),
            collation: matches!(data_type, "varchar" | "longtext")
                .then(|| "utf8mb4_unicode_ci".into()),
            default_value: default.map(str::to_owned),
            extra: String::new(),
            comment: String::new(),
            generated: None,
        });
        assert_eq!(
            evidence.expected_post_state,
            super::super::canonical::observe_operation_state(&expected, &operation).unwrap(),
            "{definition}"
        );
        let rendered = super::super::transform::transform_production_alter_table(&sql)
            .expect("render")
            .target_sql
            .expect("SQL");
        if !nullable && default.is_none() {
            assert_eq!(
                rendered,
                "ALTER TABLE `accounts` ADD COLUMN `basic_value` INT NOT NULL"
            );
        }
    }
}

#[test]
fn basic_ddl_modify_preserves_order_and_indexes_and_replaces_attributes() {
    let target = semantic_snapshot(3, None);
    let sql = "ALTER TABLE accounts MODIFY COLUMN handle VARCHAR(120) NOT NULL DEFAULT '' COMMENT 'display name'";
    let operation = parse_ddl_operation(sql).unwrap();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let mut expected = target.clone();
    let column = &mut expected.inventory.tables[0].columns[1];
    column.column_type = "varchar(120)".into();
    column.is_nullable = false;
    column.default_value = Some(String::new());
    column.character_set = Some("utf8mb4".into());
    column.collation = Some("utf8mb4_unicode_ci".into());
    column.comment = "display name".into();
    assert_eq!(
        evidence.expected_post_state,
        super::super::canonical::observe_operation_state(&expected, &operation).unwrap()
    );
    assert_eq!(
        super::super::transform::transform_production_alter_table(sql)
            .unwrap()
            .target_sql
            .as_deref(),
        Some(
            "ALTER TABLE `accounts` MODIFY COLUMN `handle` VARCHAR(120) NOT NULL DEFAULT '' COMMENT 'display name'"
        )
    );
}

#[test]
fn basic_ddl_rename_column_preserves_index_reference() {
    let target = semantic_snapshot(3, None);
    let sql = "ALTER TABLE accounts RENAME COLUMN handle TO nickname";
    let operation = parse_ddl_operation(sql).unwrap();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let mut expected = target.clone();
    expected.inventory.tables[0].columns[1].name = "nickname".into();
    expected.inventory.indexes[0].columns[0].name = "nickname".into();
    assert_eq!(
        evidence.expected_post_state,
        super::super::canonical::observe_operation_state(&expected, &operation).unwrap()
    );
    assert_eq!(
        super::super::transform::transform_production_alter_table(sql)
            .unwrap()
            .target_sql
            .as_deref(),
        Some("ALTER TABLE `accounts` RENAME COLUMN `handle` TO `nickname`")
    );
}

#[test]
fn basic_ddl_rejects_contradictory_and_out_of_range_defaults() {
    for definition in [
        "INT NOT NULL DEFAULT NULL",
        "TINYINT DEFAULT 128",
        "INT UNSIGNED DEFAULT -1",
        "DECIMAL(3,2) DEFAULT 12.34",
        "INT `DEFAULT` 3",
        "INT DEFAULT `NULL`",
        "VARCHAR(8) NOT NULL DEFAULT NULL",
        "TIME(7) NULL",
        "INT ZEROFILL",
    ] {
        let sql = format!("ALTER TABLE accounts ADD COLUMN value {definition}");
        assert!(
            parse_production_alter_table_ast(&sql).is_err(),
            "{definition}"
        );
    }
}

#[test]
fn basic_ddl_drop_plain_column_and_index_derive_target_post_state() {
    let target = semantic_snapshot(3, None);
    let sql = "ALTER TABLE accounts DROP INDEX idx_handle, DROP COLUMN handle";
    let operation = parse_ddl_operation(sql).expect("operation");
    let evidence =
        build_semantic_evidence(&operation, &target, &target).expect("basic drop evidence");
    let mut expected = target.clone();
    expected.inventory.indexes.clear();
    expected.inventory.tables[0].columns.pop();
    assert_eq!(
        evidence.expected_post_state,
        super::super::canonical::observe_operation_state(&expected, &operation).unwrap()
    );
    assert_eq!(
        super::super::transform::transform_production_alter_table(sql)
            .unwrap()
            .target_sql
            .as_deref(),
        Some("ALTER TABLE `accounts` DROP INDEX `idx_handle`, DROP COLUMN `handle`")
    );
}
