use super::*;

const RELEASES_INSTANT_ADD: &str = "/* inert */ ALTER TABLE releases ADD COLUMN free_read_start DATETIME NULL DEFAULT NULL, ADD COLUMN free_read_end DATETIME NULL DEFAULT NULL, ALGORITHM=INSTANT";

#[test]
fn instant_add_transforms_observed_releases_columns_and_derives_post_state() {
    let transformation = transform_production_alter_table(RELEASES_INSTANT_ADD)
        .expect("observed nullable DATETIME additions must transform");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `releases` ADD COLUMN `free_read_start` DATETIME NULL DEFAULT NULL, ADD COLUMN `free_read_end` DATETIME NULL DEFAULT NULL, ALGORITHM=INSTANT"
        )
    );

    let target = target_snapshot("releases");
    let operation = parse_ddl_operation(RELEASES_INSTANT_ADD).expect("ALTER operation");
    let evidence = build_semantic_evidence(&operation, &target, &target)
        .expect("INSTANT additions must derive semantic evidence");
    let expected = expected_with_columns(
        &target,
        &[
            ("free_read_start", "datetime", "datetime", true, None),
            ("free_read_end", "datetime", "datetime", true, None),
        ],
    );

    assert_eq!(
        evidence.expected_post_state,
        super::super::canonical::observe_operation_state(&expected, &operation)
            .expect("expected releases state")
    );
}

#[test]
fn instant_add_supports_other_tables_and_single_or_mixed_modeled_columns() {
    let cases = [
        (
            "ALTER TABLE archive_entries ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, ALGORITHM=INSTANT",
            "ALTER TABLE `archive_entries` ADD COLUMN `indexed_at` DATETIME NULL DEFAULT NULL, ALGORITHM=INSTANT",
            "archive_entries",
            vec![("indexed_at", "datetime", "datetime", true, None)],
        ),
        (
            "ALTER TABLE comic_imports ADD COLUMN finished_at DATETIME NULL DEFAULT NULL, ADD COLUMN attempt_count SMALLINT UNSIGNED NULL DEFAULT NULL, ALGORITHM=INSTANT",
            "ALTER TABLE `comic_imports` ADD COLUMN `finished_at` DATETIME NULL DEFAULT NULL, ADD COLUMN `attempt_count` SMALLINT UNSIGNED NULL DEFAULT NULL, ALGORITHM=INSTANT",
            "comic_imports",
            vec![
                ("finished_at", "datetime", "datetime", true, None),
                ("attempt_count", "smallint unsigned", "smallint", true, None),
            ],
        ),
    ];

    for (source_sql, expected_sql, table, columns) in cases {
        let transformation = transform_production_alter_table(source_sql)
            .unwrap_or_else(|error| panic!("INSTANT ADD rejected for {table}: {error}"));
        assert_eq!(transformation.target_sql.as_deref(), Some(expected_sql));

        let target = target_snapshot(table);
        let operation = parse_ddl_operation(source_sql).expect("ALTER operation");
        let evidence = build_semantic_evidence(&operation, &target, &target)
            .unwrap_or_else(|error| panic!("semantic evidence failed for {table}: {error}"));
        let expected = expected_with_columns(&target, &columns);
        assert_eq!(
            evidence.expected_post_state,
            super::super::canonical::observe_operation_state(&expected, &operation)
                .expect("expected table state"),
            "{table}"
        );
    }
}

#[test]
fn instant_add_rejects_unsupported_options_clauses_and_comments() {
    for sql in [
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, ALGORITHM=INSTANT, LOCK=NONE",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, ALGORITHM=INSTANT, LOCK=SHARED",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, DROP COLUMN handle, ALGORITHM=INSTANT",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, ADD INDEX idx_indexed_at (indexed_at), ALGORITHM=INSTANT",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, ALTER COLUMN handle SET DEFAULT 'x', ALGORITHM=INSTANT",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, MODIFY COLUMN handle VARCHAR(80), ALGORITHM=INSTANT",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, ALGORITHM=DEFAULT",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, ALGORITHM=INSTANT, ALGORITHM=INSTANT",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME NULL DEFAULT NULL, LOCK=NONE",
        "ALTER TABLE releases ADD COLUMN indexed_at DATETIME /* inert */ NULL DEFAULT NULL, ALGORITHM=INSTANT",
    ] {
        assert!(
            !supports_production_alter_table(sql),
            "admitted unsupported INSTANT ALTER: {sql}"
        );
        assert!(
            transform_production_alter_table(sql).is_err(),
            "transformed unsupported INSTANT ALTER: {sql}"
        );
    }
}

fn target_snapshot(table_name: &str) -> SemanticSchemaSnapshot {
    let mut target = semantic_snapshot(0, None);
    target.inventory.tables[0].name = table_name.to_string();
    target.inventory.indexes.clear();
    target.table_runtime.clear();
    target
}

fn expected_with_columns(
    target: &SemanticSchemaSnapshot,
    additions: &[(&str, &str, &str, bool, Option<&str>)],
) -> SemanticSchemaSnapshot {
    let mut expected = target.clone();
    let columns = &mut expected.inventory.tables[0].columns;
    for (name, column_type, data_type, nullable, default_value) in additions {
        columns.push(ColumnInventory {
            name: (*name).to_string(),
            ordinal_position: columns.len() as u32 + 1,
            column_type: (*column_type).to_string(),
            data_type: (*data_type).to_string(),
            is_nullable: *nullable,
            character_set: None,
            collation: None,
            default_value: default_value.map(str::to_string),
            extra: String::new(),
            comment: String::new(),
            generated: None,
        });
    }
    expected
}
