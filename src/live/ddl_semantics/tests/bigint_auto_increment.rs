use super::*;

#[test]
fn observed_unsigned_auto_increment_emits_sql_and_expected_metadata() {
    for kind in ["INT", "BIGINT"] {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS spotlights (id {kind} UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        );
        let inventory = LiveDdlSemanticInventory::new(
            InventoryConfig::default(),
            InventoryConfig::default(),
            "globalcomix".into(),
            "globalcomix".into(),
        );
        let emitted = inventory
            .transform_sql(&sql)
            .expect("unsigned auto-increment CREATE");
        assert_eq!(
            emitted.target_sql,
            Some(format!(
                "CREATE TABLE `spotlights` (`id` {kind} UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
            ))
        );
        let operation = parse_ddl_operation(&sql).expect("CREATE operation");
        let mut target = semantic_snapshot(1, Some(2));
        target.inventory.tables.clear();
        target.inventory.indexes.clear();
        target.inventory.foreign_keys.clear();
        target.table_runtime.clear();
        let head = crate::inventory::SourceMasterCoordinate {
            file: "mysql-bin.000002".into(),
            position: 1000,
        };
        let evidence = build_fenced_create_table_evidence(
            &operation,
            &target,
            &crate::inventory::SchemaDefaults {
                character_set: "utf8mb4".into(),
                collation: "utf8mb4_unicode_ci".into(),
            },
            "mysql-bin.000001",
            500,
            &head,
            &head,
        )
        .expect("expected CREATE metadata");
        let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
        let column = &post["definition"]["columns"][0];
        assert_eq!(
            column["column_type"],
            format!("{} unsigned", kind.to_ascii_lowercase())
        );
        assert_eq!(column["data_type"], kind.to_ascii_lowercase());
        assert_eq!(column["extra"], "auto_increment");
        assert_eq!(column["is_nullable"], false);
    }
}

#[test]
fn observed_unsigned_auto_increment_rejects_nullable_and_noninteger_columns() {
    for definition in [
        "BIGINT UNSIGNED NULL AUTO_INCREMENT",
        "BIGINT UNSIGNED AUTO_INCREMENT",
        "INT UNSIGNED NULL AUTO_INCREMENT",
        "VARCHAR(32) NOT NULL AUTO_INCREMENT",
        "BIGINT NOT NULL AUTO_INCREMENT",
    ] {
        let sql = format!(
            "CREATE TABLE spotlights (id {definition} PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        );
        assert!(parse_fixture_create_table(&sql).is_err(), "{definition}");
    }
}
