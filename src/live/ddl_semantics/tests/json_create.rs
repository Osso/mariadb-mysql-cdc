use super::*;

const JSON_CREATE: &str = "CREATE TABLE IF NOT EXISTS spotlight (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, payload JSON DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

#[test]
fn json_create_preserves_mariadb_text_storage_and_validation() {
    parse_fixture_create_table(JSON_CREATE).expect("JSON CREATE must be modeled");
    let post = reader_memory_create_post_state(JSON_CREATE);
    let payload = post["definition"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|column| column["name"] == "payload")
        .unwrap();
    assert_eq!(payload["column_type"], "longtext");
    assert_eq!(payload["character_set"], "utf8mb4");
    assert_eq!(payload["collation"], "utf8mb4_bin");
    assert_eq!(payload["default_value"], serde_json::Value::Null);
    let transformed = transform_fixture_create_table(JSON_CREATE).expect("JSON transform");
    assert_eq!(
        transformed.target_sql.as_deref(),
        Some(
            "CREATE TABLE `spotlight` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `payload` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL, PRIMARY KEY (`id`), CHECK (JSON_VALID(`payload`))) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
    );
}

#[test]
fn json_create_rejects_unmodeled_string_default() {
    assert!(
        parse_fixture_create_table(&JSON_CREATE.replace("JSON DEFAULT NULL", "JSON DEFAULT '{}'"))
            .is_err()
    );
}
