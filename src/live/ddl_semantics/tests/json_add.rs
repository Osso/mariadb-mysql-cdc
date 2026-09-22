use super::*;

const JSON_ADD: &str =
    include_str!("../../../../fixtures/ddl/alter-releases-pages-source-layout.sql");

#[test]
fn json_add_preserves_alias_validation_and_position() {
    let result = super::super::transform::transform_production_alter_table(JSON_ADD)
        .expect("JSON ADD must translate");
    assert_eq!(
        result.target_sql.as_deref(),
        Some(
            "ALTER TABLE `releases_pages` ADD COLUMN `source_layout` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL AFTER `comic_asset_id`, ADD CHECK (JSON_VALID(`source_layout`))"
        )
    );
    let history = JSON_ADD.replace("`releases_pages`", "`releases_pages_history`");
    assert!(super::super::transform::transform_production_alter_table(&history).is_ok());
}

#[test]
fn json_add_rejects_unmodeled_type_options() {
    for options in [
        "JSON(10) DEFAULT NULL",
        "JSON NOT NULL",
        "JSON DEFAULT '{}'",
        "JSON CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL",
        "`JSON` DEFAULT NULL",
    ] {
        let sql = JSON_ADD.replace("JSON DEFAULT NULL", options);
        assert!(parse_production_alter_table_ast(&sql).is_err(), "{sql}");
    }
}

#[test]
fn json_add_expected_metadata_preserves_text_alias() {
    let target = semantic_snapshot(7, Some(8));
    let sql = "ALTER TABLE accounts ADD COLUMN IF NOT EXISTS source_layout JSON DEFAULT NULL AFTER handle";
    let operation = parse_ddl_operation(sql).expect("JSON ADD operation");
    let evidence =
        build_semantic_evidence(&operation, &target, &target).expect("JSON ADD evidence");
    let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
    let column = post["definition"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|column| column["name"] == "source_layout")
        .unwrap();
    assert_eq!(column["column_type"], "longtext");
    assert_eq!(column["data_type"], "longtext");
    assert_eq!(column["character_set"], "utf8mb4");
    assert_eq!(column["collation"], "utf8mb4_bin");
    assert_eq!(column["is_nullable"], true);
    assert_eq!(column["default_value"], serde_json::Value::Null);
    assert_eq!(column["ordinal_position"], 3);
}
