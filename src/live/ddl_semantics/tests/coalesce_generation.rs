use super::*;

const ADD_SLOT: &str = "ADD COLUMN sample_slot VARCHAR(64) AS (COALESCE(`EXPERIMENT_KEY`, '')) PERSISTENT COMMENT 'per-run uniqueness slot'";

fn coalesce_target() -> SemanticSchemaSnapshot {
    let mut target = semantic_snapshot(3, None);
    let reference = &mut target.inventory.tables[0].columns[1];
    reference.name = "experiment_key".into();
    reference.column_type = "varchar(64)".into();
    reference.data_type = "varchar".into();
    reference.is_nullable = true;
    reference.default_value = None;
    reference.character_set = Some("utf8mb4".into());
    reference.collation = Some("utf8mb4_unicode_ci".into());
    target
}

#[test]
fn coalesce_generation_alter_renders_and_records_nullable_stored_metadata() {
    for storage in ["PERSISTENT", "STORED"] {
        let sql = format!(
            "ALTER TABLE accounts {}",
            ADD_SLOT.replace("PERSISTENT", storage)
        );
        let target = coalesce_target();
        let operation = parse_ddl_operation(&sql).expect("COALESCE operation");
        let original_operation = operation.clone();
        let evidence =
            build_semantic_evidence(&operation, &target, &target).expect("COALESCE evidence");
        let rendered =
            super::super::transform::transform_production_alter_table_with_target(&sql, &target)
                .unwrap()
                .target_sql
                .unwrap();
        assert_eq!(
            rendered,
            "ALTER TABLE `accounts` ADD COLUMN `sample_slot` VARCHAR(64) GENERATED ALWAYS AS (COALESCE(`experiment_key`, _utf8mb4'')) STORED COMMENT 'per-run uniqueness slot'"
        );
        let canonical: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).unwrap();
        assert_eq!(
            canonical["parsed_alter_table"]["clauses"][0]["generated"]["expression"],
            "coalesce(`experiment_key`,_utf8mb4\\'\\')"
        );
        let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
        let slot = column(&post, "sample_slot");
        assert_eq!(slot["column_type"], "varchar(64)");
        assert_eq!(slot["character_set"], "utf8mb4");
        assert_eq!(slot["collation"], "utf8mb4_unicode_ci");
        assert_eq!(slot["is_nullable"], true);
        assert!(slot["default_value"].is_null());
        assert_eq!(slot["extra"], "STORED GENERATED");
        assert_eq!(slot["comment"], "per-run uniqueness slot");
        assert_eq!(
            slot["generated"]["expression"],
            "coalesce(`experiment_key`,_utf8mb4\\'\\')"
        );
        assert_eq!(slot["generated"]["generation_kind"], "STORED");
        assert_eq!(
            operation, original_operation,
            "source operation must remain unchanged"
        );
    }
}

#[test]
fn coalesce_generation_binds_earlier_same_alter_add() {
    let sql = format!(
        "ALTER TABLE accounts ADD COLUMN experiment_key VARCHAR(64) DEFAULT NULL, {ADD_SLOT}"
    );
    let target = semantic_snapshot(3, None);
    let operation = parse_ddl_operation(&sql).unwrap();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
    let canonical: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).unwrap();
    assert_eq!(
        canonical["parsed_alter_table"]["clauses"][1]["generated"]["expression"],
        "coalesce(`experiment_key`,_utf8mb4\\'\\')"
    );
    assert_eq!(column(&post, "sample_slot")["ordinal_position"], 4);
    assert_eq!(
        column(&post, "sample_slot")["generated"]["expression"],
        "coalesce(`experiment_key`,_utf8mb4\\'\\')"
    );
    assert!(
        super::super::transform::transform_production_alter_table_with_target(&sql, &target)
            .unwrap()
            .target_sql
            .unwrap()
            .contains("COALESCE(`experiment_key`, _utf8mb4'')")
    );
}

#[test]
fn coalesce_generation_observed_verdict_slot_in_standalone_alter() {
    // Production clauses without the guards/CHECK/drop owned by the integrating slice.
    let sql = "ALTER TABLE `assistant_quality_verdicts`
        ADD COLUMN `experiment_key` varchar(64) DEFAULT NULL COMMENT 'NULL for the random sample',
        ADD COLUMN `rubric_version` int(11) UNSIGNED DEFAULT NULL,
        ADD COLUMN `sample_slot` varchar(64) AS (COALESCE(`experiment_key`, '')) PERSISTENT
            COMMENT 'per-run uniqueness slot: empty for the random sample, else the experiment key' AFTER `rubric_version`";
    let mut target = semantic_snapshot(3, None);
    target.inventory.tables[0].name = "assistant_quality_verdicts".into();
    let operation = parse_ddl_operation(sql).unwrap();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
    let slot = column(&post, "sample_slot");
    assert_eq!(slot["ordinal_position"], 5);
    assert_eq!(
        slot["comment"],
        "per-run uniqueness slot: empty for the random sample, else the experiment key"
    );
    assert_eq!(
        slot["generated"]["expression"],
        "coalesce(`experiment_key`,_utf8mb4\\'\\')"
    );
    let rendered =
        super::super::transform::transform_production_alter_table_with_target(sql, &target)
            .unwrap()
            .target_sql
            .unwrap();
    assert!(rendered.ends_with("ADD COLUMN `sample_slot` VARCHAR(64) GENERATED ALWAYS AS (COALESCE(`experiment_key`, _utf8mb4'')) STORED COMMENT 'per-run uniqueness slot: empty for the random sample, else the experiment key' AFTER `rubric_version`"));
}

#[test]
fn coalesce_generation_rejects_unmodeled_expression_forms() {
    for (from, to) in [
        ("''", "'fallback'"),
        ("''", "NULL"),
        ("''", "0"),
        ("COALESCE", "IFNULL"),
        ("COALESCE", "`COALESCE`"),
        ("`EXPERIMENT_KEY`", "`EXPERIMENT_KEY` + 1"),
        ("''))", "'', ''))"),
        ("PERSISTENT", "VIRTUAL"),
        ("VARCHAR(64)", "CHAR(64)"),
        ("VARCHAR(64)", "TINYINT UNSIGNED"),
        ("PERSISTENT", "PERSISTENT DEFAULT NULL"),
        ("PERSISTENT", "PERSISTENT NOT NULL"),
    ] {
        let sql = format!("ALTER TABLE accounts {}", ADD_SLOT.replace(from, to));
        assert!(
            parse_production_alter_table_ast(&sql).is_err(),
            "accepted {sql}"
        );
    }
}

#[test]
fn coalesce_generation_rejects_incompatible_or_nonordinary_references() {
    let sql = format!("ALTER TABLE accounts {ADD_SLOT}");
    for invalid in [
        "missing",
        "length",
        "type",
        "encoding",
        "generated",
        "auto_increment",
    ] {
        let mut target = coalesce_target();
        let reference = &mut target.inventory.tables[0].columns[1];
        match invalid {
            "missing" => reference.name = "other".into(),
            "length" => reference.column_type = "varchar(32)".into(),
            "type" => {
                reference.column_type = "char(64)".into();
                reference.data_type = "char".into();
            }
            "encoding" => reference.collation = Some("utf8mb4_bin".into()),
            "generated" => {
                reference.generated = Some(crate::inventory::GeneratedColumn {
                    expression: "other".into(),
                    generation_kind: "STORED".into(),
                })
            }
            "auto_increment" => reference.extra = "auto_increment".into(),
            _ => unreachable!(),
        }
        let operation = parse_ddl_operation(&sql).unwrap();
        assert!(
            build_semantic_evidence(&operation, &target, &target).is_err(),
            "accepted {invalid}"
        );
        assert!(
            super::super::transform::transform_production_alter_table_with_target(&sql, &target)
                .is_err(),
            "rendered {invalid}"
        );
    }
    let sql = format!("ALTER TABLE accounts {ADD_SLOT}, ADD COLUMN experiment_key VARCHAR(64)");
    let operation = parse_ddl_operation(&sql).unwrap();
    let target = semantic_snapshot(3, None);
    assert!(build_semantic_evidence(&operation, &target, &target).is_err());
}
