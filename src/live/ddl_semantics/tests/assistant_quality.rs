use super::*;

const RUNS: &str = include_str!("../../../../fixtures/ddl/create-assistant-quality-runs.sql");
const VERDICTS: &str =
    include_str!("../../../../fixtures/ddl/create-assistant-quality-verdicts.sql");

fn post_state_column(post: &serde_json::Value, name: &str) -> serde_json::Value {
    post["definition"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|column| column["name"] == name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .clone()
}

#[test]
fn quality_runs_create_drops_integer_display_widths_and_keeps_comments() {
    let ast = parse_fixture_create_table(RUNS).expect("assistant_quality_runs CREATE");
    assert_eq!(ast.name, "assistant_quality_runs");
    assert_eq!(ast.primary_key, ["id"]);
    assert_eq!(ast.columns.len(), 19);
    let column = |name: &str| {
        ast.columns
            .iter()
            .find(|column| column.name == name)
            .unwrap()
    };
    assert_eq!(column("id").column_type, "int unsigned");
    assert!(column("id").auto_increment);
    assert_eq!(column("window_days").column_type, "smallint unsigned");
    assert_eq!(column("creator_id").column_type, "int unsigned");
    assert!(column("creator_id").nullable);
    assert_eq!(column("is_active").column_type, "tinyint unsigned");
    assert_eq!(column("status").default_sql.as_deref(), Some("'running'"));
    assert_eq!(column("status").comment, "running|done|error");
    assert_eq!(
        column("creator_id").comment,
        "admin who pressed Run now; NULL for cron"
    );
    assert_eq!(column("window_days").comment, "");
    assert_eq!(column("summary").column_type, "longtext");

    let sql = transform_fixture_create_table(RUNS)
        .unwrap()
        .target_sql
        .unwrap();
    assert!(
        sql.contains(
            "`status` VARCHAR(16) NOT NULL DEFAULT 'running' COMMENT 'running|done|error'"
        )
    );
    assert!(sql.contains("`window_days` SMALLINT UNSIGNED NOT NULL, "));
    assert!(sql.contains(
        "`summary` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'aggregates: headline, strata, dims, tags, fbt'"
    ));
    assert!(sql.contains("CHECK (JSON_VALID(`summary`))"));

    let post = reader_memory_create_post_state(RUNS);
    assert_eq!(
        post_state_column(&post, "model")["comment"],
        "OpenRouter model id used by the judge"
    );
    assert_eq!(post_state_column(&post, "judged_count")["comment"], "");
    assert_eq!(
        post_state_column(&post, "sample_size")["column_type"],
        "smallint unsigned"
    );
}

#[test]
fn quality_verdicts_create_preserves_restrict_foreign_key() {
    let ast = parse_fixture_create_table(VERDICTS).expect("assistant_quality_verdicts CREATE");
    assert_eq!(ast.columns[0].column_type, "bigint unsigned");
    assert!(ast.columns[0].auto_increment);
    assert_eq!(
        ast.columns[4].comment,
        "1_turn|2_turns|3-4_turns|5-8_turns|9+_turns"
    );
    assert_eq!(ast.check_constraints.len(), 3);
    assert_eq!(ast.foreign_keys.len(), 1);
    assert_eq!(ast.foreign_keys[0].delete_rule, "RESTRICT");
    let sql = transform_fixture_create_table(VERDICTS)
        .unwrap()
        .target_sql
        .unwrap();
    assert!(sql.contains(
        "CONSTRAINT `fk_aqv_run` FOREIGN KEY (`run_id`) REFERENCES `assistant_quality_runs` (`id`) ON DELETE RESTRICT"
    ));
    assert!(sql.contains("COMMENT 'tag => verbatim quote'"));

    let key = crate::canonical_foreign_key::CanonicalForeignKey {
        constraint_schema: "test".into(),
        constraint_name: "fk_aqv_run".into(),
        child_schema: "test".into(),
        child_table: "assistant_quality_verdicts".into(),
        child_columns: vec!["run_id".into()],
        parent_schema: "test".into(),
        parent_table: "assistant_quality_runs".into(),
        parent_columns: vec!["id".into()],
        update_rule: "RESTRICT".into(),
        delete_rule: "RESTRICT".into(),
        match_option: "NONE".into(),
        enforced: true,
    };
    let validate = |keys: &[crate::canonical_foreign_key::CanonicalForeignKey]| {
        canonical::validate_create_foreign_keys(&ast, "test", keys)
    };
    assert!(validate(std::slice::from_ref(&key)).is_ok());
    let mut cascading = key;
    cascading.delete_rule = "CASCADE".into();
    assert!(validate(&[cascading]).is_err());
}

#[test]
fn quality_create_rejects_unmodeled_width_comment_and_action_forms() {
    for rejected in [
        RUNS.replace(
            "`window_days`             smallint(5)",
            "`window_days` smallint(0)",
        ),
        RUNS.replace(
            "`window_days`             smallint(5)",
            "`window_days` smallint(05)",
        ),
        RUNS.replace("int(12) UNSIGNED", "int(12) UNSIGNED ZEROFILL"),
        RUNS.replace("int(12) UNSIGNED", "int(12)"),
        RUNS.replace("COMMENT 'cron|manual'", "COMMENT 'cron\\\\manual'"),
        RUNS.replace("COMMENT 'cron|manual'", "COMMENT 'cron\nmanual'"),
        RUNS.replace("COMMENT 'cron|manual'", "COMMENT"),
        RUNS.replace("COMMENT 'cron|manual'", "COMMENT 'cron' COMMENT 'manual'"),
        VERDICTS.replace("ON DELETE RESTRICT", "ON DELETE SET NULL"),
        VERDICTS.replace("ON DELETE RESTRICT", "ON DELETE RESTRICT ON UPDATE CASCADE"),
    ] {
        assert!(
            parse_fixture_create_table(&rejected).is_err(),
            "accepted {rejected}"
        );
    }
}
