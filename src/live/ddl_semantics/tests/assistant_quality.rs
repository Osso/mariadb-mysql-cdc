use super::*;

const RUNS: &str = include_str!("../../../../fixtures/ddl/create-assistant-quality-runs.sql");
const VERDICTS: &str =
    include_str!("../../../../fixtures/ddl/create-assistant-quality-verdicts.sql");
const CONVERSATION_FK: &str =
    include_str!("../../../../fixtures/ddl/alter-assistant-quality-verdicts-conversation-fk.sql");
const IN_FLIGHT: &str =
    include_str!("../../../../fixtures/ddl/alter-assistant-quality-runs-in-flight-lock.sql");

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

    let sql = translate_ddl(RUNS, &[]).unwrap().target_sql.unwrap();
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
    let sql = translate_ddl(VERDICTS, &[]).unwrap().target_sql.unwrap();
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

/// The observed in-flight ALTER against the `accounts` fixture table, whose `handle` and
/// `is_active` columns stand in for `status` and `is_active`.
fn in_flight_on_accounts() -> String {
    IN_FLIGHT
        .replace("`assistant_quality_runs`", "`accounts`")
        .replace("`status`", "`handle`")
}

fn accounts_with_is_active() -> SemanticSchemaSnapshot {
    let mut target = semantic_snapshot(0, Some(1));
    let table = &mut target.inventory.tables[0];
    table.columns.push(ColumnInventory {
        name: "is_active".into(),
        ordinal_position: 3,
        column_type: "tinyint unsigned".into(),
        data_type: "tinyint".into(),
        is_nullable: false,
        character_set: None,
        collation: None,
        default_value: Some("1".into()),
        extra: String::new(),
        comment: String::new(),
        generated: None,
    });
    target
}

#[test]
fn in_flight_alter_renders_stored_generated_column_and_unique_key() {
    let result = translate_ddl(IN_FLIGHT, &[]).expect("generated ALTER must translate");
    assert_eq!(
        result.target_sql.as_deref(),
        Some(
            "ALTER TABLE `assistant_quality_runs` ADD COLUMN `in_flight_lock` TINYINT UNSIGNED GENERATED ALWAYS AS (IF(`status` = _utf8mb4'running' AND `is_active` = 1, 1, NULL)) STORED COMMENT 'single-flight slot: 1 while active+running, NULL otherwise', ADD UNIQUE KEY `uk_single_in_flight` (`in_flight_lock`)"
        )
    );
}

#[test]
fn in_flight_alter_expects_mysql_generation_metadata() {
    let target = accounts_with_is_active();
    let operation = parse_ddl_operation(&in_flight_on_accounts()).expect("operation");
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("evidence");
    let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
    let column = post_state_column(&post, "in_flight_lock");
    assert_eq!(column["column_type"], "tinyint unsigned");
    assert_eq!(column["is_nullable"], true);
    assert_eq!(column["default_value"], serde_json::Value::Null);
    assert_eq!(column["extra"], "STORED GENERATED");
    assert_eq!(column["ordinal_position"], 4);
    assert_eq!(
        column["generated"]["expression"],
        "if(((`handle` = _utf8mb4\\'running\\') and (`is_active` = 1)),1,NULL)"
    );
    assert_eq!(column["generated"]["generation_kind"], "STORED");
    let key = post["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|index| index["name"] == "uk_single_in_flight")
        .expect("unique key");
    assert_eq!(key["unique"], true);
}

#[test]
fn in_flight_alter_rejects_unmodeled_generation_forms() {
    for (from, to) in [
        ("PERSISTENT", "VIRTUAL"),
        ("PERSISTENT", ""),
        ("PERSISTENT", "PERSISTENT NOT NULL"),
        ("PERSISTENT", "PERSISTENT DEFAULT NULL"),
        ("`is_active` = 1", "`is_active` > 1"),
        ("`is_active` = 1", "`is_active` = 01"),
        ("AND", "OR"),
        ("'running'", "'run ning'"),
        (", 1, NULL", ", 1, 0"),
        (", 1, NULL", ", 256, NULL"),
        ("IF(", "COALESCE("),
        ("tinyint(1) UNSIGNED", "varchar(8)"),
    ] {
        let sql = IN_FLIGHT.replacen(from, to, 1);
        assert!(
            parse_production_alter_table_ast(&sql).is_err(),
            "accepted {sql}"
        );
    }
    let target = accounts_with_is_active();
    for sql in [
        in_flight_on_accounts().replace("`handle`", "`missing`"),
        in_flight_on_accounts().replace("`handle` = 'running'", "`id` = 1"),
    ] {
        let operation = parse_ddl_operation(&sql).expect("operation");
        assert!(
            build_semantic_evidence(&operation, &target, &target).is_err(),
            "accepted {sql}"
        );
    }
}

/// The observed conversation FK ALTER against the `accounts` fixture, whose `idx_handle`
/// supports the child column.
fn conversation_fk_on_accounts() -> String {
    CONVERSATION_FK
        .replace("`assistant_quality_verdicts`", "`accounts`")
        .replace("(`conversation_id`)", "(`handle`)")
}

#[test]
fn conversation_fk_alter_renders_named_cascading_foreign_key() {
    let result = translate_ddl(CONVERSATION_FK, &[]).expect("ADD FOREIGN KEY must translate");
    assert_eq!(
        result.target_sql.as_deref(),
        Some(
            "ALTER TABLE `assistant_quality_verdicts` ADD CONSTRAINT `fk_aqv_conversation` FOREIGN KEY (`conversation_id`) REFERENCES `llm_conversations` (`id`) ON DELETE CASCADE"
        )
    );
}

#[test]
fn conversation_fk_alter_expects_foreign_key_and_keeps_indexes() {
    let target = semantic_snapshot(0, Some(1));
    let operation = parse_ddl_operation(&conversation_fk_on_accounts()).expect("operation");
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("evidence");
    let pre: serde_json::Value = serde_json::from_str(&evidence.pre_state).unwrap();
    let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
    assert_eq!(post["indexes"], pre["indexes"]);
    assert_eq!(pre["foreign_keys"], serde_json::json!([]));
    assert_eq!(
        post["foreign_keys"],
        serde_json::json!([{
            "table": "accounts",
            "name": "fk_aqv_conversation",
            "columns": ["handle"],
            "referenced_schema": "fixture_cdc",
            "referenced_table": "llm_conversations",
            "referenced_columns": ["id"],
        }])
    );
}

#[test]
fn conversation_fk_alter_rejects_unmodeled_forms() {
    for (from, to) in [
        ("ON DELETE CASCADE", "ON DELETE SET NULL"),
        ("ON DELETE CASCADE", "ON DELETE CASCADE ON UPDATE CASCADE"),
        ("ON DELETE CASCADE", ""),
        ("(`conversation_id`)", "(`conversation_id`, `run_id`)"),
        (
            "REFERENCES `llm_conversations`",
            "REFERENCES other.`llm_conversations`",
        ),
    ] {
        let sql = CONVERSATION_FK.replacen(from, to, 1);
        assert!(
            parse_production_alter_table_ast(&sql).is_err(),
            "accepted {sql}"
        );
    }
    let target = semantic_snapshot(0, Some(1));
    let mut unindexed = target.clone();
    unindexed.inventory.indexes.clear();
    let mut existing = target.clone();
    existing
        .inventory
        .foreign_keys
        .push(crate::inventory::ForeignKeyInventory {
            table: "accounts".into(),
            name: "fk_aqv_conversation".into(),
            columns: vec!["handle".into()],
            referenced_schema: "fixture_cdc".into(),
            referenced_table: "llm_conversations".into(),
            referenced_columns: vec!["id".into()],
        });
    for (sql, snapshot) in [
        (
            CONVERSATION_FK.replace("`assistant_quality_verdicts`", "`accounts`"),
            &target,
        ),
        (conversation_fk_on_accounts(), &unindexed),
        (conversation_fk_on_accounts(), &existing),
    ] {
        let operation = parse_ddl_operation(&sql).expect("operation");
        assert!(
            build_semantic_evidence(&operation, snapshot, snapshot).is_err(),
            "accepted {sql}"
        );
    }
}

#[test]
fn conversation_fk_alter_verifies_observed_delete_rule() {
    let ast = parse_production_alter_table_ast(&conversation_fk_on_accounts()).unwrap();
    let key = crate::canonical_foreign_key::CanonicalForeignKey {
        constraint_schema: "test".into(),
        constraint_name: "fk_aqv_conversation".into(),
        child_schema: "test".into(),
        child_table: "accounts".into(),
        child_columns: vec!["handle".into()],
        parent_schema: "test".into(),
        parent_table: "llm_conversations".into(),
        parent_columns: vec!["id".into()],
        update_rule: "RESTRICT".into(),
        delete_rule: "CASCADE".into(),
        match_option: "NONE".into(),
        enforced: true,
    };
    let validate = |keys: &[crate::canonical_foreign_key::CanonicalForeignKey]| {
        canonical::validate_alter_foreign_keys(&ast, "test", keys)
    };
    assert!(validate(&[]).is_ok(), "absent pre-state key is not drift");
    assert!(validate(std::slice::from_ref(&key)).is_ok());
    let mut restricted = key.clone();
    restricted.delete_rule = "RESTRICT".into();
    assert!(validate(&[restricted]).is_err());
    let mut other_parent = key;
    other_parent.parent_table = "users".into();
    assert!(validate(&[other_parent]).is_err());
}
