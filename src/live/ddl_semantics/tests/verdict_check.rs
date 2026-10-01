use super::*;

const EXPRESSION: &str = "((sample_kind IN ('random','experiment')) AND ((sample_kind='random')=(experiment_key IS NULL)))";

fn alter(expression: &str) -> String {
    format!("ALTER TABLE reader_memory_profiles ADD CONSTRAINT verdict_shape CHECK ({expression})")
}

fn target() -> SemanticSchemaSnapshot {
    let mut target = reader_memory_profiles_target();
    let template = target.inventory.tables[0].columns[0].clone();
    for (index, name) in ["sample_kind", "experiment_key"].into_iter().enumerate() {
        let mut column = template.clone();
        column.name = name.into();
        column.ordinal_position = index as u32 + 3;
        target.inventory.tables[0].columns.push(column);
    }
    target
}

#[test]
fn verdict_check_compound_unguarded_alter_renders_and_canonicalizes() {
    let sql = alter(EXPRESSION);
    let transformed = globalcomix_inventory()
        .transform_sql(&sql)
        .expect("compound CHECK");
    assert_eq!(
        transformed.target_sql.as_deref(),
        Some(
            "ALTER TABLE `reader_memory_profiles` ADD CONSTRAINT `verdict_shape` CHECK ((`sample_kind` IN ('random','experiment')) AND ((`sample_kind` = 'random') = (`experiment_key` IS NULL)))"
        )
    );
    let operation = parse_ddl_operation(&sql).unwrap();
    let target = target();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).unwrap();
    assert_eq!(
        ast["parsed_alter_table"]["clauses"][0]["constraint"],
        serde_json::json!({
            "name": "verdict_shape", "disjuncts": [{"kind": "and",
                "left": {"kind": "in_strings", "column": "sample_kind", "values": ["random", "experiment"]},
                "right": {"kind": "boolean_equals", "left": {"kind": "string_equals", "column": "sample_kind", "value": "random"}, "right": {"kind": "is_null", "column": "experiment_key"}}
            }]
        })
    );
    for expression in [
        EXPRESSION.replace("sample_kind IN", "absent_column IN"),
        EXPRESSION.replace("sample_kind='random'", "absent_column='random'"),
        EXPRESSION.replace("experiment_key IS NULL", "absent_column IS NULL"),
    ] {
        let operation = parse_ddl_operation(&alter(&expression)).unwrap();
        assert!(
            build_semantic_evidence(&operation, &target, &target).is_err(),
            "accepted missing column in {expression}"
        );
    }
}

#[test]
fn verdict_check_changed_literal_changes_observable_output() {
    let sql = alter(&EXPRESSION.replace("='random'", "='experiment'"));
    let transformed = globalcomix_inventory()
        .transform_sql(&sql)
        .expect("changed literal");
    assert_eq!(
        transformed.target_sql.as_deref(),
        Some(
            "ALTER TABLE `reader_memory_profiles` ADD CONSTRAINT `verdict_shape` CHECK ((`sample_kind` IN ('random','experiment')) AND ((`sample_kind` = 'experiment') = (`experiment_key` IS NULL)))"
        )
    );
    let operation = parse_ddl_operation(&sql).unwrap();
    let target = target();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).unwrap();
    assert_eq!(
        ast["parsed_alter_table"]["clauses"][0]["constraint"]["disjuncts"][0]["right"]["left"]["value"],
        "experiment"
    );
}

#[test]
fn verdict_check_keeps_existing_or_output() {
    let sql = alter("sample_kind IS NULL OR sample_kind IN ('random','experiment')");
    let transformed = globalcomix_inventory().transform_sql(&sql).unwrap();
    assert_eq!(
        transformed.target_sql.as_deref(),
        Some(
            "ALTER TABLE `reader_memory_profiles` ADD CONSTRAINT `verdict_shape` CHECK (`sample_kind` IS NULL OR `sample_kind` IN ('random','experiment'))"
        )
    );
    let operation = parse_ddl_operation(&sql).unwrap();
    let target = target();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).unwrap();
    assert_eq!(
        ast["parsed_alter_table"]["clauses"][0]["constraint"],
        serde_json::json!({"name": "verdict_shape", "disjuncts": [{"kind": "is_null", "column": "sample_kind"}, {"kind": "in_strings", "column": "sample_kind", "values": ["random", "experiment"]}]})
    );
}

#[test]
fn verdict_check_rejects_unmodeled_and_malformed_expressions() {
    for expression in [
        "(sample_kind='random')=(experiment_key IS NOT NULL)",
        "(sample_kind='random')<>(experiment_key IS NULL)",
        "(sample_kind='random')=(experiment_key='experiment')",
        "sample_kind = 1",
        "sample_kind = 'random' AND",
        "sample_kind IN ()",
        "sample_kind = 'random' trailing",
        "(sample_kind = 'random'",
        "sample_kind = 'not-bounded'",
        "sample_kind = 'random' = experiment_key IS NULL",
    ] {
        assert!(
            !supports_production_alter_table(&alter(expression)),
            "accepted {expression}"
        );
    }
}

#[test]
fn verdict_check_metadata_equivalence_preserves_grouping_and_literal_case() {
    use super::super::transform::canonical_check_expression;
    let original = canonical_check_expression(EXPRESSION).unwrap();
    let metadata = "((`sample_kind` in (_utf8mb4'random',_utf8mb4'experiment')) and ((`sample_kind` = _utf8mb4'random') = (`experiment_key` is null)))";
    assert_eq!(canonical_check_expression(metadata).unwrap(), original);
    assert_ne!(
        canonical_check_expression(&metadata.replace("'random'", "'Random'")).unwrap(),
        original
    );
    assert_ne!(
        canonical_check_expression(&metadata.replace(" and ", " or ")).unwrap(),
        original
    );
    assert_ne!(
        canonical_check_expression(
            "sample_kind='random' OR (sample_kind='experiment' AND experiment_key IS NULL)"
        )
        .unwrap(),
        canonical_check_expression(
            "(sample_kind='random' OR sample_kind='experiment') AND experiment_key IS NULL"
        )
        .unwrap()
    );
    for (sql, metadata) in [
        ("JSON_VALID(payload)", "(((json_valid((`payload`)))))"),
        (
            "payload IS NULL OR JSON_VALID(payload)",
            "((`payload` is null) or (json_valid(`payload`)))",
        ),
        (
            "payload IS NULL OR OCTET_LENGTH(payload) <= 8192",
            "((`payload` is null) or (octet_length(`payload`) <= 8192))",
        ),
        (
            "status IN ('active','disabled')",
            "((`status` in (_utf8mb4'active',_utf8mb4'disabled')))",
        ),
    ] {
        assert_eq!(
            canonical_check_expression(sql).unwrap(),
            canonical_check_expression(metadata).unwrap(),
            "{metadata}"
        );
    }
    for metadata in [
        "sample_kind = _latin1'random'",
        "sample_kind = _utf8mb4 _utf8mb4'random'",
        "sample_kind = 'random';",
        "/* hidden */ sample_kind = 'random'",
        "(sample_kind='random')=(experiment_key IS NULL) trailing",
    ] {
        assert!(
            canonical_check_expression(metadata).is_err(),
            "accepted {metadata}"
        );
    }
}
