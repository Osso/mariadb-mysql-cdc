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
    for name in ["sample_kind", "experiment_key"] {
        let missing = alter(&EXPRESSION.replace(name, "absent_column"));
        let operation = parse_ddl_operation(&missing).unwrap();
        assert!(
            build_semantic_evidence(&operation, &target, &target).is_err(),
            "missing {name}"
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
