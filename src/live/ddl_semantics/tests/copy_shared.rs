use super::super::model::{ParsedAlterAlgorithm, ParsedAlterLock};
use super::super::transform::transform_production_alter_table_with_target_mode;
use super::*;

const READER_EVALUATION_SQL: &str = "ALTER TABLE reader_memory_operations
 ADD COLUMN evaluation_context_json MEDIUMTEXT NULL,
 ADD COLUMN evaluation_context_expires_at DATETIME(6) NULL,
 ADD KEY reader_memory_evaluation_retention (evaluation_context_expires_at),
 ADD CONSTRAINT reader_memory_evaluation_context_json CHECK (evaluation_context_json IS NULL OR JSON_VALID(evaluation_context_json)),
 ADD CONSTRAINT reader_memory_evaluation_context_size CHECK (evaluation_context_json IS NULL OR OCTET_LENGTH(evaluation_context_json)<=1048576),
 ALGORITHM=COPY, LOCK=SHARED";

const READER_EVALUATION_TARGET_SQL: &str = concat!(
    "ALTER TABLE `reader_memory_operations` ",
    "ADD COLUMN `evaluation_context_json` MEDIUMTEXT NULL DEFAULT NULL, ",
    "ADD COLUMN `evaluation_context_expires_at` DATETIME(6) NULL DEFAULT NULL, ",
    "ADD KEY `reader_memory_evaluation_retention` (`evaluation_context_expires_at`), ",
    "ADD CONSTRAINT `reader_memory_evaluation_context_json` CHECK (`evaluation_context_json` IS NULL OR JSON_VALID(`evaluation_context_json`)), ",
    "ADD CONSTRAINT `reader_memory_evaluation_context_size` CHECK (`evaluation_context_json` IS NULL OR OCTET_LENGTH(`evaluation_context_json`) <= 1048576), ",
    "ALGORITHM=COPY, LOCK=SHARED"
);

fn target(table_name: &str) -> SemanticSchemaSnapshot {
    let mut target = absent_target();
    let mut table = fixture_table();
    table.name = table_name.into();
    target.inventory.tables.push(table);
    target
        .check_constraints
        .insert(table_name.into(), Vec::new());
    target
}

#[test]
fn copy_shared_exact_reader_evaluation_parser() {
    let ast = parse_production_alter_table_ast(READER_EVALUATION_SQL)
        .expect("exact source COPY/SHARED must parse");
    assert_eq!(ast.table, "reader_memory_operations");
    assert_eq!(ast.algorithm, Some(ParsedAlterAlgorithm::Copy));
    assert_eq!(ast.lock, Some(ParsedAlterLock::Shared));
    assert_eq!(ast.clauses.len(), 5);
    assert!(supports_production_alter_table(READER_EVALUATION_SQL));
}

#[test]
fn copy_shared_exact_reader_evaluation_transform() {
    let result = transform_production_alter_table(READER_EVALUATION_SQL)
        .expect("exact source COPY/SHARED must transform");
    assert_eq!(
        result.target_sql.as_deref(),
        Some(READER_EVALUATION_TARGET_SQL)
    );
}

#[test]
fn copy_shared_exact_reader_evaluation_evidence() {
    let operation = parse_ddl_operation(READER_EVALUATION_SQL).expect("ALTER operation");
    let target = target("reader_memory_operations");
    let evidence = build_semantic_evidence(&operation, &target, &target)
        .expect("exact source COPY/SHARED must derive semantic evidence");
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).unwrap();
    assert_eq!(ast["parsed_alter_table"]["algorithm"], "copy");
    assert_eq!(ast["parsed_alter_table"]["lock"], "shared");
    let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
    let pre: serde_json::Value = serde_json::from_str(&evidence.pre_state).unwrap();
    assert_eq!(post["definition"]["columns"].as_array().unwrap().len(), 4);
    assert_eq!(
        post["definition"]["columns"][0],
        pre["definition"]["columns"][0]
    );
    assert_eq!(
        post["definition"]["columns"][1],
        pre["definition"]["columns"][1]
    );
    assert_eq!(
        post["definition"]["primary_key"],
        pre["definition"]["primary_key"]
    );
    for (name, column_type, data_type, position) in [
        ("evaluation_context_json", "mediumtext", "mediumtext", 3),
        (
            "evaluation_context_expires_at",
            "datetime(6)",
            "datetime",
            4,
        ),
    ] {
        let added = column(&post, name);
        assert_eq!(added["column_type"], column_type);
        assert_eq!(added["data_type"], data_type);
        assert_eq!(added["ordinal_position"], position);
        assert_eq!(added["is_nullable"], true);
        assert_eq!(added["default_value"], serde_json::Value::Null);
        assert_eq!(added["extra"], "");
    }
    assert_eq!(
        column(&post, "evaluation_context_json")["character_set"],
        "utf8mb4"
    );
    assert_eq!(
        column(&post, "evaluation_context_json")["collation"],
        "utf8mb4_unicode_ci"
    );
    assert_eq!(
        post["indexes"],
        serde_json::json!([{
            "table": "reader_memory_operations", "name": "reader_memory_evaluation_retention",
            "unique": false, "index_type": "BTREE", "visible": true, "comment": null,
            "columns": [{"name": "evaluation_context_expires_at", "sequence": 1,
                "prefix_length": null, "collation": "A", "order": "ASC"}]
        }])
    );
    assert_eq!(
        post["check_constraints"],
        serde_json::json!([
            {"name": "reader_memory_evaluation_context_json", "enforced": true,
                "expression": {"kind": "or",
                    "left": {"kind": "is_null", "column": "evaluation_context_json"},
                    "right": {"kind": "json_valid", "column": "evaluation_context_json"}}},
            {"name": "reader_memory_evaluation_context_size", "enforced": true,
                "expression": {"kind": "or",
                    "left": {"kind": "is_null", "column": "evaluation_context_json"},
                    "right": {"kind": "octet_length_at_most", "column": "evaluation_context_json", "limit": 1048576}}}
        ])
    );
}

#[test]
fn copy_shared_target_aware_render_preserves_source_pair() {
    let target = target("reader_memory_operations");
    let transformed = transform_production_alter_table_with_target_mode(
        READER_EVALUATION_SQL,
        &target,
        crate::live::query_charset_context::SourceSqlMode(Some(0)),
    )
    .expect("ordinary target-aware rendering");
    assert_eq!(
        transformed.target_sql.as_deref(),
        Some(READER_EVALUATION_TARGET_SQL)
    );
}

#[test]
fn copy_shared_same_grammar_other_identifier_is_generic() {
    let sql = READER_EVALUATION_SQL.replace("reader_memory_operations", "evaluation_archive");
    let expected =
        READER_EVALUATION_TARGET_SQL.replace("reader_memory_operations", "evaluation_archive");
    let target = target("evaluation_archive");
    let operation = parse_ddl_operation(&sql).unwrap();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
    assert_eq!(post["name"], "evaluation_archive");
    assert_eq!(post["check_constraints"].as_array().unwrap().len(), 2);
    assert_eq!(post["indexes"][0]["table"], "evaluation_archive");
    assert_eq!(
        transform_production_alter_table(&sql)
            .unwrap()
            .target_sql
            .as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        transform_production_alter_table_with_target_mode(
            &sql,
            &target,
            crate::live::query_charset_context::SourceSqlMode(Some(0)),
        )
        .unwrap()
        .target_sql
        .as_deref(),
        Some(expected.as_str())
    );
}

#[test]
fn copy_shared_rejects_other_option_pairs_and_unmodeled_clauses() {
    for options in [
        "ALGORITHM=COPY, LOCK=NONE",
        "ALGORITHM=INPLACE, LOCK=SHARED",
        "ALGORITHM=COPY",
        "LOCK=SHARED",
        "ALGORITHM=INSTANT, LOCK=SHARED",
        "ALGORITHM=INSTANT, LOCK=NONE",
        "ALGORITHM=COPY, LOCK=EXCLUSIVE",
        "ALGORITHM=DEFAULT, LOCK=SHARED",
        "ALGORITHM=COPY, LOCK=DEFAULT",
        "ALGORITHM=COPY, LOCK=SHARED, ALGORITHM=COPY",
        "LOCK=SHARED, ALGORITHM=COPY",
    ] {
        let sql = READER_EVALUATION_SQL.replace("ALGORITHM=COPY, LOCK=SHARED", options);
        assert!(!supports_production_alter_table(&sql), "admitted {sql}");
        assert!(
            transform_production_alter_table(&sql).is_err(),
            "rendered {sql}"
        );
        assert!(
            transform_production_alter_table_with_target_mode(
                &sql,
                &target("reader_memory_operations"),
                crate::live::query_charset_context::SourceSqlMode(Some(0)),
            )
            .is_err(),
            "target-rendered {sql}"
        );
    }
    for sql in [
        READER_EVALUATION_SQL.replace(
            "ADD COLUMN evaluation_context_json MEDIUMTEXT NULL",
            "ADD COLUMN evaluation_context_json REAL NULL",
        ),
        READER_EVALUATION_SQL.replace("<=1048576", ">=1048576"),
        READER_EVALUATION_SQL.replace(
            "ALGORITHM=COPY, LOCK=SHARED",
            "ALGORITHM=COPY, LOCK=SHARED, ADD COLUMN late INT",
        ),
    ] {
        assert!(!supports_production_alter_table(&sql), "admitted {sql}");
        assert!(transform_production_alter_table(&sql).is_err());
    }
}

#[test]
fn copy_shared_old_options_keep_normal_rendering() {
    for suffix in ["", ", ALGORITHM=INPLACE, LOCK=NONE"] {
        let sql = format!("ALTER TABLE accounts ADD COLUMN payload MEDIUMTEXT NULL{suffix}");
        let expected = format!(
            "ALTER TABLE `accounts` ADD COLUMN `payload` MEDIUMTEXT NULL DEFAULT NULL{suffix}"
        );
        assert!(supports_production_alter_table(&sql));
        assert_eq!(
            transform_production_alter_table(&sql)
                .unwrap()
                .target_sql
                .as_deref(),
            Some(expected.as_str())
        );
        assert_eq!(
            transform_production_alter_table_with_target_mode(
                &sql,
                &target("accounts"),
                crate::live::query_charset_context::SourceSqlMode(Some(0)),
            )
            .unwrap()
            .target_sql
            .as_deref(),
            Some(expected.as_str())
        );
    }
}

#[test]
fn copy_shared_does_not_bypass_target_semantic_fences() {
    let mode = crate::live::query_charset_context::SourceSqlMode(Some(0));
    let initial = target("reader_memory_operations");
    let mut missing_metadata = initial.clone();
    missing_metadata.check_constraints.clear();
    let mut existing_column = initial.clone();
    existing_column.inventory.tables[0].columns[1].name = "evaluation_context_json".into();
    for target in [absent_target(), missing_metadata, existing_column] {
        let operation = parse_ddl_operation(READER_EVALUATION_SQL).unwrap();
        assert!(build_semantic_evidence(&operation, &target, &target).is_err());
        assert!(
            transform_production_alter_table_with_target_mode(READER_EVALUATION_SQL, &target, mode)
                .is_err()
        );
    }
    for sql in [
        READER_EVALUATION_SQL.replace(
            "JSON_VALID(evaluation_context_json)",
            "JSON_VALID(absent_json)",
        ),
        READER_EVALUATION_SQL.replace("(evaluation_context_expires_at),", "(absent_time),"),
    ] {
        let operation = parse_ddl_operation(&sql).unwrap();
        assert!(build_semantic_evidence(&operation, &initial, &initial).is_err());
        assert!(transform_production_alter_table_with_target_mode(&sql, &initial, mode).is_err());
    }
}
