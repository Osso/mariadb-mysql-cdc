use super::*;

const EXACT: &str = include_str!("../../../../fixtures/ddl/modify-release-top-statistic-enum.sql");
const TYPE: &str = "enum('views','popularity','likes','purchases','revenue')";

#[test]
fn alter_enum_exact_event_has_typed_rendering_and_semantic_evidence() {
    let ast = parse_production_alter_table_ast(EXACT).expect("typed exact ALTER ENUM");
    let ParsedAlterClause::ModifyColumn(column) = &ast.clauses[0] else {
        panic!("expected MODIFY");
    };
    assert_eq!(column.column_type, TYPE);
    assert_eq!(column.data_type, "enum");
    assert!(!column.nullable);
    assert_eq!(
        super::super::transform::transform_production_alter_table(EXACT)
            .unwrap()
            .target_sql
            .as_deref(),
        Some(
            "ALTER TABLE `releases_top_stats` MODIFY COLUMN `statistic` ENUM('views','popularity','likes','purchases','revenue') NOT NULL"
        )
    );
    let mut target = semantic_snapshot(6, None);
    let table = &mut target.inventory.tables[0];
    table.name = "releases_top_stats".into();
    let column = &mut table.columns[1];
    column.name = "statistic".into();
    column.column_type = "enum('views','popularity','likes','purchases')".into();
    column.data_type = "enum".into();
    column.is_nullable = false;
    column.default_value = None;
    column.comment.clear();
    target.inventory.indexes[0].table = table.name.clone();
    target.inventory.indexes[0].columns[0].name = "statistic".into();
    let runtime = target.table_runtime.remove("accounts").unwrap();
    target.table_runtime.insert(table.name.clone(), runtime);
    let operation = parse_ddl_operation(EXACT).unwrap();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let mut expected = target.clone();
    expected.inventory.tables[0].columns[1].column_type = TYPE.into();
    assert_eq!(
        evidence.expected_post_state,
        super::super::canonical::observe_operation_state(&expected, &operation).unwrap()
    );
    let canonical: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).unwrap();
    assert_eq!(
        canonical["parsed_alter_table"]["clauses"][0]["column_type"],
        TYPE
    );
}

#[test]
fn alter_enum_preserves_case_order_encoding_and_literal_consumption() {
    let sql = "ALTER TABLE accounts MODIFY handle ENUM('Second_2','FIRST') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'enum comment', ADD note VARCHAR(20) DEFAULT 'later' COMMENT 'later comment'";
    let ast = parse_production_alter_table_ast(sql).unwrap();
    let ParsedAlterClause::ModifyColumn(column) = &ast.clauses[0] else {
        panic!("MODIFY")
    };
    assert_eq!(column.column_type, "enum('Second_2','FIRST')");
    assert_eq!(column.comment, "enum comment");
    assert_eq!(column.collation.as_deref(), Some("utf8mb4_bin"));
    let ParsedAlterClause::AddColumn(note) = &ast.clauses[1] else {
        panic!("ADD")
    };
    assert_eq!(note.default_value.as_deref(), Some("later"));
    assert_eq!(note.comment, "later comment");
    assert_eq!(
        super::super::transform::transform_production_alter_table(sql)
            .unwrap()
            .target_sql
            .as_deref(),
        Some(
            "ALTER TABLE `accounts` MODIFY COLUMN `handle` ENUM('Second_2','FIRST') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'enum comment', ADD COLUMN `note` VARCHAR(20) NULL DEFAULT 'later' COMMENT 'later comment'"
        )
    );
    let target = semantic_snapshot(6, None);
    let operation = parse_ddl_operation(sql).unwrap();
    let evidence = build_semantic_evidence(&operation, &target, &target).unwrap();
    let post: serde_json::Value = serde_json::from_str(&evidence.expected_post_state).unwrap();
    let column = &post["definition"]["columns"][1];
    assert_eq!(column["column_type"], "enum('Second_2','FIRST')");
    assert_eq!(column["character_set"], "utf8mb4");
    assert_eq!(column["collation"], "utf8mb4_bin");
}

#[test]
fn alter_enum_rejects_unmodeled_labels_and_syntax() {
    for definition in [
        "ENUM()",
        "ENUM('')",
        "ENUM('two words')",
        "ENUM('a-b')",
        "ENUM('é')",
        "ENUM('a''b')",
        "ENUM('a',)",
        "ENUM(1)",
        "ENUM(`a`)",
        "`ENUM`('a')",
        "ENUM('a') UNSIGNED",
        "SET('a')",
        "ENUM('a') COLLATE utf8mb4_bin",
        "ENUM('a') CHARACTER SET utf8mb4 COLLATE latin1_bin",
        "ENUM('a') NOT NULL DEFAULT NULL",
    ] {
        let sql = format!("ALTER TABLE accounts MODIFY handle {definition}");
        assert!(parse_production_alter_table_ast(&sql).is_err(), "{sql}");
    }
}
