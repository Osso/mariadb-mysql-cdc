use super::*;

const NULLABLE_DATETIME: &str =
    include_str!("../../../../fixtures/ddl/modify-curated-strip-end-time.sql");

fn curated_strips_snapshot() -> SemanticSchemaSnapshot {
    let mut target = semantic_snapshot(6, None);
    let table = &mut target.inventory.tables[0];
    table.name = "home_feed_curated_strips".into();
    table.columns[1].name = "start_time".into();
    table.columns[1].column_type = "datetime".into();
    table.columns[1].data_type = "datetime".into();
    table.columns[1].is_nullable = false;
    table.columns.push(ColumnInventory {
        name: "end_time".into(),
        ordinal_position: 3,
        column_type: "datetime".into(),
        data_type: "datetime".into(),
        is_nullable: false,
        character_set: None,
        collation: None,
        default_value: None,
        extra: String::new(),
        comment: String::new(),
        generated: None,
    });
    let index = &mut target.inventory.indexes[0];
    index.table = table.name.clone();
    index.name = "idx_hfcs_window".into();
    index.columns[0].name = "start_time".into();
    index.columns[0].prefix_length = None;
    index.columns.push(IndexColumnInventory {
        name: "end_time".into(),
        sequence: 2,
        prefix_length: None,
        collation: Some("A".into()),
        order: "ASC".into(),
    });
    let runtime = target
        .table_runtime
        .remove("accounts")
        .expect("fixture runtime");
    target.table_runtime.insert(table.name.clone(), runtime);
    target
}

#[test]
fn nullable_datetime_modify_renders_and_models_full_post_state() {
    let target = curated_strips_snapshot();
    let operation = parse_ddl_operation(NULLABLE_DATETIME).expect("operation");
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("evidence");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&evidence.canonical_ast).expect("canonical JSON")
            ["parsed_alter_table"],
        serde_json::json!({
            "table": "home_feed_curated_strips",
            "clauses": [{
                "kind": "modify_column", "name": "end_time", "column_type": "datetime",
                "data_type": "datetime", "nullable": true,
            }],
        })
    );
    assert_eq!(
        super::super::transform::transform_production_alter_table(NULLABLE_DATETIME)
            .expect("render")
            .target_sql
            .as_deref(),
        Some(
            "ALTER TABLE `home_feed_curated_strips` MODIFY COLUMN `end_time` DATETIME NULL DEFAULT NULL"
        )
    );
    let mut expected = target.clone();
    expected.inventory.tables[0].columns[2].is_nullable = true;
    assert_eq!(
        evidence.expected_post_state,
        super::super::canonical::observe_operation_state(&expected, &operation)
            .expect("post-state with untouched values/order/index/metadata")
    );
}

#[test]
fn nullable_datetime_modify_fails_closed_on_unmodeled_syntax_or_prestate() {
    for sql in [
        "ALTER TABLE home_feed_curated_strips MODIFY COLUMN end_time DATETIME NOT NULL",
        "ALTER TABLE home_feed_curated_strips MODIFY COLUMN end_time DATETIME NULL",
        "ALTER TABLE home_feed_curated_strips MODIFY COLUMN end_time DATETIME(6) DEFAULT NULL",
        "ALTER TABLE home_feed_curated_strips MODIFY COLUMN end_time TIMESTAMP DEFAULT NULL",
        "ALTER TABLE home_feed_curated_strips MODIFY COLUMN end_time DATETIME DEFAULT '2026-09-25 00:00:00'",
        "ALTER TABLE home_feed_curated_strips MODIFY COLUMN end_time DATETIME NOT NULL DEFAULT NULL",
        "ALTER TABLE home_feed_curated_strips MODIFY COLUMN end_time `DATETIME` DEFAULT NULL",
        "ALTER TABLE home_feed_curated_strips MODIFY COLUMN end_time DATETIME DEFAULT `NULL`",
    ] {
        assert!(parse_production_alter_table_ast(sql).is_err(), "{sql}");
    }
    for change in ["timestamp", "datetime(6)", "varchar(64)"] {
        let mut target = curated_strips_snapshot();
        target.inventory.tables[0].columns[2].column_type = change.into();
        if change != "datetime(6)" {
            target.inventory.tables[0].columns[2].data_type = change.into();
        }
        let operation = parse_ddl_operation(NULLABLE_DATETIME).expect("operation");
        assert!(operation.alter_table_ast.is_some(), "parsed exact event");
        assert!(
            build_semantic_evidence(&operation, &target, &target).is_err(),
            "{change}"
        );
    }
    let mut target = curated_strips_snapshot();
    target.inventory.tables[0].columns[2].extra = "DEFAULT_GENERATED".into();
    let operation = parse_ddl_operation(NULLABLE_DATETIME).expect("operation");
    assert!(operation.alter_table_ast.is_some(), "parsed exact event");
    assert!(build_semantic_evidence(&operation, &target, &target).is_err());
}

const NULLABLE_MODIFY: &str = "-- Mantle Spotlight: cta_url is no longer required. A curated spotlight may have nowhere to send\n-- the tap; the admin curation surface stopped requiring it, so the column must allow that.\nALTER TABLE `home_feed_mantle_spotlights`\n    MODIFY COLUMN `cta_url` VARCHAR(1024) DEFAULT NULL";

#[test]
fn nullable_modify_translates_exact_commented_statement() {
    let result = super::super::transform::transform_production_alter_table(NULLABLE_MODIFY)
        .expect("nullable MODIFY must translate");
    let sql = result.target_sql.expect("executable MODIFY");
    assert!(sql.ends_with("ALTER TABLE `home_feed_mantle_spotlights` MODIFY COLUMN `cta_url` VARCHAR(1024) NULL DEFAULT NULL"));
}

#[test]
fn nullable_modify_changes_nullability_without_losing_other_metadata() {
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.tables[0].columns[1].is_nullable = false;
    target.inventory.tables[0].columns[1].default_value = Some("prior-default".into());
    let sql = "ALTER TABLE accounts MODIFY COLUMN handle VARCHAR(1024) DEFAULT NULL";
    parse_production_alter_table_ast(sql).expect("typed nullable MODIFY");
    let operation = parse_ddl_operation(sql).expect("operation");
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("evidence");
    let mut expected = target.clone();
    let column = &mut expected.inventory.tables[0].columns[1];
    column.column_type = "varchar(1024)".into();
    column.is_nullable = true;
    column.default_value = None;
    column.character_set = Some("utf8mb4".into());
    column.collation = Some("utf8mb4_unicode_ci".into());
    assert_eq!(
        evidence.expected_post_state,
        super::super::canonical::observe_operation_state(&expected, &operation)
            .expect("post-state")
    );
}

#[test]
fn nullable_modify_rejects_unmodeled_defaults_and_quoted_keywords() {
    for suffix in [
        "DEFAULT 'x'",
        "NOT NULL DEFAULT NULL",
        "`DEFAULT` NULL",
        "DEFAULT `NULL`",
    ] {
        let sql = format!("ALTER TABLE accounts MODIFY COLUMN handle VARCHAR(1024) {suffix}");
        assert!(parse_production_alter_table_ast(&sql).is_err(), "{sql}");
    }
}
