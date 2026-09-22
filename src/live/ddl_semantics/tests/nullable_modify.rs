use super::*;

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
