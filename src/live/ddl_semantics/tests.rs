use super::transform::{
    DDL_TRANSFORMATION_VERSION, parse_fixture_create_table, parse_production_alter_table_ast,
    transform_drop_columns_if_exists, transform_fixture_create_table,
};
use super::*;
use crate::inventory::{
    ColumnInventory, EventInventory, ForeignKeyInventory, IndexColumnInventory, IndexInventory,
    InventoryConfig, RoutineInventory, SchemaInventory, TableInventory, TriggerInventory,
    ViewInventory,
};

fn assert_operation_cases(cases: &[(&str, DdlFamily, DdlObjectKind, &str, Option<&str>)]) {
    for (sql, family, object_kind, primary, secondary) in cases {
        assert_eq!(
            parse_ddl_operation(sql).expect(sql),
            DdlOperation {
                family: *family,
                object_kind: *object_kind,
                primary_object: (*primary).to_string(),
                secondary_object: secondary.map(str::to_string),
                index_ast: parse_simple_index_ddl(sql).ok(),
                create_table_ast: parse_fixture_create_table(sql).ok(),
                alter_table_ast: parse_production_alter_table_ast(sql).ok(),
            },
            "{sql}",
        );
    }
}

type OperationCase = (
    &'static str,
    DdlFamily,
    DdlObjectKind,
    &'static str,
    Option<&'static str>,
);

const TABLE_INDEX_CASES: &[OperationCase] = &[
    (
        "CREATE TABLE accounts (id bigint primary key)",
        DdlFamily::Table,
        DdlObjectKind::Table,
        "accounts",
        None,
    ),
    (
        "ALTER TABLE `accounts` ADD COLUMN handle varchar(64)",
        DdlFamily::Table,
        DdlObjectKind::Table,
        "accounts",
        None,
    ),
    (
        "DROP TABLE IF EXISTS accounts",
        DdlFamily::Drop,
        DdlObjectKind::Table,
        "accounts",
        None,
    ),
    (
        "CREATE INDEX idx_handle ON accounts (handle)",
        DdlFamily::Index,
        DdlObjectKind::Index,
        "idx_handle",
        Some("accounts"),
    ),
    (
        "DROP INDEX `idx_handle` ON `accounts`",
        DdlFamily::Index,
        DdlObjectKind::Index,
        "idx_handle",
        Some("accounts"),
    ),
];

const NAMED_OBJECT_CASES: &[OperationCase] = &[
    (
        "CREATE VIEW active_accounts AS SELECT id FROM accounts",
        DdlFamily::View,
        DdlObjectKind::View,
        "active_accounts",
        None,
    ),
    (
        "ALTER VIEW active_accounts AS SELECT id FROM accounts WHERE id > 0",
        DdlFamily::View,
        DdlObjectKind::View,
        "active_accounts",
        None,
    ),
    (
        "DROP VIEW active_accounts",
        DdlFamily::Drop,
        DdlObjectKind::View,
        "active_accounts",
        None,
    ),
    (
        "CREATE PROCEDURE refresh_accounts() SELECT 1",
        DdlFamily::Procedure,
        DdlObjectKind::Procedure,
        "refresh_accounts",
        None,
    ),
    (
        "ALTER PROCEDURE refresh_accounts SQL SECURITY INVOKER",
        DdlFamily::Procedure,
        DdlObjectKind::Procedure,
        "refresh_accounts",
        None,
    ),
    (
        "DROP PROCEDURE refresh_accounts",
        DdlFamily::Drop,
        DdlObjectKind::Procedure,
        "refresh_accounts",
        None,
    ),
    (
        "CREATE FUNCTION account_count() RETURNS INT RETURN 1",
        DdlFamily::Function,
        DdlObjectKind::Function,
        "account_count",
        None,
    ),
    (
        "ALTER FUNCTION account_count COMMENT 'count'",
        DdlFamily::Function,
        DdlObjectKind::Function,
        "account_count",
        None,
    ),
    (
        "DROP FUNCTION account_count",
        DdlFamily::Drop,
        DdlObjectKind::Function,
        "account_count",
        None,
    ),
];

const EVENT_TRIGGER_CASES: &[OperationCase] = &[
    (
        "CREATE EVENT expire_accounts ON SCHEDULE EVERY 1 DAY DO DELETE FROM accounts",
        DdlFamily::Event,
        DdlObjectKind::Event,
        "expire_accounts",
        None,
    ),
    (
        "ALTER EVENT expire_accounts DISABLE",
        DdlFamily::Event,
        DdlObjectKind::Event,
        "expire_accounts",
        None,
    ),
    (
        "DROP EVENT expire_accounts",
        DdlFamily::Drop,
        DdlObjectKind::Event,
        "expire_accounts",
        None,
    ),
    (
        "CREATE TRIGGER accounts_bi BEFORE INSERT ON accounts FOR EACH ROW SET NEW.id = NEW.id",
        DdlFamily::Trigger,
        DdlObjectKind::Trigger,
        "accounts_bi",
        Some("accounts"),
    ),
    (
        "DROP TRIGGER accounts_bi",
        DdlFamily::Drop,
        DdlObjectKind::Trigger,
        "accounts_bi",
        None,
    ),
    (
        "RENAME TABLE accounts TO archived_accounts",
        DdlFamily::Rename,
        DdlObjectKind::Table,
        "accounts",
        Some("archived_accounts"),
    ),
    (
        "TRUNCATE TABLE accounts",
        DdlFamily::Truncate,
        DdlObjectKind::Table,
        "accounts",
        None,
    ),
];

#[test]
fn parses_table_and_index_ddl_families() {
    assert_operation_cases(TABLE_INDEX_CASES);
}

#[test]
fn parses_named_object_ddl_families() {
    assert_operation_cases(NAMED_OBJECT_CASES);
}

#[test]
fn parses_event_trigger_rename_and_truncate_families() {
    assert_operation_cases(EVENT_TRIGGER_CASES);
}

#[test]
fn parser_ignores_comments_and_preserves_quoted_identifier_contents() {
    assert_eq!(
        parse_ddl_operation(
            "/* migration 7.2 */ ALTER TABLE `account.history` ADD COLUMN note text"
        )
        .expect("quoted identifier"),
        DdlOperation {
            family: DdlFamily::Table,
            object_kind: DdlObjectKind::Table,
            primary_object: "account.history".to_string(),
            secondary_object: None,
            index_ast: None,
            create_table_ast: None,
            alter_table_ast: None,
        }
    );
}

#[test]
fn parser_rejects_qualified_and_multi_object_ddl() {
    for sql in [
        "ALTER TABLE other_db.accounts ADD COLUMN handle varchar(64)",
        "RENAME TABLE accounts TO archived_accounts, users TO archived_users",
        "DROP TABLE accounts, users",
    ] {
        assert!(parse_ddl_operation(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn canonical_evidence_covers_every_object_family() {
    let target = semantic_snapshot(7, Some(8));
    let source = semantic_snapshot(9, Some(10));
    for sql in [
        "ALTER TABLE accounts ADD COLUMN email varchar(64)",
        "CREATE INDEX idx_new ON accounts (id)",
        "ALTER VIEW active_accounts AS SELECT id FROM accounts",
        "ALTER PROCEDURE refresh_accounts SQL SECURITY INVOKER",
        "ALTER FUNCTION account_count COMMENT 'count'",
        "ALTER EVENT expire_accounts DISABLE",
        "DROP TRIGGER accounts_bi",
    ] {
        let operation = parse_ddl_operation(sql).expect(sql);
        let evidence = build_semantic_evidence(&operation, &target, &source).expect(sql);
        assert!(!evidence.canonical_ast.is_empty(), "{sql}");
        assert_ne!(evidence.pre_state, evidence.expected_post_state, "{sql}");
    }
}

#[test]
fn only_complete_index_inventory_is_currently_automatic() {
    for sql in [
        "CREATE TABLE accounts (id bigint primary key)",
        "ALTER TABLE accounts ADD COLUMN handle varchar(64)",
        "DROP TABLE accounts",
        "CREATE VIEW active_accounts AS SELECT id FROM accounts",
        "CREATE PROCEDURE refresh_accounts() SELECT 1",
        "CREATE FUNCTION account_count() RETURNS INT RETURN 1",
        "CREATE EVENT expire_accounts ON SCHEDULE EVERY 1 DAY DO SELECT 1",
        "CREATE TRIGGER accounts_bi BEFORE INSERT ON accounts FOR EACH ROW SET NEW.id = NEW.id",
        "RENAME TABLE accounts TO archived_accounts",
        "TRUNCATE TABLE accounts",
    ] {
        let operation = parse_ddl_operation(sql).expect(sql);
        assert!(
            !supports_automatic_semantic_recovery(&operation),
            "accepted {sql}"
        );
    }
    for sql in [
        "CREATE INDEX idx_handle ON accounts (handle)",
        "DROP INDEX idx_handle ON accounts",
    ] {
        let operation = parse_ddl_operation(sql).expect(sql);
        assert!(
            supports_automatic_semantic_recovery(&operation),
            "rejected {sql}"
        );
    }
}

#[test]
fn strict_index_admission_rejects_non_simple_forms() {
    for sql in [
        "CREATE UNIQUE INDEX idx_handle ON accounts (handle)",
        "CREATE INDEX ON accounts (handle)",
        "CREATE INDEX idx_handle ON accounts ((lower(handle)))",
        "CREATE INDEX idx_handle ON accounts (handle) USING HASH",
        "CREATE INDEX idx_handle ON accounts (handle) INVISIBLE",
        "CREATE INDEX idx_handle ON accounts (handle) COMMENT 'migration'",
        "CREATE INDEX idx_handle ON accounts (handle) ALGORITHM=INPLACE",
        "CREATE INDEX idx_handle ON accounts (handle) LOCK=NONE",
        "DROP INDEX IF EXISTS idx_handle ON accounts",
        "DROP INDEX `accounts`.`idx_handle` ON accounts",
    ] {
        assert!(
            !supports_automatic_index_ddl(sql),
            "accepted unsupported index DDL: {sql}"
        );
    }
}

#[test]
fn strict_index_admission_accepts_simple_secondary_btree_forms() {
    for sql in [
        "CREATE INDEX idx_handle ON accounts (handle) USING BTREE",
        "CREATE INDEX `idx``handle` ON `accounts` (`handle`)",
        "CREATE INDEX idx_handle ON accounts (handle(8) DESC)",
        "DROP INDEX idx_handle ON accounts",
    ] {
        assert!(
            supports_automatic_index_ddl(sql),
            "rejected simple index DDL: {sql}"
        );
    }
}

#[test]
fn index_tokenizer_honors_mysql_line_comment_whitespace_rule() {
    let tokens = tokenize_ddl("CREATE INDEX idx ON accounts (handle) --not-a-comment.other")
        .expect("tokens");
    assert!(tokens.windows(2).any(|pair| pair == ["-", "-"]));
    assert!(tokens.iter().any(|token| token == "."));

    let tokens =
        tokenize_ddl("CREATE INDEX idx ON accounts (handle) -- valid comment\n").expect("tokens");
    assert!(!tokens.iter().any(|token| token == "valid"));
}

#[test]
fn index_tokenizer_skips_each_supported_comment_form() {
    let tokens = tokenize_ddl(
        "CREATE /* block */ INDEX idx ON accounts (handle) # hash comment\n -- line comment\n",
    )
    .expect("tokens");

    assert_eq!(
        tokens,
        [
            "CREATE", "INDEX", "idx", "ON", "accounts", "(", "handle", ")"
        ]
    );
}

#[test]
fn quoted_comment_markers_do_not_reclassify_index_ddl() {
    assert!(supports_automatic_index_ddl(
        "CREATE INDEX `idx--name` ON `accounts` (`handle/*name`)",
    ));
}

#[test]
fn semantic_index_validation_rejects_generated_columns() {
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.tables[0].columns[1].generated = Some(crate::inventory::GeneratedColumn {
        expression: "lower(handle)".to_string(),
        generation_kind: "VIRTUAL".to_string(),
    });
    let operation =
        parse_ddl_operation("CREATE INDEX idx_new ON accounts (handle)").expect("create index");

    assert!(build_semantic_evidence(&operation, &target, &target).is_err());
}

#[test]
fn index_post_state_is_full_target_table_state_not_source_index_state() {
    let target = semantic_snapshot(7, Some(8));
    let source = semantic_snapshot(9, Some(10));
    let operation = parse_ddl_operation("CREATE INDEX idx_new ON accounts (id) USING BTREE")
        .expect("create index");

    let evidence = build_semantic_evidence(&operation, &target, &source).expect("index evidence");

    assert!(evidence.expected_post_state.contains("\"kind\":\"table\""));
    assert!(evidence.expected_post_state.contains("idx_handle"));
    assert!(evidence.expected_post_state.contains("idx_new"));
    assert!(!evidence.expected_post_state.contains("\"row_count\""));
}

#[test]
fn strict_index_admission_rejects_every_non_simple_form() {
    for sql in [
        "CREATE UNIQUE INDEX idx_handle ON accounts (handle)",
        "CREATE INDEX idx_handle ON accounts ((lower(handle)))",
        "CREATE FULLTEXT INDEX idx_handle ON accounts (handle)",
        "CREATE SPATIAL INDEX idx_handle ON accounts (handle)",
        "CREATE INDEX idx_handle ON accounts (handle) INVISIBLE",
        "CREATE INDEX idx_handle ON accounts (handle) ALGORITHM=INPLACE",
        "CREATE INDEX idx_handle ON accounts (handle) LOCK=NONE",
        "CREATE INDEX idx_handle ON accounts (handle), idx_other ON accounts (id)",
        "CREATE INDEX idx_handle ON other_db.accounts (handle)",
        "CREATE INDEX idx_handle ON other_db . accounts (handle)",
        "CREATE INDEX idx_handle ON other_db /* comment */ . accounts (handle)",
        "CREATE INDEX idx_handle ON other_db. /* comment */ accounts (handle)",
        "CREATE INDEX `idx_handle` ON `other_db`/**/.`accounts` (`handle`)",
        "CREATE INDEX \"idx_handle\" ON \"accounts\" (\"handle\")",
        "CREATE INDEX other_db.idx_handle ON accounts (handle)",
        "CREATE INDEX idx_handle ON accounts (handle), idx_other ON accounts (id)",
        "CREATE INDEX idx_handle ON accounts (handle",
        "DROP INDEX IF EXISTS idx_handle ON accounts",
        "DROP INDEX accounts.idx_handle ON accounts",
        "/* migration */ CREATE INDEX idx_handle ON accounts (handle)",
    ] {
        let accepted = parse_ddl_operation(sql)
            .ok()
            .is_some_and(|operation| supports_automatic_semantic_recovery(&operation));
        assert!(!accepted, "automatically admitted {sql}");
    }
}

#[test]
fn create_index_expected_state_uses_translated_ast() {
    let (target, source, operation) = translated_index_fixture();
    let evidence = build_semantic_evidence(&operation, &target, &source).expect("index evidence");
    assert!(
        evidence
            .expected_post_state
            .contains("\"prefix_length\":12")
    );
    assert!(
        evidence
            .expected_post_state
            .contains("\"collation\":\"utf8mb4_bin\"")
    );
    assert!(
        !evidence
            .expected_post_state
            .contains("\"prefix_length\":99")
    );
}

fn translated_index_fixture() -> (SemanticSchemaSnapshot, SemanticSchemaSnapshot, DdlOperation) {
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.indexes.clear();
    let mut source = target.clone();
    source.inventory.indexes.push(IndexInventory {
        table: "accounts".to_string(),
        name: "idx_handle".to_string(),
        unique: false,
        index_type: "BTREE".to_string(),
        visible: true,
        comment: None,
        columns: vec![IndexColumnInventory {
            name: "handle".to_string(),
            sequence: 1,
            prefix_length: Some(99),
            collation: Some("D".to_string()),
            order: "DESC".to_string(),
        }],
    });
    let operation = parse_ddl_operation(
        "CREATE INDEX idx_handle ON accounts (handle(12) DESC COLLATE utf8mb4_bin)",
    )
    .expect("simple index");
    (target, source, operation)
}

#[test]
fn drop_index_requires_recorded_definition_and_no_foreign_key_dependency() {
    let target = semantic_snapshot(7, Some(8));
    let source = target.clone();
    let operation = parse_ddl_operation("DROP INDEX idx_handle ON accounts").expect("drop index");

    let evidence =
        build_semantic_evidence(&operation, &target, &source).expect("recorded index definition");

    assert!(evidence.pre_state.contains("idx_handle"));
    assert!(evidence.expected_post_state.contains("\"kind\":\"table\""));
    assert!(!evidence.expected_post_state.contains("idx_handle"));
}

#[test]
fn drop_index_with_fk_dependency_or_incomplete_metadata_is_manual() {
    let mut dependent_target = semantic_snapshot(7, Some(8));
    dependent_target.inventory.foreign_keys = vec![ForeignKeyInventory {
        table: "accounts".to_string(),
        name: "accounts_fk".to_string(),
        columns: vec!["handle".to_string()],
        referenced_table: "users".to_string(),
        referenced_columns: vec!["id".to_string()],
    }];
    let operation = parse_ddl_operation("DROP INDEX idx_handle ON accounts").expect("drop index");
    assert!(build_semantic_evidence(&operation, &dependent_target, &dependent_target).is_err());

    let mut incomplete_target = semantic_snapshot(7, Some(8));
    incomplete_target.inventory.indexes[0].columns[0]
        .order
        .clear();
    assert!(build_semantic_evidence(&operation, &incomplete_target, &incomplete_target).is_err());
}

#[test]
fn source_inventory_must_be_bracketed_at_exact_event_end_coordinate() {
    let expected_file = "mysqld-bin.000777";
    let expected_position = 180;
    let exact = crate::inventory::SourceMasterCoordinate {
        file: expected_file.to_string(),
        position: expected_position,
    };
    let ahead = crate::inventory::SourceMasterCoordinate {
        file: expected_file.to_string(),
        position: expected_position + 1,
    };

    assert!(
        validate_source_snapshot_coordinate(expected_file, expected_position, &exact, &exact,)
            .is_ok()
    );
    assert!(
        validate_source_snapshot_coordinate(expected_file, expected_position, &ahead, &ahead,)
            .is_err()
    );
    assert!(
        validate_source_snapshot_coordinate(expected_file, expected_position, &exact, &ahead,)
            .is_err()
    );
}

#[test]
fn target_inventory_must_be_stable_across_evidence_capture() {
    let before = semantic_snapshot(7, Some(8));
    let same = before.clone();
    let drifted = semantic_snapshot(8, Some(9));

    assert!(validate_target_snapshot_consistency(&before, &same).is_ok());
    assert!(validate_target_snapshot_consistency(&before, &drifted).is_err());
}

#[test]
fn add_column_evidence_derives_post_state_without_live_source_snapshot() {
    let target = semantic_snapshot(7, Some(8));
    let operation = parse_ddl_operation(
        "ALTER TABLE accounts ADD COLUMN profile_slug VARCHAR(64) DEFAULT NULL AFTER handle",
    )
    .expect("alter");

    let evidence = build_semantic_evidence(&operation, &target, &target).expect("table evidence");

    assert_ne!(evidence.pre_state, evidence.expected_post_state);
    assert!(evidence.canonical_ast.contains("profile_slug"));
    assert!(evidence.expected_post_state.contains("profile_slug"));
    assert!(!evidence.pre_state.contains("\"row_count\":"));
    assert!(!evidence.expected_post_state.contains("\"row_count\":"));
}

#[test]
fn drop_has_explicit_absent_postcondition() {
    let target = semantic_snapshot(7, Some(8));
    let source = semantic_snapshot(9, Some(10));
    let evidence = build_semantic_evidence(
        &parse_ddl_operation("DROP TABLE accounts").expect("drop"),
        &target,
        &source,
    )
    .expect("drop evidence");
    assert_eq!(evidence.expected_post_state, canonical_absent_state());
}

#[test]
fn rename_has_explicit_destination_postcondition() {
    let target = semantic_snapshot(7, Some(8));
    let source = semantic_snapshot(9, Some(10));
    let evidence = build_semantic_evidence(
        &parse_ddl_operation("RENAME TABLE accounts TO archived_accounts").expect("rename"),
        &target,
        &source,
    )
    .expect("rename evidence");
    assert!(evidence.expected_post_state.contains("archived_accounts"));
    assert!(evidence.expected_post_state.contains("absent"));
}

#[test]
fn truncate_has_explicit_runtime_postcondition() {
    let target = semantic_snapshot(7, Some(8));
    let source = semantic_snapshot(9, Some(10));
    let evidence = build_semantic_evidence(
        &parse_ddl_operation("TRUNCATE TABLE accounts").expect("truncate"),
        &target,
        &source,
    )
    .expect("truncate evidence");
    assert!(evidence.pre_state.contains("\"row_count\":7"));
    assert!(evidence.expected_post_state.contains("\"row_count\":0"));
    assert!(
        evidence
            .expected_post_state
            .contains("\"auto_increment\":1")
    );
}

fn semantic_snapshot(row_count: u64, auto_increment: Option<u64>) -> SemanticSchemaSnapshot {
    SemanticSchemaSnapshot {
        inventory: fixture_inventory(row_count),
        table_runtime: fixture_runtime(row_count, auto_increment),
    }
}

fn fixture_runtime(
    row_count: u64,
    auto_increment: Option<u64>,
) -> std::collections::BTreeMap<String, TableRuntimeState> {
    std::collections::BTreeMap::from([(
        "accounts".to_string(),
        TableRuntimeState {
            row_count,
            auto_increment,
        },
    )])
}

fn fixture_inventory(row_count: u64) -> SchemaInventory {
    SchemaInventory {
        schema: "fixture_cdc".to_string(),
        tables: vec![fixture_table()],
        indexes: vec![fixture_index(row_count)],
        foreign_keys: Vec::new(),
        views: vec![fixture_view(row_count)],
        triggers: vec![fixture_trigger(row_count)],
        routines: fixture_routines(row_count),
        events: vec![fixture_event(row_count)],
    }
}

fn fixture_table() -> TableInventory {
    TableInventory {
        name: "accounts".to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        collation: Some("utf8mb4_unicode_ci".to_string()),
        primary_key: vec!["id".to_string()],
        columns: fixture_columns(),
    }
}

fn fixture_columns() -> Vec<ColumnInventory> {
    vec![
        ColumnInventory {
            name: "id".to_string(),
            ordinal_position: 1,
            column_type: "bigint unsigned".to_string(),
            data_type: "bigint".to_string(),
            is_nullable: false,
            character_set: None,
            collation: None,
            default_value: None,
            extra: "auto_increment".to_string(),
            comment: String::new(),
            generated: None,
        },
        ColumnInventory {
            name: "handle".to_string(),
            ordinal_position: 2,
            column_type: "varchar(64)".to_string(),
            data_type: "varchar".to_string(),
            is_nullable: true,
            character_set: None,
            collation: None,
            default_value: None,
            extra: String::new(),
            comment: String::new(),
            generated: None,
        },
    ]
}

fn fixture_index(row_count: u64) -> IndexInventory {
    IndexInventory {
        table: "accounts".to_string(),
        name: "idx_handle".to_string(),
        unique: false,
        index_type: "BTREE".to_string(),
        visible: true,
        comment: None,
        columns: vec![IndexColumnInventory {
            name: "handle".to_string(),
            sequence: 1,
            prefix_length: Some(row_count as u32),
            collation: Some("A".to_string()),
            order: "ASC".to_string(),
        }],
    }
}

fn fixture_view(row_count: u64) -> ViewInventory {
    ViewInventory {
        name: "active_accounts".to_string(),
        definition: format!("select id from accounts where id <= {row_count}"),
    }
}

fn fixture_trigger(row_count: u64) -> TriggerInventory {
    TriggerInventory {
        name: "accounts_bi".to_string(),
        table: "accounts".to_string(),
        timing: "BEFORE".to_string(),
        event: "INSERT".to_string(),
        statement: format!("set new.id = new.id + {row_count}"),
    }
}

fn fixture_routines(row_count: u64) -> Vec<RoutineInventory> {
    vec![
        RoutineInventory {
            name: "refresh_accounts".to_string(),
            routine_type: "PROCEDURE".to_string(),
            definition: Some(format!("select {row_count}")),
        },
        RoutineInventory {
            name: "account_count".to_string(),
            routine_type: "FUNCTION".to_string(),
            definition: Some(format!("return {row_count}")),
        },
    ]
}

fn fixture_event(row_count: u64) -> EventInventory {
    EventInventory {
        name: "expire_accounts".to_string(),
        status: "ENABLED".to_string(),
        definition: format!("delete from accounts where id <= {row_count}"),
    }
}

#[test]
fn transforms_mariadb_drop_column_if_exists_for_mysql8() {
    let columns = ["id".to_string(), "handle".to_string()]
        .into_iter()
        .collect();

    let transformation = transform_drop_columns_if_exists(
        "ALTER TABLE accounts DROP COLUMN IF EXISTS handle",
        &columns,
    )
    .expect("DROP COLUMN IF EXISTS transformation");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some("ALTER TABLE `accounts` DROP COLUMN `handle`")
    );
}

#[test]
fn drop_column_if_exists_matches_target_column_case_insensitively() {
    let columns = ["id".to_string(), "handle".to_string()]
        .into_iter()
        .collect();

    let transformation = transform_drop_columns_if_exists(
        "ALTER TABLE accounts DROP COLUMN IF EXISTS HANDLE",
        &columns,
    )
    .expect("case-insensitive DROP COLUMN IF EXISTS");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some("ALTER TABLE `accounts` DROP COLUMN `handle`")
    );
}

#[test]
fn duplicate_case_variant_drop_columns_execute_target_column_once() {
    let columns = ["id".to_string(), "handle".to_string()]
        .into_iter()
        .collect();

    let transformation = transform_drop_columns_if_exists(
        "ALTER TABLE accounts DROP COLUMN IF EXISTS handle, DROP COLUMN IF EXISTS HANDLE",
        &columns,
    )
    .expect("duplicate case-variant DROP COLUMN IF EXISTS transformation");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some("ALTER TABLE `accounts` DROP COLUMN `handle`")
    );
}

#[test]
fn drop_column_if_exists_ast_removes_target_column_case_insensitively() {
    let mut target = semantic_snapshot(1, Some(2));
    target.inventory.indexes.clear();
    target.inventory.foreign_keys.clear();
    let operation = parse_ddl_operation("ALTER TABLE accounts DROP COLUMN IF EXISTS HANDLE")
        .expect("typed DROP COLUMN operation");

    let evidence = build_semantic_evidence(&operation, &target, &target)
        .expect("target-local DROP COLUMN evidence");
    let ast: serde_json::Value =
        serde_json::from_str(&evidence.canonical_ast).expect("canonical AST JSON");
    assert_eq!(
        ast["parsed_alter_table"]["clauses"],
        serde_json::json!([{"kind":"drop_column","name":"HANDLE","if_exists":true}])
    );
    let post: serde_json::Value =
        serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON");
    let columns = post["definition"]["columns"]
        .as_array()
        .expect("post-state columns");
    assert_eq!(
        columns
            .iter()
            .map(|column| column["name"].as_str().expect("column name"))
            .collect::<Vec<_>>(),
        vec!["id"]
    );
}

#[test]
fn drop_column_if_exists_is_proven_noop_when_target_column_is_absent() {
    let columns = ["id".to_string()].into_iter().collect();

    let transformation = transform_drop_columns_if_exists(
        "ALTER TABLE accounts DROP COLUMN IF EXISTS handle",
        &columns,
    )
    .expect("DROP COLUMN IF EXISTS no-op");

    assert_eq!(transformation.target_sql, None);
}

#[test]
fn transforms_mariadb_multi_clause_rename_column_if_exists_for_mysql8() {
    let columns = ["arc_start_order", "arc_end_order"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let transformation = transform_rename_columns_if_exists(
        "ALTER TABLE `home_feed_captions`\n\
         RENAME COLUMN IF EXISTS `arc_start_order` TO `deprecated_arc_start_order`,\n\
         RENAME COLUMN IF EXISTS `arc_end_order` TO `deprecated_arc_end_order`",
        &columns,
    )
    .expect("MariaDB rename transformation");

    assert_eq!(transformation.version, "mariadb-mysql8-v1");
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `home_feed_captions` \
             RENAME COLUMN `arc_start_order` TO `deprecated_arc_start_order`, \
             RENAME COLUMN `arc_end_order` TO `deprecated_arc_end_order`"
        )
    );
}

#[test]
fn rename_column_if_exists_becomes_proven_noop_when_source_columns_are_absent() {
    let columns = ["deprecated_arc_start_order", "deprecated_arc_end_order"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let transformation = transform_rename_columns_if_exists(
        "ALTER TABLE home_feed_captions \
         RENAME COLUMN IF EXISTS arc_start_order TO deprecated_arc_start_order, \
         RENAME COLUMN IF EXISTS arc_end_order TO deprecated_arc_end_order",
        &columns,
    )
    .expect("proven no-op transformation");

    assert_eq!(transformation.target_sql, None);
}

#[test]
fn live_transform_keeps_fixture_create_table_disabled_until_evidence_gates_pass() {
    let inventory = LiveDdlSemanticInventory::new(
        InventoryConfig::default(),
        InventoryConfig::default(),
        "fixture_cdc".to_string(),
        "fixture_cdc".to_string(),
    );
    let error = inventory
        .transform_sql(
            "CREATE TABLE accounts (\
                id BIGINT NOT NULL PRIMARY KEY, \
                email VARCHAR(255) NOT NULL, \
                payload VARCHAR(64) NOT NULL, \
                KEY idx_accounts_payload (payload)\
            ) ENGINE=InnoDB",
        )
        .expect_err("runtime CREATE TABLE admission must remain disabled");

    assert!(error.contains("does not support this statement"), "{error}");
}

#[test]
fn fixture_create_table_evidence_captures_fenced_source_defaults_and_explicit_sql() {
    let source_sql = "CREATE TABLE accounts (\
        id BIGINT NOT NULL PRIMARY KEY, \
        email VARCHAR(255) NOT NULL, \
        payload VARCHAR(64) NOT NULL, \
        KEY idx_accounts_payload (payload)\
    ) ENGINE=InnoDB";
    let operation = parse_ddl_operation(source_sql).expect("fixture CREATE TABLE operation");
    let target = SemanticSchemaSnapshot {
        inventory: SchemaInventory {
            schema: "fixture_cdc".to_string(),
            tables: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            routines: Vec::new(),
            events: Vec::new(),
        },
        table_runtime: Default::default(),
    };
    let coordinate = crate::inventory::SourceMasterCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 180,
    };
    let defaults = crate::inventory::SchemaDefaults {
        character_set: "utf8mb4".to_string(),
        collation: "utf8mb4_unicode_ci".to_string(),
    };

    let evidence = build_fenced_create_table_evidence(
        &operation,
        &target,
        &defaults,
        "mysqld-bin.000777",
        180,
        &coordinate,
        &coordinate,
    )
    .expect("fenced fixture CREATE TABLE evidence");

    assert_eq!(evidence.pre_state, canonical_absent_state());
    assert_eq!(
        evidence.generated_sql.as_deref(),
        Some(
            "CREATE TABLE `accounts` (`id` BIGINT NOT NULL, `email` VARCHAR(255) NOT NULL, `payload` VARCHAR(64) NOT NULL, PRIMARY KEY (`id`), KEY `idx_accounts_payload` (`payload`)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
    );
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).expect("AST JSON");
    assert_eq!(ast["source_schema_defaults"]["character_set"], "utf8mb4");
    assert_eq!(
        ast["source_schema_defaults"]["collation"],
        "utf8mb4_unicode_ci"
    );
    let post: serde_json::Value =
        serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON");
    assert_eq!(post["definition"]["collation"], "utf8mb4_unicode_ci");

    let ahead = crate::inventory::SourceMasterCoordinate {
        file: coordinate.file.clone(),
        position: coordinate.position + 1,
    };
    assert!(
        build_fenced_create_table_evidence(
            &operation,
            &target,
            &defaults,
            "mysqld-bin.000777",
            180,
            &coordinate,
            &ahead,
        )
        .is_err()
    );
}

#[test]
fn fixture_create_table_expected_post_state_matches_observed_inventory_exactly() {
    let source_sql = "CREATE TABLE accounts (\
        id BIGINT NOT NULL PRIMARY KEY, \
        email VARCHAR(255) NOT NULL, \
        payload VARCHAR(64) NOT NULL, \
        KEY idx_accounts_payload (payload)\
    ) ENGINE=InnoDB";
    let operation = parse_ddl_operation(source_sql).expect("fixture CREATE TABLE operation");
    let absent = SemanticSchemaSnapshot {
        inventory: SchemaInventory {
            schema: "fixture_cdc".to_string(),
            tables: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            routines: Vec::new(),
            events: Vec::new(),
        },
        table_runtime: Default::default(),
    };
    let coordinate = crate::inventory::SourceMasterCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 180,
    };
    let defaults = crate::inventory::SchemaDefaults {
        character_set: "utf8mb4".to_string(),
        collation: "utf8mb4_unicode_ci".to_string(),
    };
    let evidence = build_fenced_create_table_evidence(
        &operation,
        &absent,
        &defaults,
        &coordinate.file,
        coordinate.position,
        &coordinate,
        &coordinate,
    )
    .expect("fixture CREATE TABLE evidence");
    let observed = SemanticSchemaSnapshot {
        inventory: SchemaInventory {
            schema: "fixture_cdc".to_string(),
            tables: vec![TableInventory {
                name: "accounts".to_string(),
                table_type: "BASE TABLE".to_string(),
                engine: Some("InnoDB".to_string()),
                collation: Some("utf8mb4_unicode_ci".to_string()),
                primary_key: vec!["id".to_string()],
                columns: vec![
                    ColumnInventory {
                        name: "id".to_string(),
                        ordinal_position: 1,
                        column_type: "bigint".to_string(),
                        data_type: "bigint".to_string(),
                        is_nullable: false,
                        character_set: None,
                        collation: None,
                        default_value: None,
                        extra: String::new(),
                        comment: String::new(),
                        generated: None,
                    },
                    ColumnInventory {
                        name: "email".to_string(),
                        ordinal_position: 2,
                        column_type: "varchar(255)".to_string(),
                        data_type: "varchar".to_string(),
                        is_nullable: false,
                        character_set: None,
                        collation: None,
                        default_value: None,
                        extra: String::new(),
                        comment: String::new(),
                        generated: None,
                    },
                    ColumnInventory {
                        name: "payload".to_string(),
                        ordinal_position: 3,
                        column_type: "varchar(64)".to_string(),
                        data_type: "varchar".to_string(),
                        is_nullable: false,
                        character_set: None,
                        collation: None,
                        default_value: None,
                        extra: String::new(),
                        comment: String::new(),
                        generated: None,
                    },
                ],
            }],
            indexes: vec![IndexInventory {
                table: "accounts".to_string(),
                name: "idx_accounts_payload".to_string(),
                unique: false,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: None,
                columns: vec![IndexColumnInventory {
                    name: "payload".to_string(),
                    sequence: 1,
                    prefix_length: None,
                    collation: Some("A".to_string()),
                    order: "ASC".to_string(),
                }],
            }],
            foreign_keys: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            routines: Vec::new(),
            events: Vec::new(),
        },
        table_runtime: Default::default(),
    };

    let observed_state =
        observe_operation_state(&observed, &operation).expect("observed CREATE TABLE post-state");
    assert_eq!(observed_state, evidence.expected_post_state);

    let mut drifted = observed;
    drifted.inventory.tables[0].collation = Some("utf8mb4_general_ci".to_string());
    assert_ne!(
        observe_operation_state(&drifted, &operation).expect("drifted observed state"),
        evidence.expected_post_state
    );
}

#[test]
fn fixture_create_table_expected_indexes_use_observed_inventory_order() {
    let sql = "CREATE TABLE accounts (\
        id BIGINT NOT NULL PRIMARY KEY, \
        a BIGINT NOT NULL, \
        b BIGINT NOT NULL, \
        KEY z_idx (a), \
        KEY a_idx (b)\
    ) ENGINE=InnoDB";
    let operation = parse_ddl_operation(sql).expect("multi-key CREATE TABLE operation");
    let absent = SemanticSchemaSnapshot {
        inventory: SchemaInventory {
            schema: "fixture_cdc".to_string(),
            tables: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            routines: Vec::new(),
            events: Vec::new(),
        },
        table_runtime: Default::default(),
    };
    let coordinate = crate::inventory::SourceMasterCoordinate {
        file: "mysqld-bin.000777".to_string(),
        position: 180,
    };
    let evidence = build_fenced_create_table_evidence(
        &operation,
        &absent,
        &crate::inventory::SchemaDefaults {
            character_set: "utf8mb4".to_string(),
            collation: "utf8mb4_unicode_ci".to_string(),
        },
        &coordinate.file,
        coordinate.position,
        &coordinate,
        &coordinate,
    )
    .expect("multi-key CREATE TABLE evidence");
    let post: serde_json::Value =
        serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON");
    assert_eq!(
        post["indexes"]
            .as_array()
            .expect("indexes")
            .iter()
            .map(|index| index["name"].as_str().expect("index name"))
            .collect::<Vec<_>>(),
        vec!["a_idx", "z_idx"]
    );
}

#[test]
fn fixture_create_table_rejects_empty_and_punctuation_identifiers() {
    for sql in [
        "CREATE TABLE `` (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
        "CREATE TABLE accounts (`=` BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
        "CREATE TABLE accounts (id BIGINT NOT NULL PRIMARY KEY, KEY `` (id)) ENGINE=InnoDB",
    ] {
        assert!(parse_fixture_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn fixture_create_accounts_table_has_typed_ast_and_deterministic_mysql8_sql() {
    let source_sql = "CREATE TABLE accounts (\
        id BIGINT NOT NULL PRIMARY KEY, \
        email VARCHAR(255) NOT NULL, \
        payload VARCHAR(64) NOT NULL, \
        KEY idx_accounts_payload (payload)\
    ) ENGINE=InnoDB";

    let ast = parse_fixture_create_table(source_sql).expect("fixture CREATE TABLE AST");
    assert_eq!(ast.name, "accounts");
    assert_eq!(ast.engine, "InnoDB");
    assert_eq!(ast.primary_key, vec!["id"]);
    assert_eq!(ast.columns.len(), 3);
    assert_eq!(
        ast.columns
            .iter()
            .map(|column| (
                column.name.as_str(),
                column.column_type.as_str(),
                column.nullable,
            ))
            .collect::<Vec<_>>(),
        vec![
            ("id", "bigint", false),
            ("email", "varchar(255)", false),
            ("payload", "varchar(64)", false),
        ]
    );
    assert_eq!(ast.indexes.len(), 1);
    assert_eq!(ast.indexes[0].name, "idx_accounts_payload");
    assert!(!ast.indexes[0].unique);
    assert_eq!(ast.indexes[0].key_parts.len(), 1);
    assert_eq!(ast.indexes[0].key_parts[0].column, "payload");

    let transformation =
        transform_fixture_create_table(source_sql).expect("fixture CREATE TABLE transformation");
    assert_eq!(transformation.version, DDL_TRANSFORMATION_VERSION);
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "CREATE TABLE `accounts` (`id` BIGINT NOT NULL, `email` VARCHAR(255) NOT NULL, `payload` VARCHAR(64) NOT NULL, PRIMARY KEY (`id`), KEY `idx_accounts_payload` (`payload`)) ENGINE=InnoDB"
        )
    );
}

#[test]
fn production_add_column_ddl_transforms_to_deterministic_mysql8_sql() {
    let inventory = LiveDdlSemanticInventory::new(
        InventoryConfig::default(),
        InventoryConfig::default(),
        "globalcomix".to_string(),
        "globalcomix".to_string(),
    );
    let source_sql = "ALTER TABLE `home_feed_panel_candidates`\n\
         ADD COLUMN `filter_prompt_version` VARCHAR(64) DEFAULT NULL COMMENT 'sanitized description' AFTER `filter_reason`,\n\
         ADD COLUMN `filtered_time` DATETIME NULL DEFAULT NULL COMMENT 'sanitized description' AFTER `filter_prompt_version`";

    let transformation = inventory
        .transform_sql(source_sql)
        .expect("production ADD COLUMN DDL must be translatable");

    assert_eq!(transformation.version, DDL_TRANSFORMATION_VERSION);
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `home_feed_panel_candidates` ADD COLUMN `filter_prompt_version` VARCHAR(64) NULL DEFAULT NULL COMMENT 'sanitized description' AFTER `filter_reason`, ADD COLUMN `filtered_time` DATETIME NULL DEFAULT NULL COMMENT 'sanitized description' AFTER `filter_prompt_version`"
        )
    );
}

#[test]
fn production_add_column_and_key_ddl_transforms_to_deterministic_mysql8_sql() {
    let inventory = LiveDdlSemanticInventory::new(
        InventoryConfig::default(),
        InventoryConfig::default(),
        "globalcomix".to_string(),
        "globalcomix".to_string(),
    );
    let source_sql = "ALTER TABLE `home_feed_bakes`\n\
         ADD COLUMN `variant_id` SMALLINT UNSIGNED DEFAULT NULL AFTER `reading_direction`,\n\
         ADD KEY `idx_hfb_variant_status_published` (`variant_id`, `status`, `published_time`)";

    let transformation = inventory
        .transform_sql(source_sql)
        .expect("production ADD COLUMN and ADD KEY DDL must be translatable");

    assert_eq!(transformation.version, DDL_TRANSFORMATION_VERSION);
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `home_feed_bakes` ADD COLUMN `variant_id` SMALLINT UNSIGNED NULL DEFAULT NULL AFTER `reading_direction`, ADD KEY `idx_hfb_variant_status_published` (`variant_id`, `status`, `published_time`)"
        )
    );
}

#[test]
fn production_add_unique_key_transforms_to_deterministic_mysql8_sql() {
    let transformation = transform_production_alter_table(
        "ALTER TABLE accounts ADD UNIQUE KEY uq_accounts_email (email)",
    )
    .expect("named production ADD UNIQUE KEY must be translatable");

    assert_eq!(transformation.version, DDL_TRANSFORMATION_VERSION);
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some("ALTER TABLE `accounts` ADD UNIQUE KEY `uq_accounts_email` (`email`)")
    );
}

#[test]
fn production_alter_rendering_depends_only_on_typed_ast() {
    let compact = transform_production_alter_table(
        "ALTER TABLE accounts ADD COLUMN handle VARCHAR(64) COMMENT 'user''s handle' AFTER id",
    )
    .expect("compact ALTER");
    let spaced = transform_production_alter_table(
        "alter table `accounts` add column `handle` varchar ( 64 ) comment 'user''s handle' after `id`",
    )
    .expect("spaced ALTER");

    assert_eq!(compact.target_sql, spaced.target_sql);
    assert_eq!(
        compact.target_sql.as_deref(),
        Some(
            "ALTER TABLE `accounts` ADD COLUMN `handle` VARCHAR(64) NULL DEFAULT NULL COMMENT 'user''s handle' AFTER `id`"
        )
    );
}

#[test]
fn production_alter_rejects_comment_bearing_and_executable_comment_syntax() {
    for sql in [
        "-- deployment comment\nALTER TABLE accounts ADD COLUMN c VARCHAR(64)",
        "ALTER TABLE accounts /* 'decoy' */ ADD COLUMN c VARCHAR(64) COMMENT 'real'",
        "ALTER TABLE accounts ADD COLUMN c VARCHAR(64) /*M!100000 NOT NULL */",
    ] {
        assert!(
            !supports_production_alter_table(sql),
            "comment-bearing ALTER was admitted: {sql}"
        );
    }
}

#[test]
fn production_alter_rejects_noncanonical_type_lengths() {
    assert!(!supports_production_alter_table(
        "ALTER TABLE accounts ADD COLUMN c VARCHAR(00064)"
    ));
}

#[test]
fn production_alter_rejects_datetime_precision() {
    assert!(!supports_production_alter_table(
        "ALTER TABLE accounts ADD COLUMN c DATETIME(6)"
    ));
}

#[test]
fn production_alter_rejects_smallint_display_width() {
    assert!(!supports_production_alter_table(
        "ALTER TABLE accounts ADD COLUMN c SMALLINT(5) UNSIGNED"
    ));
}

#[test]
fn production_alter_rejects_quoted_datetime_type() {
    assert!(!supports_production_alter_table(
        "ALTER TABLE accounts ADD COLUMN c `DATETIME`"
    ));
}

#[test]
fn production_alter_rejects_quoted_varchar_length() {
    assert!(!supports_production_alter_table(
        "ALTER TABLE accounts ADD COLUMN c VARCHAR(`64`)"
    ));
}

#[test]
fn production_alter_rejects_quoted_unsigned_keyword() {
    assert!(!supports_production_alter_table(
        "ALTER TABLE accounts ADD COLUMN c SMALLINT `UNSIGNED`"
    ));
}

#[test]
fn rename_column_if_exists_fails_closed_when_old_and_new_columns_both_exist() {
    let columns = ["arc_start_order", "deprecated_arc_start_order"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let error = transform_rename_columns_if_exists(
        "ALTER TABLE home_feed_captions \
         RENAME COLUMN IF EXISTS arc_start_order TO deprecated_arc_start_order",
        &columns,
    )
    .expect_err("target drift must block transformation");

    assert!(error.contains("both exist"), "{error}");
}
