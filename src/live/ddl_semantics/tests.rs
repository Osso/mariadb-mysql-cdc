use super::model::ParsedAlterClause;
use super::transform::{
    DDL_TRANSFORMATION_VERSION, parse_fixture_create_table, parse_production_alter_table_ast,
    supports_drop_procedure, supports_source_only_release_move_procedure_create,
    transform_drop_columns_if_exists, transform_drop_procedure, transform_fixture_create_table,
    transform_source_only_release_move_procedure_create,
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
        "CREATE INDEX idx_handle ON accounts (handle) USING BTREE INVISIBLE COMMENT 'planner'",
        "CREATE INDEX idx_handle ON accounts (handle) VISIBLE COMMENT 'planner'",
        "DROP INDEX idx_handle ON accounts",
    ] {
        assert!(
            supports_automatic_index_ddl(sql),
            "rejected simple index DDL: {sql}"
        );
    }
}

#[test]
fn planner_index_options_are_preserved_in_the_parsed_ast() {
    let operation = parse_ddl_operation(
        "CREATE INDEX idx_handle ON accounts (handle) USING BTREE INVISIBLE COMMENT 'planner''s index'",
    )
    .expect("planner index DDL");
    let index = operation.index_ast.expect("index AST");

    assert_eq!(index.index_type, "BTREE");
    assert!(!index.visible);
    assert_eq!(index.comment.as_deref(), Some("planner's index"));
}

#[test]
fn modeled_unique_btree_options_translate_through_shared_mapping() {
    for (sql, visibility) in [
        (
            "CREATE UNIQUE INDEX `uq_handle` ON `accounts` (`handle`) USING BTREE VISIBLE COMMENT 'planner'",
            "VISIBLE",
        ),
        (
            "CREATE UNIQUE INDEX `uq_handle` ON `accounts` (`handle`) USING BTREE INVISIBLE COMMENT 'planner'",
            "INVISIBLE",
        ),
    ] {
        let translated = super::translate_modeled_ddl(sql, &[])
            .unwrap_or_else(|error| panic!("modeled unique BTREE rejected: {sql}: {error}"));
        assert_eq!(
            translated.target_sql.as_deref(),
            Some(
                format!(
                    "CREATE UNIQUE INDEX `uq_handle` ON `accounts` (`handle`) USING BTREE {visibility} COMMENT 'planner'"
                )
                .as_str()
            )
        );
    }
}

#[test]
fn unique_hash_and_unproven_generated_ddl_fail_closed_by_provenance() {
    assert!(
        super::translate_modeled_ddl(
            "CREATE UNIQUE INDEX uq_handle ON accounts (handle) USING HASH",
            &[],
        )
        .is_err()
    );
    assert!(
        super::translate_ddl(
            "CREATE UNIQUE INDEX uq_handle ON accounts (handle) USING BTREE",
            &[],
        )
        .is_err()
    );
    for sql in [
        "CREATE TABLE `items` (`id` BIGINT) ENGINE=InnoDB",
        "ALTER TABLE `items` DROP PRIMARY KEY",
    ] {
        assert!(
            super::translate_ddl(sql, &[]).is_err(),
            "unproven streamed DDL passed through: {sql}"
        );
        super::translate_modeled_ddl(sql, &[])
            .unwrap_or_else(|error| panic!("modeled planner DDL rejected: {sql}: {error}"));
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
fn generated_convergence_translation_is_precise_and_temporal_mapping_is_case_insensitive() {
    for sql in [
        "ALTER TABLE `items` DROP PRIMARY KEY",
        "ALTER TABLE `items` ADD PRIMARY KEY (`id`)",
        "ALTER TABLE `items` ADD CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES `parents` (`id`)",
        "ALTER TABLE `items` DROP FOREIGN KEY `fk_parent`",
        "ALTER TABLE `items` ADD CONSTRAINT `positive_id` CHECK (`id` > 0)",
        "ALTER TABLE `items` DROP CHECK `positive_id`",
        "ALTER TABLE `items` DROP COLUMN IF EXISTS `obsolete`",
        "CREATE TABLE `items` (`id` BIGINT, KEY `idx_id` (`id`) USING BTREE INVISIBLE COMMENT 'planner') ENGINE=InnoDB",
    ] {
        super::translate_modeled_ddl(sql, &[])
            .unwrap_or_else(|error| panic!("supported generated DDL rejected: {sql}: {error}"));
    }

    let mixed_case = super::translate_modeled_ddl(
        "ALTER TABLE `items` ADD COLUMN `expires_at` TiMeStAmP(6) NULL",
        &[],
    )
    .expect("mixed-case temporal type");
    assert!(mixed_case.target_sql.unwrap().contains("TiMeStAmP(6)"));

    for sql in [
        "CREATE TABLE `items` (`kind` SET('a','b')) ENGINE=InnoDB",
        "ALTER TABLE `items` ADD COLUMN `a` BIGINT, ADD COLUMN `b` BIGINT",
        "CREATE TABLE `items` (`id` BIGINT)",
    ] {
        assert!(
            super::translate_ddl(sql, &[]).is_err(),
            "unsupported or ambiguous DDL passed through: {sql}"
        );
    }
}

/// MySQL stores TIMESTAMP with the same meaning MariaDB gives it for every value this source
/// holds, so the type is carried across unchanged rather than widened to DATETIME.
#[test]
fn a_timestamp_column_type_is_carried_across_unchanged() {
    let translated = super::translate_modeled_ddl(
        "ALTER TABLE `items` ADD COLUMN `expires_at` TIMESTAMP NULL DEFAULT NULL",
        &[],
    )
    .expect("timestamp column translation");

    assert_eq!(
        translated.target_sql.as_deref(),
        Some("ALTER TABLE `items` ADD COLUMN `expires_at` TIMESTAMP NULL DEFAULT NULL")
    );
}

#[test]
fn timestamp_translation_preserves_admitted_unquoted_identifiers() {
    let index_sql = "CREATE INDEX timestamp ON items (created_at)";
    let translated = super::translate_ddl(index_sql, &[]).expect("admitted streamed index DDL");
    assert_eq!(translated.target_sql.as_deref(), Some(index_sql));

    for sql in [
        "ALTER TABLE items ADD CONSTRAINT timestamp CHECK (id > 0)",
        "ALTER TABLE items ADD COLUMN timestamp BIGINT NULL",
    ] {
        let translated =
            super::translate_modeled_ddl(sql, &[]).expect("admitted modeled identifier DDL");
        assert_eq!(translated.target_sql.as_deref(), Some(sql));
    }

    let temporal = super::translate_modeled_ddl(
        "ALTER TABLE items ADD COLUMN expires_at TiMeStAmP(6) NULL",
        &[],
    )
    .expect("mixed-case TIMESTAMP type");
    assert_eq!(
        temporal.target_sql.as_deref(),
        Some("ALTER TABLE items ADD COLUMN expires_at TiMeStAmP(6) NULL")
    );
}

#[test]
fn generated_foreign_keys_accept_exact_parent_qualification_only() {
    for sql in [
        "ALTER TABLE `items` ADD CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES `parents` (`id`)",
        "ALTER TABLE `items` ADD CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES `globalcomix`.`parents` (`id`)",
    ] {
        super::translate_modeled_ddl(sql, &[])
            .unwrap_or_else(|error| panic!("valid generated foreign key rejected: {sql}: {error}"));
    }

    for sql in [
        "ALTER TABLE `items` ADD CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES `globalcomix`. (`id`)",
        "ALTER TABLE `items` ADD CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES .`parents` (`id`)",
        "ALTER TABLE `items` ADD CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES `one`.`two`.`parents` (`id`)",
    ] {
        assert!(
            super::translate_modeled_ddl(sql, &[]).is_err(),
            "malformed generated foreign key passed: {sql}"
        );
    }
}

#[test]
fn generated_schema_column_definitions_are_explicitly_modeled() {
    for sql in [
        "CREATE TABLE `items` (`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, `name` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'label', `expires_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), `slug` VARCHAR(64) GENERATED ALWAYS AS (lower(`name`)) STORED, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "ALTER TABLE `items` ADD COLUMN `expires_at` DATETIME(6) NULL DEFAULT NULL COMMENT 'expiry' AFTER `name`",
        "ALTER TABLE `items` MODIFY COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT",
    ] {
        super::translate_modeled_ddl(sql, &[])
            .unwrap_or_else(|error| panic!("modeled generated DDL rejected: {sql}: {error}"));
    }

    for sql in [
        "CREATE TABLE `items` (`kind` SET('a','b')) ENGINE=InnoDB",
        "CREATE TABLE `items` (`payload` MYSTERY NULL) ENGINE=InnoDB",
        "CREATE TABLE `items` (`id` BIGINT MAGIC) ENGINE=InnoDB",
        "CREATE TABLE `items` (`id` BIGINT(foo)) ENGINE=InnoDB",
        "ALTER TABLE `items` ADD COLUMN `payload` MYSTERY NULL",
        "ALTER TABLE `items` ADD CONSTRAINT `bad` SOMETHING (`id`)",
        "ALTER TABLE `items` MODIFY COLUMN `id` BIGINT MAGIC",
    ] {
        assert!(
            super::translate_modeled_ddl(sql, &[]).is_err(),
            "unmodeled generated column definition passed through: {sql}"
        );
    }
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
        referenced_schema: "fixture_cdc".to_string(),
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
fn add_varchar_column_expected_state_uses_table_default_encoding() {
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.tables[0].collation = Some("utf8mb4_unicode_ci".to_string());
    target.inventory.tables[0].columns[1].character_set = Some("utf8mb4".to_string());
    target.inventory.tables[0].columns[1].collation = Some("utf8mb4_unicode_ci".to_string());
    let operation = parse_ddl_operation(
        "ALTER TABLE accounts ADD COLUMN profile_slug VARCHAR(64) DEFAULT NULL AFTER handle",
    )
    .expect("alter");

    let evidence = build_semantic_evidence(&operation, &target, &target).expect("table evidence");
    let post: serde_json::Value =
        serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON");
    let profile_slug = post["definition"]["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .find(|column| column["name"] == "profile_slug")
        .expect("profile_slug column");

    assert_eq!(profile_slug["character_set"], "utf8mb4");
    assert_eq!(profile_slug["collation"], "utf8mb4_unicode_ci");
}

#[test]
fn add_char_column_with_ordinary_comment() {
    let sql = "/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */ ALTER TABLE accounts ADD COLUMN `prompt_sha256` CHAR(64) DEFAULT NULL AFTER `handle`";
    assert_eq!(
        super::transform::transform_production_alter_table(sql)
            .expect("commented CHAR ADD")
            .target_sql
            .as_deref(),
        Some(
            "ALTER TABLE `accounts` ADD COLUMN `prompt_sha256` CHAR(64) NULL DEFAULT NULL AFTER `handle`"
        )
    );
    let target = semantic_snapshot(7, Some(8));
    let operation = parse_ddl_operation(sql).expect("CHAR operation");
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("CHAR evidence");
    let mut expected = target.clone();
    expected.inventory.tables[0].columns.push(ColumnInventory {
        name: "prompt_sha256".into(),
        ordinal_position: 3,
        column_type: "char(64)".into(),
        data_type: "char".into(),
        is_nullable: true,
        character_set: Some("utf8mb4".into()),
        collation: Some("utf8mb4_unicode_ci".into()),
        default_value: None,
        extra: String::new(),
        comment: String::new(),
        generated: None,
    });
    assert_eq!(
        evidence.expected_post_state,
        super::canonical::observe_operation_state(&expected, &operation).expect("CHAR post-state")
    );
    for rejected in [
        sql.replace("CHAR(64)", "CHAR(256)"),
        sql.replace("CHAR(64)", "CHAR(064)"),
        sql.replace("CHAR(64)", "CHAR(`64`)"),
        sql.replace("CHAR(64)", "CHAR(64) UNSIGNED"),
        sql.replace(
            "/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */",
            "/*!50000 SET sql_mode='' */",
        ),
    ] {
        assert!(
            !super::transform::supports_production_alter_table(&rejected),
            "{rejected}"
        );
    }
}

#[test]
fn modify_varchar_not_null_grammar() {
    let sql = "ALTER TABLE kg_comic_facets MODIFY COLUMN facet_vocab_version VARCHAR(128) NOT NULL";
    let ast = parse_production_alter_table_ast(sql).expect("observed MODIFY");
    assert_eq!(ast.table, "kg_comic_facets");
    assert!(super::transform::supports_production_alter_table(sql));
    for rejected in [
        "ALTER TABLE t MODIFY COLUMN c VARCHAR(128) NULL",
        "ALTER TABLE t MODIFY COLUMN c VARCHAR(128) NOT NULL DEFAULT 'x'",
        "ALTER TABLE t MODIFY COLUMN c VARCHAR(0128) NOT NULL",
        "ALTER TABLE t MODIFY COLUMN c DATETIME NOT NULL",
        "ALTER TABLE t MODIFY COLUMN c `VARCHAR`(128) NOT NULL",
    ] {
        assert!(
            !super::transform::supports_production_alter_table(rejected),
            "{rejected}"
        );
    }
}

#[test]
fn modify_varchar_ordinary_comments() {
    let sql = include_str!("../../../fixtures/ddl/modify-kg-comic-facets.sql");
    let expected =
        "ALTER TABLE `kg_comic_facets` MODIFY COLUMN `facet_vocab_version` VARCHAR(128) NOT NULL";
    for input in [
        sql.to_string(),
        sql.replace("VARCHAR(128)", "VARCHAR(128) -- width\n"),
    ] {
        assert_eq!(
            super::transform::transform_production_alter_table(&input)
                .expect("ordinary MODIFY comments")
                .target_sql
                .as_deref(),
            Some(expected)
        );
        assert!(
            parse_ddl_operation(&input)
                .expect("operation")
                .alter_table_ast
                .is_some()
        );
    }
    for prefix in [
        "/*!50000 SET sql_mode='' */",
        "/*M! SET sql_mode='' */",
        "/*+ hint */",
    ] {
        assert!(!super::transform::supports_production_alter_table(
            &format!("{prefix} {sql}")
        ));
    }
    for input in [
        "/* ordinary */ ALTER TABLE t MODIFY COLUMN c VARCHAR(128) NOT NULL, ADD KEY idx(c)",
        "/* ordinary */ ALTER TABLE t MODIFY COLUMN c VARCHAR(128) /*! NOT NULL */",
    ] {
        assert!(
            !super::transform::supports_production_alter_table(input),
            "{input}"
        );
    }
}

#[test]
fn modify_varchar_preserves_position_and_indexes() {
    let target = semantic_snapshot(7, Some(8));
    let operation =
        parse_ddl_operation("ALTER TABLE accounts MODIFY COLUMN handle VARCHAR(128) NOT NULL")
            .expect("MODIFY");
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("evidence");
    let mut expected = target.clone();
    let column = &mut expected.inventory.tables[0].columns[1];
    column.column_type = "varchar(128)".into();
    column.is_nullable = false;
    column.character_set = Some("utf8mb4".into());
    column.collation = Some("utf8mb4_unicode_ci".into());
    assert_eq!(
        evidence.expected_post_state,
        super::canonical::observe_operation_state(&expected, &operation).expect("observed state")
    );
    assert_eq!(
        super::transform::transform_production_alter_table(
            "ALTER TABLE accounts MODIFY COLUMN handle VARCHAR(128) NOT NULL"
        )
        .expect("render")
        .target_sql
        .as_deref(),
        Some("ALTER TABLE `accounts` MODIFY COLUMN `handle` VARCHAR(128) NOT NULL")
    );
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
fn assistant_reply_reports_create_requires_exact_source_target_structure() {
    let operation = DdlOperation {
        family: DdlFamily::Table,
        object_kind: DdlObjectKind::Table,
        primary_object: "assistant_reply_reports".to_string(),
        secondary_object: None,
        index_ast: None,
        create_table_ast: None,
        alter_table_ast: None,
    };
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.tables[0].name = "assistant_reply_reports".to_string();
    target.inventory.indexes[0].table = "assistant_reply_reports".to_string();
    let source = target.clone();

    validate_assistant_reply_reports_convergence(&source, &target)
        .expect("equal source and target structures");
    let evidence = build_assistant_reply_reports_create_evidence(&operation, &target)
        .expect("converged CREATE evidence");
    assert_eq!(evidence.pre_state, evidence.expected_post_state);

    let mut divergent_source = source;
    divergent_source.inventory.tables[0].columns[0].comment = "changed".to_string();
    let error = validate_assistant_reply_reports_convergence(&divergent_source, &target)
        .expect_err("schema mismatch must remain blocked");
    assert!(error.contains("does not converge"), "{error}");
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
fn exact_source_only_release_move_procedure_create_is_a_proven_noop() {
    let source_sql =
        include_str!("../../../fixtures/ddl/create-apply-release-move-purchase-repair.sql");

    assert!(supports_source_only_release_move_procedure_create(
        source_sql
    ));
    let transformation = transform_source_only_release_move_procedure_create(source_sql)
        .expect("source-only CREATE PROCEDURE transformation");

    assert_eq!(transformation.version, "mariadb-mysql8-v1");
    assert_eq!(transformation.target_sql, None);
}

#[test]
fn source_only_release_move_procedure_create_admits_only_observed_body_hashes() {
    let first_run =
        include_str!("../../../fixtures/ddl/create-apply-release-move-purchase-repair.sql");
    let final_run =
        include_str!("../../../fixtures/ddl/create-apply-release-move-purchase-repair-95.sql");

    assert!(supports_source_only_release_move_procedure_create(
        first_run
    ));
    assert!(supports_source_only_release_move_procedure_create(
        final_run
    ));
    assert!(!supports_source_only_release_move_procedure_create(
        &final_run.replace(
            "apply_release_move_purchase_repair",
            "another_release_move_procedure"
        )
    ));
    assert!(!supports_source_only_release_move_procedure_create(
        &format!("-- comment\n{first_run}")
    ));
    assert!(!supports_source_only_release_move_procedure_create(
        "CREATE PROCEDURE apply_release_move_purchase_repair() SELECT 1"
    ));
}

#[test]
fn source_only_release_move_procedure_create_requires_target_absence() {
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.routines.push(RoutineInventory {
        name: "apply_release_move_purchase_repair".to_string(),
        routine_type: "PROCEDURE".to_string(),
        definition: Some("select 1".to_string()),
    });
    let operation = DdlOperation {
        family: DdlFamily::Procedure,
        object_kind: DdlObjectKind::Procedure,
        primary_object: "apply_release_move_purchase_repair".to_string(),
        secondary_object: None,
        index_ast: None,
        create_table_ast: None,
        alter_table_ast: None,
    };

    let error = build_source_only_procedure_create_evidence(&operation, &target)
        .expect_err("present target procedure must block source-only no-op");

    assert!(error.contains("already exists"), "{error}");
}

#[test]
fn exact_drop_trigger_allowlist_transforms_present_and_absent_targets() {
    let source_sql = "DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives";
    let target_name = "prevent_deactivating_cloned_archives".to_string();

    let present = translate_ddl(source_sql, std::slice::from_ref(&target_name))
        .expect("exact production DROP TRIGGER must be admitted");
    assert_eq!(present.version, DDL_TRANSFORMATION_VERSION);
    assert_eq!(
        present.target_sql.as_deref(),
        Some("DROP TRIGGER `prevent_deactivating_cloned_archives`")
    );

    let absent = translate_ddl(source_sql, &[]).expect("absent trigger must be a proven no-op");
    assert_eq!(absent.version, DDL_TRANSFORMATION_VERSION);
    assert_eq!(absent.target_sql, None);
}

#[test]
fn exact_drop_trigger_allowlist_rejects_comments_quoting_qualification_extra_tokens_and_names() {
    let rejected = [
        "-- comment\nDROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives",
        "/* comment */ DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives",
        "DROP TRIGGER prevent_deactivating_cloned_archives",
        "DROP TRIGGER IF EXISTS `prevent_deactivating_cloned_archives`",
        "DROP TRIGGER IF EXISTS \"prevent_deactivating_cloned_archives\"",
        "DROP TRIGGER IF EXISTS globalcomix.prevent_deactivating_cloned_archives",
        "DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives EXTRA",
        "DROP TRIGGER IF EXISTS another_trigger",
    ];

    for source_sql in rejected {
        assert!(
            translate_ddl(source_sql, &[]).is_err(),
            "unsupported DROP TRIGGER form was admitted: {source_sql}"
        );
    }
}

#[test]
fn drop_trigger_evidence_requires_absent_canonical_post_state() {
    let operation =
        parse_ddl_operation("DROP TRIGGER IF EXISTS prevent_deactivating_cloned_archives")
            .expect("production DROP TRIGGER operation");
    let mut target = semantic_snapshot(7, Some(8));
    target.inventory.triggers.push(TriggerInventory {
        name: "prevent_deactivating_cloned_archives".to_string(),
        table: "comics_assets_archives".to_string(),
        timing: "BEFORE".to_string(),
        event: "UPDATE".to_string(),
        statement: "SET NEW.is_active = OLD.is_active".to_string(),
    });
    let evidence =
        build_semantic_evidence(&operation, &target, &target).expect("DROP TRIGGER evidence");

    assert!(
        evidence
            .pre_state
            .contains("prevent_deactivating_cloned_archives")
    );
    assert!(evidence.expected_post_state.contains("absent"));
}

#[test]
fn transforms_unqualified_drop_procedure_if_exists_for_mysql8() {
    let procedures = ["apply_release_move_purchase_repair".to_string()]
        .into_iter()
        .collect();

    let transformation = transform_drop_procedure(
        "DROP PROCEDURE IF EXISTS apply_release_move_purchase_repair",
        &procedures,
    )
    .expect("DROP PROCEDURE IF EXISTS transformation");

    assert_eq!(transformation.version, "mariadb-mysql8-v1");
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some("DROP PROCEDURE `apply_release_move_purchase_repair`")
    );
}

#[test]
fn plain_drop_release_move_procedure_is_proven_noop_when_target_is_absent() {
    let transformation = transform_drop_procedure(
        "DROP PROCEDURE apply_release_move_purchase_repair",
        &Default::default(),
    )
    .expect("plain DROP PROCEDURE no-op");

    assert_eq!(transformation.target_sql, None);
}

#[test]
fn drop_procedure_if_exists_is_proven_noop_when_target_procedure_is_absent() {
    let transformation = transform_drop_procedure(
        "DROP PROCEDURE IF EXISTS apply_release_move_purchase_repair",
        &Default::default(),
    )
    .expect("DROP PROCEDURE IF EXISTS no-op");

    assert_eq!(transformation.target_sql, None);
}

#[test]
fn drop_procedure_admission_is_limited_to_supported_forms() {
    assert!(supports_drop_procedure(
        "DROP PROCEDURE IF EXISTS apply_release_move_purchase_repair"
    ));
    assert!(supports_drop_procedure(
        "DROP PROCEDURE apply_release_move_purchase_repair"
    ));
    for sql in [
        "DROP PROCEDURE another_release_move_procedure",
        "DROP PROCEDURE IF EXISTS globalcomix.apply_release_move_purchase_repair",
        "DROP PROCEDURE IF EXISTS `apply_release_move_purchase_repair`",
        "/* migration */ DROP PROCEDURE IF EXISTS apply_release_move_purchase_repair",
        "DROP PROCEDURE IF EXISTS apply_release_move_purchase_repair, another_procedure",
    ] {
        assert!(!supports_drop_procedure(sql), "accepted {sql}");
    }
}

#[test]
fn drop_procedure_uses_target_local_absent_postcondition() {
    let target = semantic_snapshot(7, Some(8));
    let operation =
        parse_ddl_operation("DROP PROCEDURE IF EXISTS apply_release_move_purchase_repair")
            .expect("DROP PROCEDURE operation");

    let evidence = build_semantic_evidence(&operation, &target, &target)
        .expect("target-local DROP PROCEDURE evidence");

    assert_eq!(evidence.expected_post_state, canonical_absent_state());
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
fn drop_column_if_exists_accepts_leading_client_comment() {
    let sql = "/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */ ALTER TABLE kg_storefront_chip_terms DROP COLUMN IF EXISTS `role`";
    let columns = ["id".to_string(), "role".to_string()].into_iter().collect();
    let transformation = transform_drop_columns_if_exists(sql, &columns)
        .expect("leading ordinary client comment does not change conditional drop");
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some("ALTER TABLE `kg_storefront_chip_terms` DROP COLUMN `role`")
    );
    let absent = ["id".to_string()].into_iter().collect();
    assert_eq!(
        transform_drop_columns_if_exists(sql, &absent)
            .expect("absent column remains a proven no-op")
            .target_sql,
        None
    );
}

#[test]
fn drop_column_if_exists_rejects_active_and_embedded_comments() {
    let columns = ["id".to_string(), "role".to_string()].into_iter().collect();
    for sql in [
        "/*! ALTER TABLE kg_storefront_chip_terms DROP COLUMN IF EXISTS role */",
        "/*+ hint */ ALTER TABLE kg_storefront_chip_terms DROP COLUMN IF EXISTS role",
        "/*M! ALTER TABLE kg_storefront_chip_terms DROP COLUMN IF EXISTS role */",
        "ALTER TABLE kg_storefront_chip_terms /* embedded */ DROP COLUMN IF EXISTS role",
    ] {
        assert!(
            transform_drop_columns_if_exists(sql, &columns).is_err(),
            "{sql}"
        );
    }
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
fn rename_column_if_exists_comments_preserve_leading_prefix() {
    let columns = ["arc_start_order"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let source_sql = concat!(
        "-- Preserve the source migration context.\r\n",
        "ALTER TABLE `home_feed_captions`\r\n",
        "  RENAME COLUMN IF EXISTS `arc_start_order` TO `deprecated_arc_start_order`",
    );

    let transformation = transform_rename_columns_if_exists(source_sql, &columns)
        .expect("commented MariaDB rename transformation");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "-- Preserve the source migration context.\r\n\
             ALTER TABLE `home_feed_captions` RENAME COLUMN `arc_start_order` TO `deprecated_arc_start_order`"
        )
    );
}

#[test]
fn rename_column_if_exists_comments_reject_embedded_comment() {
    let columns = ["arc_start_order"]
        .into_iter()
        .map(str::to_string)
        .collect();

    let error = transform_rename_columns_if_exists(
        "ALTER TABLE `home_feed_captions` \
         RENAME COLUMN IF EXISTS `arc_start_order` /* migration context */ \
         TO `deprecated_arc_start_order`",
        &columns,
    )
    .expect_err("embedded comment must not be discarded");

    assert!(error.contains("comments are not supported"), "{error}");
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
fn live_transform_uses_shared_create_table_translation() {
    let inventory = LiveDdlSemanticInventory::new(
        InventoryConfig::default(),
        InventoryConfig::default(),
        "fixture_cdc".to_string(),
        "fixture_cdc".to_string(),
    );
    let source_sql = "CREATE TABLE accounts (\
        id BIGINT NOT NULL PRIMARY KEY, \
        email VARCHAR(255) NOT NULL, \
        payload VARCHAR(64) NOT NULL, \
        KEY idx_accounts_payload (payload)\
    ) ENGINE=InnoDB";

    let live = inventory
        .transform_sql(source_sql)
        .expect("live translation");
    let pure = translate_ddl(source_sql, &[]).expect("pure translation");

    assert_eq!(live, pure);
}

#[test]
fn shared_translator_has_no_generic_create_or_alter_fallback() {
    let unsupported = [
        "CREATE TABLE accounts (id BIGINT)",
        "ALTER TABLE accounts ADD PARTITION (PARTITION p0 VALUES LESS THAN (10))",
        "ALTER TABLE accounts RENAME TO archived_accounts",
    ];

    for sql in unsupported {
        let error = translate_ddl(sql, &[]).expect_err("unsupported DDL must fail closed");
        assert!(
            error.contains("unsupported") || error.contains("requires"),
            "{sql}: {error}"
        );
    }
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

fn create_enum_timestamp_ast() -> super::model::ParsedCreateTableAst {
    use super::model::{ParsedCreateColumnAst, ParsedCreateTableAst};
    ParsedCreateTableAst {
        name: "shelves".into(),
        if_not_exists: true,
        columns: vec![
            ParsedCreateColumnAst {
                name: "id".into(),
                column_type: "int unsigned".into(),
                nullable: false,
                default_sql: None,
                auto_increment: true,
                on_update_current_timestamp: false,
                character_set: None,
                collation: None,
            },
            ParsedCreateColumnAst {
                name: "kind".into(),
                column_type: "enum('Western','manGa','can''t')".into(),
                nullable: false,
                default_sql: None,
                auto_increment: false,
                on_update_current_timestamp: false,
                character_set: None,
                collation: None,
            },
            ParsedCreateColumnAst {
                name: "updated_at".into(),
                column_type: "timestamp".into(),
                nullable: true,
                default_sql: None,
                auto_increment: false,
                on_update_current_timestamp: true,
                character_set: None,
                collation: None,
            },
            ParsedCreateColumnAst {
                name: "created_at".into(),
                column_type: "timestamp".into(),
                nullable: false,
                default_sql: Some("CURRENT_TIMESTAMP".into()),
                auto_increment: false,
                on_update_current_timestamp: false,
                character_set: None,
                collation: None,
            },
        ],
        primary_key: vec!["id".into()],
        indexes: Vec::new(),
        check_constraints: Vec::new(),
        engine: "InnoDB".into(),
        character_set: Some("utf8mb4".into()),
        collation: Some("utf8mb4_unicode_ci".into()),
    }
}

#[test]
fn create_enum_labels_retain_case_in_rendered_sql() {
    let ast = create_enum_timestamp_ast();
    let defaults = crate::inventory::SchemaDefaults {
        character_set: "utf8mb4".into(),
        collation: "utf8mb4_unicode_ci".into(),
    };
    let rendered = super::transform::transform_fixture_create_table_with_defaults(&ast, &defaults)
        .expect("render CREATE");
    assert_eq!(
        rendered.target_sql.as_deref(),
        Some(
            "CREATE TABLE `shelves` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `kind` ENUM('Western','manGa','can''t') NOT NULL, `updated_at` TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP, `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
    );
}

#[test]
fn create_enum_labels_retain_case_in_expected_inventory() {
    let ast = create_enum_timestamp_ast();
    let defaults = crate::inventory::SchemaDefaults {
        character_set: "utf8mb4".into(),
        collation: "utf8mb4_unicode_ci".into(),
    };
    let state: serde_json::Value = serde_json::from_str(
        &super::canonical::expected_create_table_post_state(&ast, &defaults)
            .expect("expected state"),
    )
    .expect("state JSON");
    assert_eq!(
        state["definition"]["columns"][1]["column_type"],
        "enum('Western','manGa','can''t')"
    );
    assert_eq!(
        state["definition"]["columns"][1]["default_value"],
        serde_json::Value::Null
    );
    assert_eq!(
        state["definition"]["columns"][1]["character_set"],
        "utf8mb4"
    );
    assert_eq!(
        state["definition"]["columns"][1]["collation"],
        "utf8mb4_unicode_ci"
    );
}

#[test]
fn create_timestamp_on_update_does_not_require_generated_default() {
    let ast = create_enum_timestamp_ast();
    let defaults = crate::inventory::SchemaDefaults {
        character_set: "utf8mb4".into(),
        collation: "utf8mb4_unicode_ci".into(),
    };
    let state: serde_json::Value = serde_json::from_str(
        &super::canonical::expected_create_table_post_state(&ast, &defaults)
            .expect("expected state"),
    )
    .expect("state JSON");
    let columns = &state["definition"]["columns"];
    assert_eq!(columns[0]["extra"], "auto_increment");
    assert_eq!(columns[2]["extra"], "on update CURRENT_TIMESTAMP");
    assert_eq!(columns[2]["default_value"], serde_json::Value::Null);
    assert_eq!(columns[2]["is_nullable"], true);
    assert_eq!(columns[3]["extra"], "DEFAULT_GENERATED");
    assert_eq!(columns[3]["default_value"], "CURRENT_TIMESTAMP");
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
                        character_set: Some("utf8mb4".to_string()),
                        collation: Some("utf8mb4_unicode_ci".to_string()),
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
                        character_set: Some("utf8mb4".to_string()),
                        collation: Some("utf8mb4_unicode_ci".to_string()),
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
fn production_create_table_with_leading_comments_transforms_to_mysql8_sql() {
    let inventory = LiveDdlSemanticInventory::new(
        InventoryConfig::default(),
        InventoryConfig::default(),
        "globalcomix".to_string(),
        "globalcomix".to_string(),
    );
    let source_sql = "-- Exclude the full Image Comics catalog from home-feed mining and serving.\n\
-- Artist-level scope also covers newly-created Image Comics titles and its\n\
-- imprints; the PHP serve policy resolves this table on every request.\n\
\n\
CREATE TABLE IF NOT EXISTS `home_feed_artist_blacklist` (\n\
    `id`          INT(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n\
    `artist_id`   MEDIUMINT(8) UNSIGNED NOT NULL,\n\
    `reason`      VARCHAR(255) DEFAULT NULL,\n\
    `creator_id`  MEDIUMINT(8) UNSIGNED DEFAULT NULL,\n\
    `create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n\
    UNIQUE KEY `uidx_hfab_artist` (`artist_id`)\n\
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

    let expected_sql = "CREATE TABLE `home_feed_artist_blacklist` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT, `artist_id` MEDIUMINT UNSIGNED NOT NULL, `reason` VARCHAR(255) NULL DEFAULT NULL, `creator_id` MEDIUMINT UNSIGNED NULL DEFAULT NULL, `create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), UNIQUE KEY `uidx_hfab_artist` (`artist_id`)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
    let create_sql = source_sql
        .find("CREATE TABLE")
        .map(|start| &source_sql[start..])
        .expect("production CREATE TABLE body");
    let ordinary_comment_variants = [
        source_sql.to_string(),
        format!("# operational description\n{create_sql}"),
        format!("/* operational description */\n{create_sql}"),
    ];

    for commented_sql in ordinary_comment_variants {
        let transformation = inventory
            .transform_sql(&commented_sql)
            .expect("commented production CREATE TABLE must be translatable");
        assert_eq!(transformation.version, DDL_TRANSFORMATION_VERSION);
        assert_eq!(transformation.target_sql.as_deref(), Some(expected_sql));
    }
}

#[test]
fn explicit_production_create_evidence_does_not_require_historical_source_defaults() {
    let source_sql = "-- operational description\n\
CREATE TABLE IF NOT EXISTS `home_feed_artist_blacklist` (\n\
    `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\n\
    `artist_id` MEDIUMINT(8) UNSIGNED NOT NULL,\n\
    `reason` VARCHAR(255) DEFAULT NULL,\n\
    `creator_id` MEDIUMINT(8) UNSIGNED DEFAULT NULL,\n\
    `create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n\
    UNIQUE KEY `uidx_hfab_artist` (`artist_id`)\n\
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
    let operation = parse_ddl_operation(source_sql).expect("production CREATE TABLE operation");
    let target = SemanticSchemaSnapshot {
        inventory: SchemaInventory {
            schema: "globalcomix".to_string(),
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
    let live_source = crate::inventory::SourceMasterCoordinate {
        file: "mysqld-bin.002779".to_string(),
        position: 579_176_271,
    };

    let evidence = build_fenced_create_table_evidence(
        &operation,
        &target,
        &crate::inventory::SchemaDefaults {
            character_set: "utf8mb4".to_string(),
            collation: "utf8mb4_uca1400_ai_ci".to_string(),
        },
        "mysqld-bin.002778",
        750_896_630,
        &live_source,
        &live_source,
    )
    .expect("explicit CREATE semantics must not require historical source defaults");

    assert!(evidence.generated_sql.is_some());
    assert!(evidence.expected_post_state.contains("utf8mb4_unicode_ci"));
    assert!(
        evidence
            .expected_post_state
            .contains("\"data_type\":\"int\"")
    );
    assert!(
        !evidence
            .expected_post_state
            .contains("\"data_type\":\"int unsigned\"")
    );
    assert!(
        evidence
            .expected_post_state
            .contains("\"extra\":\"DEFAULT_GENERATED\"")
    );
}

#[test]
fn production_create_table_rejects_active_and_embedded_comments() {
    let inventory = LiveDdlSemanticInventory::new(
        InventoryConfig::default(),
        InventoryConfig::default(),
        "globalcomix".to_string(),
        "globalcomix".to_string(),
    );
    let create_sql = "CREATE TABLE accounts (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB";
    let rejected = [
        format!("/*!40101 SET sql_mode='' */ {create_sql}"),
        format!("/*M!100100 SET sql_mode='' */ {create_sql}"),
        format!("/*+ SET_VAR(sort_buffer_size=16M) */ {create_sql}"),
        "CREATE TABLE accounts (id BIGINT /* identity */ NOT NULL PRIMARY KEY) ENGINE=InnoDB"
            .to_string(),
    ];

    for sql in rejected {
        assert!(inventory.transform_sql(&sql).is_err(), "accepted {sql}");
    }
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
fn production_multiple_add_index_clauses_transform_to_deterministic_mysql8_sql() {
    let source_sql = "ALTER TABLE `contact_forms`\n\
         ADD INDEX `idx_type_create_time` (`contact_form_type_id`, `create_time`),\n\
         ADD INDEX `idx_context_create_time` (`context`, `create_time`)";

    let transformation = transform_production_alter_table(source_sql)
        .expect("production multiple ADD INDEX clauses must be translatable");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `contact_forms` ADD KEY `idx_type_create_time` (`contact_form_type_id`, `create_time`), ADD KEY `idx_context_create_time` (`context`, `create_time`)"
        )
    );
}

#[test]
fn observed_alter_preserves_its_leading_comment_in_generated_sql() {
    let source_sql = "-- The serve-time blacklist check resolves a blacklisted artist's imprints.\r\n\
ALTER TABLE `artists_imprints`\r\n\
    ADD KEY `idx_artist_id` (`artist_id`)";

    let transformation = transform_production_alter_table(source_sql)
        .expect("observed leading-comment ADD KEY must be translatable");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "-- The serve-time blacklist check resolves a blacklisted artist's imprints.\r\n\
ALTER TABLE `artists_imprints` ADD KEY `idx_artist_id` (`artist_id`)"
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
fn production_float_unsigned_add_column_preserves_required_options() {
    let source_sql = "ALTER TABLE `comics_top_stats`\n\
        ADD COLUMN `value_1_day` FLOAT UNSIGNED NOT NULL DEFAULT 0 AFTER `statistic`";
    let transformation =
        transform_production_alter_table(source_sql).expect("production FLOAT UNSIGNED ADD COLUMN");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `comics_top_stats` ADD COLUMN `value_1_day` FLOAT UNSIGNED NOT NULL DEFAULT 0 AFTER `statistic`"
        )
    );
    assert!(!supports_production_alter_table(
        "ALTER TABLE `comics_top_stats` ADD COLUMN `value_1_day` FLOAT UNSIGNED NOT NULL DEFAULT 1 AFTER `statistic`"
    ));
}

const CONTENT_SECTIONS_SEEN_DDL: &str = "ALTER TABLE `content_sections_events_raw`\n\
    ADD COLUMN IF NOT EXISTS `direct_seen_at` timestamp NULL DEFAULT NULL\n\
        COMMENT 'When CmsEventsBufferManager (direct write) first saw this event',\n\
    ADD COLUMN IF NOT EXISTS `sync_seen_at` timestamp NULL DEFAULT NULL\n\
        COMMENT 'When ContentSectionsEventSyncService (Mixpanel Export) first saw it',\n\
    ALGORITHM=INSTANT";
const DIRECT_SEEN_COMMENT: &str = "When CmsEventsBufferManager (direct write) first saw this event";
const SYNC_SEEN_COMMENT: &str =
    "When ContentSectionsEventSyncService (Mixpanel Export) first saw it";

#[test]
fn content_sections_seen_columns_strip_source_guards_from_mysql8_sql() {
    let transformation = transform_production_alter_table(CONTENT_SECTIONS_SEEN_DDL)
        .expect("exact guarded TIMESTAMP ALTER must translate");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `content_sections_events_raw` ADD COLUMN `direct_seen_at` TIMESTAMP NULL DEFAULT NULL COMMENT 'When CmsEventsBufferManager (direct write) first saw this event', ADD COLUMN `sync_seen_at` TIMESTAMP NULL DEFAULT NULL COMMENT 'When ContentSectionsEventSyncService (Mixpanel Export) first saw it', ALGORITHM=INSTANT"
        )
    );
}

#[test]
fn content_sections_seen_columns_near_misses_remain_unsupported() {
    let near_misses = [
        CONTENT_SECTIONS_SEEN_DDL.replace(
            "content_sections_events_raw",
            "content_sections_events",
        ),
        CONTENT_SECTIONS_SEEN_DDL.replace("direct_seen_at", "direct_seen"),
        CONTENT_SECTIONS_SEEN_DDL.replace("direct write", "direct-write"),
        CONTENT_SECTIONS_SEEN_DDL.replace(
            "ADD COLUMN IF NOT EXISTS `direct_seen_at` timestamp NULL DEFAULT NULL\nCOMMENT 'When CmsEventsBufferManager (direct write) first saw this event',\nADD COLUMN IF NOT EXISTS `sync_seen_at` timestamp NULL DEFAULT NULL\nCOMMENT 'When ContentSectionsEventSyncService (Mixpanel Export) first saw it'",
            "ADD COLUMN IF NOT EXISTS `sync_seen_at` timestamp NULL DEFAULT NULL\nCOMMENT 'When ContentSectionsEventSyncService (Mixpanel Export) first saw it',\nADD COLUMN IF NOT EXISTS `direct_seen_at` timestamp NULL DEFAULT NULL\nCOMMENT 'When CmsEventsBufferManager (direct write) first saw this event'",
        ),
        CONTENT_SECTIONS_SEEN_DDL.replace("ALGORITHM=INSTANT", "ALGORITHM=INPLACE"),
    ];

    for sql in near_misses {
        assert!(
            !supports_production_alter_table(&sql),
            "near-miss ALTER was admitted: {sql}"
        );
    }
}

#[test]
fn existing_content_sections_seen_columns_have_equal_pre_and_post_state() {
    let target = content_sections_events_raw_seen_columns(true, true);
    let operation = parse_ddl_operation(CONTENT_SECTIONS_SEEN_DDL).expect("production ALTER");

    let evidence = build_semantic_evidence(&operation, &target, &target)
        .expect("both exact columns must be a proven no-op");

    assert_eq!(evidence.pre_state, evidence.expected_post_state);
}

#[test]
fn partial_content_sections_seen_columns_pre_state_remains_blocked() {
    let operation = parse_ddl_operation(CONTENT_SECTIONS_SEEN_DDL).expect("production ALTER");

    for (case, direct_seen, sync_seen) in [("direct-only", true, false), ("sync-only", false, true)]
    {
        let target = content_sections_events_raw_seen_columns(direct_seen, sync_seen);
        let error = build_semantic_evidence(&operation, &target, &target)
            .expect_err("partial guarded ADD COLUMN state must fail closed");
        assert!(error.contains("partial"), "{case}: {error}");
    }
}

#[test]
fn divergent_content_sections_seen_columns_pre_state_remains_blocked() {
    let operation = parse_ddl_operation(CONTENT_SECTIONS_SEEN_DDL).expect("production ALTER");
    let mut target = content_sections_events_raw_seen_columns(true, true);
    target.inventory.tables[0]
        .columns
        .iter_mut()
        .find(|column| column.name == "direct_seen_at")
        .expect("direct_seen_at fixture column")
        .comment = "divergent".to_string();

    let error = build_semantic_evidence(&operation, &target, &target)
        .expect_err("divergent guarded ADD COLUMN state must fail closed");

    assert!(error.contains("already contains divergent"), "{error}");
}

fn content_sections_events_raw_seen_columns(
    direct_seen: bool,
    sync_seen: bool,
) -> SemanticSchemaSnapshot {
    let mut target = semantic_snapshot(0, None);
    let table = &mut target.inventory.tables[0];
    table.name = "content_sections_events_raw".to_string();
    if direct_seen {
        table.columns.push(seen_timestamp_column(
            "direct_seen_at",
            DIRECT_SEEN_COMMENT,
            table.columns.len() + 1,
        ));
    }
    if sync_seen {
        table.columns.push(seen_timestamp_column(
            "sync_seen_at",
            SYNC_SEEN_COMMENT,
            table.columns.len() + 1,
        ));
    }
    target.inventory.indexes.clear();
    target.table_runtime.clear();
    target
}

fn seen_timestamp_column(name: &str, comment: &str, ordinal: usize) -> ColumnInventory {
    ColumnInventory {
        name: name.to_string(),
        ordinal_position: ordinal as u32,
        column_type: "timestamp".to_string(),
        data_type: "timestamp".to_string(),
        is_nullable: true,
        character_set: None,
        collation: None,
        default_value: None,
        extra: String::new(),
        comment: comment.to_string(),
        generated: None,
    }
}

const DISABLE_SAM_DDL: &str = "ALTER TABLE `artists_settings`\n\
    ADD COLUMN `disable_sam` TINYINT(1) UNSIGNED NOT NULL DEFAULT 0 AFTER `markup_before_transaction_fee`";

#[test]
fn production_tinyint_unsigned_add_column_normalizes_display_width() {
    let transformation = transform_production_alter_table(DISABLE_SAM_DDL)
        .expect("production TINYINT(1) UNSIGNED ADD COLUMN");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `artists_settings` ADD COLUMN `disable_sam` TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER `markup_before_transaction_fee`"
        )
    );
}

#[test]
fn signed_tinyint_add_column_replay_metadata() {
    let sql = "/* ApplicationName=DBeaver 26.2.0 - SQLEditor <Script.sql> */ ALTER TABLE accounts ADD COLUMN is_admin_only TINYINT(1) NOT NULL DEFAULT 0 AFTER id";
    assert_eq!(
        transform_production_alter_table(sql)
            .expect("signed TINYINT ADD")
            .target_sql
            .as_deref(),
        Some(
            "ALTER TABLE `accounts` ADD COLUMN `is_admin_only` TINYINT NOT NULL DEFAULT 0 AFTER `id`"
        )
    );
    let target = semantic_snapshot(7, Some(8));
    let operation = parse_ddl_operation(sql).expect("signed TINYINT operation");
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("signed evidence");
    let mut expected = target.clone();
    expected.inventory.tables[0].columns[1].ordinal_position = 3;
    expected.inventory.tables[0].columns.insert(
        1,
        ColumnInventory {
            name: "is_admin_only".into(),
            ordinal_position: 2,
            column_type: "tinyint".into(),
            data_type: "tinyint".into(),
            is_nullable: false,
            character_set: None,
            collation: None,
            default_value: Some("0".into()),
            extra: String::new(),
            comment: String::new(),
            generated: None,
        },
    );
    assert_eq!(
        evidence.expected_post_state,
        super::canonical::observe_operation_state(&expected, &operation)
            .expect("signed post-state")
    );
    for rejected in [
        sql.replace("TINYINT(1)", "TINYINT(2)"),
        sql.replace("TINYINT(1)", "TINYINT(01)"),
        sql.replace("TINYINT(1)", "TINYINT(`1`)"),
        sql.replace("TINYINT(1)", "`TINYINT`(1)"),
        sql.replace("DEFAULT 0", "DEFAULT 0 UNSIGNED"),
    ] {
        assert!(!supports_production_alter_table(&rejected), "{rejected}");
    }
}

#[test]
fn already_present_tinyint_add_column_has_equal_pre_and_post_state() {
    let target = artists_settings_with_disable_sam("0");
    let operation = parse_ddl_operation(DISABLE_SAM_DDL).expect("production ALTER");

    let evidence = build_semantic_evidence(&operation, &target, &target)
        .expect("already applied column must have deterministic evidence");

    assert_eq!(evidence.pre_state, evidence.expected_post_state);
}

#[test]
fn divergent_existing_tinyint_add_column_remains_blocked() {
    let divergent_definition = artists_settings_with_disable_sam("1");
    let mut divergent_position = artists_settings_with_disable_sam("0");
    divergent_position.inventory.tables[0].columns.swap(1, 2);
    for (index, column) in divergent_position.inventory.tables[0]
        .columns
        .iter_mut()
        .enumerate()
    {
        column.ordinal_position = (index + 1) as u32;
    }
    let operation = parse_ddl_operation(DISABLE_SAM_DDL).expect("production ALTER");

    for (case, target) in [
        ("definition", divergent_definition),
        ("position", divergent_position),
    ] {
        let error = build_semantic_evidence(&operation, &target, &target)
            .expect_err("divergent existing column must remain blocked");
        assert!(
            error.contains("already contains divergent"),
            "{case}: {error}"
        );
    }
}

fn artists_settings_with_disable_sam(default_value: &str) -> SemanticSchemaSnapshot {
    let mut target = semantic_snapshot(7, Some(8));
    let table = &mut target.inventory.tables[0];
    table.name = "artists_settings".to_string();
    let mut markup_column = table.columns[1].clone();
    markup_column.name = "markup_before_transaction_fee".to_string();
    markup_column.column_type = "tinyint unsigned".to_string();
    markup_column.data_type = "tinyint".to_string();
    markup_column.is_nullable = false;
    markup_column.character_set = None;
    markup_column.collation = None;
    markup_column.default_value = Some("0".to_string());
    markup_column.comment.clear();
    table.columns[1] = markup_column.clone();
    markup_column.name = "disable_sam".to_string();
    markup_column.ordinal_position = 3;
    markup_column.default_value = Some(default_value.to_string());
    table.columns.push(markup_column);
    target.inventory.indexes.clear();
    target.table_runtime.clear();
    target
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
fn production_alter_rejects_embedded_and_semantically_active_comments() {
    for sql in [
        "/*!40101 SET sql_mode='' */ ALTER TABLE accounts ADD COLUMN c VARCHAR(64)",
        "/*+ SET_VAR(sort_buffer_size=16M) */ ALTER TABLE accounts ADD COLUMN c VARCHAR(64)",
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
fn production_alter_admits_only_microsecond_datetime_precision() {
    assert!(supports_production_alter_table(
        "ALTER TABLE accounts ADD COLUMN c DATETIME(6)"
    ));
    for sql in [
        "ALTER TABLE accounts ADD COLUMN c DATETIME(3)",
        "ALTER TABLE accounts ADD COLUMN c DATETIME(`6`)",
        "ALTER TABLE accounts ADD COLUMN c TIMESTAMP(6)",
    ] {
        assert!(!supports_production_alter_table(sql), "accepted {sql}");
    }
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

const RELEASES_DOWNLOADS_SORT_DDL: &str =
    include_str!("../../../fixtures/ddl/alter-releases-downloads-sort.sql");

#[test]
fn releases_downloads_sort_rebuild_transforms_to_deterministic_mysql8_sql() {
    let transformation = transform_production_alter_table(RELEASES_DOWNLOADS_SORT_DDL)
        .expect("observed releases index rebuild must translate");

    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `releases` DROP INDEX `idx_downloads_sort`, ADD KEY `idx_downloads_sort` (`is_deleted`, `is_published`, `is_visible`, `comic_is_visible`, `lang_id`, `published_time` DESC, `comic_id`, `id`), ALGORITHM=INPLACE, LOCK=NONE"
        )
    );
}

#[test]
fn releases_downloads_sort_rebuild_records_drop_then_descending_replacement_post_state() {
    let operation = parse_ddl_operation(RELEASES_DOWNLOADS_SORT_DDL)
        .expect("observed releases index rebuild must parse");
    let target = releases_downloads_sort_target();

    let evidence = build_semantic_evidence(&operation, &target, &target)
        .expect("target index rebuild must derive post-state from its typed AST");
    let pre_state: serde_json::Value =
        serde_json::from_str(&evidence.pre_state).expect("pre-state JSON");
    let post_state: serde_json::Value =
        serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON");

    assert_eq!(
        pre_state["indexes"][0]["columns"][5]["order"], "ASC",
        "fenced pre-state retains the historical target ordering"
    );
    assert_eq!(
        post_state["indexes"][0]["columns"][5]["order"], "DESC",
        "post-state retains the source descending key part"
    );
    assert_eq!(
        post_state["indexes"][0]["columns"]
            .as_array()
            .unwrap()
            .len(),
        8
    );
}

fn releases_downloads_sort_target() -> SemanticSchemaSnapshot {
    let columns = [
        "is_deleted",
        "is_published",
        "is_visible",
        "comic_is_visible",
        "lang_id",
        "published_time",
        "comic_id",
        "id",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, name)| ColumnInventory {
        name: name.to_string(),
        ordinal_position: (index + 1) as u32,
        column_type: "tinyint".to_string(),
        data_type: "tinyint".to_string(),
        is_nullable: false,
        character_set: None,
        collation: None,
        default_value: Some("0".to_string()),
        extra: String::new(),
        comment: String::new(),
        generated: None,
    })
    .collect();
    let old_index_columns = [
        "is_deleted",
        "is_published",
        "is_visible",
        "comic_is_visible",
        "lang_id",
        "published_time",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, name)| IndexColumnInventory {
        name: name.to_string(),
        sequence: (index + 1) as u32,
        prefix_length: None,
        collation: Some("A".to_string()),
        order: "ASC".to_string(),
    })
    .collect();
    SemanticSchemaSnapshot {
        inventory: SchemaInventory {
            schema: "globalcomix".to_string(),
            tables: vec![TableInventory {
                name: "releases".to_string(),
                table_type: "BASE TABLE".to_string(),
                engine: Some("InnoDB".to_string()),
                collation: Some("utf8mb4_unicode_ci".to_string()),
                primary_key: vec!["id".to_string()],
                columns,
            }],
            indexes: vec![IndexInventory {
                table: "releases".to_string(),
                name: "idx_downloads_sort".to_string(),
                unique: false,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: None,
                columns: old_index_columns,
            }],
            foreign_keys: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            routines: Vec::new(),
            events: Vec::new(),
        },
        table_runtime: std::collections::BTreeMap::new(),
    }
}

#[test]
fn releases_downloads_sort_rebuild_near_misses_remain_unsupported() {
    for sql in [
        RELEASES_DOWNLOADS_SORT_DDL.replace("`releases`", "`releases_history`"),
        RELEASES_DOWNLOADS_SORT_DDL.replace("LOCK=NONE", "LOCK=SHARED"),
        RELEASES_DOWNLOADS_SORT_DDL.replace("ALGORITHM=INPLACE", "ALGORITHM=COPY"),
        RELEASES_DOWNLOADS_SORT_DDL.replace("`id` ASC", "`id` DESC"),
    ] {
        assert!(
            !supports_production_alter_table(&sql),
            "near-miss ALTER was admitted: {sql}"
        );
    }
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

#[test]
fn enum_columns_translate_and_set_columns_are_rejected() {
    for sql in [
        "CREATE TABLE `items` (`id` BIGINT UNSIGNED NOT NULL, `channel` ENUM('dev','prod') NOT NULL DEFAULT 'dev', PRIMARY KEY (`id`)) ENGINE=InnoDB",
        "ALTER TABLE `items` MODIFY COLUMN `channel` ENUM('dev','prod') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'dev'",
        "ALTER TABLE `items` ADD COLUMN `channel` ENUM('dev') NULL DEFAULT NULL",
    ] {
        super::translate_modeled_ddl(sql, &[])
            .unwrap_or_else(|error| panic!("ENUM column rejected: {sql}: {error}"));
    }
    for sql in [
        "ALTER TABLE `items` MODIFY COLUMN `flags` SET('a','b') NULL DEFAULT NULL",
        "ALTER TABLE `items` MODIFY COLUMN `channel` ENUM() NULL DEFAULT NULL",
        "ALTER TABLE `items` MODIFY COLUMN `channel` ENUM(1,2) NULL DEFAULT NULL",
        "ALTER TABLE `items` MODIFY COLUMN `channel` ENUM('a' 'b') NULL DEFAULT NULL",
    ] {
        assert!(
            super::translate_modeled_ddl(sql, &[]).is_err(),
            "unsupported enumerated column passed through: {sql}"
        );
    }
}

const READER_MEMORY_PROFILES_CREATE: &str =
    include_str!("../../../fixtures/ddl/create-reader-memory-profiles.sql");
const READER_MEMORY_ITEMS_CREATE: &str =
    include_str!("../../../fixtures/ddl/create-reader-memory-items.sql");
const READER_MEMORY_OPERATIONS_CREATE: &str =
    include_str!("../../../fixtures/ddl/create-reader-memory-operations.sql");
const READER_MEMORY_PROFILES_ALTER: &str =
    include_str!("../../../fixtures/ddl/alter-reader-memory-profiles-checkpoints.sql");
const READER_MEMORY_OPERATIONS_ALTER: &str =
    include_str!("../../../fixtures/ddl/alter-reader-memory-operations-batch.sql");

fn globalcomix_inventory() -> LiveDdlSemanticInventory {
    LiveDdlSemanticInventory::new(
        InventoryConfig::default(),
        InventoryConfig::default(),
        "globalcomix".to_string(),
        "globalcomix".to_string(),
    )
}

fn absent_target() -> SemanticSchemaSnapshot {
    SemanticSchemaSnapshot {
        inventory: SchemaInventory {
            schema: "globalcomix".to_string(),
            tables: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            routines: Vec::new(),
            events: Vec::new(),
        },
        table_runtime: Default::default(),
    }
}

fn reader_memory_create_post_state(source_sql: &str) -> serde_json::Value {
    let operation = parse_ddl_operation(source_sql).expect("reader memory CREATE operation");
    assert!(
        operation.create_table_ast.is_some(),
        "CREATE must be modeled"
    );
    let coordinate = crate::inventory::SourceMasterCoordinate {
        file: "mysqld-bin.003058".to_string(),
        position: 1,
    };
    let evidence = build_fenced_create_table_evidence(
        &operation,
        &absent_target(),
        &crate::inventory::SchemaDefaults {
            character_set: "utf8mb4".to_string(),
            collation: "utf8mb4_uca1400_ai_ci".to_string(),
        },
        "mysqld-bin.003058",
        312415666,
        &coordinate,
        &coordinate,
    )
    .expect("explicit-collation CREATE evidence needs no source fence");
    serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON")
}

fn column<'a>(state: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    state["definition"]["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .find(|column| column["name"] == name)
        .unwrap_or_else(|| panic!("column {name} missing from {state}"))
}

#[test]
fn reader_memory_profiles_create_transforms_to_mysql8_sql() {
    let transformation = globalcomix_inventory()
        .transform_sql(READER_MEMORY_PROFILES_CREATE)
        .expect("reader_memory_profiles CREATE must be translatable");
    assert_eq!(transformation.version, DDL_TRANSFORMATION_VERSION);
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "CREATE TABLE `reader_memory_profiles` (\
`user_id` INT UNSIGNED NOT NULL, \
`schema_version` SMALLINT UNSIGNED NOT NULL DEFAULT 1, \
`enabled` TINYINT NOT NULL DEFAULT 0, \
`capture_enabled` TINYINT NOT NULL DEFAULT 0, \
`revision` BIGINT UNSIGNED NOT NULL DEFAULT 0, \
`deletion_epoch` BIGINT UNSIGNED NOT NULL DEFAULT 0, \
`capture_after` DATETIME(6) NULL, \
`evidence_floor` DATETIME(6) NULL, \
`prepared_json` MEDIUMTEXT NOT NULL DEFAULT (_utf8mb4'{}'), \
`updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
PRIMARY KEY (`user_id`), \
CONSTRAINT `reader_memory_profile_json` CHECK (JSON_VALID(`prepared_json`)), \
CONSTRAINT `reader_memory_profile_size` CHECK (OCTET_LENGTH(`prepared_json`) <= 32768)\
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
    );
}

#[test]
fn reader_memory_items_and_operations_create_transform_to_mysql8_sql() {
    let inventory = globalcomix_inventory();
    let items = inventory
        .transform_sql(READER_MEMORY_ITEMS_CREATE)
        .expect("reader_memory_items CREATE must be translatable");
    assert_eq!(
        items.target_sql.as_deref(),
        Some(
            "CREATE TABLE `reader_memory_items` (\
`uuid` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, \
`user_id` INT UNSIGNED NOT NULL, \
`semantic_key` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, \
`memory_group` VARCHAR(32) NOT NULL, \
`predicate` VARCHAR(64) NOT NULL, \
`payload_json` TEXT NULL, \
`status` VARCHAR(16) NOT NULL, \
`source_message_id` BIGINT UNSIGNED NOT NULL, \
`evidence_at` DATETIME(6) NOT NULL, \
`barrier_at` DATETIME(6) NULL, \
`expires_at` DATETIME(6) NULL, \
`revision` BIGINT UNSIGNED NOT NULL, \
`updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \
PRIMARY KEY (`uuid`), \
UNIQUE KEY `reader_memory_semantic` (`user_id`, `semantic_key`), \
KEY `reader_memory_owner` (`user_id`, `status`), \
CONSTRAINT `reader_memory_item_json` CHECK (`payload_json` IS NULL OR JSON_VALID(`payload_json`)), \
CONSTRAINT `reader_memory_item_size` CHECK (`payload_json` IS NULL OR OCTET_LENGTH(`payload_json`) <= 8192), \
CONSTRAINT `reader_memory_item_state` CHECK (`status` IN ('active','disabled','forgotten'))\
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
    );
    let operations = inventory
        .transform_sql(READER_MEMORY_OPERATIONS_CREATE)
        .expect("reader_memory_operations CREATE must be translatable");
    let sql = operations.target_sql.expect("operations SQL");
    assert!(sql.starts_with("CREATE TABLE `reader_memory_operations` (`uuid` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, "), "{sql}");
    assert!(
        sql.contains("`status` VARCHAR(24) NOT NULL DEFAULT 'pending', "),
        "{sql}"
    );
    assert!(
        sql.contains("`lease_token` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL, "),
        "{sql}"
    );
    assert!(
        sql.contains("`created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), "),
        "{sql}"
    );
    assert!(sql.ends_with("PRIMARY KEY (`uuid`), UNIQUE KEY `reader_memory_source` (`user_id`, `source_message_id`), KEY `reader_memory_dispatch` (`status`, `lease_until`, `created_at`), KEY `reader_memory_started` (`started_at`), KEY `reader_memory_operations_owner` (`user_id`, `status`)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci"), "{sql}");
}

#[test]
fn reader_memory_create_expected_post_state_matches_mysql8_inventory() {
    let profiles = reader_memory_create_post_state(READER_MEMORY_PROFILES_CREATE);
    assert_eq!(profiles["definition"]["collation"], "utf8mb4_unicode_ci");
    assert_eq!(
        profiles["definition"]["primary_key"],
        serde_json::json!(["user_id"])
    );
    let schema_version = column(&profiles, "schema_version");
    assert_eq!(schema_version["column_type"], "smallint unsigned");
    assert_eq!(schema_version["default_value"], "1");
    assert_eq!(schema_version["extra"], "");
    let capture_after = column(&profiles, "capture_after");
    assert_eq!(capture_after["column_type"], "datetime(6)");
    assert_eq!(capture_after["data_type"], "datetime");
    assert_eq!(capture_after["is_nullable"], true);
    assert_eq!(capture_after["default_value"], serde_json::Value::Null);
    let prepared = column(&profiles, "prepared_json");
    assert_eq!(prepared["column_type"], "mediumtext");
    assert_eq!(prepared["data_type"], "mediumtext");
    assert_eq!(prepared["character_set"], "utf8mb4");
    assert_eq!(prepared["collation"], "utf8mb4_unicode_ci");
    assert_eq!(prepared["default_value"], "_utf8mb4\\'{}\\'");
    assert_eq!(prepared["extra"], "DEFAULT_GENERATED");
    let updated = column(&profiles, "updated_at");
    assert_eq!(updated["default_value"], "CURRENT_TIMESTAMP(6)");
    assert_eq!(
        updated["extra"],
        "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)"
    );
    assert_eq!(profiles["indexes"], serde_json::json!([]));

    let items = reader_memory_create_post_state(READER_MEMORY_ITEMS_CREATE);
    let uuid = column(&items, "uuid");
    assert_eq!(uuid["column_type"], "char(36)");
    assert_eq!(uuid["character_set"], "ascii");
    assert_eq!(uuid["collation"], "ascii_bin");
    let payload = column(&items, "payload_json");
    assert_eq!(payload["data_type"], "text");
    assert_eq!(payload["character_set"], "utf8mb4");
    assert_eq!(payload["default_value"], serde_json::Value::Null);
    assert_eq!(payload["extra"], "");
    let indexes = items["indexes"].as_array().expect("indexes");
    assert_eq!(indexes.len(), 2);
    assert_eq!(indexes[0]["name"], "reader_memory_owner");
    assert_eq!(indexes[1]["name"], "reader_memory_semantic");
    assert_eq!(indexes[1]["unique"], true);

    let operations = reader_memory_create_post_state(READER_MEMORY_OPERATIONS_CREATE);
    let status = column(&operations, "status");
    assert_eq!(status["default_value"], "pending");
    assert_eq!(status["extra"], "");
    let created = column(&operations, "created_at");
    assert_eq!(created["default_value"], "CURRENT_TIMESTAMP(6)");
    assert_eq!(created["extra"], "DEFAULT_GENERATED");
}

#[test]
fn reader_memory_create_canonical_ast_records_checks_and_column_encoding() {
    let operation = parse_ddl_operation(READER_MEMORY_ITEMS_CREATE).expect("items operation");
    let evidence = build_semantic_evidence(&operation, &absent_target(), &absent_target())
        .expect("canonical AST");
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).expect("AST JSON");
    let create = &ast["parsed_create_table"];
    assert_eq!(create["columns"][0]["character_set"], "ascii");
    assert_eq!(create["columns"][0]["collation"], "ascii_bin");
    assert!(create["columns"][1].get("character_set").is_none());
    assert_eq!(
        create["check_constraints"][0]["name"],
        "reader_memory_item_json"
    );
    assert_eq!(
        create["check_constraints"][0]["disjuncts"][0]["kind"],
        "is_null"
    );
    assert_eq!(
        create["check_constraints"][0]["disjuncts"][1]["kind"],
        "json_valid"
    );
    assert_eq!(
        create["check_constraints"][1]["disjuncts"][1]["limit"],
        8192
    );
    assert_eq!(
        create["check_constraints"][2]["disjuncts"][0]["values"],
        serde_json::json!(["active", "disabled", "forgotten"])
    );
    let storefront = parse_ddl_operation(include_str!(
        "../../../fixtures/ddl/create-storefront-chips.sql"
    ))
    .expect("storefront operation");
    let storefront_ast: serde_json::Value = serde_json::from_str(
        &build_semantic_evidence(&storefront, &absent_target(), &absent_target())
            .expect("storefront AST")
            .canonical_ast,
    )
    .expect("storefront JSON");
    assert!(
        storefront_ast["parsed_create_table"]
            .get("check_constraints")
            .is_none()
    );
    assert!(
        storefront_ast["parsed_create_table"]["columns"][0]
            .get("character_set")
            .is_none()
    );
}

fn reader_memory_profiles_target() -> SemanticSchemaSnapshot {
    let mut target = absent_target();
    target.inventory.tables.push(TableInventory {
        name: "reader_memory_profiles".to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        collation: Some("utf8mb4_unicode_ci".to_string()),
        primary_key: vec!["user_id".to_string()],
        columns: vec![
            ColumnInventory {
                name: "user_id".to_string(),
                ordinal_position: 1,
                column_type: "int unsigned".to_string(),
                data_type: "int".to_string(),
                is_nullable: false,
                character_set: None,
                collation: None,
                default_value: None,
                extra: String::new(),
                comment: String::new(),
                generated: None,
            },
            ColumnInventory {
                name: "prepared_json".to_string(),
                ordinal_position: 2,
                column_type: "mediumtext".to_string(),
                data_type: "mediumtext".to_string(),
                is_nullable: false,
                character_set: Some("utf8mb4".to_string()),
                collation: Some("utf8mb4_unicode_ci".to_string()),
                default_value: Some("_utf8mb4\\'{}\\'".to_string()),
                extra: "DEFAULT_GENERATED".to_string(),
                comment: String::new(),
                generated: None,
            },
        ],
    });
    target
}

#[test]
fn reader_memory_profiles_alter_adds_text_expression_default_and_checks() {
    let transformation = globalcomix_inventory()
        .transform_sql(READER_MEMORY_PROFILES_ALTER)
        .expect("reader_memory_profiles ALTER must be translatable");
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `reader_memory_profiles` \
ADD COLUMN `checkpoints_json` TEXT NOT NULL DEFAULT (_utf8mb4'{}'), \
ADD CONSTRAINT `reader_memory_checkpoints_json` CHECK (JSON_VALID(`checkpoints_json`)), \
ADD CONSTRAINT `reader_memory_checkpoints_size` CHECK (OCTET_LENGTH(`checkpoints_json`) <= 16384)"
        )
    );
    let operation = parse_ddl_operation(READER_MEMORY_PROFILES_ALTER).expect("ALTER operation");
    assert!(operation.alter_table_ast.is_some());
    let target = reader_memory_profiles_target();
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("ALTER evidence");
    let post: serde_json::Value =
        serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON");
    let added = column(&post, "checkpoints_json");
    assert_eq!(added["ordinal_position"], 3);
    assert_eq!(added["column_type"], "text");
    assert_eq!(added["data_type"], "text");
    assert_eq!(added["is_nullable"], false);
    assert_eq!(added["character_set"], "utf8mb4");
    assert_eq!(added["collation"], "utf8mb4_unicode_ci");
    assert_eq!(added["default_value"], "_utf8mb4\\'{}\\'");
    assert_eq!(added["extra"], "DEFAULT_GENERATED");
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).expect("AST JSON");
    let clauses = &ast["parsed_alter_table"]["clauses"];
    assert_eq!(clauses[0]["kind"], "add_column");
    assert_eq!(clauses[0]["default_value"], "{}");
    assert!(clauses[0].get("character_set").is_none());
    assert_eq!(clauses[1]["kind"], "add_check");
    assert_eq!(
        clauses[1]["constraint"]["name"],
        "reader_memory_checkpoints_json"
    );
    assert_eq!(clauses[2]["constraint"]["disjuncts"][0]["limit"], 16384);

    let missing_column = READER_MEMORY_PROFILES_ALTER.replace(
        "CHECK (JSON_VALID(checkpoints_json))",
        "CHECK (JSON_VALID(absent_json))",
    );
    let operation = parse_ddl_operation(&missing_column).expect("ALTER with unknown check column");
    assert!(build_semantic_evidence(&operation, &target, &target).is_err());
}

#[test]
fn reader_memory_operations_alter_adds_ascii_char_column_and_key() {
    let transformation = globalcomix_inventory()
        .transform_sql(READER_MEMORY_OPERATIONS_ALTER)
        .expect("reader_memory_operations ALTER must be translatable");
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `reader_memory_operations` \
ADD COLUMN `batch_uuid` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL DEFAULT NULL, \
ADD KEY `reader_memory_batch` (`batch_uuid`, `status`)"
        )
    );
    let ast = parse_production_alter_table_ast(READER_MEMORY_OPERATIONS_ALTER).expect("ALTER AST");
    let ParsedAlterClause::AddColumn(added) = &ast.clauses[0] else {
        panic!("first clause must add a column: {ast:?}");
    };
    assert_eq!(added.character_set.as_deref(), Some("ascii"));
    assert_eq!(added.collation.as_deref(), Some("ascii_bin"));
    assert_eq!(added.default_value, None);
    let mut target = absent_target();
    target.inventory.tables.push(TableInventory {
        name: "reader_memory_operations".to_string(),
        table_type: "BASE TABLE".to_string(),
        engine: Some("InnoDB".to_string()),
        collation: Some("utf8mb4_unicode_ci".to_string()),
        primary_key: vec!["uuid".to_string()],
        columns: vec![
            ColumnInventory {
                name: "uuid".to_string(),
                ordinal_position: 1,
                column_type: "char(36)".to_string(),
                data_type: "char".to_string(),
                is_nullable: false,
                character_set: Some("ascii".to_string()),
                collation: Some("ascii_bin".to_string()),
                default_value: None,
                extra: String::new(),
                comment: String::new(),
                generated: None,
            },
            ColumnInventory {
                name: "status".to_string(),
                ordinal_position: 2,
                column_type: "varchar(24)".to_string(),
                data_type: "varchar".to_string(),
                is_nullable: false,
                character_set: Some("utf8mb4".to_string()),
                collation: Some("utf8mb4_unicode_ci".to_string()),
                default_value: Some("pending".to_string()),
                extra: String::new(),
                comment: String::new(),
                generated: None,
            },
        ],
    });
    let operation = parse_ddl_operation(READER_MEMORY_OPERATIONS_ALTER).expect("ALTER operation");
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("ALTER evidence");
    let post: serde_json::Value =
        serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON");
    let batch = column(&post, "batch_uuid");
    assert_eq!(batch["character_set"], "ascii");
    assert_eq!(batch["collation"], "ascii_bin");
    assert_eq!(batch["is_nullable"], true);
    assert_eq!(post["indexes"][0]["name"], "reader_memory_batch");
    assert_eq!(post["indexes"][0]["columns"][1]["name"], "status");
    let ast_json: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).expect("AST");
    assert_eq!(
        ast_json["parsed_alter_table"]["clauses"][0]["character_set"],
        "ascii"
    );
}

#[test]
fn reader_memory_alter_rejects_unmodeled_text_defaults_and_checks() {
    for sql in [
        READER_MEMORY_PROFILES_ALTER.replace(
            "TEXT NOT NULL DEFAULT '{}'",
            "TEXT NOT NULL DEFAULT '{\\\\}'",
        ),
        READER_MEMORY_PROFILES_ALTER.replace("TEXT NOT NULL DEFAULT '{}'", "TEXT NOT NULL"),
        READER_MEMORY_PROFILES_ALTER.replace(
            "JSON_VALID(checkpoints_json)",
            "JSON_VALID(checkpoints_json) AND 1",
        ),
        READER_MEMORY_PROFILES_ALTER.replace("<=16384", ">=16384"),
        READER_MEMORY_OPERATIONS_ALTER.replace("COLLATE ascii_bin", "COLLATE latin1_bin"),
        "ALTER TABLE t ADD COLUMN c VARCHAR(8) NOT NULL".to_string(),
        "ALTER TABLE t ADD COLUMN c VARCHAR(8) NOT NULL DEFAULT 'x\\\\y'".to_string(),
    ] {
        assert!(!supports_production_alter_table(&sql), "accepted {sql}");
    }
}

const READER_MEMORY_PROFILES_GUARDED_ALTER: &str =
    include_str!("../../../fixtures/ddl/alter-reader-memory-profiles-suggestions.sql");

fn reader_memory_profiles_with_suggestions_target() -> SemanticSchemaSnapshot {
    let mut target = reader_memory_profiles_target();
    let table = &mut target.inventory.tables[0];
    for (name, column_type, data_type, nullable, default_value, encoding) in [
        (
            "suggestions_status",
            "varchar(16)",
            "varchar",
            false,
            Some("idle"),
            true,
        ),
        (
            "suggestions_dispatch_after",
            "datetime(6)",
            "datetime",
            true,
            None,
            false,
        ),
        (
            "suggestions_started_at",
            "datetime(6)",
            "datetime",
            true,
            None,
            false,
        ),
    ] {
        table.columns.push(ColumnInventory {
            name: name.to_string(),
            ordinal_position: table.columns.len() as u32 + 1,
            column_type: column_type.to_string(),
            data_type: data_type.to_string(),
            is_nullable: nullable,
            character_set: encoding.then(|| "utf8mb4".to_string()),
            collation: encoding.then(|| "utf8mb4_unicode_ci".to_string()),
            default_value: default_value.map(str::to_string),
            extra: String::new(),
            comment: String::new(),
            generated: None,
        });
    }
    for (name, columns) in [
        (
            "reader_memory_suggestion_dispatch",
            vec!["suggestions_status", "suggestions_dispatch_after"],
        ),
        (
            "reader_memory_suggestion_started",
            vec!["suggestions_started_at"],
        ),
    ] {
        target.inventory.indexes.push(IndexInventory {
            table: "reader_memory_profiles".to_string(),
            name: name.to_string(),
            unique: false,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: None,
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(sequence, column)| IndexColumnInventory {
                    name: column.to_string(),
                    sequence: sequence as u32 + 1,
                    prefix_length: None,
                    collation: Some("A".to_string()),
                    order: "ASC".to_string(),
                })
                .collect(),
        });
    }
    target
}

#[test]
fn reader_memory_guarded_alter_transforms_to_unguarded_mysql8_sql() {
    let transformation = globalcomix_inventory()
        .transform_sql(READER_MEMORY_PROFILES_GUARDED_ALTER)
        .expect("guarded reader_memory_profiles ALTER must be translatable");
    assert_eq!(
        transformation.target_sql.as_deref(),
        Some(
            "ALTER TABLE `reader_memory_profiles` \
ADD COLUMN `suggestions_status` VARCHAR(16) NOT NULL DEFAULT 'idle', \
ADD COLUMN `suggestions_dispatch_after` DATETIME(6) NULL DEFAULT NULL, \
ADD COLUMN `suggestions_started_at` DATETIME(6) NULL DEFAULT NULL, \
ADD KEY `reader_memory_suggestion_dispatch` (`suggestions_status`, `suggestions_dispatch_after`), \
ADD KEY `reader_memory_suggestion_started` (`suggestions_started_at`)"
        )
    );
}

#[test]
fn reader_memory_guarded_alter_adds_absent_columns_and_keys() {
    let operation =
        parse_ddl_operation(READER_MEMORY_PROFILES_GUARDED_ALTER).expect("guarded ALTER operation");
    let target = reader_memory_profiles_target();
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("ALTER evidence");
    assert_ne!(evidence.pre_state, evidence.expected_post_state);
    let post: serde_json::Value =
        serde_json::from_str(&evidence.expected_post_state).expect("post-state JSON");
    let status = column(&post, "suggestions_status");
    assert_eq!(status["ordinal_position"], 3);
    assert_eq!(status["column_type"], "varchar(16)");
    assert_eq!(status["is_nullable"], false);
    assert_eq!(status["default_value"], "idle");
    assert_eq!(status["extra"], "");
    assert_eq!(status["character_set"], "utf8mb4");
    assert_eq!(status["collation"], "utf8mb4_unicode_ci");
    let dispatch_after = column(&post, "suggestions_dispatch_after");
    assert_eq!(dispatch_after["column_type"], "datetime(6)");
    assert_eq!(dispatch_after["data_type"], "datetime");
    assert_eq!(dispatch_after["is_nullable"], true);
    assert_eq!(dispatch_after["default_value"], serde_json::Value::Null);
    assert_eq!(
        column(&post, "suggestions_started_at")["ordinal_position"],
        5
    );
    let indexes = post["indexes"].as_array().expect("indexes");
    assert_eq!(indexes.len(), 2);
    assert_eq!(indexes[0]["name"], "reader_memory_suggestion_dispatch");
    assert_eq!(
        indexes[0]["columns"][1]["name"],
        "suggestions_dispatch_after"
    );
    assert_eq!(indexes[1]["name"], "reader_memory_suggestion_started");
    let ast: serde_json::Value = serde_json::from_str(&evidence.canonical_ast).expect("AST JSON");
    let clauses = &ast["parsed_alter_table"]["clauses"];
    assert_eq!(clauses[0]["if_not_exists"], true);
    assert_eq!(clauses[0]["default_value"], "idle");
    assert_eq!(clauses[3]["kind"], "add_key");
    assert_eq!(clauses[3]["if_not_exists"], true);
    let plain = parse_ddl_operation(READER_MEMORY_OPERATIONS_ALTER).expect("plain ALTER");
    let plain_ast: serde_json::Value = serde_json::from_str(
        &build_semantic_evidence(&plain, &absent_target(), &absent_target())
            .expect_err("plain ALTER needs its table")
            .to_string(),
    )
    .unwrap_or(serde_json::Value::Null);
    assert!(plain_ast.is_null());
}

#[test]
fn reader_memory_guarded_alter_is_a_proven_noop_when_everything_exists() {
    let operation =
        parse_ddl_operation(READER_MEMORY_PROFILES_GUARDED_ALTER).expect("guarded ALTER operation");
    let target = reader_memory_profiles_with_suggestions_target();
    let evidence = build_semantic_evidence(&operation, &target, &target).expect("no-op evidence");
    assert_eq!(evidence.pre_state, evidence.expected_post_state);

    let mut partial = reader_memory_profiles_with_suggestions_target();
    partial.inventory.indexes.clear();
    assert!(build_semantic_evidence(&operation, &partial, &partial).is_err());

    let mut divergent = reader_memory_profiles_with_suggestions_target();
    divergent.inventory.indexes[0].unique = true;
    assert!(build_semantic_evidence(&operation, &divergent, &divergent).is_err());
}

#[test]
fn reader_memory_guarded_alter_rejects_unmodeled_variants() {
    for sql in [
        READER_MEMORY_PROFILES_GUARDED_ALTER.replace("DATETIME(6) NULL,", "DATETIME(3) NULL,"),
        READER_MEMORY_PROFILES_GUARDED_ALTER.replace("NOT NULL DEFAULT 'idle'", "NOT NULL"),
        READER_MEMORY_PROFILES_GUARDED_ALTER.replace("DEFAULT 'idle'", "DEFAULT 'id''le'"),
        READER_MEMORY_PROFILES_GUARDED_ALTER.replace(
            "ADD INDEX IF NOT EXISTS reader_memory_suggestion_started (suggestions_started_at)",
            "ADD UNIQUE INDEX IF NOT EXISTS reader_memory_suggestion_started (suggestions_started_at)",
        ),
        READER_MEMORY_PROFILES_GUARDED_ALTER.replace(
            "(suggestions_started_at)",
            "(suggestions_started_at) USING HASH",
        ),
    ] {
        assert!(!supports_production_alter_table(&sql), "accepted {sql}");
    }
}
