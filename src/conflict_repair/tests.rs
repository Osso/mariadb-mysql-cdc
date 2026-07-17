use super::schema::*;
use super::*;
use crate::snapshot::SnapshotRow;
use std::collections::BTreeMap;

#[test]
fn canonical_fk_inventory_preserves_schema_columns_and_rules() {
    let rows = vec![CanonicalForeignKeyRow {
        constraint_schema: "app".to_string(),
        constraint_name: "child_parent_fk".to_string(),
        child_schema: "app".to_string(),
        child_table: "children".to_string(),
        child_column: "parent_id".to_string(),
        ordinal_position: 1,
        parent_schema: "app".to_string(),
        parent_table: "parents".to_string(),
        parent_column: "id".to_string(),
        update_rule: "RESTRICT".to_string(),
        delete_rule: "CASCADE".to_string(),
        match_option: "NONE".to_string(),
        enforced: true,
    }];
    let inventory = canonicalize_foreign_keys(rows).expect("canonical inventory");
    assert_eq!(inventory[0].child_schema, "app");
    assert_eq!(inventory[0].parent_schema, "app");
    assert_eq!(inventory[0].child_columns, vec!["parent_id"]);
    assert_eq!(inventory[0].parent_columns, vec!["id"]);
    assert_eq!(inventory[0].delete_rule, "CASCADE");
    assert!(inventory[0].enforced);
}

#[test]
fn secondary_unique_conflict_keeps_owner_unchanged_and_records_source_debt() {
    let mut ledger = InMemoryConflictStore::default();
    ledger
        .observe(test_conflict("users", "A"))
        .expect("record conflict");
    let record = &ledger.records()[0];
    assert_eq!(record.key.source_primary_key, vec!["A"]);
    assert_eq!(
        record.duplicate_owner_primary_key,
        Some(vec!["B".to_string()])
    );
    assert_eq!(record.status, ConflictStatus::Unresolved);
    assert_eq!(record.attempt_count, 1);
}

#[test]
fn replay_is_idempotent_for_same_primary_key_and_isolated_for_different_primary_key() {
    let mut ledger = InMemoryConflictStore::default();
    let first = test_conflict("users", "A");
    let second = test_conflict("users", "B");

    ledger.observe(first.clone()).expect("first observation");
    ledger.observe(first).expect("same-PK replay");
    ledger.observe(second).expect("different-PK observation");

    let records = ledger.records();
    assert_eq!(records.len(), 2);
    let first_record = records
        .iter()
        .find(|record| record.key.source_primary_key == ["A"])
        .expect("first PK record");
    let second_record = records
        .iter()
        .find(|record| record.key.source_primary_key == ["B"])
        .expect("second PK record");
    assert_eq!(first_record.attempt_count, 2);
    assert_eq!(second_record.attempt_count, 1);
    assert_ne!(
        conflict_identity(&first_record.key),
        conflict_identity(&second_record.key)
    );
}

#[test]
fn canonical_foreign_keys_treat_no_action_as_restrict_for_cross_engine_parity() {
    let mut rows = vec![CanonicalForeignKeyRow {
        constraint_schema: "app".to_string(),
        constraint_name: "child_parent_fk".to_string(),
        child_schema: "app".to_string(),
        child_table: "children".to_string(),
        child_column: "parent_id".to_string(),
        ordinal_position: 1,
        parent_schema: "app".to_string(),
        parent_table: "parents".to_string(),
        parent_column: "id".to_string(),
        update_rule: "NO ACTION".to_string(),
        delete_rule: "NO ACTION".to_string(),
        match_option: "NONE".to_string(),
        enforced: true,
    }];
    let canonical = canonicalize_foreign_keys(std::mem::take(&mut rows)).expect("canonical FK");
    assert_eq!(canonical[0].update_rule, "RESTRICT");
    assert_eq!(canonical[0].delete_rule, "RESTRICT");
}

#[test]
fn duplicate_classification_distinguishes_primary_secondary_and_malformed() {
    assert_eq!(
        classify_duplicate_error(
            1062,
            "Duplicate entry '1' for key 'PRIMARY'",
            &["A".into()],
            None
        ),
        DuplicateClassification::SamePrimary
    );
    assert_eq!(
        classify_duplicate_error(
            1062,
            "Duplicate entry 'e' for key 'uq_email'",
            &["A".into()],
            Some(&["B".into()])
        ),
        DuplicateClassification::SecondaryUnique {
            owner_differs: true
        }
    );
    assert_eq!(
        classify_duplicate_error(1062, "not parseable", &["A".into()], None),
        DuplicateClassification::Malformed
    );
}

#[test]
fn planner_deletes_child_before_parent_and_inserts_parent_before_child() {
    let inventory = repair_inventory(&["parents", "children"], &[fk("children", "parents")]);
    let plan =
        build_repair_plan("run-1", "source", "target", &inventory, &inventory, 10).expect("plan");
    assert_eq!(plan.insert_order, vec!["parents", "children"]);
    assert_eq!(plan.delete_order, vec!["children", "parents"]);
}

#[test]
fn cycle_blocks_before_any_mutation() {
    let inventory = repair_inventory(&["a", "b"], &[fk("a", "b"), fk("b", "a")]);
    let error = build_repair_plan("run-cycle", "source", "target", &inventory, &inventory, 10)
        .expect_err("cycle must block");
    assert!(matches!(error, RepairPlanError::Cycle(_)));
}

#[test]
fn delete_ceiling_preflight_performs_zero_mutations() {
    let inventory = repair_inventory(&["accounts"], &[]);
    let plan = build_repair_plan("run-limit", "source", "target", &inventory, &inventory, 0)
        .expect("plan");
    let input = RepairInput {
        source_rows: rows(&[("accounts", "1", "new")]),
        target_rows: rows(&[("accounts", "1", "new"), ("accounts", "2", "extra")]),
    };
    let mut store = InMemoryRepairProgressStore::default();
    let mut target = InMemoryRepairExecutor::from_rows(input.target_rows.clone());
    let mut conflicts = InMemoryConflictStore::default();
    let error = run_phased_repair(&plan, &input, &mut store, &mut target, &mut conflicts)
        .expect_err("ceiling");
    assert!(error.contains("delete safety threshold"));
    assert!(target.operations.is_empty());
}

#[test]
fn interrupted_phase_resumes_exact_plan_without_repeating_completed_deletes() {
    let inventory = repair_inventory(&["accounts"], &[]);
    let plan = build_repair_plan("run-resume", "source", "target", &inventory, &inventory, 2)
        .expect("plan");
    let input = RepairInput {
        source_rows: rows(&[("accounts", "1", "one")]),
        target_rows: rows(&[
            ("accounts", "1", "one"),
            ("accounts", "2", "two"),
            ("accounts", "3", "three"),
        ]),
    };
    let mut store = InMemoryRepairProgressStore::default();
    let mut target = InMemoryRepairExecutor::from_rows(input.target_rows.clone());
    target.fail_after_operations = Some(1);
    let mut conflicts = InMemoryConflictStore::default();
    assert!(run_phased_repair(&plan, &input, &mut store, &mut target, &mut conflicts).is_err());
    let first_operation_count = target.operations.len();
    target.fail_after_operations = None;
    assert!(run_phased_repair(&plan, &input, &mut store, &mut target, &mut conflicts).is_ok());
    assert_eq!(
        target
            .operations
            .iter()
            .filter(|op| matches!(op, RepairOperation::Delete { .. }))
            .count(),
        2
    );
    assert_eq!(first_operation_count, 1);
}

#[test]
fn durable_conflict_store_is_a_validating_conflict_store() {
    let mut store = DurableConflictStore::new(RecordingSql::default(), "cdc.row_conflicts");
    let observation = test_conflict("accounts", "1");

    let conflict_store: &mut dyn ConflictStore = &mut store;
    conflict_store.ensure().expect("schema validation");
    conflict_store.observe(observation).expect("observation");
    conflict_store
        .resolve_if_equal("accounts", &["1".to_string()], true, "run", "equal")
        .expect("resolution");

    let sql = store.executor.sql.join("\\n");
    assert!(sql.contains("SELECT"));
    assert!(!sql.contains("CREATE TABLE"));
    assert!(sql.contains("first_observed_at_ms"));
    assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
    assert!(sql.contains("status='resolved'"));
}

#[test]
fn rejects_missing_conflict_column_before_runtime_mutation() {
    let mut columns = expected_conflict_columns();
    columns.pop();
    assert!(validate_conflict_columns(&columns).is_err());
}

#[test]
fn conflict_identity_schema_is_compact_and_unprefixed() {
    let columns = expected_conflict_columns();
    assert_eq!(columns[0].0, "conflict_identity");
    assert_eq!(columns[0].1, "char(64)");
    let keys = expected_conflict_keys();
    assert_eq!(
        keys,
        vec![(
            "PRIMARY".to_string(),
            0,
            1,
            "conflict_identity".to_string(),
            None,
        )]
    );
}

#[test]
fn long_multibyte_identity_does_not_use_a_primary_key_prefix() {
    let mut observation = test_conflict("表".repeat(300).as_str(), &"鍵".repeat(600));
    observation.schema = "スキーマ".repeat(300);
    observation.table = "テーブル".repeat(300);
    observation.coordinate.file = "binlog-".to_string() + &"é".repeat(300);
    let sql = build_conflict_observation_sql("cdc.row_conflicts", &observation);
    assert!(sql.contains("conflict_identity"));
    assert!(!sql.contains("source_primary_key_json("));
    assert!(sql.contains(&serde_json::to_string(&observation.source_primary_key).unwrap()));
}

#[test]
fn distinct_full_conflict_identities_produce_distinct_hashes() {
    let first = test_conflict("accounts", "1");
    let mut second = first.clone();
    second.source_primary_key = vec!["2".to_string()];
    assert_ne!(
        conflict_identity(&first.key()),
        conflict_identity(&second.key())
    );

    second.coordinate.start_position += 1;
    assert_ne!(
        conflict_identity(&first.key()),
        conflict_identity(&second.key())
    );
}

#[test]
fn mutated_conflict_identity_is_rejected() {
    let observation = test_conflict("accounts", "1");
    let identity = observation.conflict_identity();
    let mut mutated = observation.key();
    mutated.table = "other_accounts".to_string();
    assert!(validate_conflict_identity(&identity, &mutated).is_err());
}

#[test]
fn accepts_mariadb_and_mysql8_status_check_metadata_fixtures() {
    let mariadb = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/conflict-metadata-mariadb-check-clause.txt"
    ))
    .trim()
    .to_string();
    let mysql8 = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/conflict-metadata-mysql8-check-clause.txt"
    ))
    .trim()
    .to_string();

    assert!(validate_conflict_status_checks(&[mariadb]).is_ok());
    assert!(validate_conflict_status_checks(&[mysql8]).is_ok());
}

#[test]
fn rejects_non_exact_conflict_status_check_expressions() {
    for check in [
        "status IN ('unresolved')",
        "status IN ('unresolved','resolved','other')",
        "state IN ('unresolved','resolved')",
        "status = 'unresolved'",
    ] {
        assert!(
            validate_conflict_status_checks(&[check.to_string()]).is_err(),
            "accepted invalid status check: {check}"
        );
    }
}

#[test]
fn rejects_wrong_conflict_primary_key_and_status_constraint() {
    let mut keys = expected_conflict_keys();
    keys[0].3 = "table_name".to_string();
    assert!(validate_conflict_keys(&keys).is_err());

    let constraints = vec![("PRIMARY KEY".to_string(), "YES".to_string())];
    assert!(validate_conflict_constraints(&constraints).is_err());
}

fn valid_conflict_triggers() -> Vec<TriggerMetadata> {
    vec![
        trigger_metadata_from_sql_row((
            "row_conflicts_insert_guard".into(),
            "cdc".into(),
            "row_conflicts".into(),
            "INSERT".into(),
            "BEFORE".into(),
            CONFLICT_INSERT_GUARD_BODY.into(),
            1,
        )),
        trigger_metadata_from_sql_row((
            "row_conflicts_update_guard".into(),
            "cdc".into(),
            "row_conflicts".into(),
            "UPDATE".into(),
            "BEFORE".into(),
            CONFLICT_UPDATE_GUARD_BODY.into(),
            1,
        )),
    ]
}

fn valid_trigger_inventory_routine() -> &'static str {
    "CREATE DEFINER=`root`@`%` PROCEDURE `cdc`.`row_conflicts_trigger_inventory`() SQL SECURITY DEFINER READS SQL DATA BEGIN SELECT trigger_name,event_object_schema,event_object_table,event_manipulation,action_timing,action_statement,action_order FROM information_schema.triggers WHERE event_object_schema = 'cdc' AND event_object_table = 'row_conflicts' ORDER BY event_manipulation, action_order; END"
}

#[test]
fn validates_call_rows_and_admin_inventory_routine_evidence() {
    assert_eq!(
        conflict_trigger_inventory_routine_path("cdc.row_conflicts").unwrap(),
        "`cdc`.`row_conflicts_trigger_inventory`"
    );
    assert!(conflict_trigger_inventory_routine_path("cdc.other_table").is_err());
    validate_conflict_triggers("cdc", "row_conflicts", &valid_conflict_triggers())
        .expect("exact conflict guards");
    validate_conflict_trigger_inventory_routine_definition(
        "cdc",
        "row_conflicts",
        valid_trigger_inventory_routine(),
    )
    .expect("exact conflict inventory procedure");
}

#[test]
fn rejects_malformed_conflict_trigger_inventory_rows() {
    let mut triggers = valid_conflict_triggers();
    triggers[0].event = "BROKEN".into();
    assert!(validate_conflict_triggers("cdc", "row_conflicts", &triggers).is_err());
    triggers = valid_conflict_triggers();
    triggers[1].body = "BEGIN END".into();
    assert!(validate_conflict_triggers("cdc", "row_conflicts", &triggers).is_err());
}

#[test]
fn rejects_unsafe_conflict_inventory_routine_definitions() {
    for routine in [
        "CREATE DEFINER=`root`@`%` PROCEDURE `cdc`.`row_conflicts_trigger_inventory`() SQL SECURITY INVOKER READS SQL DATA BEGIN SELECT 1; END",
        "CREATE DEFINER=`root`@`%` PROCEDURE `cdc`.`row_conflicts_trigger_inventory`() SQL SECURITY DEFINER READS SQL DATA BEGIN SELECT 1; END",
        "CREATE DEFINER=`root`@`%` PROCEDURE `cdc`.`other_trigger_inventory`() SQL SECURITY DEFINER READS SQL DATA BEGIN SELECT 1; END",
        "CREATE DEFINER=`root`@`%` PROCEDURE `cdc`.`row_conflicts_trigger_inventory`() SQL SECURITY DEFINER READS SQL DATA BEGIN SELECT trigger_name,event_object_schema,event_object_table,event_manipulation,action_timing,action_statement,action_order FROM information_schema.triggers WHERE event_object_schema = 'cdc' AND event_object_table = 'row_conflicts' ORDER BY event_manipulation, action_order; END SELECT 1",
    ] {
        assert!(
            validate_conflict_trigger_inventory_routine_definition("cdc", "row_conflicts", routine)
                .is_err()
        );
    }
}

#[test]
fn conflict_resolution_sql_has_guarded_resolution_fields() {
    let sql = build_conflict_resolution_by_table_sql(
        "cdc.row_conflicts",
        "accounts",
        &["1".into()],
        "run",
        "verified equality",
    );
    assert!(sql.contains("status='unresolved'"));
    assert!(sql.contains("repair_run_id"));
    assert!(sql.contains("resolution_evidence"));
}

#[test]
fn accepts_real_trigger_inventory_row_order() {
    let triggers = valid_conflict_triggers();
    assert_eq!(
        triggers[0],
        TriggerMetadata {
            name: "row_conflicts_insert_guard".into(),
            schema: "cdc".into(),
            table: "row_conflicts".into(),
            event: "INSERT".into(),
            timing: "BEFORE".into(),
            body: CONFLICT_INSERT_GUARD_BODY.into(),
            action_order: 1,
        }
    );
    validate_conflict_triggers("cdc", "row_conflicts", &triggers)
        .expect("real procedure row order should validate");
}

#[test]
fn rejects_swapped_real_trigger_inventory_columns() {
    let mut triggers = valid_conflict_triggers();
    triggers[0].schema = "row_conflicts".into();
    triggers[0].table = "cdc".into();
    assert!(validate_conflict_triggers("cdc", "row_conflicts", &triggers).is_err());
}

#[test]
fn builds_conflict_observation_sql_with_resolution_preservation() {
    let sql = build_conflict_observation_sql("cdc.row_conflicts", &test_conflict("accounts", "1"));
    assert!(sql.contains("status=IF(status='resolved',status,'unresolved')"));
    assert!(sql.contains("conflict_identity=IF("));
    assert!(sql.contains("source_primary_key_json <=> VALUES(source_primary_key_json)"));
    assert!(sql.contains("conflict_identity,NULL"));
}

#[test]
fn rejects_wrong_conflict_identity_charset_or_collation() {
    assert!(
        validate_conflict_identity_definition(&("utf8mb4".to_string(), "utf8mb4_bin".to_string(),))
            .is_err()
    );
}

#[test]
fn fresh_second_run_converges_and_only_then_resolves_conflict() {
    let (first, second) = two_repair_plans();
    assert_ne!(first.run_id, second.run_id);
    let input = RepairInput {
        source_rows: rows(&[("accounts", "1", "new")]),
        target_rows: rows(&[("accounts", "1", "old")]),
    };
    let mut store = InMemoryRepairProgressStore::default();
    let mut target = InMemoryRepairExecutor::from_rows(input.target_rows.clone());
    let mut conflicts = InMemoryConflictStore::default();
    conflicts
        .observe(test_conflict("accounts", "1"))
        .expect("conflict");
    assert_eq!(conflicts.unresolved_count(), 1);
    run_phased_repair(&first, &input, &mut store, &mut target, &mut conflicts).expect("repair");
    assert_eq!(conflicts.unresolved_count(), 0);
    let second_input = RepairInput {
        source_rows: input.source_rows,
        target_rows: target.rows(),
    };
    let report = run_phased_repair(
        &second,
        &second_input,
        &mut store,
        &mut target,
        &mut conflicts,
    )
    .expect("converged");
    assert_eq!(report.actionable_mismatches, 0);
}

fn two_repair_plans() -> (RepairPlan, RepairPlan) {
    let inventory = repair_inventory(&["accounts"], &[]);
    let first = build_repair_plan("run-first", "source", "target", &inventory, &inventory, 1)
        .expect("first plan");
    let second = build_repair_plan("run-second", "source", "target", &inventory, &inventory, 1)
        .expect("second plan");
    (first, second)
}

fn fk(child: &str, parent: &str) -> CanonicalForeignKey {
    CanonicalForeignKey {
        constraint_schema: "app".to_string(),
        constraint_name: format!("{child}_{parent}_fk"),
        child_schema: "app".to_string(),
        child_table: child.to_string(),
        child_columns: vec![format!("{parent}_id")],
        parent_schema: "app".to_string(),
        parent_table: parent.to_string(),
        parent_columns: vec!["id".to_string()],
        update_rule: "RESTRICT".to_string(),
        delete_rule: "RESTRICT".to_string(),
        match_option: "NONE".to_string(),
        enforced: true,
    }
}
fn repair_inventory(tables: &[&str], foreign_keys: &[CanonicalForeignKey]) -> RepairInventory {
    RepairInventory {
        schema: "app".to_string(),
        tables: tables.iter().map(|table| table.to_string()).collect(),
        foreign_keys: foreign_keys.to_vec(),
    }
}
fn rows(values: &[(&str, &str, &str)]) -> BTreeMap<String, Vec<SnapshotRow>> {
    let mut result = BTreeMap::new();
    for (table, id, value) in values {
        result
            .entry((*table).to_string())
            .or_insert_with(Vec::new)
            .push(SnapshotRow {
                primary_key: vec![(*id).to_string()],
                values: BTreeMap::from([
                    ("id".to_string(), Some((*id).to_string())),
                    ("value".to_string(), Some((*value).to_string())),
                ]),
            });
    }
    result
}
#[derive(Default)]
struct RecordingSql {
    sql: Vec<String>,
}

impl ConflictSqlExecutor for RecordingSql {
    fn execute(&mut self, sql: &str) -> Result<(), String> {
        self.sql.push(sql.to_string());
        Ok(())
    }
}

fn test_conflict(table: &str, id: &str) -> ConflictObservation {
    ConflictObservation {
        source_identity: "source".to_string(),
        source_server_id: 1,
        coordinate: ConflictCoordinate {
            file: "binlog.1".to_string(),
            start_position: 1,
            end_position: 2,
        },
        schema: "app".to_string(),
        table: table.to_string(),
        operation: ConflictOperation::Update,
        source_primary_key: vec![id.to_string()],
        duplicate_index: Some("uq_email".to_string()),
        duplicate_owner_primary_key: Some(vec!["B".to_string()]),
        error_code: 1062,
        error_text: "duplicate".to_string(),
        observed_at_ms: 1,
    }
}
