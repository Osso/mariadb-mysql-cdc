use super::schema::*;
use super::sql::*;
use super::*;

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
        keys[0],
        (
            "PRIMARY".to_string(),
            0,
            1,
            "conflict_identity".to_string(),
            None,
        )
    );
}

#[test]
fn conflict_schema_requires_indexed_source_row_identity() {
    let columns = expected_conflict_columns();
    assert_eq!(
        columns
            .iter()
            .find(|column| column.0 == "source_row_identity")
            .expect("source row identity column"),
        &(
            "source_row_identity".to_string(),
            "char(64)".to_string(),
            "NO".to_string(),
            "<null>".to_string(),
            "stored generated".to_string(),
        )
    );
    assert_eq!(
        expected_conflict_keys(),
        vec![
            (
                "PRIMARY".to_string(),
                0,
                1,
                "conflict_identity".to_string(),
                None,
            ),
            (
                "row_conflicts_source_row_status".to_string(),
                1,
                1,
                "source_row_identity".to_string(),
                None,
            ),
            (
                "row_conflicts_source_row_status".to_string(),
                1,
                2,
                "status".to_string(),
                None,
            ),
        ]
    );
}

#[test]
fn distinct_full_conflict_identities_produce_distinct_hashes() {
    let first = test_conflict_key("accounts", "1");
    let mut second = first.clone();
    second.source_primary_key = vec!["2".to_string()];
    assert_ne!(conflict_identity(&first), conflict_identity(&second));

    second.coordinate.start_position += 1;
    assert_ne!(conflict_identity(&first), conflict_identity(&second));
}

#[test]
fn mutated_conflict_identity_is_rejected() {
    let observation = test_conflict_key("accounts", "1");
    let identity = observation.conflict_identity();
    let mut mutated = observation.clone();
    mutated.table = "other_accounts".to_string();
    assert!(validate_conflict_identity(&identity, &mutated).is_err());
}

#[test]
fn source_row_identity_ignores_event_identity_but_isolates_source_rows() {
    let first = test_conflict_key("accounts", "1");
    let mut same_row = first.clone();
    same_row.source_server_id += 1;
    same_row.coordinate.file = "other-binlog".into();
    same_row.coordinate.start_position += 10;
    same_row.operation = ConflictOperation::Update;
    assert_eq!(first.source_row_identity(), same_row.source_row_identity());

    same_row.source_primary_key = vec!["2".into()];
    assert_ne!(first.source_row_identity(), same_row.source_row_identity());
    same_row = first.clone();
    same_row.source_identity = "other-source".into();
    assert_ne!(first.source_row_identity(), same_row.source_row_identity());
    same_row = first.clone();
    same_row.schema = "other_schema".into();
    assert_ne!(first.source_row_identity(), same_row.source_row_identity());
    same_row = first.clone();
    same_row.table = "other_accounts".into();
    assert_ne!(first.source_row_identity(), same_row.source_row_identity());
}

#[test]
fn accepts_mysql_source_row_identity_generation_expression() {
    let definition = (
        "ascii".to_string(),
        "ascii_bin".to_string(),
        "sha2(concat(unhex(lpad(hex(length(`source_identity`)),16,_latin1\\'0\\')),convert(`source_identity` using binary),unhex(lpad(hex(length(`schema_name`)),16,_latin1\\'0\\')),convert(`schema_name` using binary),unhex(lpad(hex(length(`table_name`)),16,_latin1\\'0\\')),convert(`table_name` using binary),unhex(lpad(hex(length(`source_primary_key_json`)),16,_latin1\\'0\\')),convert(`source_primary_key_json` using binary)),256)".to_string(),
    );
    validate_source_row_identity_definition(&definition)
        .expect("MySQL normalized generation expression");

    let mut wrong = definition;
    wrong.2 = wrong.2.replace("table_name", "schema_name");
    assert!(validate_source_row_identity_definition(&wrong).is_err());
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
fn source_row_resolution_uses_canonical_identity_and_exact_collision_guard() {
    let resolution = ConflictResolution {
        source_identity: "source-a".into(),
        schema: "globalcomix".into(),
        table: "accounts".into(),
        source_primary_key: vec!["1".into()],
        repair_run_id: "repair-1".into(),
        evidence: "verified".into(),
    };

    let sql = build_conflict_resolution_for_source_row_sql("cdc.row_conflicts", &resolution);

    assert!(sql.contains(
        "source_row_identity='baf8a9d5f0a3a73572a16ebced65c51b92a720dd0cd1de3a29cec93e40e55e5c'"
    ));
    assert!(sql.contains("source_identity='source-a'"));
    assert!(sql.contains("schema_name='globalcomix'"));
    assert!(sql.contains("table_name='accounts'"));
    assert!(sql.contains("source_primary_key_json='[\"1\"]'"));
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
fn rejects_wrong_conflict_identity_charset_or_collation() {
    assert!(
        validate_conflict_identity_definition(&("utf8mb4".to_string(), "utf8mb4_bin".to_string(),))
            .is_err()
    );
}

#[test]
fn duplicate_key_name_extracts_historical_index() {
    assert_eq!(
        duplicate_key_name("Duplicate entry 'abc' for key 'guests.idx_guest_hash'"),
        Some("guests.idx_guest_hash".to_string())
    );
    assert_eq!(duplicate_key_name("duplicate without index"), None);
}

fn test_conflict_key(table: &str, id: &str) -> ConflictKey {
    ConflictKey {
        source_identity: "source".to_string(),
        source_server_id: 1,
        coordinate: ConflictCoordinate {
            file: "binlog.1".to_string(),
            start_position: 4,
            end_position: 8,
        },
        schema: "app".to_string(),
        table: table.to_string(),
        operation: ConflictOperation::Update,
        source_primary_key: vec![id.to_string()],
    }
}
