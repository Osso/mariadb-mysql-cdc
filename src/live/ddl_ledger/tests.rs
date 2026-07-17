use super::*;

#[test]
fn ledger_target_connection_uses_configured_ca() {
    let target = TargetMySqlConfig {
        host: "target-db".to_string(),
        port: 3306,
        user: "cdc".to_string(),
        password: "secret".to_string(),
        database: "globalcomix".to_string(),
        tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
        insert_conflict_policy: super::super::InsertConflictPolicy::Error,
    };

    let opts = target_opts(&target).expect("ledger target options");

    assert_eq!(
        opts.get_ssl_opts().and_then(|ssl| ssl.root_cert_path()),
        Some(std::path::Path::new(&target.tls_ca_file))
    );
}

#[test]
fn builds_trigger_inventory_call_from_schema_qualified_ledger_table() {
    assert_eq!(
        ddl_trigger_inventory_routine_name("ddl_events"),
        "ddl_events_trigger_inventory"
    );
    assert_eq!(
        build_ddl_trigger_inventory_call_sql("cdc.ddl_events"),
        "CALL `cdc`.`ddl_events_trigger_inventory`()"
    );
    assert_eq!(
        build_ddl_trigger_inventory_call_sql("custom.ddl_ledger"),
        "CALL `custom`.`ddl_ledger_trigger_inventory`()"
    );
}

#[test]
fn validates_trigger_inventory_returned_by_definer_routine() {
    let rows = valid_trigger_inventory_rows();
    let (insert_triggers, update_triggers) =
        validate_trigger_inventory_metadata("cdc", "ddl_events", &rows).expect("trigger metadata");

    assert_eq!(insert_triggers.len(), 1);
    assert_eq!(update_triggers.len(), 1);
    assert!(
        validate_pending_trigger_inventory("ddl_events_pending_insert_guard", &insert_triggers)
            .is_ok()
    );
    assert!(
        validate_resolution_trigger_inventory(
            "ddl_events_monotonic_resolution_guard",
            &update_triggers,
        )
        .is_ok()
    );
}

#[test]
fn rejects_trigger_inventory_for_wrong_table() {
    let mut rows = valid_trigger_inventory_rows();
    rows[0].2 = "other_ledger".to_string();
    assert!(validate_trigger_inventory_metadata("cdc", "ddl_events", &rows).is_err());
}

#[test]
fn rejects_trigger_inventory_with_wrong_timing() {
    let mut rows = valid_trigger_inventory_rows();
    rows[0].4 = "AFTER".to_string();
    assert!(validate_trigger_inventory_metadata("cdc", "ddl_events", &rows).is_err());
}

#[test]
fn rejects_trigger_inventory_with_wrong_event() {
    let mut rows = valid_trigger_inventory_rows();
    rows[0].3 = "DELETE".to_string();
    assert!(validate_trigger_inventory_metadata("cdc", "ddl_events", &rows).is_err());
}

fn valid_trigger_inventory_rows() -> Vec<TriggerMetadata> {
    vec![
        (
            "ddl_events_pending_insert_guard".to_string(),
            "cdc".to_string(),
            "ddl_events".to_string(),
            "INSERT".to_string(),
            "BEFORE".to_string(),
            PENDING_ONLY_TRIGGER_BODY.to_string(),
            1,
        ),
        (
            "ddl_events_monotonic_resolution_guard".to_string(),
            "cdc".to_string(),
            "ddl_events".to_string(),
            "UPDATE".to_string(),
            "BEFORE".to_string(),
            MONOTONIC_RESOLUTION_TRIGGER_BODY.to_string(),
            1,
        ),
    ]
}

#[test]
fn creates_manual_ddl_resolution_ledger() {
    let sql = build_create_ddl_ledger_table_sql("cdc.ddl_events");

    assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `cdc`.`ddl_events`"));
    assert!(sql.contains("source_identity VARCHAR(384) NOT NULL"));
    assert!(sql.contains("source_server_id INT UNSIGNED NOT NULL"));
    assert!(sql.contains("binlog_file VARCHAR(255) NOT NULL"));
    assert!(sql.contains("event_start_position BIGINT UNSIGNED NOT NULL"));
    assert!(sql.contains("event_end_position BIGINT UNSIGNED NOT NULL"));
    assert!(sql.contains("status VARCHAR(32) NOT NULL"));
    assert!(sql.contains("raw_sql LONGTEXT NOT NULL"));
    assert!(sql.contains("PRIMARY KEY (source_identity,binlog_file,event_start_position)"));
}

#[test]
fn validates_existing_ledger_columns_and_primary_key() {
    let columns = expected_ddl_ledger_columns();
    assert!(validate_ddl_ledger_columns(&columns).is_ok());
    assert!(
        validate_ddl_ledger_primary_key(&[
            "source_identity".to_string(),
            "binlog_file".to_string(),
            "event_start_position".to_string(),
        ])
        .is_ok()
    );

    let mut wrong_columns = columns;
    wrong_columns[0].1 = "varchar(512)".to_string();
    assert!(validate_ddl_ledger_columns(&wrong_columns).is_err());
    assert!(
        validate_ddl_ledger_primary_key(&[
            "binlog_file".to_string(),
            "source_identity".to_string(),
            "event_start_position".to_string(),
        ])
        .is_err()
    );
}

#[test]
fn requires_exact_status_check() {
    assert!(
        validate_ddl_status_checks(&[
            "(`status` in (_utf8mb4'pending',_utf8mb4'resolved'))".to_string()
        ])
        .is_ok()
    );
    assert!(validate_ddl_status_checks(&["status <> ''".to_string()]).is_err());
}

#[test]
fn requires_pending_only_insert_trigger() {
    let trigger_sql = build_pending_only_ddl_trigger_sql("cdc.ddl_events");
    assert!(trigger_sql.contains("BEFORE INSERT ON `cdc`.`ddl_events`"));
    assert!(trigger_sql.contains("NEW.status <> 'pending'"));
    assert!(trigger_sql.contains("NEW.resolution_note IS NOT NULL"));
    assert!(validate_pending_only_trigger(PENDING_ONLY_TRIGGER_BODY).is_ok());
    assert!(validate_pending_only_trigger("SET NEW.status = 'resolved'").is_err());
}

#[test]
fn requires_enforced_ledger_constraints() {
    assert!(
        validate_ddl_constraints(&[
            ("CHECK".to_string(), "YES".to_string()),
            ("PRIMARY KEY".to_string(), "YES".to_string()),
        ])
        .is_ok()
    );
    assert!(
        validate_ddl_constraints(&[
            ("CHECK".to_string(), "NO".to_string()),
            ("PRIMARY KEY".to_string(), "YES".to_string()),
        ])
        .is_err()
    );
}

#[test]
fn requires_exact_pending_trigger_inventory() {
    let valid = [(
        "ddl_events_pending_insert_guard".to_string(),
        PENDING_ONLY_TRIGGER_BODY.to_string(),
        1,
    )];
    assert!(validate_pending_trigger_inventory("ddl_events_pending_insert_guard", &valid).is_ok());

    let bypass = [
        valid[0].clone(),
        (
            "later_bypass".to_string(),
            "SET NEW.status='resolved'".to_string(),
            2,
        ),
    ];
    assert!(
        validate_pending_trigger_inventory("ddl_events_pending_insert_guard", &bypass).is_err()
    );
}

#[test]
fn requires_monotonic_resolution_trigger() {
    let update_trigger_sql = build_monotonic_ddl_resolution_trigger_sql("cdc.ddl_events");
    assert!(update_trigger_sql.contains("BEFORE UPDATE ON `cdc`.`ddl_events`"));
    assert!(update_trigger_sql.contains("OLD.event_end_position <=> NEW.event_end_position"));
    assert!(update_trigger_sql.contains("OLD.status <> 'pending'"));
    assert!(update_trigger_sql.contains("NEW.status <> 'resolved'"));
    let valid = [(
        "ddl_events_monotonic_resolution_guard".to_string(),
        MONOTONIC_RESOLUTION_TRIGGER_BODY.to_string(),
        1,
    )];
    assert!(
        validate_resolution_trigger_inventory("ddl_events_monotonic_resolution_guard", &valid,)
            .is_ok()
    );
}

#[test]
fn accepts_escaped_status_literals_returned_by_information_schema() {
    assert!(
        validate_ddl_status_checks(&[
            "(`status` in (_utf8mb4\\'pending\\',_utf8mb4\\'resolved'))".to_string()
        ])
        .is_ok()
    );
}

#[test]
fn records_pending_event_without_overwriting_existing_resolution() {
    let event = ddl_event();
    let sql = build_record_pending_ddl_sql("cdc.ddl_events", &event);

    assert!(sql.starts_with("INSERT INTO `cdc`.`ddl_events`"));
    assert!(sql.contains("'pending'"));
    assert!(sql.contains("ALTER TABLE accounts ADD COLUMN handle varchar(64)"));
    assert!(!sql.contains("ON DUPLICATE KEY UPDATE"));
}

#[test]
fn selects_status_and_sql_by_immutable_event_coordinate() {
    let event = ddl_event();
    let sql = build_ddl_status_select_sql("cdc.ddl_events", &event);

    assert!(sql.contains("source_identity='production-source#server-id=3'"));
    assert!(sql.contains("binlog_file='mysqld-bin.000777'"));
    assert!(sql.contains("event_start_position=99"));
    assert_eq!(
        parse_ddl_status("resolved\tALTER TABLE accounts ADD COLUMN handle varchar(64)\n")
            .expect("status"),
        Some(DdlEventStatus::Resolved {
            raw_sql: "ALTER TABLE accounts ADD COLUMN handle varchar(64)".to_string(),
        })
    );
}

fn ddl_event() -> DdlEvent {
    DdlEvent {
        source_identity: "production-source#server-id=3".to_string(),
        source_server_id: 3,
        binlog_file: "mysqld-bin.000777".to_string(),
        event_start_position: 99,
        event_end_position: 180,
        schema_name: "fixture_cdc".to_string(),
        raw_sql: "ALTER TABLE accounts ADD COLUMN handle varchar(64)".to_string(),
    }
}
