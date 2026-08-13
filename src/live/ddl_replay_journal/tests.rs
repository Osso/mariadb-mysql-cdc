use super::*;

fn target() -> TargetMySqlConfig {
    TargetMySqlConfig {
        host: "target-db".to_string(),
        port: 3306,
        user: "cdc".to_string(),
        password: "secret".to_string(),
        database: "globalcomix".to_string(),
        tls_ca_file: concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string(),
        insert_conflict_policy: super::super::InsertConflictPolicy::Error,
    }
}

fn event() -> DdlEvent {
    DdlEvent {
        source_identity: "prod#server-id=3".to_string(),
        source_server_id: 3,
        binlog_file: "mysqld-bin.000777".to_string(),
        event_start_position: 100,
        event_end_position: 200,
        schema_name: "globalcomix".to_string(),
        raw_sql: "TRUNCATE TABLE accounts".to_string(),
    }
}

fn evidence() -> DdlSemanticEvidence {
    DdlSemanticEvidence {
        transformation_version: "mariadb-mysql8-v1".to_string(),
        generated_sql: Some("TRUNCATE TABLE `accounts`".to_string()),
        canonical_ast: "{\"family\":\"truncate\"}".to_string(),
        pre_state: "{\"row_count\":7}".to_string(),
        expected_post_state: "{\"row_count\":0}".to_string(),
    }
}

fn journal_trigger_row(
    name: &str,
    event: &str,
    order: u64,
    statement: &str,
) -> JournalTriggerMetadata {
    (
        name.to_string(),
        "cdc".to_string(),
        "ddl_replay_journal".to_string(),
        event.to_string(),
        "BEFORE".to_string(),
        statement.to_string(),
        order,
    )
}

fn triggers() -> Vec<JournalTriggerMetadata> {
    vec![
        journal_trigger_row(
            "ddl_replay_journal_insert_guard",
            "INSERT",
            1,
            JOURNAL_PENDING_INSERT_TRIGGER_BODY,
        ),
        journal_trigger_row(
            "ddl_replay_journal_update_guard",
            "UPDATE",
            1,
            JOURNAL_MONOTONIC_UPDATE_TRIGGER_BODY,
        ),
    ]
}

fn grants() -> Vec<String> {
    vec![
        "GRANT USAGE ON *.* TO `cdc_stream`@`%`".to_string(),
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, REFERENCES, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, EXECUTE, EVENT, TRIGGER ON `globalcomix`.* TO `cdc_stream`@`%`".to_string(),
        "GRANT SELECT, INSERT, UPDATE ON `cdc`.`stream_checkpoint` TO `cdc_stream`@`%`".to_string(),
        "GRANT SELECT, INSERT, UPDATE ON `cdc`.`row_conflicts` TO `cdc_stream`@`%`".to_string(),
        "GRANT SELECT, INSERT, UPDATE ON `cdc`.`ddl_replay_journal` TO `cdc_stream`@`%`".to_string(),
        "GRANT SELECT, INSERT, UPDATE ON `cdc`.`table_sync_runs` TO `cdc_stream`@`%`".to_string(),
        "GRANT EXECUTE ON PROCEDURE `cdc`.`ddl_replay_journal_trigger_inventory` TO `cdc_stream`@`%`".to_string(),
    ]
}

fn runtime_contract<'a>(
    columns: &'a [JournalColumn],
    keys: &'a [JournalKey],
    constraints: &'a [JournalConstraint],
    checks: &'a [String],
    triggers: &'a [JournalTriggerMetadata],
    grants: &'a [String],
) -> JournalRuntimeContract<'a> {
    JournalRuntimeContract {
        expected_schema: "cdc",
        expected_table: "ddl_replay_journal",
        columns,
        keys,
        constraints,
        checks,
        triggers,
        grants,
        application_schema: "globalcomix",
        checkpoint_table: "cdc.stream_checkpoint",
        journal_table: "cdc.ddl_replay_journal",
        conflict_table: "cdc.row_conflicts",
        inventory_procedure: "cdc.ddl_replay_journal_trigger_inventory",
    }
}

#[test]
fn replay_journal_target_connection_uses_configured_ca() {
    let target = target();
    let opts = target_opts(&target).expect("replay journal target options");
    assert_eq!(
        opts.get_ssl_opts().and_then(|ssl| ssl.root_cert_path()),
        Some(std::path::Path::new(&target.tls_ca_file))
    );
}

#[test]
fn validates_observable_journal_schema_contract() {
    assert!(validate_ddl_replay_journal_columns(&expected_ddl_replay_journal_columns()).is_ok());
    assert!(validate_ddl_replay_journal_keys(&expected_ddl_replay_journal_keys()).is_ok());
    assert!(
        validate_ddl_replay_journal_constraints(&expected_ddl_replay_journal_constraints()).is_ok()
    );
    assert_schema_drift_is_rejected();
}

fn assert_schema_drift_is_rejected() {
    let mut columns = expected_ddl_replay_journal_columns();
    columns[8].1 = "varchar(255)".to_string();
    assert!(validate_ddl_replay_journal_columns(&columns).is_err());
    let mut keys = expected_ddl_replay_journal_keys();
    keys.push(("unexpected_unique".to_string(), 0, 1, "raw_sql".to_string()));
    assert!(validate_ddl_replay_journal_keys(&keys).is_err());
}

#[test]
fn validates_status_check_contract() {
    let checks = vec!["status IN ('prepared','applied','checkpointed')".to_string()];
    assert!(validate_ddl_replay_journal_status_checks(&checks).is_err());
}

#[test]
fn validates_call_rows_and_admin_inventory_routine_evidence() {
    assert!(validate_journal_trigger_inventory("cdc", "ddl_replay_journal", &triggers()).is_ok());
    assert!(validate_inventory_routine_definition(
        "cdc",
        "ddl_replay_journal",
        "CREATE DEFINER=`root`@`%` PROCEDURE `cdc`.`ddl_replay_journal_trigger_inventory`() SQL SECURITY DEFINER READS SQL DATA BEGIN SELECT 1; END"
    ).is_ok());
    assert_trigger_and_routine_drift_is_rejected();
}

fn assert_trigger_and_routine_drift_is_rejected() {
    let mut malformed = triggers();
    malformed[0].5 = "BROKEN".to_string();
    assert!(validate_journal_trigger_inventory("cdc", "ddl_replay_journal", &malformed).is_err());
    let mut wrong = triggers();
    wrong[1].5 = "BEGIN SET NEW.status='blocked'; END".to_string();
    assert!(validate_journal_trigger_inventory("cdc", "ddl_replay_journal", &wrong).is_err());
    assert!(validate_inventory_routine_definition(
        "cdc",
        "ddl_replay_journal",
        "CREATE DEFINER=`root`@`%` PROCEDURE `cdc`.`ddl_replay_journal_trigger_inventory`() SQL SECURITY INVOKER BEGIN SELECT 1; END"
    ).is_err());
}

#[test]
fn validates_journal_runtime_contract_with_call_rows_and_exact_execute_only() {
    let columns = expected_ddl_replay_journal_columns();
    let keys = expected_ddl_replay_journal_keys();
    let constraints = expected_ddl_replay_journal_constraints();
    let checks = vec![
        "(status in ('translation_pending','prepared','applied','checkpointed','blocked'))"
            .to_string(),
    ];
    let trigger_rows = triggers();
    let grant_rows = grants();
    assert_runtime_contract(
        &columns,
        &keys,
        &constraints,
        &checks,
        &trigger_rows,
        &grant_rows,
    );
    assert_runtime_contract_rejects_blocked_transition(
        &columns,
        &keys,
        &constraints,
        &checks,
        &trigger_rows,
        &grant_rows,
    );
}

fn assert_runtime_contract(
    columns: &[JournalColumn],
    keys: &[JournalKey],
    constraints: &[JournalConstraint],
    checks: &[String],
    triggers: &[JournalTriggerMetadata],
    grants: &[String],
) {
    assert!(
        validate_journal_runtime_contract(runtime_contract(
            columns,
            keys,
            constraints,
            checks,
            triggers,
            grants,
        ))
        .is_ok()
    );
}

fn assert_runtime_contract_rejects_blocked_transition(
    columns: &[JournalColumn],
    keys: &[JournalKey],
    constraints: &[JournalConstraint],
    checks: &[String],
    triggers: &[JournalTriggerMetadata],
    grants: &[String],
) {
    let mut blocked = triggers.to_vec();
    blocked[1].5 = "BEGIN SET NEW.status='blocked'; END".to_string();
    assert!(
        validate_journal_runtime_contract(runtime_contract(
            columns,
            keys,
            constraints,
            checks,
            &blocked,
            grants,
        ))
        .is_err()
    );
}

#[test]
fn validates_required_runtime_grants_and_rejects_control_plane_bypass() {
    let grant_rows = grants();
    assert!(
        validate_runtime_grants(
            &grant_rows,
            "globalcomix",
            "cdc.stream_checkpoint",
            "cdc.ddl_replay_journal",
            "cdc.row_conflicts",
            "cdc.ddl_replay_journal_trigger_inventory"
        )
        .is_ok()
    );
    assert_missing_application_privilege_is_rejected(&grant_rows);
    assert_missing_conflict_update_is_rejected(&grant_rows);
    assert_bad_grants_are_rejected(&grant_rows);
}

fn assert_missing_conflict_update_is_rejected(grant_rows: &[String]) {
    let mut missing = grant_rows
        .iter()
        .filter(|grant| !grant.contains("`cdc`.`row_conflicts`"))
        .cloned()
        .collect::<Vec<_>>();
    missing.push("GRANT SELECT, INSERT ON `cdc`.`row_conflicts` TO `cdc_stream`@`%`".to_string());
    let error = validate_runtime_grants(
        &missing,
        "globalcomix",
        "cdc.stream_checkpoint",
        "cdc.ddl_replay_journal",
        "cdc.row_conflicts",
        "cdc.ddl_replay_journal_trigger_inventory",
    )
    .expect_err("missing row-conflict UPDATE must fail startup grant validation");
    assert!(error.contains("CDC.ROW_CONFLICTS"), "{error}");
    assert!(error.contains("UPDATE"), "{error}");
}

fn assert_missing_application_privilege_is_rejected(grant_rows: &[String]) {
    for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE", "EXECUTE"] {
        let mut missing = grant_rows.to_vec();
        missing[1] = missing[1].replace(&format!("{privilege}, "), "");
        missing[1] = missing[1].replace(&format!(", {privilege}"), "");
        let error = validate_runtime_grants(
            &missing,
            "globalcomix",
            "cdc.stream_checkpoint",
            "cdc.ddl_replay_journal",
            "cdc.row_conflicts",
            "cdc.ddl_replay_journal_trigger_inventory",
        )
        .expect_err("missing application privilege must fail startup grant validation");
        assert!(error.contains(privilege), "missing {privilege}: {error}");
    }
}

fn assert_bad_grants_are_rejected(grant_rows: &[String]) {
    for bad in [
        "GRANT UPDATE ON `cdc`.* TO `cdc_stream`@`%`",
        "GRANT SELECT ON `admin`.* TO `cdc_stream`@`%`",
        "GRANT ALL PRIVILEGES ON *.* TO `cdc_stream`@`%`",
        "GRANT PROXY ON `admin`@`%` TO `cdc_stream`@`%`",
        "GRANT `ddl_admin`@`%` TO `cdc_stream`@`%`",
        "GRANT SELECT ON `cdc`.`ddl_replay_journal` TO `cdc_stream`@`%` WITH GRANT OPTION",
        "GRANT DELETE ON `cdc`.`row_conflicts` TO `cdc_stream`@`%`",
        "GRANT EXECUTE ON `cdc`.* TO `cdc_stream`@`%`",
    ] {
        let mut drifted = grant_rows.to_vec();
        drifted.push(bad.to_string());
        assert!(
            validate_runtime_grants(
                &drifted,
                "globalcomix",
                "cdc.stream_checkpoint",
                "cdc.ddl_replay_journal",
                "cdc.row_conflicts",
                "cdc.ddl_replay_journal_trigger_inventory"
            )
            .is_err(),
            "accepted {bad}"
        );
    }
}

#[test]
fn journal_sql_uses_immutable_source_coordinate_and_monotonic_states() {
    let event = event();
    let prepare = build_prepare_sql("cdc.ddl_replay_journal", &event, &evidence());
    let select = build_status_select_sql("cdc.ddl_replay_journal", &event);
    let applied = build_transition_sql(
        "cdc.ddl_replay_journal",
        &event,
        DdlReplayStatus::Prepared,
        DdlReplayStatus::Applied,
    );
    let checkpointed = build_transition_sql(
        "cdc.ddl_replay_journal",
        &event,
        DdlReplayStatus::Applied,
        DdlReplayStatus::Checkpointed,
    );
    assert_prepare_sql(&prepare);
    assert_status_sql(&select, &applied, &checkpointed);
    for sql in [&prepare, &applied, &checkpointed] {
        assert!(!sql.contains("lease_name"), "lease residue in {sql}");
        assert!(!sql.contains("fence_token"), "fence residue in {sql}");
        assert!(
            !sql.contains("CONNECTION_ID()"),
            "connection fence residue in {sql}"
        );
        assert!(!sql.contains("IS_USED_LOCK"), "live-lock residue in {sql}");
    }
}

fn assert_prepare_sql(sql: &str) {
    for expected in [
        "INSERT INTO `cdc`.`ddl_replay_journal`",
        "'prod#server-id=3'",
        "'mysqld-bin.000777'",
        "100,200",
        "transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state",
        "'{\"family\":\"truncate\"}'",
        "'{\"row_count\":7}'",
        "'{\"row_count\":0}'",
        "'prepared'",
    ] {
        assert!(sql.contains(expected), "missing {expected}");
    }
    assert!(!sql.contains("ON DUPLICATE KEY UPDATE"));
}

fn assert_status_sql(select: &str, applied: &str, checkpointed: &str) {
    for expected in [
        "source_identity='prod#server-id=3'",
        "event_start_position=100",
        "source_server_id",
        "schema_name",
        "transformation_version,generated_sql,canonical_ast,pre_state,expected_post_state",
    ] {
        assert!(select.contains(expected), "missing {expected}");
    }
    for expected in [
        "source_server_id=3",
        "schema_name='globalcomix'",
        "status='applied'",
        "status='prepared'",
    ] {
        assert!(applied.contains(expected), "missing {expected}");
    }
    assert!(checkpointed.contains("status='checkpointed'"));
    assert!(checkpointed.contains("status='applied'"));
}

#[test]
fn absent_journal_row_prepares_before_execution() {
    assert_eq!(
        replay_action(&event(), None),
        Ok(DdlReplayAction::PrepareAndExecute)
    );
}

#[test]
fn prepared_journal_row_is_ambiguous_and_fails_closed() {
    let error = replay_action(&event(), Some(DdlReplayStatus::Prepared)).unwrap_err();
    assert!(error.contains("ambiguous automatic DDL"));
    assert!(error.contains("mysqld-bin.000777:100"));
}

#[test]
fn blocked_journal_row_fails_closed_without_replay() {
    let error = replay_action(&event(), Some(DdlReplayStatus::Blocked)).unwrap_err();
    assert!(error.contains("blocked automatic DDL"));
    assert!(error.contains("mysqld-bin.000777:100"));
}

#[test]
fn applied_journal_row_advances_checkpoint_without_reexecution() {
    assert_eq!(
        replay_action(&event(), Some(DdlReplayStatus::Applied)),
        Ok(DdlReplayAction::CheckpointOnly)
    );
}

#[test]
fn checkpointed_journal_row_never_reexecutes() {
    assert_eq!(
        replay_action(&event(), Some(DdlReplayStatus::Checkpointed)),
        Ok(DdlReplayAction::AlreadyCheckpointed)
    );
}

#[test]
fn every_automatic_ddl_family_requires_unique_post_state_proof() {
    for family in [
        DdlFamily::Table,
        DdlFamily::Index,
        DdlFamily::View,
        DdlFamily::Procedure,
        DdlFamily::Function,
        DdlFamily::Event,
        DdlFamily::Trigger,
        DdlFamily::Rename,
        DdlFamily::Truncate,
        DdlFamily::Drop,
    ] {
        assert_family_reconciliation(family);
    }
}

fn assert_family_reconciliation(family: DdlFamily) {
    let evidence = DdlSemanticEvidence {
        transformation_version: "mariadb-mysql8-v1".to_string(),
        generated_sql: Some("translated DDL".to_string()),
        canonical_ast: format!("{{\"family\":\"{}\"}}", family.as_str()),
        pre_state: "before".to_string(),
        expected_post_state: "after".to_string(),
    };
    assert_eq!(
        reconcile_prepared(&evidence, "after"),
        PreparedReconciliation::ProvenApplied
    );
    assert_eq!(
        reconcile_prepared(&evidence, "before"),
        PreparedReconciliation::Blocked
    );
    assert_eq!(
        reconcile_prepared(&evidence, "external-drift"),
        PreparedReconciliation::Blocked
    );
}

#[test]
fn identical_pre_and_post_state_is_ambiguous_and_blocks() {
    let mut same = evidence();
    same.pre_state = "empty-table".to_string();
    same.expected_post_state = "empty-table".to_string();
    assert_eq!(
        reconcile_prepared(&same, "empty-table"),
        PreparedReconciliation::Blocked
    );
}

#[test]
fn unresolved_entry_blocks_later_source_events() {
    let unresolved = JournalBarrier {
        binlog_file: "mysqld-bin.000777".to_string(),
        event_start_position: 100,
        status: DdlReplayStatus::Prepared,
    };
    assert!(enforce_no_overtake(Some(&unresolved), "mysqld-bin.000777", 200).is_err());
    assert!(enforce_no_overtake(None, "mysqld-bin.000777", 200).is_ok());
}

#[test]
fn startup_barrier_query_is_source_scoped_and_ordered() {
    let sql = build_barrier_select_sql("cdc.ddl_replay_journal", "prod%_source");
    assert!(sql.contains("status IN ('translation_pending','prepared','blocked')"));
    assert!(sql.contains("prod=%=_source#server-id=%"));
    assert!(sql.contains("ESCAPE '='"));
    assert!(sql.contains("ORDER BY binlog_file,event_start_position LIMIT 1"));
    assert!(!sql.contains("stream_recovery_records"));
}
