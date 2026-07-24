use super::config::{
    default_repair_drift_config, parse_repair_drift_config, repair_drift_option,
    validate_repair_drift_config,
};
use super::plan::{
    build_runtime_repair_plan, exclude_progress_table, order_table_names,
    reduce_to_dependency_closure,
};
use super::run::{
    build_drift_check_config, can_resolve_verified_conflicts,
    can_resolve_verified_conflicts_after_verify, fresh_run_id, verified_conflict_evidence,
};
use super::*;
use crate::drift_check::{ContentDriftSummary, DriftComparison};
use crate::mysql_support::target_mysql_opts;
use crate::table_sync::SyncPhase;

#[test]
fn fk_aware_plan_is_available_without_lexical_guessing() {
    let inventory = RepairInventory {
        schema: "app".to_string(),
        tables: vec!["children".to_string(), "parents".to_string()],
        foreign_keys: vec![CanonicalForeignKey {
            constraint_schema: "app".to_string(),
            constraint_name: "children_parent_fk".to_string(),
            child_schema: "app".to_string(),
            child_table: "children".to_string(),
            child_columns: vec!["parent_id".to_string()],
            parent_schema: "app".to_string(),
            parent_table: "parents".to_string(),
            parent_columns: vec!["id".to_string()],
            update_rule: "RESTRICT".to_string(),
            delete_rule: "RESTRICT".to_string(),
            match_option: "NONE".to_string(),
            enforced: true,
        }],
    };
    let plan = build_fk_aware_repair_plan("run", "source", "target", &inventory, &inventory)
        .expect("fk-aware plan");
    assert_eq!(plan.insert_order, vec!["parents", "children"]);
    assert_eq!(plan.delete_order, vec!["children", "parents"]);
}

#[test]
fn orders_explicit_parent_first_tables_then_remaining_lexically() {
    let all = ["children", "accounts", "releases", "authors"]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let parents = ["accounts", "authors"]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(
        order_table_names(&all, &parents).expect("order"),
        vec!["accounts", "authors", "children", "releases"]
    );
}

#[test]
fn rejects_parent_first_table_missing_from_inventory() {
    let error = order_table_names(&["accounts".to_string()], &["missing".to_string()])
        .expect_err("missing parent");
    assert_eq!(
        error,
        "parent-first table `missing` is not in the repair inventory"
    );
}

#[test]
fn selects_count_or_content_drifted_tables() {
    let comparisons = vec![
        comparison("accounts", 10, 10, None),
        comparison("children", 10, 9, None),
        comparison(
            "releases",
            10,
            10,
            Some(ContentDriftSummary {
                mismatched_chunks: 1,
                ..Default::default()
            }),
        ),
        DriftComparison {
            table: "missing".to_string(),
            source_count: Some(10),
            target_count: None,
            content: None,
        },
    ];
    assert_eq!(
        drifted_table_names(&comparisons),
        vec!["children", "releases", "missing"]
    );
}

fn comparison(
    table: &str,
    source_count: u64,
    target_count: u64,
    content: Option<ContentDriftSummary>,
) -> DriftComparison {
    DriftComparison {
        table: table.to_string(),
        source_count: Some(source_count),
        target_count: Some(target_count),
        content,
    }
}

#[test]
fn passes_content_check_to_drift_check_config() {
    let mut config = default_repair_drift_config();
    config.content_check = false;
    let drift_config = build_drift_check_config(&config, vec!["accounts".to_string()]);
    assert!(!drift_config.content_check);
    assert_eq!(drift_config.tables, vec!["accounts"]);
}

fn valid_config() -> RepairDriftConfig {
    let mut config = default_repair_drift_config();
    config.source.host = "source".to_string();
    config.source.user = "user".to_string();
    config.source.password = "password".to_string();
    config.source.database = "database".to_string();
    config.target.host = "target".to_string();
    config.target.user = "user".to_string();
    config.target.password = "password".to_string();
    config.target.database = "database".to_string();
    config
}

fn empty_schema_inventory() -> crate::inventory::SchemaInventory {
    crate::inventory::SchemaInventory {
        schema: "database".to_string(),
        tables: Vec::new(),
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        views: Vec::new(),
        triggers: Vec::new(),
        routines: Vec::new(),
        events: Vec::new(),
    }
}

fn canonical_fk(child_table: &str, parent_table: &str) -> CanonicalForeignKey {
    CanonicalForeignKey {
        constraint_schema: "globalcomix".to_string(),
        constraint_name: format!("{child_table}_{parent_table}_fk"),
        child_schema: "globalcomix".to_string(),
        child_table: child_table.to_string(),
        child_columns: vec!["parent_id".to_string()],
        parent_schema: "globalcomix".to_string(),
        parent_table: parent_table.to_string(),
        parent_columns: vec!["id".to_string()],
        update_rule: "RESTRICT".to_string(),
        delete_rule: "RESTRICT".to_string(),
        match_option: "NONE".to_string(),
        enforced: true,
    }
}

#[test]
fn dependency_closure_ignores_unrelated_cycle() {
    let inventory = RepairInventory {
        schema: "globalcomix".to_string(),
        tables: vec![
            "guests".to_string(),
            "unrelated_a".to_string(),
            "unrelated_b".to_string(),
        ],
        foreign_keys: vec![
            canonical_fk("unrelated_a", "unrelated_b"),
            canonical_fk("unrelated_b", "unrelated_a"),
        ],
    };

    let reduced = reduce_to_dependency_closure(inventory, vec!["guests".to_string()]);

    assert_eq!(reduced.tables, vec!["guests"]);
    assert!(reduced.foreign_keys.is_empty());
}

#[test]
fn selected_orders_do_not_pull_sibling_cycle_into_repair_plan() {
    let inventory = RepairInventory {
        schema: "globalcomix".to_string(),
        tables: vec![
            "customers".to_string(),
            "orders".to_string(),
            "invoices".to_string(),
            "ledger".to_string(),
        ],
        foreign_keys: vec![
            canonical_fk("orders", "customers"),
            canonical_fk("invoices", "customers"),
            canonical_fk("invoices", "ledger"),
            canonical_fk("ledger", "invoices"),
        ],
    };

    let reduced = reduce_to_dependency_closure(inventory, vec!["orders".to_string()]);

    assert_eq!(reduced.tables, vec!["customers", "orders"]);
    let plan =
        build_fk_aware_repair_plan("selected-orders", "source", "target", &reduced, &reduced)
            .expect("selected orders plan must not be blocked by sibling cycle");
    assert_eq!(plan.insert_order, vec!["customers", "orders"]);
    assert_eq!(plan.delete_order, vec!["orders", "customers"]);
}

#[test]
fn dependency_closure_preserves_ancestors_children_and_selected_cycles() {
    let inventory = RepairInventory {
        schema: "globalcomix".to_string(),
        tables: vec![
            "guest_parents".to_string(),
            "guests".to_string(),
            "sessions".to_string(),
            "cycle_peer".to_string(),
        ],
        foreign_keys: vec![
            canonical_fk("guests", "guest_parents"),
            canonical_fk("sessions", "guests"),
            canonical_fk("guests", "cycle_peer"),
            canonical_fk("cycle_peer", "guests"),
        ],
    };

    let reduced = reduce_to_dependency_closure(inventory, vec!["guests".to_string()]);

    assert_eq!(
        reduced.tables,
        vec![
            "guest_parents".to_string(),
            "guests".to_string(),
            "sessions".to_string(),
            "cycle_peer".to_string(),
        ]
    );
    assert_eq!(reduced.foreign_keys.len(), 4);
    let error =
        build_fk_aware_repair_plan("selected-cycle", "source", "target", &reduced, &reduced)
            .expect_err("selected dependency cycle must remain blocked");
    assert!(matches!(error, RepairPlanError::Cycle(_)));
}

#[test]
fn bounded_windows_defer_table_wide_conflict_resolution() {
    let mut config = default_repair_drift_config();
    assert!(can_resolve_verified_conflicts(&config));
    config.start_after = Some(vec!["1".to_string()]);
    assert!(!can_resolve_verified_conflicts(&config));
}

#[test]
fn verified_conflicts_require_zero_difference_report() {
    let config = default_repair_drift_config();
    let equal = table_sync::SyncTableReport::default();
    assert!(can_resolve_verified_conflicts_after_verify(
        &config,
        SyncPhase::Verify,
        &equal
    ));

    for report in [
        table_sync::SyncTableReport {
            inserts: 1,
            ..Default::default()
        },
        table_sync::SyncTableReport {
            updates: 1,
            ..Default::default()
        },
        table_sync::SyncTableReport {
            extra_target_rows: 1,
            ..Default::default()
        },
    ] {
        assert!(!can_resolve_verified_conflicts_after_verify(
            &config,
            SyncPhase::Verify,
            &report
        ));
    }
}

#[test]
fn verified_conflict_evidence_names_full_table_scope() {
    assert_eq!(
        verified_conflict_evidence("accounts"),
        "verified source/target equality for table `accounts` across full-table scope"
    );
}

#[test]
fn excludes_configured_progress_table_from_target_repair_inventory() {
    let mut inventory = RepairInventory {
        schema: "globalcomix".to_string(),
        tables: vec!["accounts".to_string(), "table_sync_runs".to_string()],
        foreign_keys: Vec::new(),
    };
    exclude_progress_table(&mut inventory, "globalcomix.table_sync_runs");
    assert_eq!(inventory.tables, vec!["accounts"]);
}

#[test]
fn parses_selected_primary_key_window_for_bounded_repair() {
    let mut config = default_repair_drift_config();
    repair_drift_option(&mut config, "--start-after", "10").expect("start bound");
    repair_drift_option(&mut config, "--end-at", "20").expect("end bound");
    assert_eq!(config.start_after, Some(vec!["10".to_string()]));
    assert_eq!(config.end_at, Some(vec!["20".to_string()]));
}

#[test]
fn repair_source_inventory_uses_plaintext_without_tls_ca() {
    let mut config = valid_config();
    config.source.host = "127.0.0.1".to_string();
    config.source.port = 1;

    let inventory = empty_schema_inventory();
    let error = build_runtime_repair_plan(&config, "repair-run", &inventory, &inventory)
        .expect_err("source inventory connection should fail");
    let message = error.to_string();

    assert!(message.contains("repair drift inventory failed"));
    assert!(!message.contains("TLS CA file"));
}

#[test]
fn repair_target_still_requires_tls_ca() {
    let mut config = valid_config();
    config.target.tls_ca_file = String::new();

    let error = validate_repair_drift_config(&config).expect_err("missing target TLS CA");

    assert_eq!(error, "target TLS CA file is required");
}

#[test]
fn repair_target_dns_keeps_hostname_verification() {
    let mut target = valid_config().target;
    target.tls_ca_file = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem").to_string();

    let opts = target_mysql_opts(&target).expect("target TLS opts");
    let ssl = opts.get_ssl_opts().expect("target TLS configured");

    assert!(!ssl.skip_domain_validation());
    assert!(!ssl.accept_invalid_certs());
}

#[test]
fn parses_repeated_tables_parent_first_prefix_and_content_check() {
    let mut config = default_repair_drift_config();
    repair_drift_option(&mut config, "--table", "children").expect("table");
    repair_drift_option(&mut config, "--table", "accounts").expect("table");
    repair_drift_option(&mut config, "--parent-first", "accounts,authors").expect("order");
    repair_drift_option(&mut config, "--content-check", "false").expect("content check");
    assert_eq!(config.tables, vec!["children", "accounts"]);
    assert_eq!(config.parent_first, vec!["accounts", "authors"]);
    assert!(!config.content_check);
}

#[test]
fn rejects_source_tls_ca_file_option() {
    let args = [
        "--source-tls-ca-file",
        "/tmp/source-ca.pem",
        "--source-host",
        "source-db",
        "--source-user",
        "reader",
        "--source-password-env",
        "MISSING_SOURCE_PASSWORD",
        "--source-database",
        "globalcomix",
        "--source-identity",
        "source-identity",
        "--target-host",
        "target-db",
        "--target-user",
        "writer",
        "--target-password-env",
        "MISSING_TARGET_PASSWORD",
        "--target-database",
        "globalcomix",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    let error = parse_repair_drift_config(args).expect_err("source CA option");

    assert_eq!(error, "unknown repair-drift option: --source-tls-ca-file");
}

#[test]
fn dispatches_source_options() {
    let mut config = default_repair_drift_config();
    repair_drift_option(&mut config, "--source-host", "source.example").expect("host");
    repair_drift_option(&mut config, "--source-port", "3310").expect("port");
    repair_drift_option(&mut config, "--source-user", "source-user").expect("user");
    repair_drift_option(&mut config, "--source-database", "source-db").expect("database");
    repair_drift_option(&mut config, "--source-identity", "source-identity").expect("identity");
    assert_eq!(config.source.host, "source.example");
    assert_eq!(config.source.port, 3310);
    assert_eq!(config.source.user, "source-user");
    assert_eq!(config.source.database, "source-db");
    assert_eq!(config.source_identity, "source-identity");
}

#[test]
fn dispatches_target_options() {
    let mut config = default_repair_drift_config();
    repair_drift_option(&mut config, "--target-host", "target.example").expect("host");
    repair_drift_option(&mut config, "--target-port", "3311").expect("port");
    repair_drift_option(&mut config, "--target-user", "target-user").expect("user");
    repair_drift_option(&mut config, "--target-database", "target-db").expect("database");
    repair_drift_option(&mut config, "--target-tls-ca-file", "/tmp/target-ca.pem").expect("TLS CA");
    assert_eq!(config.target.host, "target.example");
    assert_eq!(config.target.port, 3311);
    assert_eq!(config.target.user, "target-user");
    assert_eq!(config.target.database, "target-db");
    assert_eq!(config.target.tls_ca_file, "/tmp/target-ca.pem");
}

#[test]
fn dispatches_run_options() {
    let mut config = default_repair_drift_config();
    repair_drift_option(&mut config, "--run-id", "repair-123").expect("run ID");
    repair_drift_option(&mut config, "--run-id-prefix", "manual-repair").expect("prefix");
    assert_eq!(config.run_id, Some("repair-123".to_string()));
    assert_eq!(config.run_id_prefix, "manual-repair");
}

#[test]
fn preserves_unknown_and_numeric_option_errors() {
    let mut config = default_repair_drift_config();
    assert_eq!(
        repair_drift_option(&mut config, "--unknown", "value").expect_err("unknown option"),
        "unknown repair-drift option: --unknown"
    );
    assert_eq!(
        repair_drift_option(&mut config, "--source-port", "not-a-port")
            .expect_err("invalid source port"),
        "--source-port must be an integer"
    );
}

#[test]
fn preserves_environment_and_window_option_errors() {
    let mut config = default_repair_drift_config();
    assert_eq!(
        repair_drift_option(
            &mut config,
            "--source-password-env",
            "MISSING_SOURCE_PASSWORD"
        )
        .expect_err("missing source password"),
        "MISSING_SOURCE_PASSWORD is not set"
    );
    assert_eq!(
        repair_drift_option(
            &mut config,
            "--target-password-env",
            "MISSING_TARGET_PASSWORD"
        )
        .expect_err("missing target password"),
        "MISSING_TARGET_PASSWORD is not set"
    );
    assert_eq!(
        repair_drift_option(&mut config, "--start-after-json", "[]")
            .expect_err("empty start bound"),
        "--start-after-json must contain at least one primary-key value"
    );
}

#[test]
fn fresh_run_ids_are_unique_within_one_process() {
    let first = fresh_run_id("repair-drift");
    let second = fresh_run_id("repair-drift");
    assert_ne!(first, second);
    assert!(first.starts_with("repair-drift-"));
    assert!(second.starts_with("repair-drift-"));
}
