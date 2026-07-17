use super::*;

#[test]
fn count_sql_quotes_table_identifier() {
    assert_eq!(
        build_count_sql("accounts"),
        "SELECT COUNT(*) FROM `accounts`"
    );
    assert_eq!(
        build_count_sql("weird`table"),
        "SELECT COUNT(*) FROM `weird``table`"
    );
}

#[test]
fn list_tables_sql_is_read_only_and_bounded_to_current_database() {
    assert_eq!(
        build_list_tables_sql(),
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
    );
}

#[test]
fn primary_key_endpoint_sql_rejects_bad_bound_arity() {
    let error = build_primary_key_endpoints_sql(
        "accounts",
        &["tenant_id".to_string(), "id".to_string()],
        Some(vec!["10".to_string()]),
        100,
    )
    .expect_err("bad arity");

    assert_eq!(
        error.to_string(),
        "start_after has 1 values for 2 primary-key columns"
    );
}

#[test]
fn marks_mariadb_json_alias_columns_from_json_valid_checks() {
    let mut columns = vec![ChecksumColumn {
        name: "payload".to_string(),
        data_type: "longtext".to_string(),
        column_type: "longtext".to_string(),
    }];

    mark_json_alias_columns(&mut columns, &["json_valid(`payload`)".to_string()]);

    assert_eq!(columns[0].data_type, "json");
}

#[test]
fn partitions_unsupported_columns_out_of_checksum_set() {
    let columns = vec![
        checksum_column("id", "bigint"),
        checksum_column("score", "float"),
        checksum_column("payload", "json"),
        checksum_column("name", "varchar"),
    ];

    let (supported, skipped) = partition_checksum_columns(columns);

    assert_eq!(
        supported
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"]
    );
    assert_eq!(skipped, vec!["score".to_string(), "payload".to_string()]);
}

#[test]
fn formats_skipped_columns_and_skip_reason_in_content_summary() {
    assert_eq!(
        format_content_summary(Some(&ContentDriftSummary {
            chunks: 2,
            skipped_columns: vec!["score".to_string(), "payload".to_string()],
            ..ContentDriftSummary::default()
        })),
        " content_chunks=2 content_mismatches=0 content_ranges=0 content_range_limit_exceeded=false content_skipped_columns=score,payload"
    );
    assert_eq!(
        format_content_summary(Some(&ContentDriftSummary {
            skipped_reason: Some("no primary key".to_string()),
            ..ContentDriftSummary::default()
        })),
        " content_skipped=no_primary_key"
    );
}

fn checksum_column(name: &str, data_type: &str) -> ChecksumColumn {
    ChecksumColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        column_type: data_type.to_string(),
    }
}

#[test]
fn formats_drift_report_with_match_and_mismatch_status() {
    let report = DriftCheckReport {
        comparisons: vec![
            DriftComparison {
                table: "accounts".to_string(),
                source_count: Some(10),
                target_count: Some(10),
                content: None,
            },
            DriftComparison {
                table: "releases".to_string(),
                source_count: Some(7),
                target_count: Some(5),
                content: None,
            },
        ],
    };

    assert!(report.has_mismatches());
    assert!(!report.is_clean());
    assert_eq!(
        format_drift_report(&report),
        [
            "drift_check tables=2 mismatches=1",
            "drift_check_table table=accounts source_count=10 target_count=10 delta=0 status=ok",
            "drift_check_table table=releases source_count=7 target_count=5 delta=-2 status=drift",
        ]
        .join("\n")
    );
}

#[test]
fn formats_content_drift_as_mismatch_even_when_counts_match() {
    let report = DriftCheckReport {
        comparisons: vec![DriftComparison {
            table: "accounts".to_string(),
            source_count: Some(10),
            target_count: Some(10),
            content: Some(ContentDriftSummary {
                chunks: 3,
                mismatched_chunks: 1,
                mismatched_ranges: vec![ContentDriftRange {
                    start_after: Some(vec!["10,tenant".to_string()]),
                    end_at: Some(vec!["11".to_string()]),
                    source_count: 1,
                    target_count: 1,
                }],
                range_limit_exceeded: false,
                ..ContentDriftSummary::default()
            }),
        }],
    };

    assert!(report.has_mismatches());
    assert_eq!(
        format_drift_report(&report),
        [
            "drift_check tables=1 mismatches=1",
            "drift_check_table table=accounts source_count=10 target_count=10 delta=0 status=drift content_chunks=3 content_mismatches=1 content_ranges=1 content_range_limit_exceeded=false",
            "drift_check_range table=accounts start_after_json=[\"10,tenant\"] end_at_json=[\"11\"] source_count=1 target_count=1",
        ]
        .join("\n")
    );
}

#[test]
fn clean_report_has_no_mismatches() {
    let report = DriftCheckReport {
        comparisons: vec![DriftComparison {
            table: "accounts".to_string(),
            source_count: Some(10),
            target_count: Some(10),
            content: None,
        }],
    };

    assert!(!report.has_mismatches());
    assert!(report.is_clean());
}

#[test]
fn only_1146_42s02_errors_are_treated_as_missing_tables() {
    assert!(is_missing_table_error(
        "ERROR 1146 (42S02): Table 'db.accounts' doesn't exist"
    ));
    assert!(!is_missing_table_error(
        "ERROR 1146 (HY000): Table metadata lock failed"
    ));
    assert!(!is_missing_table_error(
        "ERROR 1051 (42S02): Unknown table 'db.accounts'"
    ));
}

#[test]
fn drift_check_uses_endpoint_specific_tls_ca_paths() {
    assert_eq!(
        source_query_config(&MySqlConnectionConfig::default()).tls_ca_file,
        SOURCE_TLS_CA_FILE
    );
    let custom_source = MySqlConnectionConfig {
        tls_ca_file: Some("/tmp/custom-source-ca.pem".to_string()),
        ..MySqlConnectionConfig::default()
    };
    assert_eq!(
        source_query_config(&custom_source).tls_ca_file,
        "/tmp/custom-source-ca.pem"
    );
    let default_target = TargetMySqlConfig {
        tls_ca_file: "/tmp/custom-target-ca.pem".to_string(),
        ..TargetMySqlConfig::default()
    };
    assert_eq!(
        target_query_config(&default_target).tls_ca_file,
        "/tmp/custom-target-ca.pem"
    );

    let source_ca = temporary_ca_path("source");
    let target_ca = temporary_ca_path("target");
    let ca_fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/test-ca.pem");
    std::fs::copy(ca_fixture, &source_ca).expect("write source CA fixture");
    std::fs::copy(ca_fixture, &target_ca).expect("write target CA fixture");

    let source = QueryConnectionConfig {
        host: "source".to_string(),
        port: 3306,
        user: "source-user".to_string(),
        password: "source-password".to_string(),
        database: "source-db".to_string(),
        tls_ca_file: source_ca.to_string_lossy().into_owned(),
        endpoint_role: "source",
    };
    let target = QueryConnectionConfig {
        host: "target".to_string(),
        port: 3306,
        user: "target-user".to_string(),
        password: "target-password".to_string(),
        database: "target-db".to_string(),
        tls_ca_file: target_ca.to_string_lossy().into_owned(),
        endpoint_role: "target",
    };

    let source_opts = connection_opts(&source).expect("source TLS options");
    let target_opts = connection_opts(&target).expect("target TLS options");
    let source_root = source_opts
        .get_ssl_opts()
        .and_then(|ssl| ssl.root_cert_path());
    let target_root = target_opts
        .get_ssl_opts()
        .and_then(|ssl| ssl.root_cert_path());

    assert_eq!(source_root, Some(source_ca.as_path()));
    assert_eq!(target_root, Some(target_ca.as_path()));
    assert_ne!(source_root, target_root);

    let _ = std::fs::remove_file(source_ca);
    let _ = std::fs::remove_file(target_ca);
}

fn temporary_ca_path(endpoint: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "mariadb-mysql-cdc-drift-check-{endpoint}-{}-ca.pem",
        std::process::id()
    ))
}
