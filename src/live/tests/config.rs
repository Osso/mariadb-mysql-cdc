use super::*;

#[test]
fn stream_requires_schema_qualified_checkpoint_table() {
    let mut config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            host: "source-db".to_string(),
            user: "cdc_reader".to_string(),
            password: "secret".to_string(),
            database: Some("globalcomix".to_string()),
            binlog_file: "mysqld-bin.000001".to_string(),
            ..SourceBinlogConfig::default()
        },
        source_identity: "production-source".to_string(),
        target: TargetMySqlConfig {
            host: "target-db".to_string(),
            user: "cdc_stream".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            ..TargetMySqlConfig::default()
        },
        ..ApplyBinlogConfig::default()
    };

    config.checkpoint_table = "stream_checkpoint".to_string();
    assert!(config.validate().is_err());

    for malformed in [".stream_checkpoint", "cdc.", "cdc.stream.checkpoint"] {
        config.checkpoint_table = malformed.to_string();
        assert!(
            config.validate().is_err(),
            "accepted malformed table {malformed}"
        );
    }
}

#[test]
fn stream_accepts_plaintext_source_without_tls_ca() {
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            host: "source-db".to_string(),
            user: "cdc_reader".to_string(),
            password: "secret".to_string(),
            database: Some("globalcomix".to_string()),
            tls_ca_file: String::new(),
            binlog_file: "mysqld-bin.000001".to_string(),
            ..SourceBinlogConfig::default()
        },
        source_identity: "production-source".to_string(),
        target: TargetMySqlConfig {
            host: "target-db".to_string(),
            user: "cdc_stream".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            ..TargetMySqlConfig::default()
        },
        ..ApplyBinlogConfig::default()
    };

    config
        .validate()
        .expect("plaintext source without CA should be accepted");
}

#[test]
fn stream_target_still_requires_tls_ca() {
    let config = ApplyBinlogConfig {
        source: SourceBinlogConfig {
            host: "source-db".to_string(),
            user: "cdc_reader".to_string(),
            password: "secret".to_string(),
            database: Some("globalcomix".to_string()),
            binlog_file: "mysqld-bin.000001".to_string(),
            ..SourceBinlogConfig::default()
        },
        source_identity: "production-source".to_string(),
        target: TargetMySqlConfig {
            host: "target-db".to_string(),
            user: "cdc_stream".to_string(),
            password: "secret".to_string(),
            database: "globalcomix".to_string(),
            tls_ca_file: String::new(),
            ..TargetMySqlConfig::default()
        },
        ..ApplyBinlogConfig::default()
    };

    let error = config.validate().expect_err("target CA remains mandatory");

    assert_eq!(error.to_string(), "target TLS CA file is required");
}

#[cfg(feature = "integration-failpoints")]
#[test]
fn integration_failpoint_parser_accepts_only_named_recovery_boundaries() {
    assert_eq!(
        IntegrationFailpoint::parse("prepare-failure"),
        Ok(IntegrationFailpoint::PrepareFailure)
    );
    assert_eq!(
        IntegrationFailpoint::parse("post-ddl-pre-applied"),
        Ok(IntegrationFailpoint::PostDdlPreApplied)
    );
    assert_eq!(
        IntegrationFailpoint::parse("applied-pre-checkpoint"),
        Ok(IntegrationFailpoint::AppliedPreCheckpoint)
    );
    assert_eq!(
        IntegrationFailpoint::parse("checkpoint-transaction"),
        Ok(IntegrationFailpoint::CheckpointTransaction)
    );
    assert!(IntegrationFailpoint::parse("anything-from-env").is_err());
}
