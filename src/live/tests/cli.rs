use super::*;

#[test]
fn stop_never_args_keep_binlog_file_last() {
    let source = SourceBinlogConfig {
        host: "10.0.0.1".to_string(),
        user: "cdc".to_string(),
        password: "secret".to_string(),
        database: Some("test".to_string()),
        binlog_file: "mysqld-bin.000001".to_string(),
        start_position: 4,
        ..SourceBinlogConfig::default()
    };

    let args = binlog_command::stop_never_args(&source);

    assert!(args.contains(&"--stop-never".to_string()));
    assert_eq!(args.last(), Some(&"mysqld-bin.000001".to_string()));
}

#[test]
fn target_session_init_removes_ansi_quotes() {
    assert_eq!(
        target_session_init_command(),
        "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"
    );
    assert!(!target_session_init_command().contains("ANSI_QUOTES"));
}

#[test]
fn target_client_uses_utf8mb4_connection_charset() {
    assert_eq!(
        target_client_character_set_arg(),
        "--default-character-set=utf8mb4"
    );
}

#[test]
fn slow_target_query_log_includes_bounded_sql_preview() {
    let statement = SqlStatement {
        sql: "INSERT INTO events VALUES ('alpha')".repeat(200),
        params: Vec::new(),
    };
    let started_at = Instant::now() - Duration::from_secs(21);

    let log_line = format_slow_target_query_log(&statement, started_at);

    assert!(log_line.starts_with("cdc_target_slow_query elapsed_seconds="));
    assert!(log_line.contains(&format!("sql_bytes={}", statement.sql.len())));
    assert!(log_line.contains("sql_truncated=true"));
    assert!(log_line.contains("INSERT INTO events VALUES"));
    assert!(log_line.len() < statement.sql.len());
}

#[test]
fn truncate_sql_for_log_keeps_utf8_boundary() {
    let sql = "éééSELECT";

    assert_eq!(truncate_sql_for_log(sql, 3), "ééé");
}
