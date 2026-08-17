use crate::snapshot::SnapshotRow;
use crate::sync::{
    SyncChunkReadRequest, SyncPrimaryKeyOrdering, SyncProgressRow, SyncProgressStatus, SyncStage,
    SyncTable, build_create_sync_progress_schema_sql, build_create_sync_progress_table_sql,
    build_lock_table_write_sql, build_strict_delete_rows_statement,
    build_strict_delete_statement, build_strict_insert_statement,
    build_strict_update_rows_statement, build_strict_update_statement,
    build_sync_progress_select_sql,
    build_sync_progress_upsert_sql, build_sync_select_sql, parse_sync_progress_row,
};
use mysql::Value;
use std::collections::BTreeMap;

#[test]
fn sync_mysql_contract_selects_keyset_windows_in_source_primary_key_order() {
    let table = ordered_table();
    let request = SyncChunkReadRequest {
        start_after: Some(strings(["7", "draft"])),
        end_at: Some(strings(["10", "live"])),
        limit: 250,
    };

    assert_eq!(
        table.primary_key_ordering,
        vec![
            SyncPrimaryKeyOrdering::Native,
            SyncPrimaryKeyOrdering::Enum(strings(["draft", "live", "archived"])),
        ]
    );
    assert_eq!(
        build_sync_select_sql(&table, &request),
        "SELECT `series_id`, `state`, `title` FROM `episodes``current` WHERE ((`series_id` > '7') OR (`series_id` = '7' AND FIELD(`state`, 'draft', 'live', 'archived') > FIELD('draft', 'draft', 'live', 'archived'))) AND NOT ((`series_id` > '10') OR (`series_id` = '10' AND FIELD(`state`, 'draft', 'live', 'archived') > FIELD('live', 'draft', 'live', 'archived'))) ORDER BY `series_id`, FIELD(`state`, 'draft', 'live', 'archived') LIMIT 250"
    );
}

#[test]
fn sync_mysql_contract_builds_only_the_quoted_target_write_lock() {
    let sql = build_lock_table_write_sql("target`db", "episodes`current");

    assert_eq!(
        sql,
        "LOCK TABLES `target``db`.`episodes``current` WRITE"
    );
    assert!(!sql.to_ascii_uppercase().contains("START TRANSACTION"));
}

#[test]
fn sync_mysql_contract_builds_strict_bound_row_mutations() {
    let table = mutation_table();
    let row = row("7", "live", "Now");

    let insert = build_strict_insert_statement(&table, std::slice::from_ref(&row));
    assert_eq!(
        insert.sql,
        "INSERT INTO `episodes` (`id`, `status`, `title`) VALUES (?, ?, ?)"
    );
    assert_eq!(
        insert.params,
        vec![bytes("7"), bytes("live"), bytes("Now")]
    );

    let update = build_strict_update_statement(&table, &row);
    assert_eq!(
        update.sql,
        "UPDATE `episodes` SET `status` = ?, `title` = ? WHERE `id` = ?"
    );
    assert_eq!(
        update.params,
        vec![bytes("live"), bytes("Now"), bytes("7")]
    );

    let delete = build_strict_delete_statement(&table, &row.primary_key);
    assert_eq!(delete.sql, "DELETE FROM `episodes` WHERE `id` = ?");
    assert_eq!(delete.params, vec![bytes("7")]);

    let mutation_sql = [insert.sql, update.sql, delete.sql]
        .join("\n")
        .to_ascii_uppercase();
    for forbidden in ["INSERT IGNORE", "ON DUPLICATE KEY UPDATE", "REPLACE"] {
        assert!(
            !mutation_sql.contains(forbidden),
            "strict mutation SQL contained `{forbidden}`:\n{mutation_sql}"
        );
    }
}

#[test]
fn sync_mysql_contract_builds_bounded_batched_strict_updates_and_deletes() {
    let table = mutation_table();
    let rows = [row("7", "live", "Now"), row("8", "archived", "Later")];

    let update = build_strict_update_rows_statement(&table, &rows);
    assert_eq!(
        update.sql,
        "UPDATE `episodes` SET `status` = CASE WHEN `id` = ? THEN ? WHEN `id` = ? THEN ? ELSE `status` END, `title` = CASE WHEN `id` = ? THEN ? WHEN `id` = ? THEN ? ELSE `title` END WHERE `id` IN (?, ?) ORDER BY `id`"
    );
    assert_eq!(
        update.params,
        vec![
            bytes("7"),
            bytes("live"),
            bytes("8"),
            bytes("archived"),
            bytes("7"),
            bytes("Now"),
            bytes("8"),
            bytes("Later"),
            bytes("7"),
            bytes("8"),
        ]
    );

    let delete_table = composite_delete_table();
    let delete = build_strict_delete_rows_statement(
        &delete_table,
        &[strings(["7", "2"]), strings(["8", "1"])],
    );
    assert_eq!(
        delete.sql,
        "DELETE FROM `episode_revisions` WHERE (`series_id`, `revision`) IN ((?, ?), (?, ?))"
    );
    assert_eq!(
        delete.params,
        vec![bytes("7"), bytes("2"), bytes("8"), bytes("1")]
    );

    let mutation_sql = [update.sql, delete.sql].join("\n").to_ascii_uppercase();
    for forbidden in ["INSERT IGNORE", "ON DUPLICATE KEY UPDATE", "REPLACE"] {
        assert!(
            !mutation_sql.contains(forbidden),
            "strict batched mutation SQL contained `{forbidden}`:\n{mutation_sql}"
        );
    }
}

#[test]
fn sync_mysql_contract_defines_one_staged_run_progress_table() {
    assert_eq!(
        [
            SyncStage::PrerequisiteSchema.as_str(),
            SyncStage::Rows.as_str(),
            SyncStage::FinalConstraints.as_str(),
        ],
        ["prerequisite_schema", "rows", "final_constraints"]
    );
    assert_eq!(
        build_create_sync_progress_schema_sql("cdc.sync_runs"),
        Some("CREATE DATABASE IF NOT EXISTS `cdc`".to_string())
    );
    assert_eq!(
        build_create_sync_progress_table_sql("cdc.sync_runs"),
        "CREATE TABLE IF NOT EXISTS `cdc`.`sync_runs` (run_id VARCHAR(128) NOT NULL, stage VARCHAR(32) NOT NULL, table_name VARCHAR(255) NOT NULL, run_spec_json LONGTEXT NOT NULL, last_primary_key_json TEXT NULL, chunks BIGINT UNSIGNED NOT NULL DEFAULT 0, rows_scanned BIGINT UNSIGNED NOT NULL DEFAULT 0, inserts_applied BIGINT UNSIGNED NOT NULL DEFAULT 0, updates_applied BIGINT UNSIGNED NOT NULL DEFAULT 0, deletes_applied BIGINT UNSIGNED NOT NULL DEFAULT 0, status VARCHAR(16) NOT NULL, last_error TEXT NULL, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), completed_at TIMESTAMP(6) NULL, CHECK (stage IN ('prerequisite_schema', 'rows', 'final_constraints')), CHECK (status IN ('running', 'complete', 'error')), CHECK (JSON_VALID(run_spec_json)), CHECK (last_primary_key_json IS NULL OR JSON_VALID(last_primary_key_json)), PRIMARY KEY (run_id, stage, table_name)) ENGINE=InnoDB"
    );
}

#[test]
fn sync_mysql_contract_selects_and_upserts_exact_progress_identity() {
    let progress = progress_row();

    assert_eq!(
        build_sync_progress_select_sql(
            "cdc.sync_runs",
            "sync-run-42",
            SyncStage::Rows,
            "episodes",
        ),
        "SELECT run_id, stage, table_name, run_spec_json, COALESCE(last_primary_key_json, ''), chunks, rows_scanned, inserts_applied, updates_applied, deletes_applied, status, COALESCE(last_error, ''), DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'), DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s.%f'), COALESCE(DATE_FORMAT(completed_at, '%Y-%m-%d %H:%i:%s.%f'), '') FROM `cdc`.`sync_runs` WHERE run_id = 'sync-run-42' AND stage = 'rows' AND table_name = 'episodes' LIMIT 1"
    );

    let upsert = build_sync_progress_upsert_sql("cdc.sync_runs", &progress);
    assert_eq!(
        upsert.sql,
        "INSERT INTO `cdc`.`sync_runs` (run_id, stage, table_name, run_spec_json, last_primary_key_json, chunks, rows_scanned, inserts_applied, updates_applied, deletes_applied, status, last_error, completed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new ON DUPLICATE KEY UPDATE last_primary_key_json = new.last_primary_key_json, chunks = new.chunks, rows_scanned = new.rows_scanned, inserts_applied = new.inserts_applied, updates_applied = new.updates_applied, deletes_applied = new.deletes_applied, status = new.status, last_error = new.last_error, completed_at = new.completed_at"
    );
    assert_eq!(
        upsert.params,
        vec![
            bytes("sync-run-42"),
            bytes("rows"),
            bytes("episodes"),
            bytes(r#"{"chunk_size":250,"tables":["episodes"]}"#),
            bytes(r#"["7","live"]"#),
            Value::UInt(3),
            Value::UInt(750),
            Value::UInt(4),
            Value::UInt(5),
            Value::UInt(6),
            bytes("running"),
            Value::NULL,
            Value::NULL,
        ]
    );
}

#[test]
fn sync_mysql_contract_parses_concrete_progress_and_rejects_malformed_rows() {
    let parsed = parse_sync_progress_row(
        "sync-run-42\trows\tepisodes\t{\"chunk_size\":250,\"tables\":[\"episodes\"]}\t[\"7\",\"live\"]\t3\t750\t4\t5\t6\trunning\t\t2026-08-17 12:00:00.123456\t2026-08-17 12:05:00.654321\t",
    )
    .expect("valid staged progress row");

    assert_eq!(parsed, progress_row());

    for (label, malformed) in [
        ("field count", "sync-run-42\trows\tepisodes"),
        (
            "stage",
            "sync-run-42\tcopy\tepisodes\t{}\t\t0\t0\t0\t0\t0\trunning\t\t2026-08-17 12:00:00.000000\t2026-08-17 12:00:00.000000\t",
        ),
        (
            "status",
            "sync-run-42\trows\tepisodes\t{}\t\t0\t0\t0\t0\t0\tdone\t\t2026-08-17 12:00:00.000000\t2026-08-17 12:00:00.000000\t",
        ),
        (
            "run spec JSON",
            "sync-run-42\trows\tepisodes\t{broken\t\t0\t0\t0\t0\t0\trunning\t\t2026-08-17 12:00:00.000000\t2026-08-17 12:00:00.000000\t",
        ),
        (
            "cursor JSON",
            "sync-run-42\trows\tepisodes\t{}\t[\"7\"\t0\t0\t0\t0\t0\trunning\t\t2026-08-17 12:00:00.000000\t2026-08-17 12:00:00.000000\t",
        ),
        (
            "numeric field",
            "sync-run-42\trows\tepisodes\t{}\t\tnot-a-count\t0\t0\t0\t0\trunning\t\t2026-08-17 12:00:00.000000\t2026-08-17 12:00:00.000000\t",
        ),
    ] {
        assert!(
            parse_sync_progress_row(malformed).is_err(),
            "accepted malformed {label}: {malformed}"
        );
    }
}

fn ordered_table() -> SyncTable {
    SyncTable {
        name: "episodes`current".to_string(),
        primary_key: strings(["series_id", "state"]),
        primary_key_ordering: vec![
            SyncPrimaryKeyOrdering::Native,
            SyncPrimaryKeyOrdering::Enum(strings(["draft", "live", "archived"])),
        ],
        columns: strings(["series_id", "state", "title"]),
    }
}

fn mutation_table() -> SyncTable {
    SyncTable {
        name: "episodes".to_string(),
        primary_key: strings(["id"]),
        primary_key_ordering: vec![SyncPrimaryKeyOrdering::Native],
        columns: strings(["id", "status", "title"]),
    }
}

fn composite_delete_table() -> SyncTable {
    SyncTable {
        name: "episode_revisions".to_string(),
        primary_key: strings(["series_id", "revision"]),
        primary_key_ordering: vec![
            SyncPrimaryKeyOrdering::Native,
            SyncPrimaryKeyOrdering::Native,
        ],
        columns: strings(["series_id", "revision", "title"]),
    }
}

fn row(id: &str, status: &str, title: &str) -> SnapshotRow {
    SnapshotRow {
        primary_key: vec![id.to_string()],
        values: BTreeMap::from([
            ("id".to_string(), Some(id.to_string())),
            ("status".to_string(), Some(status.to_string())),
            ("title".to_string(), Some(title.to_string())),
        ]),
    }
}

fn progress_row() -> SyncProgressRow {
    SyncProgressRow {
        run_id: "sync-run-42".to_string(),
        stage: SyncStage::Rows,
        table_name: "episodes".to_string(),
        run_spec_json: r#"{"chunk_size":250,"tables":["episodes"]}"#.to_string(),
        last_primary_key: Some(strings(["7", "live"])),
        chunks: 3,
        rows_scanned: 750,
        inserts: 4,
        updates: 5,
        deletes: 6,
        status: SyncProgressStatus::Running,
        last_error: None,
        created_at: "2026-08-17 12:00:00.123456".to_string(),
        updated_at: "2026-08-17 12:05:00.654321".to_string(),
        completed_at: None,
    }
}

fn strings<const N: usize>(values: [&str; N]) -> Vec<String> {
    values.into_iter().map(str::to_string).collect()
}

fn bytes(value: &str) -> Value {
    Value::Bytes(value.as_bytes().to_vec())
}
