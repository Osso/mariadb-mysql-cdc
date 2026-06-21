use std::process::Command;

#[test]
fn mixed_binlog_fixture_covers_required_event_types() {
    let output = Command::new("mariadb-binlog")
        .args([
            "--base64-output=DECODE-ROWS",
            "--verbose",
            "fixtures/mixed-binlog/mysql-bin.000001",
            "fixtures/mixed-binlog/mysql-bin.000002",
        ])
        .output()
        .expect("run mariadb-binlog");

    assert!(
        output.status.success(),
        "mariadb-binlog failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let decoded = String::from_utf8_lossy(&output.stdout);
    assert_contains(&decoded, "GTID 0-17-");
    assert_contains(&decoded, "\tQuery\t");
    assert_contains(&decoded, "Table_map:");
    assert_contains(&decoded, "Write_rows:");
    assert_contains(&decoded, "Update_rows:");
    assert_contains(&decoded, "Delete_rows:");
    assert_contains(&decoded, "Rotate to mysql-bin.000002");
    assert_contains(&decoded, "CREATE DATABASE fixture_cdc");
    assert_contains(&decoded, "CREATE TABLE accounts");
    assert_contains(&decoded, "ALTER TABLE accounts ADD COLUMN status");
    assert_contains(&decoded, "### INSERT INTO `fixture_cdc`.`accounts`");
    assert_contains(&decoded, "### DELETE FROM `fixture_cdc`.`accounts`");
}

fn assert_contains(decoded: &str, needle: &str) {
    assert!(
        decoded.contains(needle),
        "decoded fixture missing expected text: {needle}"
    );
}
